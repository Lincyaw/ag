package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func (server *server) submitStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	if !server.reserveOperation() {
		return sdk.Operation{}, errors.New("RPC server is closed")
	}
	started := false
	defer func() {
		if !started {
			server.wait.Done()
		}
	}()
	record, created, err := server.operations.Submit(ctx, sdk.OperationRecord{
		Operation:        sdk.Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:             kind,
		Resource:         resource,
		ResourceRevision: server.resourceRevision(kind, resource),
		Input:            request.Input,
		Invocation:       sdk.CloneInvocation(request.Invocation),
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	if created {
		server.startReservedOperation(ctx, record.Operation.ID)
		started = true
	}
	return record.Operation, nil
}

func (server *server) getStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	record, err := server.operations.Get(ctx, id)
	if err != nil {
		return sdk.Operation{}, err
	}
	if record.Kind != kind || record.Resource != resource {
		return sdk.Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
	}
	if err := server.validateResourceRevision(record); err != nil {
		return sdk.Operation{}, err
	}
	return record.Operation, nil
}

func (server *server) cancelStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	for {
		record, err := server.operations.Get(ctx, id)
		if err != nil {
			return sdk.Operation{}, err
		}
		if record.Kind != kind || record.Resource != resource {
			return sdk.Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
		}
		if err := server.validateResourceRevision(record); err != nil {
			return sdk.Operation{}, err
		}
		if record.Operation.Terminal() {
			return record.Operation, nil
		}
		cancelled, err := server.operations.Transition(
			ctx,
			id,
			record.Operation.Revision,
			sdk.OperationCancelled,
			nil,
			"",
		)
		if errors.Is(err, sdk.ErrOperationConflict) {
			continue
		}
		if err != nil {
			return sdk.Operation{}, err
		}
		server.cancelMu.Lock()
		cancel := server.operationCancels[id]
		server.cancelMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return cancelled.Operation, nil
	}
}

func (server *server) recoverOperations(ctx context.Context) error {
	records, err := server.operations.List(ctx)
	if err != nil {
		return fmt.Errorf("list operations for recovery: %w", err)
	}
	for _, record := range records {
		if record.Operation.Terminal() {
			continue
		}
		if revisionErr := server.validateResourceRevision(record); revisionErr != nil {
			_, err := server.operations.Transition(
				ctx,
				record.Operation.ID,
				record.Operation.Revision,
				sdk.OperationFailed,
				nil,
				revisionErr.Error(),
			)
			if errors.Is(err, sdk.ErrOperationConflict) {
				continue
			}
			if err != nil {
				return fmt.Errorf(
					"fail stale operation %q: %w",
					record.Operation.ID,
					err,
				)
			}
			continue
		}
		if server.reserveOperation() {
			server.startReservedOperation(ctx, record.Operation.ID)
		}
	}
	return nil
}

func (server *server) reserveOperation() bool {
	server.lifecycleMu.Lock()
	defer server.lifecycleMu.Unlock()
	if server.closed {
		return false
	}
	server.wait.Add(1)
	return true
}

func (server *server) startReservedOperation(parent context.Context, id string) {
	go func() {
		defer server.wait.Done()
		server.executeStored(parent, id)
	}()
}

func (server *server) executeStored(parent context.Context, id string) {
	operationContext, cancel := context.WithCancel(context.WithoutCancel(parent))
	stopServerCancel := context.AfterFunc(server.context, cancel)
	defer func() {
		stopServerCancel()
		cancel()
	}()
	record, err := server.operations.Claim(
		operationContext,
		id,
		server.operationWorkerID,
		time.Now().UTC(),
		server.operationLease,
	)
	if err != nil {
		if !errors.Is(err, sdk.ErrOperationClaimed) {
			server.logger.Debug(
				"claim stored operation",
				"operation_id",
				id,
				"error",
				err,
			)
		}
		return
	}
	if err := server.validateResourceRevision(record); err != nil {
		server.logger.Warn(
			"claimed operation has stale resource revision",
			"operation_id",
			id,
			"error",
			err,
		)
		return
	}
	operationContext = sdk.WithInvocation(
		operationContext,
		record.Invocation,
	)
	token := record.Execution.Token
	server.cancelMu.Lock()
	server.operationCancels[id] = cancel
	server.cancelMu.Unlock()
	executionContext, cancelExecution := context.WithCancel(operationContext)
	heartbeatContext, stopHeartbeat := context.WithCancel(operationContext)
	heartbeatDone := make(chan struct{})
	leaseLost := make(chan error, 1)
	go server.renewOperationLease(
		heartbeatContext,
		id,
		token,
		cancelExecution,
		heartbeatDone,
		leaseLost,
	)
	output, executeErr := server.executeLocal(executionContext, record)
	stopHeartbeat()
	<-heartbeatDone
	cancelExecution()
	server.cancelMu.Lock()
	delete(server.operationCancels, id)
	server.cancelMu.Unlock()
	select {
	case lostErr := <-leaseLost:
		server.logger.Warn(
			"stored operation lease lost",
			"operation_id",
			id,
			"error",
			lostErr,
		)
		return
	default:
	}
	if operationContext.Err() != nil {
		_, releaseErr := server.operations.Release(
			context.Background(),
			id,
			token,
		)
		if releaseErr != nil &&
			!errors.Is(releaseErr, sdk.ErrOperationFence) {
			server.logger.Error(
				"release stored operation",
				"operation_id",
				id,
				"error",
				releaseErr,
			)
		}
		return
	}
	state := sdk.OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = sdk.OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = server.operations.Complete(
		context.Background(),
		id,
		token,
		state,
		output,
		errorText,
	)
	if err != nil && !errors.Is(err, sdk.ErrOperationFence) {
		server.logger.Error("complete stored operation", "operation_id", id, "error", err)
	}
}

func (server *server) renewOperationLease(
	ctx context.Context,
	id string,
	token string,
	cancelExecution context.CancelFunc,
	done chan<- struct{},
	lost chan<- error,
) {
	defer close(done)
	interval := server.operationLease / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, err := server.operations.Renew(
				ctx,
				id,
				token,
				now.UTC(),
				server.operationLease,
			)
			if err != nil {
				select {
				case lost <- err:
				default:
				}
				cancelExecution()
				return
			}
		}
	}
}

func (server *server) resourceRevision(
	kind sdk.OperationKind,
	resource string,
) string {
	var spec any
	switch kind {
	case sdk.OperationKindProvider:
		if provider, exists := server.registrar.providers[resource]; exists {
			spec = provider.spec
		}
	case sdk.OperationKindTool:
		if tool, exists := server.registrar.tools[resource]; exists {
			spec = tool.spec
		}
	case sdk.OperationKindCapability:
		if capability, exists := server.registrar.capabilities[resource]; exists {
			spec = capability.spec
		}
	}
	return sdk.ResourceRevision(server.manifest, string(kind), resource, spec)
}

func (server *server) validateResourceRevision(
	record sdk.OperationRecord,
) error {
	if record.ResourceRevision == "" {
		return nil
	}
	current := server.resourceRevision(record.Kind, record.Resource)
	if record.ResourceRevision != current {
		return fmt.Errorf(
			"operation %q resource revision %s does not match current revision %s",
			record.Operation.ID,
			record.ResourceRevision,
			current,
		)
	}
	return nil
}

func (server *server) executeLocal(
	ctx context.Context,
	record sdk.OperationRecord,
) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("plugin operation panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	switch record.Kind {
	case sdk.OperationKindProvider:
		provider, ok := server.registrar.providers[record.Resource].value.(sdk.SyncProvider)
		if !ok {
			return nil, fmt.Errorf("provider %q is not synchronous", record.Resource)
		}
		var request sdk.ModelRequest
		if err := json.Unmarshal(record.Input, &request); err != nil {
			return nil, err
		}
		response, err := provider.Complete(ctx, request)
		if err != nil {
			return nil, err
		}
		return json.Marshal(response)
	case sdk.OperationKindTool:
		tool, ok := server.registrar.tools[record.Resource].value.(sdk.SyncTool)
		if !ok {
			return nil, fmt.Errorf("tool %q is not synchronous", record.Resource)
		}
		result, err := tool.Call(ctx, record.Input)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	case sdk.OperationKindCapability:
		capability, ok := server.registrar.capabilities[record.Resource].value.(sdk.SyncCapability)
		if !ok {
			return nil, fmt.Errorf("capability %q is not synchronous", record.Resource)
		}
		return capability.Invoke(ctx, record.Input)
	default:
		return nil, fmt.Errorf("unsupported stored operation kind %q", record.Kind)
	}
}
