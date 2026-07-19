package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lincyaw/ag/internal/operationworker"
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
		cancelled, err := server.operations.Cancel(
			ctx,
			id,
			record.Operation.Revision,
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
			_, err := server.operations.Fail(
				ctx,
				record.Operation.ID,
				record.Operation.Revision,
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
	server.cancelMu.Lock()
	if _, running := server.operationCancels[id]; running {
		server.cancelMu.Unlock()
		return
	}
	server.operationCancels[id] = cancel
	server.cancelMu.Unlock()
	defer func() {
		server.cancelMu.Lock()
		delete(server.operationCancels, id)
		server.cancelMu.Unlock()
	}()

	worker := operationworker.Runner{
		Store:  server.operations,
		Logger: server.logger,
		Owner:  server.operationWorkerID,
		Lease:  server.operationLease,
	}
	worker.Run(
		operationContext,
		id,
		server.validateResourceRevision,
		server.executeLocal,
	)
}

func (server *server) resourceRevision(
	kind sdk.OperationKind,
	resource string,
) string {
	var spec any
	switch kind {
	case sdk.OperationKindProvider:
		if provider, exists := server.registrar.Providers[resource]; exists {
			spec = provider.Spec
		}
	case sdk.OperationKindTool:
		if tool, exists := server.registrar.Tools[resource]; exists {
			spec = tool.Spec
		}
	case sdk.OperationKindCapability:
		if capability, exists := server.registrar.Capabilities[resource]; exists {
			spec = capability.Spec
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
) (json.RawMessage, error) {
	switch record.Kind {
	case sdk.OperationKindProvider:
		provider, ok := server.registrar.Providers[record.Resource].Value.(sdk.SyncProvider)
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
		tool, ok := server.registrar.Tools[record.Resource].Value.(sdk.SyncTool)
		if !ok {
			return nil, fmt.Errorf("tool %q is not synchronous", record.Resource)
		}
		result, err := tool.Call(ctx, record.Input)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	case sdk.OperationKindCapability:
		capability, ok := server.registrar.Capabilities[record.Resource].Value.(sdk.SyncCapability)
		if !ok {
			return nil, fmt.Errorf("capability %q is not synchronous", record.Resource)
		}
		return capability.Invoke(ctx, record.Input)
	default:
		return nil, fmt.Errorf("unsupported stored operation kind %q", record.Kind)
	}
}
