package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type operationExecutor func(context.Context, json.RawMessage) (json.RawMessage, error)

type localAsyncProvider struct {
	runtime     *Runtime
	synchronous SyncProvider
	revision    string
}

func (provider localAsyncProvider) Spec() ProviderSpec {
	return provider.synchronous.Spec()
}

func (provider localAsyncProvider) SubmitCompletion(
	ctx context.Context,
	request OperationRequest,
) (Operation, error) {
	return provider.runtime.submitLocalOperation(
		ctx,
		OperationKindProvider,
		provider.Spec().Name,
		provider.revision,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var modelRequest ModelRequest
			if err := json.Unmarshal(input, &modelRequest); err != nil {
				return nil, err
			}
			response, err := provider.synchronous.Complete(ctx, modelRequest)
			if err != nil {
				return nil, err
			}
			return json.Marshal(response)
		},
	)
}

func (provider localAsyncProvider) PollCompletion(
	ctx context.Context,
	id string,
	_ uint64,
) (Operation, error) {
	return provider.runtime.pollLocalOperation(ctx, OperationKindProvider, provider.Spec().Name, id)
}

func (provider localAsyncProvider) CancelCompletion(
	ctx context.Context,
	id string,
) (Operation, error) {
	return provider.runtime.cancelLocalOperation(ctx, OperationKindProvider, provider.Spec().Name, id)
}

type localAsyncTool struct {
	runtime     *Runtime
	synchronous SyncTool
	revision    string
}

type localAsyncCapability struct {
	runtime     *Runtime
	synchronous SyncCapability
	revision    string
}

func (capability localAsyncCapability) Spec() CapabilitySpec {
	return capability.synchronous.Spec()
}

func (capability localAsyncCapability) SubmitInvoke(
	ctx context.Context,
	request OperationRequest,
) (Operation, error) {
	return capability.runtime.submitLocalOperation(
		ctx,
		OperationKindCapability,
		capability.Spec().Name,
		capability.revision,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return capability.synchronous.Invoke(ctx, input)
		},
	)
}

func (capability localAsyncCapability) PollInvoke(
	ctx context.Context,
	id string,
	_ uint64,
) (Operation, error) {
	return capability.runtime.pollLocalOperation(
		ctx, OperationKindCapability, capability.Spec().Name, id,
	)
}

func (capability localAsyncCapability) CancelInvoke(
	ctx context.Context,
	id string,
) (Operation, error) {
	return capability.runtime.cancelLocalOperation(
		ctx, OperationKindCapability, capability.Spec().Name, id,
	)
}

func (tool localAsyncTool) Spec() ToolSpec { return tool.synchronous.Spec() }

func (tool localAsyncTool) SubmitCall(
	ctx context.Context,
	request OperationRequest,
) (Operation, error) {
	return tool.runtime.submitLocalOperation(
		ctx,
		OperationKindTool,
		tool.Spec().Name,
		tool.revision,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			result, err := tool.synchronous.Call(ctx, input)
			if err != nil {
				return nil, err
			}
			return json.Marshal(result)
		},
	)
}

func (tool localAsyncTool) PollCall(
	ctx context.Context,
	id string,
	_ uint64,
) (Operation, error) {
	return tool.runtime.pollLocalOperation(ctx, OperationKindTool, tool.Spec().Name, id)
}

func (tool localAsyncTool) CancelCall(
	ctx context.Context,
	id string,
) (Operation, error) {
	return tool.runtime.cancelLocalOperation(ctx, OperationKindTool, tool.Spec().Name, id)
}

func (runtime *Runtime) wrapSynchronousResources(
	registrar *stagingRegistrar,
	manifest Manifest,
) {
	for name, provider := range registrar.providers {
		if _, asynchronous := provider.(AsyncProvider); asynchronous {
			continue
		}
		registrar.providers[name] = localAsyncProvider{
			runtime:     runtime,
			synchronous: provider.(SyncProvider),
			revision: ResourceRevision(
				manifest,
				OperationKindProvider,
				name,
				provider.Spec(),
			),
		}
	}
	for name, tool := range registrar.tools {
		if _, asynchronous := tool.(AsyncTool); asynchronous {
			continue
		}
		registrar.tools[name] = localAsyncTool{
			runtime:     runtime,
			synchronous: tool.(SyncTool),
			revision: ResourceRevision(
				manifest,
				OperationKindTool,
				name,
				tool.Spec(),
			),
		}
	}
	for name, capability := range registrar.capabilities {
		if _, asynchronous := capability.(AsyncCapability); asynchronous {
			continue
		}
		registrar.capabilities[name] = localAsyncCapability{
			runtime:     runtime,
			synchronous: capability.(SyncCapability),
			revision: ResourceRevision(
				manifest,
				OperationKindCapability,
				name,
				capability.Spec(),
			),
		}
	}
}

func (runtime *Runtime) submitLocalOperation(
	ctx context.Context,
	kind OperationKind,
	resource string,
	resourceRevision string,
	request OperationRequest,
	execute operationExecutor,
) (Operation, error) {
	record, _, err := runtime.operations.Submit(ctx, OperationRecord{
		Operation:        Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:             kind,
		Resource:         resource,
		ResourceRevision: resourceRevision,
		Input:            append(json.RawMessage(nil), request.Input...),
	})
	if err != nil {
		return Operation{}, err
	}
	if !record.Operation.Terminal() {
		runtime.startLocalOperation(ctx, record.Operation.ID, execute)
	}
	return record.Operation, nil
}

func (runtime *Runtime) startLocalOperation(
	parent context.Context,
	id string,
	execute operationExecutor,
) {
	runtime.operationMu.Lock()
	if _, running := runtime.operationCancels[id]; running {
		runtime.operationMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stopRuntimeCancel := context.AfterFunc(runtime.operationContext, cancel)
	runtime.operationCancels[id] = cancel
	runtime.operationWait.Add(1)
	runtime.operationMu.Unlock()
	go func() {
		defer runtime.operationWait.Done()
		defer func() {
			stopRuntimeCancel()
			cancel()
			runtime.operationMu.Lock()
			delete(runtime.operationCancels, id)
			runtime.operationMu.Unlock()
		}()
		runtime.executeLocalOperation(ctx, id, execute)
	}()
}

func (runtime *Runtime) executeLocalOperation(
	ctx context.Context,
	id string,
	execute operationExecutor,
) {
	record, err := runtime.operations.Claim(
		ctx,
		id,
		runtime.operationWorkerID,
		time.Now().UTC(),
		runtime.operationLease,
	)
	if err != nil {
		if !errors.Is(err, ErrOperationClaimed) {
			runtime.logger.Debug(
				"claim local operation",
				"operation_id",
				id,
				"error",
				err,
			)
		}
		return
	}
	token := record.Execution.Token
	executionCtx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	heartbeatDone := make(chan struct{})
	leaseLost := make(chan error, 1)
	go runtime.renewOperationLease(
		heartbeatCtx,
		id,
		token,
		cancelExecution,
		heartbeatDone,
		leaseLost,
	)
	output, executeErr := execute(executionCtx, record.Input)
	stopHeartbeat()
	<-heartbeatDone
	select {
	case lostErr := <-leaseLost:
		runtime.logger.Warn(
			"local operation lease lost",
			"operation_id",
			id,
			"error",
			lostErr,
		)
		return
	default:
	}
	if ctx.Err() != nil {
		_, releaseErr := runtime.operations.Release(
			context.Background(),
			id,
			token,
		)
		if releaseErr != nil && !errors.Is(releaseErr, ErrOperationFence) {
			runtime.logger.Error(
				"release local operation",
				"operation_id",
				id,
				"error",
				releaseErr,
			)
		}
		return
	}
	state := OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = runtime.operations.Complete(
		context.Background(),
		id,
		token,
		state,
		output,
		errorText,
	)
	if err != nil && !errors.Is(err, ErrOperationFence) {
		runtime.logger.Error("complete local operation", "operation_id", id, "error", err)
	}
}

func (runtime *Runtime) renewOperationLease(
	ctx context.Context,
	id string,
	token string,
	cancelExecution context.CancelFunc,
	done chan<- struct{},
	lost chan<- error,
) {
	defer close(done)
	interval := runtime.operationLease / 3
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
			_, err := runtime.operations.Renew(
				ctx,
				id,
				token,
				now.UTC(),
				runtime.operationLease,
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

func (runtime *Runtime) pollLocalOperation(
	ctx context.Context,
	kind OperationKind,
	resource string,
	id string,
) (Operation, error) {
	record, err := runtime.operations.Get(ctx, id)
	if err != nil {
		return Operation{}, err
	}
	if record.Kind != kind || record.Resource != resource {
		return Operation{}, fmt.Errorf(
			"operation %q belongs to %s %q, not %s %q",
			id, record.Kind, record.Resource, kind, resource,
		)
	}
	return record.Operation, nil
}

func (runtime *Runtime) cancelLocalOperation(
	ctx context.Context,
	kind OperationKind,
	resource string,
	id string,
) (Operation, error) {
	for {
		record, err := runtime.operations.Get(ctx, id)
		if err != nil {
			return Operation{}, err
		}
		if record.Kind != kind || record.Resource != resource {
			return Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
		}
		if record.Operation.Terminal() {
			return record.Operation, nil
		}
		cancelled, err := runtime.operations.Transition(
			ctx, id, record.Operation.Revision, OperationCancelled, nil, "",
		)
		if errors.Is(err, ErrOperationConflict) {
			continue
		}
		if err != nil {
			return Operation{}, err
		}
		runtime.operationMu.Lock()
		cancel := runtime.operationCancels[id]
		runtime.operationMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return cancelled.Operation, nil
	}
}
