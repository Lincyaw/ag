package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type operationExecutor func(context.Context, json.RawMessage) (json.RawMessage, error)

type localAsyncProvider struct {
	runtime     *Runtime
	synchronous SyncProvider
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
}

type localAsyncCapability struct {
	runtime     *Runtime
	synchronous SyncCapability
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

func (runtime *Runtime) wrapSynchronousResources(registrar *stagingRegistrar) {
	for name, provider := range registrar.providers {
		if _, asynchronous := provider.(AsyncProvider); asynchronous {
			continue
		}
		registrar.providers[name] = localAsyncProvider{
			runtime: runtime, synchronous: provider.(SyncProvider),
		}
	}
	for name, tool := range registrar.tools {
		if _, asynchronous := tool.(AsyncTool); asynchronous {
			continue
		}
		registrar.tools[name] = localAsyncTool{
			runtime: runtime, synchronous: tool.(SyncTool),
		}
	}
	for name, capability := range registrar.capabilities {
		if _, asynchronous := capability.(AsyncCapability); asynchronous {
			continue
		}
		registrar.capabilities[name] = localAsyncCapability{
			runtime: runtime, synchronous: capability.(SyncCapability),
		}
	}
}

func (runtime *Runtime) submitLocalOperation(
	ctx context.Context,
	kind OperationKind,
	resource string,
	request OperationRequest,
	execute operationExecutor,
) (Operation, error) {
	record, _, err := runtime.operations.Submit(ctx, OperationRecord{
		Operation: Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:      kind,
		Resource:  resource,
		Input:     append(json.RawMessage(nil), request.Input...),
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
	record, err := runtime.operations.Get(ctx, id)
	if err != nil || record.Operation.Terminal() {
		return
	}
	if record.Operation.State == OperationPending {
		record, err = runtime.operations.Transition(
			ctx, id, record.Operation.Revision, OperationRunning, nil, "",
		)
		if err != nil {
			return
		}
	}
	output, executeErr := execute(ctx, record.Input)
	if errors.Is(executeErr, context.Canceled) && ctx.Err() != nil {
		return
	}
	state := OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = runtime.operations.Transition(
		context.Background(), id, record.Operation.Revision, state, output, errorText,
	)
	if err != nil && !errors.Is(err, ErrOperationConflict) {
		runtime.logger.Error("complete local operation", "operation_id", id, "error", err)
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
