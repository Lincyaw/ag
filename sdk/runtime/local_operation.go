package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type operationExecutor func(context.Context, json.RawMessage) (json.RawMessage, error)

type operationRuntime struct {
	store    sdk.OperationStore
	context  context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	cancels  map[string]context.CancelFunc
	wait     sync.WaitGroup
	poll     time.Duration
	lease    time.Duration
	workerID string
}

type localAsyncProvider struct {
	runtime     *Runtime
	synchronous sdk.SyncProvider
	spec        sdk.ProviderSpec
	revision    string
}

func (provider localAsyncProvider) Spec() sdk.ProviderSpec {
	return provider.spec
}

func (provider localAsyncProvider) SubmitCompletion(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return provider.runtime.submitLocalOperation(
		ctx,
		sdk.OperationKindProvider,
		provider.spec.Name,
		provider.revision,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			var modelRequest sdk.ModelRequest
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
) (sdk.Operation, error) {
	return provider.runtime.pollLocalOperation(ctx, sdk.OperationKindProvider, provider.spec.Name, id)
}

func (provider localAsyncProvider) CancelCompletion(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return provider.runtime.cancelLocalOperation(ctx, sdk.OperationKindProvider, provider.spec.Name, id)
}

type localAsyncTool struct {
	runtime     *Runtime
	synchronous sdk.SyncTool
	spec        sdk.ToolSpec
	revision    string
}

type localAsyncCapability struct {
	runtime     *Runtime
	synchronous sdk.SyncCapability
	spec        sdk.CapabilitySpec
	revision    string
}

func (capability localAsyncCapability) Spec() sdk.CapabilitySpec {
	return cloneCapabilitySpec(capability.spec)
}

func (capability localAsyncCapability) SubmitInvoke(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return capability.runtime.submitLocalOperation(
		ctx,
		sdk.OperationKindCapability,
		capability.spec.Name,
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
) (sdk.Operation, error) {
	return capability.runtime.pollLocalOperation(
		ctx, sdk.OperationKindCapability, capability.spec.Name, id,
	)
}

func (capability localAsyncCapability) CancelInvoke(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return capability.runtime.cancelLocalOperation(
		ctx, sdk.OperationKindCapability, capability.spec.Name, id,
	)
}

func (tool localAsyncTool) Spec() sdk.ToolSpec { return cloneToolSpec(tool.spec) }

func (tool localAsyncTool) SubmitCall(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return tool.runtime.submitLocalOperation(
		ctx,
		sdk.OperationKindTool,
		tool.spec.Name,
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
) (sdk.Operation, error) {
	return tool.runtime.pollLocalOperation(ctx, sdk.OperationKindTool, tool.spec.Name, id)
}

func (tool localAsyncTool) CancelCall(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return tool.runtime.cancelLocalOperation(ctx, sdk.OperationKindTool, tool.spec.Name, id)
}

func (runtime *Runtime) wrapSynchronousResources(
	registrar *stagingRegistrar,
	manifest sdk.Manifest,
) {
	for name, staged := range registrar.providers {
		if _, asynchronous := staged.value.(sdk.AsyncProvider); asynchronous {
			continue
		}
		staged.value = localAsyncProvider{
			runtime:     runtime,
			synchronous: staged.value.(sdk.SyncProvider),
			spec:        staged.spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindProvider),
				name,
				staged.spec,
			),
		}
		registrar.providers[name] = staged
	}
	for name, staged := range registrar.tools {
		if _, asynchronous := staged.value.(sdk.AsyncTool); asynchronous {
			continue
		}
		staged.value = localAsyncTool{
			runtime:     runtime,
			synchronous: staged.value.(sdk.SyncTool),
			spec:        staged.spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindTool),
				name,
				staged.spec,
			),
		}
		registrar.tools[name] = staged
	}
	for name, staged := range registrar.capabilities {
		if _, asynchronous := staged.value.(sdk.AsyncCapability); asynchronous {
			continue
		}
		staged.value = localAsyncCapability{
			runtime:     runtime,
			synchronous: staged.value.(sdk.SyncCapability),
			spec:        staged.spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindCapability),
				name,
				staged.spec,
			),
		}
		registrar.capabilities[name] = staged
	}
}

func (runtime *Runtime) submitLocalOperation(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	resourceRevision string,
	request sdk.OperationRequest,
	execute operationExecutor,
) (sdk.Operation, error) {
	runtime.mu.Lock()
	if runtime.closed {
		runtime.mu.Unlock()
		return sdk.Operation{}, errors.New("runtime is closed")
	}
	runtime.operation.wait.Add(1)
	runtime.mu.Unlock()
	started := false
	defer func() {
		if !started {
			runtime.operation.wait.Done()
		}
	}()

	record, _, err := runtime.operation.store.Submit(ctx, sdk.OperationRecord{
		Operation:        sdk.Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:             kind,
		Resource:         resource,
		ResourceRevision: resourceRevision,
		Input:            append(json.RawMessage(nil), request.Input...),
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	if !record.Operation.Terminal() {
		started = runtime.startLocalOperation(ctx, record.Operation.ID, execute)
	}
	return record.Operation, nil
}

func (runtime *Runtime) startLocalOperation(
	parent context.Context,
	id string,
	execute operationExecutor,
) bool {
	runtime.operation.mu.Lock()
	if _, running := runtime.operation.cancels[id]; running {
		runtime.operation.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stopRuntimeCancel := context.AfterFunc(runtime.operation.context, cancel)
	runtime.operation.cancels[id] = cancel
	runtime.operation.mu.Unlock()
	go func() {
		defer runtime.operation.wait.Done()
		defer func() {
			stopRuntimeCancel()
			cancel()
			runtime.operation.mu.Lock()
			delete(runtime.operation.cancels, id)
			runtime.operation.mu.Unlock()
		}()
		runtime.executeLocalOperation(ctx, id, execute)
	}()
	return true
}

func (runtime *Runtime) executeLocalOperation(
	ctx context.Context,
	id string,
	execute operationExecutor,
) {
	record, err := runtime.operation.store.Claim(
		ctx,
		id,
		runtime.operation.workerID,
		time.Now().UTC(),
		runtime.operation.lease,
	)
	if err != nil {
		if !errors.Is(err, sdk.ErrOperationClaimed) {
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
		_, releaseErr := runtime.operation.store.Release(
			context.Background(),
			id,
			token,
		)
		if releaseErr != nil && !errors.Is(releaseErr, sdk.ErrOperationFence) {
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
	state := sdk.OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = sdk.OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = runtime.operation.store.Complete(
		context.Background(),
		id,
		token,
		state,
		output,
		errorText,
	)
	if err != nil && !errors.Is(err, sdk.ErrOperationFence) {
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
	interval := runtime.operation.lease / 3
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
			_, err := runtime.operation.store.Renew(
				ctx,
				id,
				token,
				now.UTC(),
				runtime.operation.lease,
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
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	record, err := runtime.operation.store.Get(ctx, id)
	if err != nil {
		return sdk.Operation{}, err
	}
	if record.Kind != kind || record.Resource != resource {
		return sdk.Operation{}, fmt.Errorf(
			"operation %q belongs to %s %q, not %s %q",
			id, record.Kind, record.Resource, kind, resource,
		)
	}
	return record.Operation, nil
}

func (runtime *Runtime) cancelLocalOperation(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	for {
		record, err := runtime.operation.store.Get(ctx, id)
		if err != nil {
			return sdk.Operation{}, err
		}
		if record.Kind != kind || record.Resource != resource {
			return sdk.Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
		}
		if record.Operation.Terminal() {
			return record.Operation, nil
		}
		cancelled, err := runtime.operation.store.Transition(
			ctx, id, record.Operation.Revision, sdk.OperationCancelled, nil, "",
		)
		if errors.Is(err, sdk.ErrOperationConflict) {
			continue
		}
		if err != nil {
			return sdk.Operation{}, err
		}
		runtime.operation.mu.Lock()
		cancel := runtime.operation.cancels[id]
		runtime.operation.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return cancelled.Operation, nil
	}
}
