package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

// operationExecutor adapts synchronous resource behavior to durable operations.
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

type syncProviderAdapter struct {
	runtime     *Runtime
	synchronous sdk.SyncProvider
	spec        sdk.ProviderSpec
	revision    string
}

func (provider syncProviderAdapter) Spec() sdk.ProviderSpec {
	return provider.spec
}

func (provider syncProviderAdapter) SubmitCompletion(
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

func (provider syncProviderAdapter) PollCompletion(
	ctx context.Context,
	id string,
	_ uint64,
) (sdk.Operation, error) {
	return provider.runtime.pollLocalOperation(ctx, sdk.OperationKindProvider, provider.spec.Name, id)
}

func (provider syncProviderAdapter) CancelCompletion(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return provider.runtime.cancelLocalOperation(ctx, sdk.OperationKindProvider, provider.spec.Name, id)
}

type syncToolAdapter struct {
	runtime     *Runtime
	synchronous sdk.SyncTool
	spec        sdk.ToolSpec
	revision    string
}

type syncCapabilityAdapter struct {
	runtime     *Runtime
	synchronous sdk.SyncCapability
	spec        sdk.CapabilitySpec
	revision    string
}

func (capability syncCapabilityAdapter) Spec() sdk.CapabilitySpec {
	return plugincontract.CloneCapabilitySpec(capability.spec)
}

func (capability syncCapabilityAdapter) SubmitInvoke(
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

func (capability syncCapabilityAdapter) PollInvoke(
	ctx context.Context,
	id string,
	_ uint64,
) (sdk.Operation, error) {
	return capability.runtime.pollLocalOperation(
		ctx, sdk.OperationKindCapability, capability.spec.Name, id,
	)
}

func (capability syncCapabilityAdapter) CancelInvoke(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return capability.runtime.cancelLocalOperation(
		ctx, sdk.OperationKindCapability, capability.spec.Name, id,
	)
}

func (tool syncToolAdapter) Spec() sdk.ToolSpec {
	return plugincontract.CloneToolSpec(tool.spec)
}

func (tool syncToolAdapter) SubmitCall(
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

func (tool syncToolAdapter) PollCall(
	ctx context.Context,
	id string,
	_ uint64,
) (sdk.Operation, error) {
	return tool.runtime.pollLocalOperation(ctx, sdk.OperationKindTool, tool.spec.Name, id)
}

func (tool syncToolAdapter) CancelCall(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return tool.runtime.cancelLocalOperation(ctx, sdk.OperationKindTool, tool.spec.Name, id)
}

func (runtime *Runtime) adaptSynchronousResources(
	registrar *plugincontract.AgentRegistrar,
	manifest sdk.Manifest,
) {
	for name, staged := range registrar.Providers {
		if _, asynchronous := staged.Value.(sdk.AsyncProvider); asynchronous {
			continue
		}
		staged.Value = syncProviderAdapter{
			runtime:     runtime,
			synchronous: staged.Value.(sdk.SyncProvider),
			spec:        staged.Spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindProvider),
				name,
				staged.Spec,
			),
		}
		registrar.Providers[name] = staged
	}
	for name, staged := range registrar.Tools {
		if _, asynchronous := staged.Value.(sdk.AsyncTool); asynchronous {
			continue
		}
		staged.Value = syncToolAdapter{
			runtime:     runtime,
			synchronous: staged.Value.(sdk.SyncTool),
			spec:        staged.Spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindTool),
				name,
				staged.Spec,
			),
		}
		registrar.Tools[name] = staged
	}
	for name, staged := range registrar.Capabilities {
		if _, asynchronous := staged.Value.(sdk.AsyncCapability); asynchronous {
			continue
		}
		staged.Value = syncCapabilityAdapter{
			runtime:     runtime,
			synchronous: staged.Value.(sdk.SyncCapability),
			spec:        staged.Spec,
			revision: sdk.ResourceRevision(
				manifest,
				string(sdk.OperationKindCapability),
				name,
				staged.Spec,
			),
		}
		registrar.Capabilities[name] = staged
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
		Invocation:       sdk.CloneInvocation(request.Invocation),
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	if !record.Operation.Terminal() {
		started = runtime.startLocalOperation(
			ctx,
			record.Operation.ID,
			execute,
		)
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
	worker := operationworker.Runner{
		Store:  runtime.operation.store,
		Logger: runtime.logger,
		Owner:  runtime.operation.workerID,
		Lease:  runtime.operation.lease,
	}
	worker.Run(
		ctx,
		id,
		nil,
		func(
			ctx context.Context,
			record sdk.OperationRecord,
		) (json.RawMessage, error) {
			return execute(ctx, record.Input)
		},
	)
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
		cancelled, err := runtime.operation.store.Cancel(
			ctx, id, record.Operation.Revision,
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
