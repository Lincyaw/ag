package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
)

// operationExecutor adapts synchronous resource behavior to durable operations.
type operationExecutor func(context.Context, json.RawMessage) (json.RawMessage, error)

type localOperationTarget struct {
	runtime          *Runtime
	kind             sdk.OperationKind
	resource         string
	resourceRevision string
	owner            *mountState
	snapshot         *registrySnapshot
}

func (target localOperationTarget) submit(
	ctx context.Context,
	request sdk.OperationRequest,
	execute operationExecutor,
) (sdk.Operation, error) {
	releaseExecutionLease, err := target.acquireExecutionLease()
	if err != nil {
		return sdk.Operation{}, err
	}
	return target.runtime.submitLocalOperation(
		ctx,
		target,
		request,
		releaseExecutionLease,
		execute,
	)
}

func (target localOperationTarget) acquireExecutionLease() (func(), error) {
	if target.snapshot != nil {
		lease, err := target.runtime.acquireRegistrySnapshot(target.snapshot)
		if err != nil {
			return nil, err
		}
		return lease.release, nil
	}
	if target.owner != nil {
		lease, err := target.runtime.acquireMounts(target.owner)
		if err != nil {
			return nil, err
		}
		return lease.release, nil
	}
	return func() {}, nil
}

func (target localOperationTarget) identity() operationworker.Target {
	return operationworker.Target{
		Kind:             target.kind,
		Resource:         target.resource,
		ResourceRevision: target.resourceRevision,
	}
}

func (target localOperationTarget) poll(
	ctx context.Context,
	id string,
	_ uint64,
) (sdk.Operation, error) {
	return target.runtime.pollLocalOperation(
		ctx,
		target.identity(),
		id,
	)
}

func (target localOperationTarget) cancel(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return target.runtime.cancelLocalOperation(
		ctx,
		target.identity(),
		id,
	)
}

type operationRuntime struct {
	store         sdk.OperationStore
	cancel        context.CancelFunc
	inflight      operationworker.Inflight
	wait          sync.WaitGroup
	poll          time.Duration
	cancelTimeout time.Duration
	lease         time.Duration
	workerID      string
}

const defaultOperationCancelTimeout = 2 * time.Second

func (operation *operationRuntime) effectiveCancelTimeout() time.Duration {
	if operation == nil {
		return defaultOperationCancelTimeout
	}
	if operation.cancelTimeout > 0 {
		return operation.cancelTimeout
	}
	return defaultOperationCancelTimeout
}

func (operation *operationRuntime) stop() {
	if operation.cancel != nil {
		operation.cancel()
	}
}

func (operation *operationRuntime) beginWork(runtime *Runtime) (func(), bool) {
	return runtime.beginRuntimeWork(&operation.wait)
}

func (operation *operationRuntime) waitStopped() {
	operation.wait.Wait()
}

type syncProviderAdapter struct {
	synchronous sdk.SyncProvider
	spec        sdk.ProviderSpec
	target      localOperationTarget
}

func (provider syncProviderAdapter) Spec() sdk.ProviderSpec {
	return provider.spec
}

func (provider syncProviderAdapter) SubmitCompletion(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return provider.target.submit(
		ctx,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return pluginpolicy.InvokeProviderOperation(ctx, provider.synchronous, input)
		},
	)
}

func (provider syncProviderAdapter) PollCompletion(
	ctx context.Context,
	id string,
	revision uint64,
) (sdk.Operation, error) {
	return provider.target.poll(ctx, id, revision)
}

func (provider syncProviderAdapter) CancelCompletion(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return provider.target.cancel(ctx, id)
}

type syncToolAdapter struct {
	synchronous sdk.SyncTool
	spec        sdk.ToolSpec
	target      localOperationTarget
}

type syncCapabilityAdapter struct {
	synchronous sdk.SyncCapability
	spec        sdk.CapabilitySpec
	target      localOperationTarget
}

func (capability syncCapabilityAdapter) Spec() sdk.CapabilitySpec {
	return sdk.CloneCapabilitySpec(capability.spec)
}

func (capability syncCapabilityAdapter) SubmitInvoke(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return capability.target.submit(
		ctx,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return pluginpolicy.InvokeCapabilityOperation(ctx, capability.synchronous, input)
		},
	)
}

func (capability syncCapabilityAdapter) PollInvoke(
	ctx context.Context,
	id string,
	revision uint64,
) (sdk.Operation, error) {
	return capability.target.poll(ctx, id, revision)
}

func (capability syncCapabilityAdapter) CancelInvoke(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return capability.target.cancel(ctx, id)
}

func (tool syncToolAdapter) Spec() sdk.ToolSpec {
	return sdk.CloneToolSpec(tool.spec)
}

func (tool syncToolAdapter) SubmitCall(
	ctx context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	return tool.target.submit(
		ctx,
		request,
		func(ctx context.Context, input json.RawMessage) (json.RawMessage, error) {
			return pluginpolicy.InvokeToolOperation(ctx, tool.synchronous, input)
		},
	)
}

func (tool syncToolAdapter) PollCall(
	ctx context.Context,
	id string,
	revision uint64,
) (sdk.Operation, error) {
	return tool.target.poll(ctx, id, revision)
}

func (tool syncToolAdapter) CancelCall(
	ctx context.Context,
	id string,
) (sdk.Operation, error) {
	return tool.target.cancel(ctx, id)
}

func (runtime *Runtime) localOperationTarget(
	owner *mountState,
	kind sdk.OperationKind,
	name string,
	spec any,
) localOperationTarget {
	return localOperationTarget{
		runtime:  runtime,
		kind:     kind,
		resource: name,
		resourceRevision: sdk.NewResourceIdentity(
			owner.manifest,
			sdk.ResourceKind(kind),
			name,
			spec,
		).Revision(),
		owner: owner,
	}
}

func (runtime *Runtime) adaptSynchronousResources(
	registrar *plugincontract.AgentRegistrar,
	owner *mountState,
) {
	for name, staged := range registrar.Providers {
		if _, asynchronous := staged.Value.(sdk.AsyncProvider); asynchronous {
			continue
		}
		staged.Value = syncProviderAdapter{
			synchronous: staged.Value.(sdk.SyncProvider),
			spec:        staged.Spec,
			target: runtime.localOperationTarget(
				owner,
				sdk.OperationKindProvider,
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
			synchronous: staged.Value.(sdk.SyncTool),
			spec:        staged.Spec,
			target: runtime.localOperationTarget(
				owner,
				sdk.OperationKindTool,
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
			synchronous: staged.Value.(sdk.SyncCapability),
			spec:        staged.Spec,
			target: runtime.localOperationTarget(
				owner,
				sdk.OperationKindCapability,
				name,
				staged.Spec,
			),
		}
		registrar.Capabilities[name] = staged
	}
}

func (runtime *Runtime) submitLocalOperation(
	ctx context.Context,
	target localOperationTarget,
	request sdk.OperationRequest,
	releaseExecutionLease func(),
	execute operationExecutor,
) (sdk.Operation, error) {
	if releaseExecutionLease == nil {
		releaseExecutionLease = func() {}
	}
	releaseOperationWork, ok := runtime.operation.beginWork(runtime)
	if !ok {
		releaseExecutionLease()
		return sdk.Operation{}, errors.New("runtime is closed")
	}

	identity := target.identity()
	operation, err := runtime.operationHost().SubmitReserved(
		ctx,
		identity,
		request,
		func(
			ctx context.Context,
			record sdk.OperationRecord,
		) (json.RawMessage, error) {
			return execute(ctx, record.Input)
		},
		func() {
			releaseExecutionLease()
			releaseOperationWork()
		},
	)
	if err != nil {
		return sdk.Operation{}, err
	}
	return operation, nil
}

func (runtime *Runtime) operationHost() operationworker.Host {
	return operationworker.Host{
		Inflight: &runtime.operation.inflight,
		Runner: operationworker.Runner{
			Store:  runtime.operation.store,
			Logger: runtime.logger,
			Owner:  runtime.operation.workerID,
			Lease:  runtime.operation.lease,
		},
	}
}

func (runtime *Runtime) pollLocalOperation(
	ctx context.Context,
	identity operationworker.Target,
	id string,
) (sdk.Operation, error) {
	record, err := runtime.loadLocalOperation(ctx, identity, id)
	if err != nil {
		return sdk.Operation{}, err
	}
	return record.Operation, nil
}

func (runtime *Runtime) cancelLocalOperation(
	ctx context.Context,
	identity operationworker.Target,
	id string,
) (sdk.Operation, error) {
	cancelled, err := runtime.operationHost().Cancel(
		ctx,
		id,
		identity.ValidateTarget,
	)
	if err != nil {
		return sdk.Operation{}, err
	}
	return cancelled.Operation, nil
}

func (runtime *Runtime) loadLocalOperation(
	ctx context.Context,
	identity operationworker.Target,
	id string,
) (sdk.OperationRecord, error) {
	record, err := runtime.operation.store.Get(ctx, id)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := identity.Validate(record); err != nil {
		return sdk.OperationRecord{}, err
	}
	return record, nil
}
