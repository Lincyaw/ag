package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (runtime *Runtime) InvokeCapability(
	ctx context.Context,
	name string,
	input json.RawMessage,
) (json.RawMessage, error) {
	return runtime.InvokeCapabilityWithRequest(ctx, name, sdk.OperationRequest{
		IdempotencyKey: sdk.NewID(),
		Input:          append(json.RawMessage(nil), input...),
	})
}

// InvokeCapabilityWithRequest invokes a capability with caller-owned operation
// identity, allowing retries to reuse the same durable operation.
func (runtime *Runtime) InvokeCapabilityWithRequest(
	ctx context.Context,
	name string,
	request sdk.OperationRequest,
) (json.RawMessage, error) {
	if request.IdempotencyKey == "" {
		return nil, fmt.Errorf(
			"invoke capability %q: operation idempotency key is empty",
			name,
		)
	}
	target, err := runtime.acquireCapabilityInvocation(name)
	if err != nil {
		return nil, err
	}
	defer target.release()
	ctx, span := runtime.tracer.Start(
		ctx,
		"capability "+name,
		trace.WithAttributes(
			attribute.String("agentm.capability.name", name),
			attribute.String("agentm.plugin.name", target.pluginName),
		),
	)
	defer span.End()
	operationRequest := sdk.CloneOperationRequest(request)
	initial, err := target.capability.SubmitInvoke(ctx, operationRequest)
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("submit capability %q: %w", name, err)
	}
	output, err := awaitOperationRequestRawJSON(
		runtime,
		ctx,
		operationRequest,
		initial,
		target.capability.PollInvoke,
		target.capability.CancelInvoke,
		fmt.Sprintf("invoke capability %q", name),
		fmt.Sprintf("capability %q", name),
	)
	if err != nil {
		recordSpanError(span, err)
		return nil, err
	}
	return output, nil
}

type capabilityInvocation struct {
	capability sdk.AsyncCapability
	pluginName string
	lease      *snapshotLease
}

func (runtime *Runtime) acquireCapabilityInvocation(
	name string,
) (capabilityInvocation, error) {
	snapshotLease, err := runtime.acquireSnapshot()
	if err != nil {
		return capabilityInvocation{}, err
	}
	owned, exists := snapshotLease.snapshot.capabilities[name]
	if !exists {
		snapshotLease.release()
		return capabilityInvocation{}, fmt.Errorf(
			"capability %q is not registered",
			name,
		)
	}
	if owned.owner == nil {
		snapshotLease.release()
		return capabilityInvocation{}, fmt.Errorf(
			"capability %q has no plugin owner",
			name,
		)
	}
	capability, ok := owned.value.(sdk.AsyncCapability)
	if !ok {
		snapshotLease.release()
		return capabilityInvocation{}, fmt.Errorf(
			"capability %q has no asynchronous execution implementation",
			name,
		)
	}
	ownerLease, err := runtime.acquireMounts(owned.owner)
	pluginName := owned.owner.manifest.Name
	snapshotLease.release()
	if err != nil {
		return capabilityInvocation{}, fmt.Errorf(
			"acquire capability %q owner plugin %q: %w",
			name,
			pluginName,
			err,
		)
	}
	return capabilityInvocation{
		capability: capability,
		pluginName: pluginName,
		lease:      ownerLease,
	}, nil
}

func (target capabilityInvocation) release() {
	target.lease.release()
}
