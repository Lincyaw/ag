package pluginpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func SubscriberTimeout(
	defaultTimeout time.Duration,
	subscriberTimeout time.Duration,
) time.Duration {
	if subscriberTimeout > 0 && subscriberTimeout < defaultTimeout {
		return subscriberTimeout
	}
	return defaultTimeout
}

func HandleHook(
	ctx context.Context,
	hook sdk.Hook,
	spec sdk.HookSpec,
	event sdk.Event,
) (effect sdk.Effect, err error) {
	if spec.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, spec.Timeout)
		defer cancel()
	}
	defer recoverPluginPanic(&err, "hook panic")
	effect, err = hook.Handle(ctx, sdk.CloneEvent(event))
	if err != nil {
		return sdk.Effect{}, err
	}
	return sdk.CloneEffect(effect), nil
}

func ReceiveSubscriber(
	ctx context.Context,
	subscriber sdk.Subscriber,
	delivery sdk.Delivery,
	timeout time.Duration,
) (err error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer recoverPluginPanic(&err, "subscriber panic")
	return subscriber.Receive(ctx, sdk.CloneDelivery(delivery))
}

func InvokeOperation(
	ctx context.Context,
	record sdk.OperationRecord,
	execute func(context.Context, sdk.OperationRecord) (json.RawMessage, error),
) (output json.RawMessage, err error) {
	defer recoverPluginPanic(&err, "plugin operation panic")
	output, err = execute(ctx, sdk.CloneOperationRecord(record))
	if err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), output...), nil
}

func InvokeProviderOperation(
	ctx context.Context,
	provider sdk.SyncProvider,
	input json.RawMessage,
) (json.RawMessage, error) {
	var request sdk.ModelRequest
	if err := json.Unmarshal(input, &request); err != nil {
		return nil, err
	}
	response, err := provider.Complete(ctx, request)
	if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

func InvokeToolOperation(
	ctx context.Context,
	tool sdk.SyncTool,
	input json.RawMessage,
) (json.RawMessage, error) {
	result, err := tool.Call(ctx, append(json.RawMessage(nil), input...))
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func InvokeCapabilityOperation(
	ctx context.Context,
	capability sdk.SyncCapability,
	input json.RawMessage,
) (json.RawMessage, error) {
	return capability.Invoke(ctx, append(json.RawMessage(nil), input...))
}

func recoverPluginPanic(err *error, prefix string) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("%s: %v\n%s", prefix, recovered, debug.Stack())
	}
}
