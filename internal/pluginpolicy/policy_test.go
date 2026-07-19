package pluginpolicy

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestHandleHookSnapshotsMutableEffect(t *testing.T) {
	t.Parallel()
	effect := sdk.Effect{
		Patch: map[string]json.RawMessage{
			"field": json.RawMessage(`{"value":1}`),
		},
		Block: &sdk.Block{Reason: "original"},
		Action: &sdk.Action{
			Kind:  sdk.ActionInject,
			Cause: &sdk.Cause{Code: "original"},
			Messages: []sdk.Message{{
				Content: "original",
				ToolCalls: []sdk.ToolCall{{
					ID:        "call",
					Arguments: json.RawMessage(`{"value":1}`),
				}},
			}},
		},
	}
	hook := sdk.HookFunc{
		HookSpec: sdk.HookSpec{Name: "snapshot-effect", Event: "example.event"},
		HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
			return effect, nil
		},
	}
	snapshot, err := HandleHook(
		context.Background(),
		hook,
		hook.Spec(),
		sdk.Event{Name: "example.event", Payload: json.RawMessage(`{}`)},
	)
	if err != nil {
		t.Fatal(err)
	}

	effect.Patch["field"][0] = '['
	effect.Block.Reason = "changed"
	effect.Action.Cause.Code = "changed"
	effect.Action.Messages[0].Content = "changed"
	effect.Action.Messages[0].ToolCalls[0].Arguments[0] = '['

	if string(snapshot.Patch["field"]) != `{"value":1}` ||
		snapshot.Block.Reason != "original" ||
		snapshot.Action.Cause.Code != "original" ||
		snapshot.Action.Messages[0].Content != "original" ||
		string(snapshot.Action.Messages[0].ToolCalls[0].Arguments) != `{"value":1}` {
		t.Fatalf("hook effect changed with plugin-owned value: %#v", snapshot)
	}
}

func TestHandleHookLabelsPanics(t *testing.T) {
	t.Parallel()
	hook := sdk.HookFunc{
		HookSpec: sdk.HookSpec{Name: "panicking-hook", Event: "example.event"},
		HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
			panic("broken hook")
		},
	}
	_, err := HandleHook(
		context.Background(),
		hook,
		hook.Spec(),
		sdk.Event{Name: "example.event", Payload: json.RawMessage(`{}`)},
	)
	if err == nil || !strings.Contains(err.Error(), "hook panic: broken hook") {
		t.Fatalf("hook panic error = %v", err)
	}
}

func TestReceiveSubscriberAppliesTimeout(t *testing.T) {
	t.Parallel()
	subscriber := sdk.SubscriberFunc{
		SubscriberSpec: sdk.SubscriberSpec{
			Name:   "timeout-subscriber",
			Events: []string{"example.event"},
		},
		ReceiveFunc: func(ctx context.Context, _ sdk.Delivery) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	err := ReceiveSubscriber(
		context.Background(),
		subscriber,
		sdk.Delivery{
			ID:           "delivery",
			Plugin:       "plugin",
			Subscription: "timeout-subscriber",
			Event: sdk.Event{
				ID:      "event",
				Name:    "example.event",
				Payload: json.RawMessage(`{}`),
			},
		},
		time.Millisecond,
	)
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("ReceiveSubscriber() error = %v, want context deadline", err)
	}
}

func TestInvokeOperationSnapshotsRecordAndOutput(t *testing.T) {
	t.Parallel()
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "operation",
			IdempotencyKey: "operation-key",
			State:          sdk.OperationRunning,
			Revision:       2,
			Output:         json.RawMessage(`{"previous":true}`),
		},
		Kind:     sdk.OperationKindTool,
		Resource: "tool",
		Input:    json.RawMessage(`{"input":1}`),
		Invocation: sdk.Invocation{
			ID:           "node",
			RootID:       "root",
			SessionID:    "session",
			ExecutionID:  "execution",
			Dependencies: []string{"dependency"},
		},
		Execution: &sdk.OperationLease{Owner: "owner", Token: "token"},
	}
	pluginOutput := json.RawMessage(`{"output":1}`)

	output, err := InvokeOperation(
		context.Background(),
		record,
		func(_ context.Context, received sdk.OperationRecord) (json.RawMessage, error) {
			received.Operation.Output[0] = '['
			received.Input[0] = '['
			received.Invocation.Dependencies[0] = "changed"
			received.Execution.Token = "changed"
			return pluginOutput, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	pluginOutput[0] = '['

	if string(record.Operation.Output) != `{"previous":true}` ||
		string(record.Input) != `{"input":1}` ||
		record.Invocation.Dependencies[0] != "dependency" ||
		record.Execution.Token != "token" {
		t.Fatalf("operation record was mutated by plugin call: %#v", record)
	}
	if string(output) != `{"output":1}` {
		t.Fatalf("operation output changed with plugin-owned value: %s", output)
	}
}
