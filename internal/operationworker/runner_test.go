package operationworker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestRunnerStoresPanicAndRestoresInvocation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	invocation := sdk.Invocation{
		ID:          "panic-node",
		RootID:      "panic-root",
		SessionID:   "panic-session",
		ExecutionID: "panic-execution",
	}
	record, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation:  sdk.Operation{IdempotencyKey: "panicking-operation"},
		Kind:       sdk.OperationKindTool,
		Resource:   "panicking-tool",
		Input:      json.RawMessage(`{}`),
		Invocation: invocation,
	})
	if err != nil {
		t.Fatal(err)
	}

	var observed sdk.Invocation
	Runner{Store: store, Owner: "panic-test", Lease: time.Second}.Run(
		ctx,
		record.Operation.ID,
		nil,
		func(ctx context.Context, _ sdk.OperationRecord) (json.RawMessage, error) {
			observed, _ = sdk.InvocationFromContext(ctx)
			panic("broken plugin")
		},
	)

	failed, err := store.Get(ctx, record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Operation.State != sdk.OperationFailed || !strings.Contains(
		failed.Operation.Error,
		"plugin operation panic: broken plugin",
	) {
		t.Fatalf("operation after panic = %#v", failed.Operation)
	}
	if observed.ID != invocation.ID ||
		observed.RootID != invocation.RootID ||
		observed.SessionID != invocation.SessionID ||
		observed.ExecutionID != invocation.ExecutionID {
		t.Fatalf("executor invocation = %#v, want %#v", observed, invocation)
	}
}

func TestRunnerReleasesClaimWhenWorkerStops(t *testing.T) {
	t.Parallel()
	store := sdkstorage.NewMemoryOperationStore()
	record, _, err := store.Submit(context.Background(), sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "cancelled-worker"},
		Kind:      sdk.OperationKindTool,
		Resource:  "blocking-tool",
		Input:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		Runner{Store: store, Owner: "cancel-test", Lease: time.Millisecond}.Run(
			ctx,
			record.Operation.ID,
			nil,
			func(ctx context.Context, _ sdk.OperationRecord) (json.RawMessage, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		)
	}()
	<-entered
	cancel()
	<-done

	released, err := store.Get(context.Background(), record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if released.Operation.State != sdk.OperationRunning ||
		released.Execution != nil {
		t.Fatalf("operation after worker stop = %#v", released)
	}
}
