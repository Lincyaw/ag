package operationworker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type panicClaimStore struct {
	sdk.OperationStore
}

func (store panicClaimStore) Claim(
	context.Context,
	string,
	string,
	time.Time,
	time.Duration,
) (sdk.OperationRecord, error) {
	panic("broken operation store")
}

type panicRenewStore struct {
	sdk.OperationStore
}

func (store panicRenewStore) Renew(
	context.Context,
	string,
	string,
	time.Time,
	time.Duration,
) (sdk.OperationRecord, error) {
	panic("broken operation renew")
}

func TestRunnerPanicAtWorkerBoundaryDoesNotEscape(t *testing.T) {
	t.Parallel()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("Run() panic = %v", recovered)
		}
	}()
	Runner{
		Store: panicClaimStore{
			OperationStore: sdkstorage.NewMemoryOperationStore(),
		},
		Owner: "panic-boundary-test",
		Lease: time.Second,
	}.Run(
		context.Background(),
		"panic-operation",
		nil,
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			t.Fatal("executor should not run after claim panic")
			return nil, nil
		},
	)
}

func TestRunnerRenewPanicCancelsExecutionAndKeepsOperationRecoverable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	record, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "panic-renew"},
		Kind:      sdk.OperationKindTool,
		Resource:  "renewing-tool",
		Input:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		Runner{
			Store: panicRenewStore{OperationStore: store},
			Owner: "panic-renew-test",
			Lease: 3 * time.Millisecond,
		}.Run(
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
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("executor was not entered")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after renew panic")
	}

	recoverable, err := store.Get(ctx, record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoverable.Operation.State != sdk.OperationRunning ||
		recoverable.Execution == nil {
		t.Fatalf("operation after renew panic = %#v", recoverable)
	}
}

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

func TestRunnerReleasesClaimWhenWorkerStopsDuringValidation(t *testing.T) {
	t.Parallel()
	store := sdkstorage.NewMemoryOperationStore()
	record, _, err := store.Submit(context.Background(), sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "cancelled-validator"},
		Kind:      sdk.OperationKindTool,
		Resource:  "closing-tool",
		Input:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	Runner{Store: store, Owner: "validate-cancel-test", Lease: time.Second}.Run(
		ctx,
		record.Operation.ID,
		func(sdk.OperationRecord) error {
			cancel()
			return errors.New("target is closing")
		},
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			t.Fatal("executor should not run after validation cancellation")
			return nil, nil
		},
	)

	released, err := store.Get(context.Background(), record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if released.Operation.State != sdk.OperationRunning ||
		released.Execution != nil ||
		released.Operation.Error != "" {
		t.Fatalf("operation after validation cancellation = %#v", released)
	}
}

func TestRunnerFailsClaimedOperationWhenValidationFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	record, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "invalid-resource"},
		Kind:      sdk.OperationKindTool,
		Resource:  "stale-tool",
		Input:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	Runner{Store: store, Owner: "validation-test", Lease: time.Second}.Run(
		ctx,
		record.Operation.ID,
		func(sdk.OperationRecord) error {
			return errors.New("resource revision is stale")
		},
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			t.Fatal("executor should not run after validation failure")
			return nil, nil
		},
	)

	failed, err := store.Get(ctx, record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Operation.State != sdk.OperationFailed ||
		!strings.Contains(failed.Operation.Error, "resource revision is stale") ||
		failed.Execution != nil {
		t.Fatalf("operation after validation failure = %#v", failed)
	}
}

func TestHostRejectsDuplicateInflightOperation(t *testing.T) {
	t.Parallel()
	inflight := NewInflight(context.Background())
	host := Host{Inflight: &inflight}

	slot := host.Start(context.Background(), "operation-1")
	if !slot.Acquired() {
		t.Fatal("first host start was rejected")
	}
	defer slot.Finish()
	duplicate := host.Start(
		context.Background(),
		"operation-1",
	)
	if duplicate.Acquired() {
		duplicate.Finish()
		t.Fatal("duplicate host start succeeded")
	}
}

func TestHostRequiresInflight(t *testing.T) {
	t.Parallel()
	if (Host{}).Start(
		context.Background(),
		"operation-without-inflight",
	).Acquired() {
		t.Fatal("host without inflight started")
	}
}
