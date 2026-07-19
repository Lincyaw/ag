package operationworker

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestHostSubmitReservedReleasesReservationWithoutStartedWorker(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	inflight := NewInflight(ctx)
	host := Host{
		Inflight: &inflight,
		Runner: Runner{
			Store: store,
			Owner: "host-test",
			Lease: time.Second,
		},
	}
	target := Target{Kind: sdk.OperationKindTool, Resource: "cached-tool"}
	request := sdk.OperationRequest{
		IdempotencyKey: "cached-tool-call",
		Input:          json.RawMessage(`{}`),
	}

	firstDone := make(chan struct{})
	if _, err := host.SubmitReserved(
		ctx,
		target,
		request,
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			return json.RawMessage(`{"ok":true}`), nil
		},
		func() { close(firstDone) },
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("initial operation worker did not finish")
	}

	var releases atomic.Int64
	var ran atomic.Bool
	operation, err := host.SubmitReserved(
		ctx,
		target,
		request,
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			ran.Store(true)
			return nil, nil
		},
		func() { releases.Add(1) },
	)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != sdk.OperationSucceeded {
		t.Fatalf("operation state = %q, want succeeded", operation.State)
	}
	if ran.Load() {
		t.Fatal("terminal idempotent submit started a worker")
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("reservation releases = %d, want 1", got)
	}
}

func TestHostSubmitReservedRejectsMissingInflightBeforePersisting(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()

	var releases atomic.Int64
	_, err := (Host{
		Runner: Runner{
			Store: store,
			Owner: "host-test",
			Lease: time.Second,
		},
	}).SubmitReserved(
		ctx,
		Target{Kind: sdk.OperationKindTool, Resource: "missing-inflight"},
		sdk.OperationRequest{
			IdempotencyKey: "missing-inflight-call",
			Input:          json.RawMessage(`{}`),
		},
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			t.Fatal("executor should not run without an inflight registry")
			return nil, nil
		},
		func() { releases.Add(1) },
	)
	if err == nil ||
		!strings.Contains(err.Error(), "operation inflight registry is nil") {
		t.Fatalf("SubmitReserved() error = %v", err)
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("reservation releases = %d, want 1", got)
	}
	if records, listErr := store.List(ctx); listErr != nil || len(records) != 0 {
		t.Fatalf("persisted operations = %#v, error = %v", records, listErr)
	}
}

func TestHostStartReservedAsyncReleasesReservationWhenSlotUnavailable(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	inflight := NewInflight(ctx)
	host := Host{Inflight: &inflight}

	slot := host.Start(ctx, "operation-1")
	if !slot.Acquired() {
		t.Fatal("initial execution slot was not acquired")
	}
	defer slot.Finish()

	var releases atomic.Int64
	var ran atomic.Bool
	host.StartReservedAsync(
		ctx,
		"operation-1",
		nil,
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			ran.Store(true)
			return nil, nil
		},
		func() { releases.Add(1) },
	)

	if ran.Load() {
		t.Fatal("duplicate execution slot ran operation")
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("reservation releases = %d, want 1", got)
	}
}
