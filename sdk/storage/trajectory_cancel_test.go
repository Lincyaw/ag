package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestTrajectoryStoresFenceDurableCancellation(t *testing.T) {
	for name, factory := range trajectoryStoreFactories() {
		t.Run(name, func(t *testing.T) {
			testTrajectoryExecutionCancellation(t, factory(t))
		})
	}
}

func testTrajectoryExecutionCancellation(
	t *testing.T,
	store sdk.TrajectoryStore,
) {
	t.Helper()
	ctx := t.Context()
	if err := store.Create(ctx, sdk.Trajectory{ID: "cancel-session"}); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID: "cancel-input", Kind: sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(), Payload: []byte(`{"role":"user","content":"stop"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"cancel-session",
		"",
		sdk.TrajectoryExecutionStart{
			ID: "cancel-execution", Provider: "test", MaxTurns: 3,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimExecution(
		ctx,
		"cancel-session",
		"worker-a",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelExecution(
		ctx,
		"cancel-session",
		claimed.ID,
		"user interrupted the tool",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution == nil ||
		cancelled.Execution.State != sdk.TrajectoryExecutionCancelled ||
		cancelled.Execution.LeaseToken != "" ||
		cancelled.Execution.LastError != "user interrupted the tool" {
		t.Fatalf("cancelled metadata = %#v", cancelled.Execution)
	}
	revision := cancelled.Execution.Revision
	retried, err := store.CancelExecution(
		ctx,
		"cancel-session",
		claimed.ID,
		"duplicate request",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Execution == nil || retried.Execution.Revision != revision {
		t.Fatalf("idempotent cancellation = %#v", retried.Execution)
	}
	if _, err := store.RenewExecution(
		ctx,
		"cancel-session",
		claimed.ID,
		claimed.LeaseToken,
		time.Now().UTC(),
		time.Minute,
	); !errors.Is(err, sdk.ErrTrajectoryFence) {
		t.Fatalf("renew after cancellation error = %v", err)
	}
	recoverable, err := store.ListRecoverable(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 0 {
		t.Fatalf("cancelled execution is recoverable: %#v", recoverable)
	}
}
