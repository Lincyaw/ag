package sdk

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestAwaitOperationPollsMonotonicRevisionsToCompletion(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operationPoll: time.Microsecond}
	initial := Operation{
		ID:             "operation-1",
		IdempotencyKey: "trajectory-entry-1",
		State:          OperationPending,
		Revision:       1,
	}
	states := []Operation{
		{
			ID:             initial.ID,
			IdempotencyKey: initial.IdempotencyKey,
			State:          OperationRunning,
			Revision:       2,
		},
		{
			ID:             initial.ID,
			IdempotencyKey: initial.IdempotencyKey,
			State:          OperationSucceeded,
			Revision:       3,
			Output:         []byte(`{"content":"done"}`),
		},
	}
	var polls atomic.Int64
	result, err := runtime.awaitOperation(
		context.Background(),
		initial,
		func(_ context.Context, id string, revision uint64) (Operation, error) {
			index := int(polls.Add(1) - 1)
			if id != initial.ID || revision != uint64(index+1) {
				t.Fatalf("poll(%q, %d) at index %d", id, revision, index)
			}
			return states[index], nil
		},
		func(context.Context, string) (Operation, error) {
			t.Fatal("cancel called for successful operation")
			return Operation{}, nil
		},
	)
	if err != nil {
		t.Fatalf("await operation: %v", err)
	}
	if result.State != OperationSucceeded || result.Revision != 3 || polls.Load() != 2 {
		t.Fatalf("result = %#v, polls = %d", result, polls.Load())
	}
}

func TestAwaitOperationCancellationUsesFreshContext(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operationPoll: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var cancelled atomic.Bool
	_, err := runtime.awaitOperation(
		ctx,
		Operation{
			ID:             "operation-cancel",
			IdempotencyKey: "entry-cancel",
			State:          OperationRunning,
			Revision:       4,
		},
		func(context.Context, string, uint64) (Operation, error) {
			t.Fatal("poll called after context cancellation")
			return Operation{}, nil
		},
		func(cancelCtx context.Context, id string) (Operation, error) {
			if err := cancelCtx.Err(); err != nil {
				t.Fatalf("cancel context inherited cancellation: %v", err)
			}
			if id != "operation-cancel" {
				t.Fatalf("cancel ID = %q", id)
			}
			cancelled.Store(true)
			return Operation{
				ID:             id,
				IdempotencyKey: "entry-cancel",
				State:          OperationCancelled,
				Revision:       5,
			}, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
	if !cancelled.Load() {
		t.Fatal("cancel was not called")
	}
}

func TestAwaitOperationRejectsRemoteStateCorruption(t *testing.T) {
	t.Parallel()
	tests := map[string]Operation{
		"different ID": {
			ID:             "other",
			IdempotencyKey: "entry",
			State:          OperationRunning,
			Revision:       3,
		},
		"regressed revision": {
			ID:             "operation",
			IdempotencyKey: "entry",
			State:          OperationPending,
			Revision:       1,
		},
		"changed key": {
			ID:             "operation",
			IdempotencyKey: "different",
			State:          OperationRunning,
			Revision:       3,
		},
	}
	for name, next := range tests {
		next := next
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runtime := &Runtime{operationPoll: time.Microsecond}
			_, err := runtime.awaitOperation(
				context.Background(),
				Operation{
					ID:             "operation",
					IdempotencyKey: "entry",
					State:          OperationRunning,
					Revision:       2,
				},
				func(context.Context, string, uint64) (Operation, error) {
					return next, nil
				},
				func(context.Context, string) (Operation, error) {
					return Operation{}, nil
				},
			)
			if err == nil {
				t.Fatal("corrupt state was accepted")
			}
		})
	}
}
