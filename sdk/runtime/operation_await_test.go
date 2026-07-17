package runtime

// Operation tests cover asynchronous polling and cancellation.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestAwaitOperationPollsMonotonicRevisionsToCompletion(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{poll: time.Microsecond}}
	initial := sdk.Operation{
		ID:             "operation-1",
		IdempotencyKey: "trajectory-entry-1",
		State:          sdk.OperationPending,
		Revision:       1,
	}
	states := []sdk.Operation{
		{
			ID:             initial.ID,
			IdempotencyKey: initial.IdempotencyKey,
			State:          sdk.OperationRunning,
			Revision:       2,
		},
		{
			ID:             initial.ID,
			IdempotencyKey: initial.IdempotencyKey,
			State:          sdk.OperationSucceeded,
			Revision:       3,
			Output:         []byte(`{"content":"done"}`),
		},
	}
	var polls atomic.Int64
	result, err := runtime.awaitOperation(
		context.Background(),
		initial,
		func(_ context.Context, id string, revision uint64) (sdk.Operation, error) {
			index := int(polls.Add(1) - 1)
			if id != initial.ID || revision != uint64(index+1) {
				t.Fatalf("poll(%q, %d) at index %d", id, revision, index)
			}
			return states[index], nil
		},
		func(context.Context, string) (sdk.Operation, error) {
			t.Fatal("cancel called for successful operation")
			return sdk.Operation{}, nil
		},
	)
	if err != nil {
		t.Fatalf("await operation: %v", err)
	}
	if result.State != sdk.OperationSucceeded || result.Revision != 3 || polls.Load() != 2 {
		t.Fatalf("result = %#v, polls = %d", result, polls.Load())
	}
}

func TestAwaitOperationCancellationUsesFreshContext(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{poll: time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var cancelled atomic.Bool
	_, err := runtime.awaitOperation(
		ctx,
		sdk.Operation{
			ID:             "operation-cancel",
			IdempotencyKey: "entry-cancel",
			State:          sdk.OperationRunning,
			Revision:       4,
		},
		func(context.Context, string, uint64) (sdk.Operation, error) {
			t.Fatal("poll called after context cancellation")
			return sdk.Operation{}, nil
		},
		func(cancelCtx context.Context, id string) (sdk.Operation, error) {
			if err := cancelCtx.Err(); err != nil {
				t.Fatalf("cancel context inherited cancellation: %v", err)
			}
			if id != "operation-cancel" {
				t.Fatalf("cancel ID = %q", id)
			}
			cancelled.Store(true)
			return sdk.Operation{
				ID:             id,
				IdempotencyKey: "entry-cancel",
				State:          sdk.OperationCancelled,
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

func TestAwaitOperationShutdownDoesNotCancelResource(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{
		closed:    true,
		operation: operationRuntime{poll: time.Second},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runtime.awaitOperation(
		ctx,
		sdk.Operation{
			ID:             "operation-shutdown",
			IdempotencyKey: "entry-shutdown",
			State:          sdk.OperationRunning,
			Revision:       4,
		},
		func(context.Context, string, uint64) (sdk.Operation, error) {
			t.Fatal("poll called after runtime shutdown")
			return sdk.Operation{}, nil
		},
		func(context.Context, string) (sdk.Operation, error) {
			t.Fatal("resource cancelled during runtime shutdown")
			return sdk.Operation{}, nil
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestAwaitOperationRejectsRemoteStateCorruption(t *testing.T) {
	t.Parallel()
	tests := map[string]sdk.Operation{
		"different ID": {
			ID:             "other",
			IdempotencyKey: "entry",
			State:          sdk.OperationRunning,
			Revision:       3,
		},
		"regressed revision": {
			ID:             "operation",
			IdempotencyKey: "entry",
			State:          sdk.OperationPending,
			Revision:       1,
		},
		"changed key": {
			ID:             "operation",
			IdempotencyKey: "different",
			State:          sdk.OperationRunning,
			Revision:       3,
		},
	}
	for name, next := range tests {
		next := next
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runtime := &Runtime{
				operation: operationRuntime{poll: time.Microsecond},
			}
			_, err := runtime.awaitOperation(
				context.Background(),
				sdk.Operation{
					ID:             "operation",
					IdempotencyKey: "entry",
					State:          sdk.OperationRunning,
					Revision:       2,
				},
				func(context.Context, string, uint64) (sdk.Operation, error) {
					return next, nil
				},
				func(context.Context, string) (sdk.Operation, error) {
					return sdk.Operation{}, nil
				},
			)
			if err == nil {
				t.Fatal("corrupt state was accepted")
			}
		})
	}
}
