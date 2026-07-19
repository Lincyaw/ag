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
		operationAwait{
			expectedIdempotencyKey: initial.IdempotencyKey,
			initial:                initial,
			poll: func(_ context.Context, id string, revision uint64) (sdk.Operation, error) {
				index := int(polls.Add(1) - 1)
				if id != initial.ID || revision != uint64(index+1) {
					t.Fatalf("poll(%q, %d) at index %d", id, revision, index)
				}
				return states[index], nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				t.Fatal("cancel called for successful operation")
				return sdk.Operation{}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("await operation: %v", err)
	}
	if result.State != sdk.OperationSucceeded || result.Revision != 3 || polls.Load() != 2 {
		t.Fatalf("result = %#v, polls = %d", result, polls.Load())
	}
}

func TestAwaitOperationUsesWatcherBeforePollDelay(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{poll: time.Hour}}
	initial := sdk.Operation{
		ID:             "operation-watch",
		IdempotencyKey: "entry-watch",
		State:          sdk.OperationRunning,
		Revision:       7,
	}
	result, err := runtime.awaitOperation(
		context.Background(),
		operationAwait{
			expectedIdempotencyKey: initial.IdempotencyKey,
			initial:                initial,
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				t.Fatal("poll called even though watcher is available")
				return sdk.Operation{}, nil
			},
			watch: func(_ context.Context, id string, revision uint64) (sdk.Operation, error) {
				if id != initial.ID || revision != initial.Revision {
					t.Fatalf("watch(%q, %d)", id, revision)
				}
				return sdk.Operation{
					ID:             initial.ID,
					IdempotencyKey: initial.IdempotencyKey,
					State:          sdk.OperationSucceeded,
					Revision:       initial.Revision + 1,
					Output:         []byte(`{"content":"watched"}`),
				}, nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				t.Fatal("cancel called for successful watched operation")
				return sdk.Operation{}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("await watched operation: %v", err)
	}
	if result.State != sdk.OperationSucceeded || result.Revision != 8 {
		t.Fatalf("watched result = %#v", result)
	}
}

func TestAwaitOperationWatcherFallbackPollsUnchangedSnapshot(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{poll: time.Hour}}
	initial := sdk.Operation{
		ID:             "operation-watch-unchanged",
		IdempotencyKey: "entry-watch-unchanged",
		State:          sdk.OperationRunning,
		Revision:       11,
	}
	var watches atomic.Int64
	var polls atomic.Int64
	result, err := runtime.awaitOperation(
		context.Background(),
		operationAwait{
			expectedIdempotencyKey: initial.IdempotencyKey,
			initial:                initial,
			poll: func(_ context.Context, id string, revision uint64) (sdk.Operation, error) {
				if id != initial.ID || revision != initial.Revision {
					t.Fatalf("poll(%q, %d)", id, revision)
				}
				polls.Add(1)
				return sdk.Operation{
					ID:             initial.ID,
					IdempotencyKey: initial.IdempotencyKey,
					State:          sdk.OperationSucceeded,
					Revision:       initial.Revision + 1,
					Output:         []byte(`{"content":"polled"}`),
				}, nil
			},
			watch: func(_ context.Context, id string, revision uint64) (sdk.Operation, error) {
				if id != initial.ID || revision != initial.Revision {
					t.Fatalf("watch(%q, %d)", id, revision)
				}
				watches.Add(1)
				return initial, nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				t.Fatal("cancel called for successful watched operation")
				return sdk.Operation{}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("await watched operation: %v", err)
	}
	if result.State != sdk.OperationSucceeded || result.Revision != 12 {
		t.Fatalf("watched fallback result = %#v", result)
	}
	if watches.Load() != 1 || polls.Load() != 1 {
		t.Fatalf("watches = %d, polls = %d", watches.Load(), polls.Load())
	}
}

func TestAwaitOperationWatcherUnchangedPollWaitsBeforeRetry(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{
		poll:          time.Hour,
		cancelTimeout: 2500 * time.Millisecond,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	initial := sdk.Operation{
		ID:             "operation-watch-still-unchanged",
		IdempotencyKey: "entry-watch-still-unchanged",
		State:          sdk.OperationRunning,
		Revision:       3,
	}
	var watches atomic.Int64
	var polls atomic.Int64
	var cancelled atomic.Bool
	_, err := runtime.awaitOperation(
		ctx,
		operationAwait{
			expectedIdempotencyKey: initial.IdempotencyKey,
			initial:                initial,
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				polls.Add(1)
				cancel()
				return initial, nil
			},
			watch: func(context.Context, string, uint64) (sdk.Operation, error) {
				if watches.Add(1) > 1 {
					t.Fatal("watch retried before waiting after unchanged poll")
				}
				return initial, nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				cancelled.Store(true)
				return sdk.Operation{
					ID:             initial.ID,
					IdempotencyKey: initial.IdempotencyKey,
					State:          sdk.OperationCancelled,
					Revision:       initial.Revision + 1,
				}, nil
			},
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
	if watches.Load() != 1 || polls.Load() != 1 || !cancelled.Load() {
		t.Fatalf(
			"watches = %d, polls = %d, cancelled = %v",
			watches.Load(),
			polls.Load(),
			cancelled.Load(),
		)
	}
}

func TestAwaitOperationCancellationUsesFreshContext(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{
		poll:          time.Second,
		cancelTimeout: 2500 * time.Millisecond,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var cancelled atomic.Bool
	_, err := runtime.awaitOperation(
		ctx,
		operationAwait{
			expectedIdempotencyKey: "entry-cancel",
			initial: sdk.Operation{
				ID:             "operation-cancel",
				IdempotencyKey: "entry-cancel",
				State:          sdk.OperationRunning,
				Revision:       4,
			},
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				t.Fatal("poll called after context cancellation")
				return sdk.Operation{}, nil
			},
			cancel: func(cancelCtx context.Context, id string) (sdk.Operation, error) {
				if err := cancelCtx.Err(); err != nil {
					t.Fatalf("cancel context inherited cancellation: %v", err)
				}
				deadline, ok := cancelCtx.Deadline()
				if !ok {
					t.Fatal("cancel context has no deadline")
				}
				if remaining := time.Until(deadline); remaining < 1500*time.Millisecond {
					t.Fatalf("cancel context deadline remaining = %s", remaining)
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
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
	if !cancelled.Load() {
		t.Fatal("cancel was not called")
	}
}

func TestAwaitOperationWatcherCancellationUsesFreshContext(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{
		poll:          time.Hour,
		cancelTimeout: 2500 * time.Millisecond,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var watched atomic.Bool
	var cancelled atomic.Bool
	_, err := runtime.awaitOperation(
		ctx,
		operationAwait{
			expectedIdempotencyKey: "entry-watch-cancel",
			initial: sdk.Operation{
				ID:             "operation-watch-cancel",
				IdempotencyKey: "entry-watch-cancel",
				State:          sdk.OperationRunning,
				Revision:       4,
			},
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				t.Fatal("poll called after watcher cancellation")
				return sdk.Operation{}, nil
			},
			watch: func(ctx context.Context, _ string, _ uint64) (sdk.Operation, error) {
				watched.Store(true)
				return sdk.Operation{}, ctx.Err()
			},
			cancel: func(cancelCtx context.Context, id string) (sdk.Operation, error) {
				if err := cancelCtx.Err(); err != nil {
					t.Fatalf("cancel context inherited cancellation: %v", err)
				}
				if id != "operation-watch-cancel" {
					t.Fatalf("cancel ID = %q", id)
				}
				cancelled.Store(true)
				return sdk.Operation{
					ID:             id,
					IdempotencyKey: "entry-watch-cancel",
					State:          sdk.OperationCancelled,
					Revision:       5,
				}, nil
			},
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
	if !watched.Load() || !cancelled.Load() {
		t.Fatalf("watched = %v, cancelled = %v", watched.Load(), cancelled.Load())
	}
}

func TestAwaitOperationReturnsStructuredTerminalErrors(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		operation sdk.Operation
		want      error
	}{
		"failed": {
			operation: sdk.Operation{
				ID:             "operation-failed",
				IdempotencyKey: "entry-failed",
				State:          sdk.OperationFailed,
				Revision:       2,
				Error:          "provider rejected request",
			},
			want: sdk.ErrOperationFailed,
		},
		"cancelled": {
			operation: sdk.Operation{
				ID:             "operation-cancelled",
				IdempotencyKey: "entry-cancelled",
				State:          sdk.OperationCancelled,
				Revision:       2,
			},
			want: sdk.ErrOperationCancelled,
		},
	}
	for name, tt := range tests {
		tt := tt
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runtime := &Runtime{operation: operationRuntime{poll: time.Microsecond}}
			_, err := runtime.awaitOperation(
				context.Background(),
				operationAwait{
					expectedIdempotencyKey: tt.operation.IdempotencyKey,
					initial:                tt.operation,
					poll: func(context.Context, string, uint64) (sdk.Operation, error) {
						t.Fatal("poll called for terminal operation")
						return sdk.Operation{}, nil
					},
					cancel: func(context.Context, string) (sdk.Operation, error) {
						t.Fatal("cancel called for terminal operation")
						return sdk.Operation{}, nil
					},
				},
			)
			if !errors.Is(err, tt.want) {
				t.Fatalf("await error = %v, want %v", err, tt.want)
			}
			var terminalErr *sdk.OperationTerminalError
			if !errors.As(err, &terminalErr) {
				t.Fatalf("await error = %T, want OperationTerminalError", err)
			}
			if terminalErr.Operation.ID != tt.operation.ID ||
				terminalErr.Operation.State != tt.operation.State ||
				terminalErr.Operation.Error != tt.operation.Error {
				t.Fatalf("terminal snapshot = %#v, want %#v", terminalErr.Operation, tt.operation)
			}
		})
	}
}

func TestAwaitOperationShutdownHandoffLeavesOperationRecoverable(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{
		closed:    true,
		operation: operationRuntime{poll: time.Second},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runtime.awaitOperation(
		ctx,
		operationAwait{
			expectedIdempotencyKey: "entry-shutdown",
			initial: sdk.Operation{
				ID:             "operation-shutdown",
				IdempotencyKey: "entry-shutdown",
				State:          sdk.OperationRunning,
				Revision:       4,
			},
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				t.Fatal("poll called after runtime shutdown")
				return sdk.Operation{}, nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				t.Fatal("resource cancelled during runtime shutdown")
				return sdk.Operation{}, nil
			},
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
				operationAwait{
					expectedIdempotencyKey: "entry",
					initial: sdk.Operation{
						ID:             "operation",
						IdempotencyKey: "entry",
						State:          sdk.OperationRunning,
						Revision:       2,
					},
					poll: func(context.Context, string, uint64) (sdk.Operation, error) {
						return next, nil
					},
					cancel: func(context.Context, string) (sdk.Operation, error) {
						return sdk.Operation{}, nil
					},
				},
			)
			if err == nil {
				t.Fatal("corrupt state was accepted")
			}
		})
	}
}

func TestAwaitOperationRejectsUnexpectedAcceptedKey(t *testing.T) {
	t.Parallel()
	runtime := &Runtime{operation: operationRuntime{poll: time.Microsecond}}
	_, err := runtime.awaitOperation(
		context.Background(),
		operationAwait{
			expectedIdempotencyKey: "expected-entry",
			initial: sdk.Operation{
				ID:             "operation",
				IdempotencyKey: "other-entry",
				State:          sdk.OperationPending,
				Revision:       1,
			},
			poll: func(context.Context, string, uint64) (sdk.Operation, error) {
				t.Fatal("poll called after rejected accepted operation")
				return sdk.Operation{}, nil
			},
			cancel: func(context.Context, string) (sdk.Operation, error) {
				t.Fatal("cancel called after rejected accepted operation")
				return sdk.Operation{}, nil
			},
		},
	)
	if err == nil {
		t.Fatal("operation with mismatched idempotency key was accepted")
	}
}
