package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

// pollOperation is the narrow read boundary used while awaiting async work.
type pollOperation func(context.Context, string, uint64) (sdk.Operation, error)
type cancelOperation func(context.Context, string) (sdk.Operation, error)

func (runtime *Runtime) awaitOperation(
	ctx context.Context,
	initial sdk.Operation,
	poll pollOperation,
	cancel cancelOperation,
) (sdk.Operation, error) {
	current := initial
	if err := sdk.ValidateOperation(current); err != nil {
		return sdk.Operation{}, err
	}
	for !current.Terminal() {
		if !waitContext(ctx, runtime.operation.poll) {
			runtime.mu.Lock()
			closed := runtime.closed
			runtime.mu.Unlock()
			if closed {
				return sdk.Operation{}, ctx.Err()
			}
			cancelCtx, cancelFunc := context.WithTimeout(
				context.WithoutCancel(ctx),
				2*time.Second,
			)
			defer cancelFunc()
			_, cancelErr := cancel(cancelCtx, current.ID)
			return sdk.Operation{}, errors.Join(ctx.Err(), cancelErr)
		}
		next, err := poll(ctx, current.ID, current.Revision)
		if err != nil {
			return sdk.Operation{}, err
		}
		if err := sdk.ValidateOperation(next); err != nil {
			return sdk.Operation{}, err
		}
		if next.ID != current.ID {
			return sdk.Operation{}, fmt.Errorf(
				"operation poll returned ID %q, expected %q",
				next.ID,
				current.ID,
			)
		}
		if next.IdempotencyKey != current.IdempotencyKey {
			return sdk.Operation{}, fmt.Errorf(
				"operation %q idempotency key changed during poll",
				current.ID,
			)
		}
		if next.Revision < current.Revision {
			return sdk.Operation{}, fmt.Errorf(
				"operation %q revision regressed from %d to %d",
				current.ID,
				current.Revision,
				next.Revision,
			)
		}
		if next.Revision == current.Revision && next.State != current.State {
			return sdk.Operation{}, fmt.Errorf(
				"operation %q changed state without a revision increment",
				current.ID,
			)
		}
		if current.State == sdk.OperationRunning && next.State == sdk.OperationPending {
			return sdk.Operation{}, fmt.Errorf(
				"operation %q regressed from running to pending",
				current.ID,
			)
		}
		current = next
	}
	switch current.State {
	case sdk.OperationSucceeded:
		return current, nil
	case sdk.OperationFailed:
		return sdk.Operation{}, fmt.Errorf(
			"operation %q failed: %s",
			current.ID,
			current.Error,
		)
	case sdk.OperationCancelled:
		return sdk.Operation{}, fmt.Errorf("operation %q was cancelled", current.ID)
	default:
		panic("unreachable operation state")
	}
}
