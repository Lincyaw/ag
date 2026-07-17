package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type pollOperation func(context.Context, string, uint64) (Operation, error)
type cancelOperation func(context.Context, string) (Operation, error)

func (runtime *Runtime) awaitOperation(
	ctx context.Context,
	initial Operation,
	poll pollOperation,
	cancel cancelOperation,
) (Operation, error) {
	current := initial
	if err := validateOperation(current); err != nil {
		return Operation{}, err
	}
	for !current.Terminal() {
		if !waitContext(ctx, runtime.operationPoll) {
			cancelCtx, cancelFunc := context.WithTimeout(
				context.WithoutCancel(ctx),
				2*time.Second,
			)
			defer cancelFunc()
			_, cancelErr := cancel(cancelCtx, current.ID)
			return Operation{}, errors.Join(ctx.Err(), cancelErr)
		}
		next, err := poll(ctx, current.ID, current.Revision)
		if err != nil {
			return Operation{}, err
		}
		if err := validateOperation(next); err != nil {
			return Operation{}, err
		}
		if next.ID != current.ID {
			return Operation{}, fmt.Errorf(
				"operation poll returned ID %q, expected %q",
				next.ID,
				current.ID,
			)
		}
		if next.IdempotencyKey != current.IdempotencyKey {
			return Operation{}, fmt.Errorf(
				"operation %q idempotency key changed during poll",
				current.ID,
			)
		}
		if next.Revision < current.Revision {
			return Operation{}, fmt.Errorf(
				"operation %q revision regressed from %d to %d",
				current.ID,
				current.Revision,
				next.Revision,
			)
		}
		if next.Revision == current.Revision && next.State != current.State {
			return Operation{}, fmt.Errorf(
				"operation %q changed state without a revision increment",
				current.ID,
			)
		}
		if current.State == OperationRunning && next.State == OperationPending {
			return Operation{}, fmt.Errorf(
				"operation %q regressed from running to pending",
				current.ID,
			)
		}
		current = next
	}
	switch current.State {
	case OperationSucceeded:
		return current, nil
	case OperationFailed:
		return Operation{}, fmt.Errorf(
			"operation %q failed: %s",
			current.ID,
			current.Error,
		)
	case OperationCancelled:
		return Operation{}, fmt.Errorf("operation %q was cancelled", current.ID)
	default:
		panic("unreachable operation state")
	}
}
