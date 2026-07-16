package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type OperationState string

const (
	OperationPending   OperationState = "pending"
	OperationRunning   OperationState = "running"
	OperationSucceeded OperationState = "succeeded"
	OperationFailed    OperationState = "failed"
	OperationCancelled OperationState = "cancelled"
)

type OperationRequest struct {
	IdempotencyKey string          `json:"idempotency_key"`
	Input          json.RawMessage `json:"input"`
}

type Operation struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	State          OperationState  `json:"state"`
	Revision       uint64          `json:"revision"`
	Output         json.RawMessage `json:"output,omitempty"`
	Error          string          `json:"error,omitempty"`
	SubmittedAt    time.Time       `json:"submitted_at,omitempty"`
	UpdatedAt      time.Time       `json:"updated_at,omitempty"`
}

func (operation Operation) Terminal() bool {
	switch operation.State {
	case OperationSucceeded, OperationFailed, OperationCancelled:
		return true
	default:
		return false
	}
}

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

func validateOperation(operation Operation) error {
	if operation.ID == "" {
		return errors.New("operation ID is empty")
	}
	if operation.IdempotencyKey == "" {
		return fmt.Errorf("operation %q idempotency key is empty", operation.ID)
	}
	if operation.Revision == 0 {
		return fmt.Errorf("operation %q revision must be positive", operation.ID)
	}
	switch operation.State {
	case OperationPending, OperationRunning:
		if len(operation.Output) != 0 || operation.Error != "" {
			return fmt.Errorf("unfinished operation %q contains a result", operation.ID)
		}
	case OperationSucceeded:
		if !json.Valid(operation.Output) {
			return fmt.Errorf("operation %q output is invalid JSON", operation.ID)
		}
		if operation.Error != "" {
			return fmt.Errorf("succeeded operation %q contains an error", operation.ID)
		}
	case OperationFailed:
		if operation.Error == "" {
			return fmt.Errorf("failed operation %q has no error", operation.ID)
		}
	case OperationCancelled:
	default:
		return fmt.Errorf("operation %q has invalid state %q", operation.ID, operation.State)
	}
	return nil
}
