package sdk

import (
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

func ValidateOperation(operation Operation) error {
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
