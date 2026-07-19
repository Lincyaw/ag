package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrOperationNotFound = errors.New("operation not found")
	ErrOperationConflict = errors.New("operation revision conflict")
	ErrOperationClaimed  = errors.New("operation is claimed by another worker")
	ErrOperationFence    = errors.New("operation execution lease is no longer valid")
)

type OperationKind string

const (
	OperationKindProvider   OperationKind = "provider"
	OperationKindTool       OperationKind = "tool"
	OperationKindAgent      OperationKind = "agent"
	OperationKindWorkflow   OperationKind = "workflow"
	OperationKindCapability OperationKind = "capability"
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
	Invocation     Invocation      `json:"invocation,omitempty"`
}

func CloneOperationRequest(request OperationRequest) OperationRequest {
	request.Input = append(json.RawMessage(nil), request.Input...)
	request.Invocation = CloneInvocation(request.Invocation)
	return request
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

func CloneOperation(operation Operation) Operation {
	operation.Output = append(json.RawMessage(nil), operation.Output...)
	return operation
}

// ValidateOperationTransition validates one aggregate state transition.
func ValidateOperationTransition(current, next OperationState) error {
	switch current {
	case OperationPending:
		switch next {
		case OperationRunning,
			OperationSucceeded,
			OperationFailed,
			OperationCancelled:
			return nil
		}
	case OperationRunning:
		switch next {
		case OperationRunning,
			OperationSucceeded,
			OperationFailed,
			OperationCancelled:
			return nil
		}
	case OperationSucceeded,
		OperationFailed,
		OperationCancelled:
		return fmt.Errorf(
			"terminal operation in state %q cannot transition",
			current,
		)
	}
	return fmt.Errorf("invalid operation transition %q -> %q", current, next)
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

type OperationRecord struct {
	Operation        Operation       `json:"operation"`
	Kind             OperationKind   `json:"kind"`
	Resource         string          `json:"resource"`
	ResourceRevision string          `json:"resource_revision,omitempty"`
	Input            json.RawMessage `json:"input"`
	Invocation       Invocation      `json:"invocation,omitempty"`
	Execution        *OperationLease `json:"execution,omitempty"`
}

func CloneOperationRecord(record OperationRecord) OperationRecord {
	record.Operation = CloneOperation(record.Operation)
	record.Input = append(json.RawMessage(nil), record.Input...)
	record.Invocation = CloneInvocation(record.Invocation)
	if record.Execution != nil {
		execution := *record.Execution
		record.Execution = &execution
	}
	return record
}

type OperationLease struct {
	Owner     string    `json:"owner"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type OperationPage struct {
	Items []OperationRecord `json:"items"`
	Next  string            `json:"next,omitempty"`
}

// OperationStore persists the aggregate state and worker lease used by every
// provider, tool, agent, workflow, and capability invocation.
type OperationStore interface {
	Submit(context.Context, OperationRecord) (OperationRecord, bool, error)
	Get(context.Context, string) (OperationRecord, error)
	// Cancel requests cancellation without owning the execution lease.
	Cancel(context.Context, string, uint64) (OperationRecord, error)
	// Fail records a non-lease failure, such as a stale resource revision.
	Fail(context.Context, string, uint64, string) (OperationRecord, error)
	Claim(
		context.Context,
		string,
		string,
		time.Time,
		time.Duration,
	) (OperationRecord, error)
	Renew(
		context.Context,
		string,
		string,
		time.Time,
		time.Duration,
	) (OperationRecord, error)
	Complete(
		context.Context,
		string,
		string,
		OperationState,
		json.RawMessage,
		string,
	) (OperationRecord, error)
	Release(context.Context, string, string) (OperationRecord, error)
	List(context.Context) ([]OperationRecord, error)
	ListByInvocationRoot(context.Context, string) ([]OperationRecord, error)
	ListNonTerminal(context.Context) ([]OperationRecord, error)
	ListRecoverable(context.Context, time.Time) ([]OperationRecord, error)
	ListPage(context.Context, PageRequest) (OperationPage, error)
	PurgeTerminal(context.Context, time.Time) (int, error)
}
