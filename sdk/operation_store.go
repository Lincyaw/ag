package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrOperationNotFound = errors.New("operation not found")
	ErrOperationConflict = errors.New("operation revision conflict")
)

type OperationKind string

const (
	OperationKindProvider   OperationKind = "provider"
	OperationKindTool       OperationKind = "tool"
	OperationKindCapability OperationKind = "capability"
	OperationKindRun        OperationKind = "run"
)

type OperationRecord struct {
	Operation Operation       `json:"operation"`
	Kind      OperationKind   `json:"kind"`
	Resource  string          `json:"resource"`
	Input     json.RawMessage `json:"input"`
}

type OperationStore interface {
	Submit(context.Context, OperationRecord) (OperationRecord, bool, error)
	Get(context.Context, string) (OperationRecord, error)
	Transition(
		context.Context,
		string,
		uint64,
		OperationState,
		json.RawMessage,
		string,
	) (OperationRecord, error)
	List(context.Context) ([]OperationRecord, error)
}

type MemoryOperationStore struct {
	mu         sync.Mutex
	operations map[string]OperationRecord
	keys       map[string]string
}

func NewMemoryOperationStore() *MemoryOperationStore {
	return &MemoryOperationStore{
		operations: make(map[string]OperationRecord),
		keys:       make(map[string]string),
	}
}

func (store *MemoryOperationStore) Submit(
	ctx context.Context,
	record OperationRecord,
) (OperationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, false, err
	}
	if record.Operation.ID == "" {
		record.Operation.ID = newDispatchID()
	}
	if err := validateNewOperationRecord(record); err != nil {
		return OperationRecord{}, false, err
	}
	now := time.Now().UTC()
	if record.Operation.SubmittedAt.IsZero() {
		record.Operation.SubmittedAt = now
	}
	record.Operation.UpdatedAt = record.Operation.SubmittedAt
	record.Operation.State = OperationPending
	record.Operation.Revision = 1
	record.Operation.Output = nil
	record.Operation.Error = ""
	record = cloneOperationRecord(record)
	key := operationIdempotencyIndex(record)

	store.mu.Lock()
	defer store.mu.Unlock()
	if id, exists := store.keys[key]; exists {
		existing := store.operations[id]
		if !bytes.Equal(existing.Input, record.Input) {
			return OperationRecord{}, false, fmt.Errorf(
				"operation idempotency key %q was reused with different input",
				record.Operation.IdempotencyKey,
			)
		}
		return cloneOperationRecord(existing), false, nil
	}
	if _, exists := store.operations[record.Operation.ID]; exists {
		return OperationRecord{}, false, fmt.Errorf(
			"operation ID %q already exists",
			record.Operation.ID,
		)
	}
	store.operations[record.Operation.ID] = record
	store.keys[key] = record.Operation.ID
	return cloneOperationRecord(record), true, nil
}

func (store *MemoryOperationStore) Get(
	ctx context.Context,
	id string,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return OperationRecord{}, fmt.Errorf("%w: %s", ErrOperationNotFound, id)
	}
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) Transition(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	state OperationState,
	output json.RawMessage,
	operationError string,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return OperationRecord{}, fmt.Errorf("%w: %s", ErrOperationNotFound, id)
	}
	if record.Operation.Revision != expectedRevision {
		return OperationRecord{}, fmt.Errorf(
			"%w: operation %s has revision %d, expected %d",
			ErrOperationConflict,
			id,
			record.Operation.Revision,
			expectedRevision,
		)
	}
	if err := validateOperationTransition(record.Operation.State, state); err != nil {
		return OperationRecord{}, err
	}
	next := record.Operation
	next.State = state
	next.Revision++
	next.Output = append(json.RawMessage(nil), output...)
	next.Error = operationError
	next.UpdatedAt = time.Now().UTC()
	if err := validateOperation(next); err != nil {
		return OperationRecord{}, err
	}
	record.Operation = next
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) List(
	ctx context.Context,
) ([]OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]OperationRecord, 0, len(store.operations))
	for _, record := range store.operations {
		result = append(result, cloneOperationRecord(record))
	}
	slices.SortFunc(result, func(left, right OperationRecord) int {
		if order := left.Operation.SubmittedAt.Compare(right.Operation.SubmittedAt); order != 0 {
			return order
		}
		return strings.Compare(left.Operation.ID, right.Operation.ID)
	})
	return result, nil
}

func validateNewOperationRecord(record OperationRecord) error {
	if record.Operation.ID == "" {
		return errors.New("operation ID is empty")
	}
	if record.Operation.IdempotencyKey == "" {
		return errors.New("operation idempotency key is empty")
	}
	switch record.Kind {
	case OperationKindProvider, OperationKindTool,
		OperationKindCapability, OperationKindRun:
	default:
		return fmt.Errorf("invalid operation kind %q", record.Kind)
	}
	if err := validateResourceName(string(record.Kind), record.Resource); err != nil {
		return err
	}
	if !json.Valid(record.Input) {
		return errors.New("operation input is invalid JSON")
	}
	return nil
}

func validateOperationTransition(current, next OperationState) error {
	switch current {
	case OperationPending:
		switch next {
		case OperationRunning, OperationFailed, OperationCancelled:
			return nil
		}
	case OperationRunning:
		switch next {
		case OperationRunning, OperationSucceeded, OperationFailed, OperationCancelled:
			return nil
		}
	case OperationSucceeded, OperationFailed, OperationCancelled:
		return fmt.Errorf("terminal operation in state %q cannot transition", current)
	}
	return fmt.Errorf("invalid operation transition %q -> %q", current, next)
}

func operationIdempotencyIndex(record OperationRecord) string {
	return string(record.Kind) + "\x00" + record.Resource + "\x00" +
		record.Operation.IdempotencyKey
}

func cloneOperationRecord(record OperationRecord) OperationRecord {
	record.Input = append(json.RawMessage(nil), record.Input...)
	record.Operation.Output = append(json.RawMessage(nil), record.Operation.Output...)
	return record
}
