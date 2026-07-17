package storage

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

	. "github.com/lincyaw/ag/sdk"
)

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
		record.Operation.ID = NewID()
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
	if state != OperationRunning {
		record.Execution = nil
	}
	if err := ValidateOperation(next); err != nil {
		return OperationRecord{}, err
	}
	record.Operation = next
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	if strings.TrimSpace(owner) == "" {
		return OperationRecord{}, errors.New("operation lease owner is empty")
	}
	if ttl <= 0 {
		return OperationRecord{}, errors.New("operation lease TTL must be positive")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return OperationRecord{}, fmt.Errorf("%w: %s", ErrOperationNotFound, id)
	}
	if record.Operation.Terminal() {
		return OperationRecord{}, fmt.Errorf(
			"terminal operation %q cannot be claimed",
			id,
		)
	}
	if record.Execution != nil && record.Execution.ExpiresAt.After(now) {
		return OperationRecord{}, fmt.Errorf(
			"%w: operation %s is owned by %s until %s",
			ErrOperationClaimed,
			id,
			record.Execution.Owner,
			record.Execution.ExpiresAt.Format(time.RFC3339Nano),
		)
	}
	record.Operation.State = OperationRunning
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	record.Execution = &OperationLease{
		Owner:     owner,
		Token:     NewID(),
		ExpiresAt: now.Add(ttl),
	}
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) Renew(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	ttl time.Duration,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	if ttl <= 0 {
		return OperationRecord{}, errors.New("operation lease TTL must be positive")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return OperationRecord{}, fmt.Errorf("%w: %s", ErrOperationNotFound, id)
	}
	if record.Operation.State != OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(now) {
		return OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			ErrOperationFence,
			id,
		)
	}
	record.Execution.ExpiresAt = now.Add(ttl)
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) Complete(
	ctx context.Context,
	id string,
	token string,
	state OperationState,
	output json.RawMessage,
	operationError string,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	if state != OperationSucceeded && state != OperationFailed {
		return OperationRecord{}, fmt.Errorf(
			"claimed operation cannot complete as %q",
			state,
		)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return OperationRecord{}, fmt.Errorf("%w: %s", ErrOperationNotFound, id)
	}
	if record.Operation.State != OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(time.Now().UTC()) {
		return OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			ErrOperationFence,
			id,
		)
	}
	next := record.Operation
	next.State = state
	next.Revision++
	next.Output = append(json.RawMessage(nil), output...)
	next.Error = operationError
	next.UpdatedAt = time.Now().UTC()
	if err := ValidateOperation(next); err != nil {
		return OperationRecord{}, err
	}
	record.Operation = next
	record.Execution = nil
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *MemoryOperationStore) Release(
	ctx context.Context,
	id string,
	token string,
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
	if record.Operation.State != OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token {
		return OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale",
			ErrOperationFence,
			id,
		)
	}
	record.Execution = nil
	record.Operation.Revision++
	record.Operation.UpdatedAt = time.Now().UTC()
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

func (store *MemoryOperationStore) ListPage(
	ctx context.Context,
	request PageRequest,
) (OperationPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return OperationPage{}, err
	}
	page, next, err := PageWindow(
		items,
		request,
		func(item OperationRecord) string { return item.Operation.ID },
	)
	if err != nil {
		return OperationPage{}, err
	}
	return OperationPage{Items: page, Next: next}, nil
}

func (store *MemoryOperationStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("operation purge cutoff is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := 0
	for id, record := range store.operations {
		if record.Operation.Terminal() &&
			record.Operation.UpdatedAt.Before(before) {
			delete(store.operations, id)
			delete(store.keys, operationIdempotencyIndex(record))
			removed++
		}
	}
	return removed, nil
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
	if err := ValidateResourceName(string(record.Kind), record.Resource); err != nil {
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
		record.ResourceRevision + "\x00" +
		record.Operation.IdempotencyKey
}

func cloneOperationRecord(record OperationRecord) OperationRecord {
	record.Input = append(json.RawMessage(nil), record.Input...)
	record.Operation.Output = append(json.RawMessage(nil), record.Operation.Output...)
	if record.Execution != nil {
		execution := *record.Execution
		record.Execution = &execution
	}
	return record
}
