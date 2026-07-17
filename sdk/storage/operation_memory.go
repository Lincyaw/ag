package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type memoryOperationStore struct {
	mu         sync.Mutex
	operations map[string]sdk.OperationRecord
	keys       map[string]string
}

func NewMemoryOperationStore() sdk.OperationStore {
	return newMemoryOperationStore()
}

func newMemoryOperationStore() *memoryOperationStore {
	return &memoryOperationStore{
		operations: make(map[string]sdk.OperationRecord),
		keys:       make(map[string]string),
	}
}

func (store *memoryOperationStore) Submit(
	ctx context.Context,
	record sdk.OperationRecord,
) (sdk.OperationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	if record.Operation.ID == "" {
		record.Operation.ID = sdk.NewID()
	}
	if err := validateNewOperationRecord(record); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	now := time.Now().UTC()
	if record.Operation.SubmittedAt.IsZero() {
		record.Operation.SubmittedAt = now
	}
	record.Operation.UpdatedAt = record.Operation.SubmittedAt
	record.Operation.State = sdk.OperationPending
	record.Operation.Revision = 1
	record.Operation.Output = nil
	record.Operation.Error = ""
	record = cloneOperationRecord(record)
	key := operationIdempotencyIndex(record)

	store.mu.Lock()
	defer store.mu.Unlock()
	if id, exists := store.keys[key]; exists {
		existing := store.operations[id]
		if !sameOperationSubmission(existing, record) {
			return sdk.OperationRecord{}, false, fmt.Errorf(
				"operation idempotency key %q was reused with a different submission",
				record.Operation.IdempotencyKey,
			)
		}
		return cloneOperationRecord(existing), false, nil
	}
	if _, exists := store.operations[record.Operation.ID]; exists {
		return sdk.OperationRecord{}, false, fmt.Errorf(
			"operation ID %q already exists",
			record.Operation.ID,
		)
	}
	store.operations[record.Operation.ID] = record
	store.keys[key] = record.Operation.ID
	return cloneOperationRecord(record), true, nil
}

func (store *memoryOperationStore) Get(
	ctx context.Context,
	id string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) Transition(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if record.Operation.Revision != expectedRevision {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s has revision %d, expected %d",
			sdk.ErrOperationConflict,
			id,
			record.Operation.Revision,
			expectedRevision,
		)
	}
	if err := validateOperationTransition(record.Operation.State, state); err != nil {
		return sdk.OperationRecord{}, err
	}
	next := record.Operation
	next.State = state
	next.Revision++
	next.Output = append(json.RawMessage(nil), output...)
	next.Error = operationError
	next.UpdatedAt = time.Now().UTC()
	if state != sdk.OperationRunning {
		record.Execution = nil
	}
	if err := sdk.ValidateOperation(next); err != nil {
		return sdk.OperationRecord{}, err
	}
	record.Operation = next
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	if strings.TrimSpace(owner) == "" {
		return sdk.OperationRecord{}, errors.New("operation lease owner is empty")
	}
	if ttl <= 0 {
		return sdk.OperationRecord{}, errors.New("operation lease TTL must be positive")
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
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if record.Operation.Terminal() {
		return sdk.OperationRecord{}, fmt.Errorf(
			"terminal operation %q cannot be claimed",
			id,
		)
	}
	if record.Execution != nil && record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s is owned by %s until %s",
			sdk.ErrOperationClaimed,
			id,
			record.Execution.Owner,
			record.Execution.ExpiresAt.Format(time.RFC3339Nano),
		)
	}
	record.Operation.State = sdk.OperationRunning
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	record.Execution = &sdk.OperationLease{
		Owner:     owner,
		Token:     sdk.NewID(),
		ExpiresAt: now.Add(ttl),
	}
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) Renew(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	if ttl <= 0 {
		return sdk.OperationRecord{}, errors.New("operation lease TTL must be positive")
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
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			sdk.ErrOperationFence,
			id,
		)
	}
	record.Execution.ExpiresAt = now.Add(ttl)
	record.Operation.Revision++
	record.Operation.UpdatedAt = now
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) Complete(
	ctx context.Context,
	id string,
	token string,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	if state != sdk.OperationSucceeded && state != sdk.OperationFailed {
		return sdk.OperationRecord{}, fmt.Errorf(
			"claimed operation cannot complete as %q",
			state,
		)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(time.Now().UTC()) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			sdk.ErrOperationFence,
			id,
		)
	}
	next := record.Operation
	next.State = state
	next.Revision++
	next.Output = append(json.RawMessage(nil), output...)
	next.Error = operationError
	next.UpdatedAt = time.Now().UTC()
	if err := sdk.ValidateOperation(next); err != nil {
		return sdk.OperationRecord{}, err
	}
	record.Operation = next
	record.Execution = nil
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	record, exists := store.operations[id]
	if !exists {
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale",
			sdk.ErrOperationFence,
			id,
		)
	}
	record.Execution = nil
	record.Operation.Revision++
	record.Operation.UpdatedAt = time.Now().UTC()
	store.operations[id] = cloneOperationRecord(record)
	return cloneOperationRecord(record), nil
}

func (store *memoryOperationStore) List(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]sdk.OperationRecord, 0, len(store.operations))
	for _, record := range store.operations {
		result = append(result, cloneOperationRecord(record))
	}
	slices.SortFunc(result, func(left, right sdk.OperationRecord) int {
		if order := left.Operation.SubmittedAt.Compare(right.Operation.SubmittedAt); order != 0 {
			return order
		}
		return strings.Compare(left.Operation.ID, right.Operation.ID)
	})
	return result, nil
}

func (store *memoryOperationStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.OperationPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	page, next, err := pageWindow(
		items,
		request,
		func(item sdk.OperationRecord) string { return item.Operation.ID },
	)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	return sdk.OperationPage{Items: page, Next: next}, nil
}

func (store *memoryOperationStore) PurgeTerminal(
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
