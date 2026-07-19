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

type operationRecordMutation func(sdk.OperationRecord) (sdk.OperationRecord, error)

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
	record, err := prepareNewOperationRecord(record, time.Now().UTC())
	if err != nil {
		return sdk.OperationRecord{}, false, err
	}
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

func (store *memoryOperationStore) Cancel(
	ctx context.Context,
	id string,
	expectedRevision uint64,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return cancelOperation(record, expectedRevision, time.Now().UTC())
	})
}

func (store *memoryOperationStore) Fail(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	operationError string,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return failOperation(
			record,
			expectedRevision,
			operationError,
			time.Now().UTC(),
		)
	})
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
	if err := validateOperationClaim(owner, ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = normalizeOperationMutationTime(now)
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return claimOperation(record, owner, now, ttl)
	})
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
	if err := validateOperationLeaseDuration(ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = normalizeOperationMutationTime(now)
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return renewOperation(record, token, now, ttl)
	})
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
	if err := validateOperationCompletion(state); err != nil {
		return sdk.OperationRecord{}, err
	}
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return completeOperation(
			record,
			token,
			state,
			output,
			operationError,
			time.Now().UTC(),
		)
	})
}

func (store *memoryOperationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return releaseOperation(record, token, time.Now().UTC())
	})
}

func (store *memoryOperationStore) mutate(
	ctx context.Context,
	id string,
	mutation operationRecordMutation,
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
	record, err := mutation(record)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
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

func (store *memoryOperationStore) ListByInvocationRoot(
	ctx context.Context,
	rootID string,
) ([]sdk.OperationRecord, error) {
	if err := sdk.ValidateResourceName("invocation root", rootID); err != nil {
		return nil, err
	}
	records, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]sdk.OperationRecord, 0)
	for _, record := range records {
		if record.Invocation.RootID == rootID {
			result = append(result, record)
		}
	}
	return result, nil
}

func (store *memoryOperationStore) ListNonTerminal(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]sdk.OperationRecord, 0)
	for _, record := range store.operations {
		if !record.Operation.Terminal() {
			result = append(result, cloneOperationRecord(record))
		}
	}
	sortOperationRecords(result)
	return result, nil
}

func (store *memoryOperationStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now = normalizeOperationMutationTime(now)
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]sdk.OperationRecord, 0)
	for _, record := range store.operations {
		if operationRecoverableAt(record, now) {
			result = append(result, cloneOperationRecord(record))
		}
	}
	sortOperationRecords(result)
	return result, nil
}

func sortOperationRecords(records []sdk.OperationRecord) {
	slices.SortFunc(records, func(left, right sdk.OperationRecord) int {
		if order := left.Operation.SubmittedAt.Compare(
			right.Operation.SubmittedAt,
		); order != 0 {
			return order
		}
		return strings.Compare(left.Operation.ID, right.Operation.ID)
	})
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
