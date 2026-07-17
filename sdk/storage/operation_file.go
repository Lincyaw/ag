package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
	sdk "github.com/lincyaw/ag/sdk"
)

const operationStoreSchemaVersion uint32 = 2

type fileOperationState struct {
	SchemaVersion uint32                         `json:"schema_version"`
	Operations    map[string]sdk.OperationRecord `json:"operations"`
}

type fileOperationStore struct {
	directory string
	path      string
	lockPath  string
}

func NewFileOperationStore(directory string) (sdk.OperationStore, error) {
	absolute, err := filestate.PrepareDirectory("operation", directory)
	if err != nil {
		return nil, err
	}
	return &fileOperationStore{
		directory: absolute,
		path:      filepath.Join(absolute, "operations.json"),
		lockPath:  filepath.Join(absolute, "operations.json.lock"),
	}, nil
}

func (store *fileOperationStore) Submit(
	ctx context.Context,
	record sdk.OperationRecord,
) (sdk.OperationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	var result sdk.OperationRecord
	var created bool
	err := filestate.WithExclusiveLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, created, readErr = memory.Submit(ctx, record)
		if readErr != nil || !created {
			return readErr
		}
		return store.writeLocked(ctx, memory)
	})
	if err != nil {
		return sdk.OperationRecord{}, false, err
	}
	return result, created, nil
}

func (store *fileOperationStore) Get(
	ctx context.Context,
	id string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	var result sdk.OperationRecord
	err := filestate.WithSharedLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.Get(ctx, id)
		return readErr
	})
	return result, err
}

func (store *fileOperationStore) Transition(
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
	var result sdk.OperationRecord
	err := filestate.WithExclusiveLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.Transition(
			ctx,
			id,
			expectedRevision,
			state,
			output,
			operationError,
		)
		if readErr != nil {
			return readErr
		}
		return store.writeLocked(ctx, memory)
	})
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	return result, nil
}

func (store *fileOperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, func(memory *memoryOperationStore) (sdk.OperationRecord, error) {
		return memory.Claim(ctx, id, owner, now, ttl)
	})
}

func (store *fileOperationStore) Renew(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, func(memory *memoryOperationStore) (sdk.OperationRecord, error) {
		return memory.Renew(ctx, id, token, now, ttl)
	})
}

func (store *fileOperationStore) Complete(
	ctx context.Context,
	id string,
	token string,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, func(memory *memoryOperationStore) (sdk.OperationRecord, error) {
		return memory.Complete(
			ctx,
			id,
			token,
			state,
			output,
			operationError,
		)
	})
}

func (store *fileOperationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (sdk.OperationRecord, error) {
	return store.mutate(ctx, func(memory *memoryOperationStore) (sdk.OperationRecord, error) {
		return memory.Release(ctx, id, token)
	})
}

func (store *fileOperationStore) mutate(
	ctx context.Context,
	mutation func(*memoryOperationStore) (sdk.OperationRecord, error),
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	var result sdk.OperationRecord
	err := filestate.WithExclusiveLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = mutation(memory)
		if readErr != nil {
			return readErr
		}
		return store.writeLocked(ctx, memory)
	})
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	return result, nil
}

func (store *fileOperationStore) List(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []sdk.OperationRecord
	err := filestate.WithSharedLock(store.lockPath, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.List(ctx)
		return readErr
	})
	return result, err
}

func (store *fileOperationStore) ListPage(
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

func (store *fileOperationStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	removed := 0
	_, err := store.mutate(ctx, func(memory *memoryOperationStore) (sdk.OperationRecord, error) {
		var purgeErr error
		removed, purgeErr = memory.PurgeTerminal(ctx, before)
		return sdk.OperationRecord{}, purgeErr
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

func (store *fileOperationStore) readLocked() (*memoryOperationStore, error) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newMemoryOperationStore(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read operations: %w", err)
	}
	var state fileOperationState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode operations: %w", err)
	}
	if state.SchemaVersion > operationStoreSchemaVersion {
		return nil, fmt.Errorf(
			"operation schema version %d is newer than supported version %d",
			state.SchemaVersion,
			operationStoreSchemaVersion,
		)
	}
	memory := newMemoryOperationStore()
	for id, record := range state.Operations {
		if id != record.Operation.ID {
			return nil, fmt.Errorf("operation map key %q contains ID %q", id, record.Operation.ID)
		}
		if err := validateLoadedOperationRecord(record); err != nil {
			return nil, fmt.Errorf("validate operation %q: %w", id, err)
		}
		key := operationIdempotencyIndex(record)
		if existing, exists := memory.keys[key]; exists {
			return nil, fmt.Errorf("operations %q and %q share an idempotency key", existing, id)
		}
		memory.operations[id] = cloneOperationRecord(record)
		memory.keys[key] = id
	}
	return memory, nil
}

func (store *fileOperationStore) writeLocked(
	ctx context.Context,
	memory *memoryOperationStore,
) error {
	memory.mu.Lock()
	state := fileOperationState{
		SchemaVersion: operationStoreSchemaVersion,
		Operations:    make(map[string]sdk.OperationRecord, len(memory.operations)),
	}
	for id, record := range memory.operations {
		state.Operations[id] = cloneOperationRecord(record)
	}
	memory.mu.Unlock()
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.path,
		"operations",
		state,
	)
}
