package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/lincyaw/ag/sdk"
)

var operationDirectoryLocks sync.Map

const operationStoreSchemaVersion uint32 = 2

type fileOperationState struct {
	SchemaVersion uint32                     `json:"schema_version"`
	Operations    map[string]OperationRecord `json:"operations"`
}

type FileOperationStore struct {
	directory string
	path      string
	lockPath  string
	mu        *sync.Mutex
}

func NewFileOperationStore(directory string) (*FileOperationStore, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(directory))
	if err != nil {
		return nil, fmt.Errorf("resolve operation directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create operation directory: %w", err)
	}
	value, _ := operationDirectoryLocks.LoadOrStore(absolute, &sync.Mutex{})
	return &FileOperationStore{
		directory: absolute,
		path:      filepath.Join(absolute, "operations.json"),
		lockPath:  filepath.Join(absolute, "operations.json.lock"),
		mu:        value.(*sync.Mutex),
	}, nil
}

func (store *FileOperationStore) Directory() string {
	if store == nil {
		return ""
	}
	return store.directory
}

func (store *FileOperationStore) Submit(
	ctx context.Context,
	record OperationRecord,
) (OperationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result OperationRecord
	var created bool
	err := WithFileLock(store.lockPath, true, func() error {
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
	return result, created, err
}

func (store *FileOperationStore) Get(
	ctx context.Context,
	id string,
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result OperationRecord
	err := WithFileLock(store.lockPath, false, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.Get(ctx, id)
		return readErr
	})
	return result, err
}

func (store *FileOperationStore) Transition(
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
	var result OperationRecord
	err := WithFileLock(store.lockPath, true, func() error {
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
	return result, err
}

func (store *FileOperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (OperationRecord, error) {
	return store.mutate(ctx, func(memory *MemoryOperationStore) (OperationRecord, error) {
		return memory.Claim(ctx, id, owner, now, ttl)
	})
}

func (store *FileOperationStore) Renew(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	ttl time.Duration,
) (OperationRecord, error) {
	return store.mutate(ctx, func(memory *MemoryOperationStore) (OperationRecord, error) {
		return memory.Renew(ctx, id, token, now, ttl)
	})
}

func (store *FileOperationStore) Complete(
	ctx context.Context,
	id string,
	token string,
	state OperationState,
	output json.RawMessage,
	operationError string,
) (OperationRecord, error) {
	return store.mutate(ctx, func(memory *MemoryOperationStore) (OperationRecord, error) {
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

func (store *FileOperationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (OperationRecord, error) {
	return store.mutate(ctx, func(memory *MemoryOperationStore) (OperationRecord, error) {
		return memory.Release(ctx, id, token)
	})
}

func (store *FileOperationStore) mutate(
	ctx context.Context,
	mutation func(*MemoryOperationStore) (OperationRecord, error),
) (OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return OperationRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result OperationRecord
	err := WithFileLock(store.lockPath, true, func() error {
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
	return result, err
}

func (store *FileOperationStore) List(
	ctx context.Context,
) ([]OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result []OperationRecord
	err := WithFileLock(store.lockPath, false, func() error {
		memory, readErr := store.readLocked()
		if readErr != nil {
			return readErr
		}
		result, readErr = memory.List(ctx)
		return readErr
	})
	return result, err
}

func (store *FileOperationStore) ListPage(
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

func (store *FileOperationStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	removed := 0
	_, err := store.mutate(ctx, func(memory *MemoryOperationStore) (OperationRecord, error) {
		var purgeErr error
		removed, purgeErr = memory.PurgeTerminal(ctx, before)
		return OperationRecord{}, purgeErr
	})
	return removed, err
}

func (store *FileOperationStore) readLocked() (*MemoryOperationStore, error) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewMemoryOperationStore(), nil
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
	memory := NewMemoryOperationStore()
	for id, record := range state.Operations {
		if id != record.Operation.ID {
			return nil, fmt.Errorf("operation map key %q contains ID %q", id, record.Operation.ID)
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

func (store *FileOperationStore) writeLocked(
	ctx context.Context,
	memory *MemoryOperationStore,
) error {
	memory.mu.Lock()
	state := fileOperationState{
		SchemaVersion: operationStoreSchemaVersion,
		Operations:    make(map[string]OperationRecord, len(memory.operations)),
	}
	for id, record := range memory.operations {
		state.Operations[id] = cloneOperationRecord(record)
	}
	memory.mu.Unlock()
	return WriteJSONAtomic(
		ctx,
		store.directory,
		store.path,
		".operations-*.tmp",
		"operations",
		state,
	)
}
