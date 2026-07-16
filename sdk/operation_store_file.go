package sdk

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
)

var operationDirectoryLocks sync.Map

type fileOperationState struct {
	Operations map[string]OperationRecord `json:"operations"`
}

type FileOperationStore struct {
	directory string
	path      string
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
	memory, err := store.readLocked()
	if err != nil {
		return OperationRecord{}, false, err
	}
	result, created, err := memory.Submit(ctx, record)
	if err != nil || !created {
		return result, created, err
	}
	if err := store.writeLocked(ctx, memory); err != nil {
		return OperationRecord{}, false, err
	}
	return result, true, nil
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
	memory, err := store.readLocked()
	if err != nil {
		return OperationRecord{}, err
	}
	return memory.Get(ctx, id)
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
	memory, err := store.readLocked()
	if err != nil {
		return OperationRecord{}, err
	}
	result, err := memory.Transition(
		ctx,
		id,
		expectedRevision,
		state,
		output,
		operationError,
	)
	if err != nil {
		return OperationRecord{}, err
	}
	if err := store.writeLocked(ctx, memory); err != nil {
		return OperationRecord{}, err
	}
	return result, nil
}

func (store *FileOperationStore) List(
	ctx context.Context,
) ([]OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	memory, err := store.readLocked()
	if err != nil {
		return nil, err
	}
	return memory.List(ctx)
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
		Operations: make(map[string]OperationRecord, len(memory.operations)),
	}
	for id, record := range memory.operations {
		state.Operations[id] = cloneOperationRecord(record)
	}
	memory.mu.Unlock()
	return writeJSONAtomic(
		ctx,
		store.directory,
		store.path,
		".operations-*.tmp",
		"operations",
		state,
	)
}
