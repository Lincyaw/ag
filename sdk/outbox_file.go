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
	"time"
)

var outboxDirectoryLocks sync.Map

type fileOutboxState struct {
	NextSequence uint64              `json:"next_sequence"`
	Deliveries   map[string]Delivery `json:"deliveries"`
}

type FileOutboxStore struct {
	directory string
	path      string
	mu        *sync.Mutex
}

func NewFileOutboxStore(directory string) (*FileOutboxStore, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(directory))
	if err != nil {
		return nil, fmt.Errorf("resolve outbox directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create outbox directory: %w", err)
	}
	value, _ := outboxDirectoryLocks.LoadOrStore(absolute, &sync.Mutex{})
	return &FileOutboxStore{
		directory: absolute,
		path:      filepath.Join(absolute, "outbox.json"),
		mu:        value.(*sync.Mutex),
	}, nil
}

func (store *FileOutboxStore) Directory() string {
	if store == nil {
		return ""
	}
	return store.directory
}

func (store *FileOutboxStore) Enqueue(
	ctx context.Context,
	deliveries ...Delivery,
) error {
	return store.mutate(ctx, func(memory *MemoryOutboxStore) error {
		return memory.Enqueue(ctx, deliveries...)
	})
}

func (store *FileOutboxStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (Delivery, error) {
	var delivery Delivery
	err := store.mutate(ctx, func(memory *MemoryOutboxStore) error {
		var leaseErr error
		delivery, leaseErr = memory.Lease(ctx, now, duration)
		return leaseErr
	})
	return delivery, err
}

func (store *FileOutboxStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.mutate(ctx, func(memory *MemoryOutboxStore) error {
		return memory.Ack(ctx, id, token, now)
	})
}

func (store *FileOutboxStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *MemoryOutboxStore) error {
		return memory.Retry(ctx, id, token, availableAt, lastError)
	})
}

func (store *FileOutboxStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *MemoryOutboxStore) error {
		return memory.DeadLetter(ctx, id, token, now, lastError)
	})
}

func (store *FileOutboxStore) List(
	ctx context.Context,
) ([]Delivery, error) {
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

func (store *FileOutboxStore) mutate(
	ctx context.Context,
	mutation func(*MemoryOutboxStore) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	memory, err := store.readLocked()
	if err != nil {
		return err
	}
	if err := mutation(memory); err != nil {
		return err
	}
	return store.writeLocked(ctx, memory)
}

func (store *FileOutboxStore) readLocked() (*MemoryOutboxStore, error) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewMemoryOutboxStore(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read outbox: %w", err)
	}
	var state fileOutboxState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode outbox: %w", err)
	}
	if state.Deliveries == nil {
		state.Deliveries = make(map[string]Delivery)
	}
	memory := &MemoryOutboxStore{
		deliveries:   make(map[string]Delivery, len(state.Deliveries)),
		nextSequence: state.NextSequence,
	}
	for id, delivery := range state.Deliveries {
		memory.deliveries[id] = cloneDelivery(delivery)
	}
	return memory, nil
}

func (store *FileOutboxStore) writeLocked(
	ctx context.Context,
	memory *MemoryOutboxStore,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memory.mu.Lock()
	state := fileOutboxState{
		NextSequence: memory.nextSequence,
		Deliveries:   make(map[string]Delivery, len(memory.deliveries)),
	}
	for id, delivery := range memory.deliveries {
		state.Deliveries[id] = cloneDelivery(delivery)
	}
	memory.mu.Unlock()
	return writeJSONAtomic(
		ctx,
		store.directory,
		store.path,
		".outbox-*.tmp",
		"outbox",
		state,
	)
}
