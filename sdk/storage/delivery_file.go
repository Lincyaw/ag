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

var deliveryFileLocks sync.Map

const deliveryStoreSchemaVersion uint32 = 2

type fileDeliveryState struct {
	SchemaVersion uint32              `json:"schema_version"`
	NextSequence  uint64              `json:"next_sequence"`
	Deliveries    map[string]Delivery `json:"deliveries"`
}

type FileDeliveryStore struct {
	directory string
	path      string
	lockPath  string
	mu        *sync.Mutex
}

// FileOutboxStore is kept as a source-compatible alias.
type FileOutboxStore = FileDeliveryStore

func NewFileDeliveryStore(directory string) (*FileDeliveryStore, error) {
	return newFileDeliveryStore(directory, "deliveries.json")
}

func NewFileOutboxStore(directory string) (*FileDeliveryStore, error) {
	return newFileDeliveryStore(directory, "outbox.json")
}

func newFileDeliveryStore(
	directory string,
	filename string,
) (*FileDeliveryStore, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(directory))
	if err != nil {
		return nil, fmt.Errorf("resolve delivery directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create delivery directory: %w", err)
	}
	lockKey := filepath.Join(absolute, filename)
	value, _ := deliveryFileLocks.LoadOrStore(lockKey, &sync.Mutex{})
	return &FileDeliveryStore{
		directory: absolute,
		path:      filepath.Join(absolute, filename),
		lockPath:  filepath.Join(absolute, filename+".lock"),
		mu:        value.(*sync.Mutex),
	}, nil
}

func (store *FileDeliveryStore) Directory() string {
	if store == nil {
		return ""
	}
	return store.directory
}

func (store *FileDeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...Delivery,
) error {
	return store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		return memory.Enqueue(ctx, deliveries...)
	})
}

func (store *FileDeliveryStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (Delivery, error) {
	var delivery Delivery
	err := store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		var leaseErr error
		delivery, leaseErr = memory.Lease(ctx, now, duration)
		return leaseErr
	})
	return delivery, err
}

func (store *FileDeliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		return memory.Ack(ctx, id, token, now)
	})
}

func (store *FileDeliveryStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		return memory.Retry(ctx, id, token, availableAt, lastError)
	})
}

func (store *FileDeliveryStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		return memory.DeadLetter(ctx, id, token, now, lastError)
	})
}

func (store *FileDeliveryStore) List(
	ctx context.Context,
) ([]Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var result []Delivery
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

func (store *FileDeliveryStore) ListPage(
	ctx context.Context,
	request PageRequest,
) (DeliveryPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return DeliveryPage{}, err
	}
	page, next, err := PageWindow(items, request, func(item Delivery) string {
		return item.ID
	})
	if err != nil {
		return DeliveryPage{}, err
	}
	return DeliveryPage{Items: page, Next: next}, nil
}

func (store *FileDeliveryStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	removed := 0
	err := store.mutate(ctx, func(memory *MemoryDeliveryStore) error {
		var purgeErr error
		removed, purgeErr = memory.PurgeTerminal(ctx, before)
		return purgeErr
	})
	return removed, err
}

func (store *FileDeliveryStore) mutate(
	ctx context.Context,
	mutation func(*MemoryDeliveryStore) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return WithFileLock(store.lockPath, true, func() error {
		memory, err := store.readLocked()
		if err != nil {
			return err
		}
		if err := mutation(memory); err != nil {
			return err
		}
		return store.writeLocked(ctx, memory)
	})
}

func (store *FileDeliveryStore) readLocked() (*MemoryDeliveryStore, error) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewMemoryDeliveryStore(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read deliveries: %w", err)
	}
	var state fileDeliveryState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode deliveries: %w", err)
	}
	if state.SchemaVersion > deliveryStoreSchemaVersion {
		return nil, fmt.Errorf(
			"delivery schema version %d is newer than supported version %d",
			state.SchemaVersion,
			deliveryStoreSchemaVersion,
		)
	}
	if state.Deliveries == nil {
		state.Deliveries = make(map[string]Delivery)
	}
	memory := &MemoryDeliveryStore{
		deliveries:   make(map[string]Delivery, len(state.Deliveries)),
		nextSequence: state.NextSequence,
	}
	for id, delivery := range state.Deliveries {
		if id != delivery.ID {
			return nil, fmt.Errorf(
				"delivery map key %q contains ID %q",
				id,
				delivery.ID,
			)
		}
		memory.deliveries[id] = cloneDelivery(delivery)
	}
	return memory, nil
}

func (store *FileDeliveryStore) writeLocked(
	ctx context.Context,
	memory *MemoryDeliveryStore,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memory.mu.Lock()
	state := fileDeliveryState{
		SchemaVersion: deliveryStoreSchemaVersion,
		NextSequence:  memory.nextSequence,
		Deliveries:    make(map[string]Delivery, len(memory.deliveries)),
	}
	for id, delivery := range memory.deliveries {
		state.Deliveries[id] = cloneDelivery(delivery)
	}
	memory.mu.Unlock()
	return WriteJSONAtomic(
		ctx,
		store.directory,
		store.path,
		".deliveries-*.tmp",
		"deliveries",
		state,
	)
}
