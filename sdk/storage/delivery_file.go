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

const deliveryStoreSchemaVersion uint32 = 2

type fileDeliveryState struct {
	SchemaVersion uint32                  `json:"schema_version"`
	NextSequence  uint64                  `json:"next_sequence"`
	Deliveries    map[string]sdk.Delivery `json:"deliveries"`
}

type fileDeliveryStore struct {
	directory string
	path      string
	lockPath  string
}

func NewFileDeliveryStore(directory string) (sdk.DeliveryStore, error) {
	return newFileDeliveryStore(directory, "deliveries.json")
}

func newFileDeliveryStore(
	directory string,
	filename string,
) (*fileDeliveryStore, error) {
	absolute, err := filestate.PrepareDirectory("delivery", directory)
	if err != nil {
		return nil, err
	}
	return &fileDeliveryStore{
		directory: absolute,
		path:      filepath.Join(absolute, filename),
		lockPath:  filepath.Join(absolute, filename+".lock"),
	}, nil
}

func (store *fileDeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...sdk.Delivery,
) error {
	return store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		return memory.Enqueue(ctx, deliveries...)
	})
}

func (store *fileDeliveryStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (sdk.Delivery, error) {
	var delivery sdk.Delivery
	err := store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		var leaseErr error
		delivery, leaseErr = memory.Lease(ctx, now, duration)
		return leaseErr
	})
	if err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (store *fileDeliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		return memory.Ack(ctx, id, token, now)
	})
}

func (store *fileDeliveryStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		return memory.Retry(ctx, id, token, availableAt, lastError)
	})
}

func (store *fileDeliveryStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		return memory.DeadLetter(ctx, id, token, now, lastError)
	})
}

func (store *fileDeliveryStore) List(
	ctx context.Context,
) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []sdk.Delivery
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

func (store *fileDeliveryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.DeliveryPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	page, next, err := pageWindow(items, request, func(item sdk.Delivery) string {
		return item.ID
	})
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	return sdk.DeliveryPage{Items: page, Next: next}, nil
}

func (store *fileDeliveryStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	removed := 0
	err := store.mutate(ctx, func(memory *memoryDeliveryStore) error {
		var purgeErr error
		removed, purgeErr = memory.PurgeTerminal(ctx, before)
		return purgeErr
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}

func (store *fileDeliveryStore) mutate(
	ctx context.Context,
	mutation func(*memoryDeliveryStore) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return filestate.WithExclusiveLock(store.lockPath, func() error {
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

func (store *fileDeliveryStore) readLocked() (*memoryDeliveryStore, error) {
	raw, err := os.ReadFile(store.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newMemoryDeliveryStore(), nil
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
		state.Deliveries = make(map[string]sdk.Delivery)
	}
	memory := &memoryDeliveryStore{
		deliveries:   make(map[string]sdk.Delivery, len(state.Deliveries)),
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
		if err := validateLoadedDelivery(delivery); err != nil {
			return nil, fmt.Errorf("validate delivery %q: %w", id, err)
		}
		memory.deliveries[id] = sdk.CloneDelivery(delivery)
	}
	return memory, nil
}

func (store *fileDeliveryStore) writeLocked(
	ctx context.Context,
	memory *memoryDeliveryStore,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	memory.mu.Lock()
	state := fileDeliveryState{
		SchemaVersion: deliveryStoreSchemaVersion,
		NextSequence:  memory.nextSequence,
		Deliveries:    make(map[string]sdk.Delivery, len(memory.deliveries)),
	}
	for id, delivery := range memory.deliveries {
		state.Deliveries[id] = sdk.CloneDelivery(delivery)
	}
	memory.mu.Unlock()
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.path,
		"deliveries",
		state,
	)
}
