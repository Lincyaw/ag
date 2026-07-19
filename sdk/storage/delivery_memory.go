package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type memoryDeliveryStore struct {
	mu           sync.Mutex
	deliveries   map[string]sdk.Delivery
	nextSequence uint64
}

func NewMemoryDeliveryStore() sdk.DeliveryStore {
	return newMemoryDeliveryStore()
}

func newMemoryDeliveryStore() *memoryDeliveryStore {
	return &memoryDeliveryStore{deliveries: make(map[string]sdk.Delivery)}
}

func (store *memoryDeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...sdk.Delivery,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewDeliveries(deliveries, time.Now().UTC())
	if err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	for _, delivery := range prepared {
		if existing, exists := store.deliveries[delivery.ID]; exists {
			if sameDeliveryIdentity(existing, delivery) {
				continue
			}
			return fmt.Errorf("delivery %q already exists with different identity", delivery.ID)
		}
	}
	for _, delivery := range prepared {
		if _, exists := store.deliveries[delivery.ID]; !exists {
			store.nextSequence++
			delivery.Sequence = store.nextSequence
			store.deliveries[delivery.ID] = delivery
		}
	}
	return nil
}

func (store *memoryDeliveryStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Delivery{}, err
	}
	if err := validateDeliveryLeaseDuration(duration); err != nil {
		return sdk.Delivery{}, err
	}
	now = normalizeDeliveryMutationTime(now)
	store.mu.Lock()
	defer store.mu.Unlock()
	heads := make(map[string]sdk.Delivery)
	for _, delivery := range store.deliveries {
		if delivery.Terminal() {
			continue
		}
		partition := deliveryPartition(delivery)
		head, exists := heads[partition]
		if !exists || delivery.Sequence < head.Sequence {
			heads[partition] = delivery
		}
	}
	candidates := make([]sdk.Delivery, 0, len(heads))
	for _, delivery := range heads {
		if deliveryAvailable(delivery, now) {
			candidates = append(candidates, delivery)
		}
	}
	if len(candidates) == 0 {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	slices.SortFunc(candidates, compareDeliveries)
	delivery := candidates[0]
	delivery, err := leaseDelivery(delivery, sdk.NewID(), now, duration)
	if err != nil {
		return sdk.Delivery{}, err
	}
	store.deliveries[delivery.ID] = delivery
	return sdk.CloneDelivery(delivery), nil
}

func (store *memoryDeliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.transition(ctx, id, token, now, sdk.DeliveryDelivered, time.Time{}, "")
}

func (store *memoryDeliveryStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return store.transition(
		ctx,
		id,
		token,
		time.Now().UTC(),
		sdk.DeliveryPending,
		availableAt.UTC(),
		lastError,
	)
}

func (store *memoryDeliveryStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return store.transition(
		ctx,
		id,
		token,
		now,
		sdk.DeliveryDeadLetter,
		time.Time{},
		lastError,
	)
}

func (store *memoryDeliveryStore) transition(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	state sdk.DeliveryState,
	availableAt time.Time,
	lastError string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	delivery, exists := store.deliveries[id]
	if !exists {
		return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
	}
	delivery, err := finishDeliveryLease(
		delivery,
		token,
		now,
		state,
		availableAt,
		lastError,
	)
	if err != nil {
		return err
	}
	store.deliveries[id] = delivery
	return nil
}

func (store *memoryDeliveryStore) List(
	ctx context.Context,
) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]sdk.Delivery, 0, len(store.deliveries))
	for _, delivery := range store.deliveries {
		result = append(result, sdk.CloneDelivery(delivery))
	}
	slices.SortFunc(result, compareDeliveries)
	return result, nil
}

func (store *memoryDeliveryStore) ListNonTerminal(
	ctx context.Context,
) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]sdk.Delivery, 0)
	for _, delivery := range store.deliveries {
		if !delivery.Terminal() {
			result = append(result, sdk.CloneDelivery(delivery))
		}
	}
	slices.SortFunc(result, compareDeliveries)
	return result, nil
}

func (store *memoryDeliveryStore) ListPage(
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

func (store *memoryDeliveryStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("delivery purge cutoff is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	removed := 0
	for id, delivery := range store.deliveries {
		if delivery.Terminal() && delivery.UpdatedAt.Before(before) {
			delete(store.deliveries, id)
			removed++
		}
	}
	return removed, nil
}
