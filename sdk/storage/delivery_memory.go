package storage

import (
	"context"
	"encoding/json"
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
	prepared := make([]sdk.Delivery, len(deliveries))
	for index, delivery := range deliveries {
		if err := validateNewDelivery(delivery); err != nil {
			return err
		}
		now := delivery.CreatedAt.UTC()
		if now.IsZero() {
			now = time.Now().UTC()
		}
		delivery.State = sdk.DeliveryPending
		delivery.Attempt = 0
		delivery.AvailableAt = delivery.AvailableAt.UTC()
		if delivery.AvailableAt.IsZero() {
			delivery.AvailableAt = now
		}
		delivery.LeaseToken = ""
		delivery.LeaseExpiresAt = time.Time{}
		delivery.CreatedAt = now
		delivery.UpdatedAt = now
		delivery.Event = sdk.CloneEvent(delivery.Event)
		prepared[index] = delivery
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
	if duration <= 0 {
		return sdk.Delivery{}, errors.New("delivery lease duration must be positive")
	}
	now = now.UTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	heads := make(map[string]sdk.Delivery)
	for _, delivery := range store.deliveries {
		if delivery.State == sdk.DeliveryDelivered || delivery.State == sdk.DeliveryDeadLetter {
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
		available := delivery.State == sdk.DeliveryPending &&
			!delivery.AvailableAt.After(now)
		expired := delivery.State == sdk.DeliveryLeased &&
			!delivery.LeaseExpiresAt.After(now)
		if available || expired {
			candidates = append(candidates, delivery)
		}
	}
	if len(candidates) == 0 {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	slices.SortFunc(candidates, compareDeliveries)
	delivery := candidates[0]
	delivery.State = sdk.DeliveryLeased
	delivery.Attempt++
	delivery.LeaseToken = sdk.NewID()
	delivery.LeaseExpiresAt = now.Add(duration)
	delivery.UpdatedAt = now
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
		return fmt.Errorf("delivery %q not found", id)
	}
	if delivery.State != sdk.DeliveryLeased || delivery.LeaseToken != token {
		return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
	}
	delivery.State = state
	delivery.AvailableAt = availableAt
	delivery.LeaseToken = ""
	delivery.LeaseExpiresAt = time.Time{}
	if state != sdk.DeliveryDelivered || lastError != "" {
		delivery.LastError = lastError
	}
	delivery.UpdatedAt = now.UTC()
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
		if (delivery.State == sdk.DeliveryDelivered ||
			delivery.State == sdk.DeliveryDeadLetter) &&
			delivery.UpdatedAt.Before(before) {
			delete(store.deliveries, id)
			removed++
		}
	}
	return removed, nil
}

func validateNewDelivery(delivery sdk.Delivery) error {
	if delivery.ID == "" {
		return errors.New("delivery ID is empty")
	}
	if err := sdk.ValidateResourceName("plugin", delivery.Plugin); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName("subscription", delivery.Subscription); err != nil {
		return err
	}
	if delivery.Event.ID == "" || delivery.Event.Name == "" {
		return errors.New("delivery event ID and name are required")
	}
	if !json.Valid(delivery.Event.Payload) {
		return errors.New("delivery event payload is invalid JSON")
	}
	return nil
}

func sameDeliveryIdentity(left, right sdk.Delivery) bool {
	return left.ID == right.ID &&
		left.Plugin == right.Plugin &&
		left.PluginVersion == right.PluginVersion &&
		left.Subscription == right.Subscription &&
		left.ResourceRevision == right.ResourceRevision &&
		left.Event.ID == right.Event.ID
}

func compareDeliveries(left, right sdk.Delivery) int {
	if left.Sequence != 0 || right.Sequence != 0 {
		if left.Sequence < right.Sequence {
			return -1
		}
		if left.Sequence > right.Sequence {
			return 1
		}
	}
	if order := left.AvailableAt.Compare(right.AvailableAt); order != 0 {
		return order
	}
	if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
		return order
	}
	if left.ID < right.ID {
		return -1
	}
	if left.ID > right.ID {
		return 1
	}
	return 0
}

func deliveryPartition(delivery sdk.Delivery) string {
	if delivery.Partition != "" {
		return delivery.Partition
	}
	return delivery.Plugin + "/" + delivery.Subscription
}
