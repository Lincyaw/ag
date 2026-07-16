package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var (
	ErrNoDelivery    = errors.New("no delivery available")
	ErrDeliveryLease = errors.New("delivery lease lost")
)

type DeliveryState string

const (
	DeliveryPending    DeliveryState = "pending"
	DeliveryLeased     DeliveryState = "leased"
	DeliveryDelivered  DeliveryState = "delivered"
	DeliveryDeadLetter DeliveryState = "dead_letter"
)

type Delivery struct {
	ID             string        `json:"id"`
	Sequence       uint64        `json:"sequence"`
	Plugin         string        `json:"plugin"`
	Subscription   string        `json:"subscription"`
	Partition      string        `json:"partition,omitempty"`
	Event          Event         `json:"event"`
	State          DeliveryState `json:"state"`
	Attempt        int           `json:"attempt"`
	AvailableAt    time.Time     `json:"available_at"`
	LeaseToken     string        `json:"lease_token,omitempty"`
	LeaseExpiresAt time.Time     `json:"lease_expires_at,omitempty"`
	LastError      string        `json:"last_error,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

type OutboxStore interface {
	Enqueue(context.Context, ...Delivery) error
	Lease(context.Context, time.Time, time.Duration) (Delivery, error)
	Ack(context.Context, string, string, time.Time) error
	Retry(context.Context, string, string, time.Time, string) error
	DeadLetter(context.Context, string, string, time.Time, string) error
	List(context.Context) ([]Delivery, error)
}

type MemoryOutboxStore struct {
	mu           sync.Mutex
	deliveries   map[string]Delivery
	nextSequence uint64
}

func NewMemoryOutboxStore() *MemoryOutboxStore {
	return &MemoryOutboxStore{deliveries: make(map[string]Delivery)}
}

func (store *MemoryOutboxStore) Enqueue(
	ctx context.Context,
	deliveries ...Delivery,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared := make([]Delivery, len(deliveries))
	for index, delivery := range deliveries {
		if err := validateNewDelivery(delivery); err != nil {
			return err
		}
		now := delivery.CreatedAt.UTC()
		if now.IsZero() {
			now = time.Now().UTC()
		}
		delivery.State = DeliveryPending
		delivery.Attempt = 0
		delivery.AvailableAt = delivery.AvailableAt.UTC()
		if delivery.AvailableAt.IsZero() {
			delivery.AvailableAt = now
		}
		delivery.LeaseToken = ""
		delivery.LeaseExpiresAt = time.Time{}
		delivery.CreatedAt = now
		delivery.UpdatedAt = now
		delivery.Event = cloneEvent(delivery.Event)
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

func (store *MemoryOutboxStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (Delivery, error) {
	if err := ctx.Err(); err != nil {
		return Delivery{}, err
	}
	if duration <= 0 {
		return Delivery{}, errors.New("delivery lease duration must be positive")
	}
	now = now.UTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	heads := make(map[string]Delivery)
	for _, delivery := range store.deliveries {
		if delivery.State == DeliveryDelivered || delivery.State == DeliveryDeadLetter {
			continue
		}
		partition := deliveryPartition(delivery)
		head, exists := heads[partition]
		if !exists || delivery.Sequence < head.Sequence {
			heads[partition] = delivery
		}
	}
	candidates := make([]Delivery, 0, len(heads))
	for _, delivery := range heads {
		available := delivery.State == DeliveryPending &&
			!delivery.AvailableAt.After(now)
		expired := delivery.State == DeliveryLeased &&
			!delivery.LeaseExpiresAt.After(now)
		if available || expired {
			candidates = append(candidates, delivery)
		}
	}
	if len(candidates) == 0 {
		return Delivery{}, ErrNoDelivery
	}
	slices.SortFunc(candidates, compareDeliveries)
	delivery := candidates[0]
	delivery.State = DeliveryLeased
	delivery.Attempt++
	delivery.LeaseToken = newDispatchID()
	delivery.LeaseExpiresAt = now.Add(duration)
	delivery.UpdatedAt = now
	store.deliveries[delivery.ID] = delivery
	return cloneDelivery(delivery), nil
}

func (store *MemoryOutboxStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.transition(ctx, id, token, now, DeliveryDelivered, time.Time{}, "")
}

func (store *MemoryOutboxStore) Retry(
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
		DeliveryPending,
		availableAt.UTC(),
		lastError,
	)
}

func (store *MemoryOutboxStore) DeadLetter(
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
		DeliveryDeadLetter,
		time.Time{},
		lastError,
	)
}

func (store *MemoryOutboxStore) transition(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	state DeliveryState,
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
	if delivery.State != DeliveryLeased || delivery.LeaseToken != token {
		return fmt.Errorf("%w: %s", ErrDeliveryLease, id)
	}
	delivery.State = state
	delivery.AvailableAt = availableAt
	delivery.LeaseToken = ""
	delivery.LeaseExpiresAt = time.Time{}
	delivery.LastError = lastError
	delivery.UpdatedAt = now.UTC()
	store.deliveries[id] = delivery
	return nil
}

func (store *MemoryOutboxStore) List(
	ctx context.Context,
) ([]Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]Delivery, 0, len(store.deliveries))
	for _, delivery := range store.deliveries {
		result = append(result, cloneDelivery(delivery))
	}
	slices.SortFunc(result, compareDeliveries)
	return result, nil
}

func validateNewDelivery(delivery Delivery) error {
	if delivery.ID == "" {
		return errors.New("delivery ID is empty")
	}
	if err := validateResourceName("plugin", delivery.Plugin); err != nil {
		return err
	}
	if err := validateResourceName("subscription", delivery.Subscription); err != nil {
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

func sameDeliveryIdentity(left, right Delivery) bool {
	return left.ID == right.ID &&
		left.Plugin == right.Plugin &&
		left.Subscription == right.Subscription &&
		left.Event.ID == right.Event.ID
}

func compareDeliveries(left, right Delivery) int {
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

func deliveryPartition(delivery Delivery) string {
	if delivery.Partition != "" {
		return delivery.Partition
	}
	return delivery.Plugin + "/" + delivery.Subscription
}

func cloneEvent(event Event) Event {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}

func cloneDelivery(delivery Delivery) Delivery {
	delivery.Event = cloneEvent(delivery.Event)
	return delivery
}
