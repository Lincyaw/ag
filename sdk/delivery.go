package sdk

import (
	"context"
	"encoding/json"
	"errors"
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
	ID               string        `json:"id"`
	Sequence         uint64        `json:"sequence"`
	Plugin           string        `json:"plugin"`
	PluginVersion    string        `json:"plugin_version,omitempty"`
	Subscription     string        `json:"subscription"`
	ResourceRevision string        `json:"resource_revision,omitempty"`
	Partition        string        `json:"partition,omitempty"`
	Event            Event         `json:"event"`
	State            DeliveryState `json:"state"`
	Attempt          int           `json:"attempt"`
	AvailableAt      time.Time     `json:"available_at"`
	LeaseToken       string        `json:"lease_token,omitempty"`
	LeaseExpiresAt   time.Time     `json:"lease_expires_at,omitempty"`
	LastError        string        `json:"last_error,omitempty"`
	CreatedAt        time.Time     `json:"created_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

type DeliveryPage struct {
	Items []Delivery `json:"items"`
	Next  string     `json:"next,omitempty"`
}

// DeliveryStore is neutral about topology: "inbox" and "outbox" are named
// queue roles, while persistence, leasing, retry, and acknowledgement live here.
type DeliveryStore interface {
	Enqueue(context.Context, ...Delivery) error
	Lease(context.Context, time.Time, time.Duration) (Delivery, error)
	Ack(context.Context, string, string, time.Time) error
	Retry(context.Context, string, string, time.Time, string) error
	DeadLetter(context.Context, string, string, time.Time, string) error
	List(context.Context) ([]Delivery, error)
	ListPage(context.Context, PageRequest) (DeliveryPage, error)
	PurgeTerminal(context.Context, time.Time) (int, error)
}

// OutboxStore is a compatibility alias. New code should use DeliveryStore.
type OutboxStore = DeliveryStore

func CloneEvent(event Event) Event {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}

func CloneDelivery(delivery Delivery) Delivery {
	delivery.Event = CloneEvent(delivery.Event)
	return delivery
}
