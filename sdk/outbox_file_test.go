package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFileOutboxSurvivesRestartAndRecoversExpiredLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	firstStore, err := NewFileOutboxStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 17, 14, 0, 0, 0, time.UTC)
	delivery := Delivery{
		ID:           "durable-1",
		Plugin:       "observer",
		Subscription: "events",
		Partition:    "events/session",
		Event: Event{
			ID:      "event-durable-1",
			Name:    EventAgentStart,
			Payload: []byte(`{}`),
		},
		CreatedAt: base,
	}
	if err := firstStore.Enqueue(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	firstLease, err := firstStore.Lease(ctx, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := NewFileOutboxStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Lease(ctx, base.Add(30*time.Second), time.Minute); !errors.Is(err, ErrNoDelivery) {
		t.Fatalf("unexpired lease was redelivered: %v", err)
	}
	secondLease, err := reopened.Lease(ctx, base.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if secondLease.Attempt != 2 || secondLease.Sequence != 1 ||
		secondLease.LeaseToken == firstLease.LeaseToken {
		t.Fatalf("recovered lease = %#v, first = %#v", secondLease, firstLease)
	}
	if err := firstStore.Ack(ctx, firstLease.ID, firstLease.LeaseToken, base.Add(time.Minute)); !errors.Is(err, ErrDeliveryLease) {
		t.Fatalf("stale post-restart ack = %v", err)
	}
	if err := reopened.Retry(
		ctx,
		secondLease.ID,
		secondLease.LeaseToken,
		base.Add(3*time.Minute),
		"retry after restart",
	); err != nil {
		t.Fatal(err)
	}

	thirdStore, err := NewFileOutboxStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := thirdStore.Lease(ctx, base.Add(2*time.Minute), time.Minute); !errors.Is(err, ErrNoDelivery) {
		t.Fatalf("scheduled retry was leased early: %v", err)
	}
	thirdLease, err := thirdStore.Lease(ctx, base.Add(3*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if thirdLease.Attempt != 3 || thirdLease.Sequence != 1 {
		t.Fatalf("third lease = %#v", thirdLease)
	}
	if err := thirdStore.Ack(ctx, thirdLease.ID, thirdLease.LeaseToken, base.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	listed, err := thirdStore.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].State != DeliveryDelivered || listed[0].LastError != "retry after restart" {
		t.Fatalf("persisted deliveries = %#v", listed)
	}
}

func TestFileOutboxSerializesConcurrentStoreInstances(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	left, err := NewFileOutboxStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewFileOutboxStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	stores := []*FileOutboxStore{left, right}
	const count = 32
	var wait sync.WaitGroup
	errorsChannel := make(chan error, count)
	for index := range count {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			delivery := Delivery{
				ID:           fmt.Sprintf("delivery-%02d", index),
				Plugin:       "observer",
				Subscription: "events",
				Partition:    fmt.Sprintf("partition-%02d", index),
				Event: Event{
					ID:      fmt.Sprintf("event-%02d", index),
					Name:    EventAgentEnd,
					Payload: []byte(`{}`),
				},
			}
			if enqueueErr := stores[index%len(stores)].Enqueue(ctx, delivery); enqueueErr != nil {
				errorsChannel <- enqueueErr
			}
		}(index)
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent enqueue: %v", err)
	}
	listed, err := left.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != count {
		t.Fatalf("delivery count = %d, want %d", len(listed), count)
	}
	seenSequences := make(map[uint64]struct{}, count)
	for _, delivery := range listed {
		if delivery.Sequence == 0 {
			t.Fatal("zero sequence persisted")
		}
		seenSequences[delivery.Sequence] = struct{}{}
	}
	if len(seenSequences) != count {
		t.Fatalf("unique sequences = %d, want %d", len(seenSequences), count)
	}
}
