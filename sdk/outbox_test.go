package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryOutboxLeaseRecoveryAndConcurrentDeduplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	base := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	delivery := Delivery{
		ID:           "event-1:observe",
		Plugin:       "observer",
		Subscription: "observe",
		Partition:    "session-1",
		Event: Event{
			ID:        "event-1",
			Name:      EventAgentStart,
			SessionID: "session-1",
			Payload:   []byte(`{"messages":[]}`),
		},
		CreatedAt: base,
	}

	const enqueueWorkers = 64
	var enqueueErrors atomic.Int64
	var wait sync.WaitGroup
	for range enqueueWorkers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := store.Enqueue(ctx, delivery); err != nil {
				enqueueErrors.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := enqueueErrors.Load(); got != 0 {
		t.Fatalf("idempotent concurrent enqueue errors = %d, want 0", got)
	}

	const leaseWorkers = 64
	var leased atomic.Int64
	leases := make(chan Delivery, leaseWorkers)
	for range leaseWorkers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			candidate, err := store.Lease(ctx, base, time.Minute)
			switch {
			case err == nil:
				leased.Add(1)
				leases <- candidate
			case errors.Is(err, ErrNoDelivery):
			default:
				t.Errorf("lease: %v", err)
			}
		}()
	}
	wait.Wait()
	close(leases)
	if got := leased.Load(); got != 1 {
		t.Fatalf("successful leases = %d, want 1", got)
	}
	first := <-leases

	second, err := store.Lease(ctx, base.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("lease expired delivery: %v", err)
	}
	if second.Attempt != 2 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("re-lease = %#v, first = %#v", second, first)
	}
	if err := store.Ack(ctx, first.ID, first.LeaseToken, base.Add(time.Minute)); !errors.Is(err, ErrDeliveryLease) {
		t.Fatalf("stale ack error = %v, want ErrDeliveryLease", err)
	}
	if err := store.Retry(
		ctx,
		second.ID,
		second.LeaseToken,
		base.Add(3*time.Minute),
		"subscriber unavailable",
	); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, err := store.Lease(ctx, base.Add(2*time.Minute), time.Minute); !errors.Is(err, ErrNoDelivery) {
		t.Fatalf("early retry lease error = %v, want ErrNoDelivery", err)
	}
	third, err := store.Lease(ctx, base.Add(3*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("lease scheduled retry: %v", err)
	}
	if third.Attempt != 3 {
		t.Fatalf("retry attempt = %d, want 3", third.Attempt)
	}
	if err := store.Ack(ctx, third.ID, third.LeaseToken, base.Add(3*time.Minute)); err != nil {
		t.Fatalf("ack retry: %v", err)
	}

	listed, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 || listed[0].State != DeliveryDelivered {
		t.Fatalf("deliveries = %#v", listed)
	}
}

func TestMemoryOutboxAtomicBatchAndCancellation(t *testing.T) {
	t.Parallel()
	store := NewMemoryOutboxStore()
	valid := func(id string) Delivery {
		return Delivery{
			ID:           id,
			Plugin:       "observer",
			Subscription: "all",
			Event: Event{
				ID:      "event-" + id,
				Name:    EventAgentEnd,
				Payload: []byte(`{}`),
			},
		}
	}
	invalid := valid("bad")
	invalid.Event.Payload = []byte(`{`)
	if err := store.Enqueue(context.Background(), valid("good"), invalid); err == nil {
		t.Fatal("invalid batch enqueue unexpectedly succeeded")
	}
	listed, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Fatalf("invalid batch partially committed: %v", listed)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Enqueue(cancelled, valid("cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled enqueue = %v", err)
	}
	if _, err := store.Lease(cancelled, time.Now(), time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled lease = %v", err)
	}

	conflict := valid("same")
	if err := store.Enqueue(context.Background(), conflict); err != nil {
		t.Fatal(err)
	}
	conflict.Plugin = "different"
	if err := store.Enqueue(context.Background(), conflict); err == nil {
		t.Fatal("conflicting delivery identity unexpectedly accepted")
	}
	if got, _ := store.List(context.Background()); len(got) != 1 {
		t.Fatalf("conflict changed outbox: %s", fmt.Sprint(got))
	}
}

func TestMemoryOutboxPreservesPartitionOrderAcrossWorkers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOutboxStore()
	now := time.Date(2026, 7, 17, 13, 0, 0, 0, time.UTC)
	makeDelivery := func(id, partition string) Delivery {
		return Delivery{
			ID:           id,
			Plugin:       "observer",
			Subscription: "events",
			Partition:    partition,
			Event: Event{
				ID:      "event-" + id,
				Name:    EventTurnStart,
				Payload: []byte(`{}`),
			},
			CreatedAt: now,
		}
	}
	if err := store.Enqueue(
		ctx,
		makeDelivery("session-a-1", "session-a"),
		makeDelivery("session-a-2", "session-a"),
		makeDelivery("session-b-1", "session-b"),
	); err != nil {
		t.Fatal(err)
	}
	first, err := store.Lease(ctx, now, time.Minute)
	if err != nil || first.ID != "session-a-1" {
		t.Fatalf("first lease = %#v, %v", first, err)
	}
	second, err := store.Lease(ctx, now, time.Minute)
	if err != nil || second.ID != "session-b-1" {
		t.Fatalf("parallel partition lease = %#v, %v", second, err)
	}
	if _, err := store.Lease(ctx, now, time.Minute); !errors.Is(err, ErrNoDelivery) {
		t.Fatalf("same partition successor leased early: %v", err)
	}
	if err := store.Ack(ctx, first.ID, first.LeaseToken, now); err != nil {
		t.Fatal(err)
	}
	third, err := store.Lease(ctx, now, time.Minute)
	if err != nil || third.ID != "session-a-2" {
		t.Fatalf("successor lease = %#v, %v", third, err)
	}
}
