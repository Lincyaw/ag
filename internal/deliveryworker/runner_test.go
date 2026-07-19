package deliveryworker

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/storage"
)

type panicAckStore struct {
	sdk.DeliveryStore
	panicID string
}

func (store panicAckStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	if id == store.panicID {
		panic("broken delivery ack")
	}
	return store.DeliveryStore.Ack(ctx, id, token, now)
}

func TestRunnerReleasesCancelledDeliveryImmediately(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fail func(context.Context) error
	}{
		{
			name: "plain cancellation",
			fail: func(ctx context.Context) error {
				return ctx.Err()
			},
		},
		{
			name: "permanent cancellation",
			fail: func(ctx context.Context) error {
				return Permanent(ctx.Err())
			},
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := storage.NewMemoryDeliveryStore()
			now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
			if err := store.Enqueue(ctx, sdk.Delivery{
				ID:           "delivery-cancelled",
				Plugin:       "observer",
				Subscription: "events",
				Event: sdk.Event{
					ID:      "event-cancelled",
					Name:    sdk.EventAgentEnd,
					Payload: []byte(`{}`),
				},
				CreatedAt: now,
			}); err != nil {
				t.Fatal(err)
			}
			leased, err := store.Lease(ctx, now, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			runner := Runner{
				Store:       store,
				Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
				Queue:       "subscriber outbox",
				Poll:        time.Hour,
				MaxAttempts: 1,
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()

			runner.deliver(
				cancelled,
				runner.logger(),
				leased,
				func(ctx context.Context, _ sdk.Delivery) error {
					return testCase.fail(ctx)
				},
			)

			deliveries, err := store.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(deliveries) != 1 ||
				deliveries[0].State != sdk.DeliveryPending ||
				deliveries[0].LeaseToken != "" ||
				deliveries[0].LastError != context.Canceled.Error() {
				t.Fatalf("released delivery = %#v", deliveries)
			}
			released, err := store.Lease(
				ctx,
				time.Now().Add(time.Second),
				time.Hour,
			)
			if err != nil {
				t.Fatalf("re-lease released delivery: %v", err)
			}
			if released.ID != leased.ID ||
				released.LeaseToken == leased.LeaseToken {
				t.Fatalf(
					"re-lease = %#v, first lease = %#v",
					released,
					leased,
				)
			}
		})
	}
}

func TestRunnerContinuesAfterWorkerProtocolPanic(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := storage.NewMemoryDeliveryStore()
	now := time.Now().UTC().Add(-time.Minute)
	for _, delivery := range []sdk.Delivery{
		{
			ID:           "delivery-ack-panic",
			Plugin:       "observer",
			Subscription: "events",
			Event: sdk.Event{
				ID:      "event-ack-panic",
				Name:    sdk.EventAgentEnd,
				Payload: []byte(`{}`),
			},
			CreatedAt: now,
		},
		{
			ID:           "delivery-after-panic",
			Plugin:       "observer",
			Subscription: "other-events",
			Event: sdk.Event{
				ID:      "event-after-panic",
				Name:    sdk.EventAgentEnd,
				Payload: []byte(`{}`),
			},
			CreatedAt: now.Add(time.Millisecond),
		},
	} {
		if err := store.Enqueue(ctx, delivery); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(chan string, 2)
	done := make(chan struct{})
	runner := Runner{
		Store: panicAckStore{
			DeliveryStore: store,
			panicID:       "delivery-ack-panic",
		},
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Context:     ctx,
		Queue:       "subscriber outbox",
		Lease:       time.Hour,
		Poll:        time.Millisecond,
		MaxAttempts: 2,
	}
	go func() {
		defer close(done)
		runner.Run(0, func(_ context.Context, delivery sdk.Delivery) error {
			seen <- delivery.ID
			return nil
		})
	}()
	handled := make(map[string]bool)
	for range 2 {
		select {
		case got := <-seen:
			handled[got] = true
		case <-time.After(time.Second):
			t.Fatalf("runner did not continue to a second delivery; handled=%#v", handled)
		}
	}
	for _, want := range []string{"delivery-ack-panic", "delivery-after-panic"} {
		if !handled[want] {
			t.Fatalf("handled deliveries = %#v, missing %q", handled, want)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not stop after context cancellation")
	}
}

func TestRunnerRetriesHandlerPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := storage.NewMemoryDeliveryStore()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	if err := store.Enqueue(ctx, sdk.Delivery{
		ID:           "delivery-panic",
		Plugin:       "observer",
		Subscription: "events",
		Event: sdk.Event{
			ID:      "event-panic",
			Name:    sdk.EventAgentEnd,
			Payload: []byte(`{}`),
		},
		CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	leased, err := store.Lease(ctx, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runner := Runner{
		Store:       store,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Queue:       "subscriber outbox",
		Poll:        time.Millisecond,
		MaxAttempts: 2,
	}

	runner.deliver(
		ctx,
		runner.logger(),
		leased,
		func(context.Context, sdk.Delivery) error {
			panic("subscriber exploded")
		},
	)

	deliveries, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 ||
		deliveries[0].State != sdk.DeliveryPending ||
		!strings.Contains(deliveries[0].LastError, "subscriber exploded") {
		t.Fatalf("retried delivery after panic = %#v", deliveries)
	}
}
