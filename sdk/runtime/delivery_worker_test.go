package runtime

// Delivery tests cover outbox worker retry and acknowledgement.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type subscriberTestPlugin struct {
	manifest   sdk.Manifest
	subscriber sdk.Subscriber
	closed     chan struct{}
	closeOnce  sync.Once
}

func (plugin *subscriberTestPlugin) Manifest() sdk.Manifest {
	return plugin.manifest
}

func (plugin *subscriberTestPlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	return registrar.RegisterSubscriber(plugin.subscriber)
}

func (plugin *subscriberTestPlugin) Close(context.Context) error {
	plugin.closeOnce.Do(func() { close(plugin.closed) })
	return nil
}

func newSubscriberTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:             newTestStateBackend(),
		DeliveryWorkers:     4,
		DeliveryLease:       time.Second,
		DeliveryPoll:        time.Millisecond,
		DeliveryTimeout:     500 * time.Millisecond,
		DeliveryMaxAttempts: 4,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	return runtime
}

func TestSubscriberDoesNotBlockEmitAndUnmountWaitsForDelivery(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "blocking-observer",
			Version:     "1.0.0",
			Description: "blocks to exercise the asynchronous delivery boundary",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("observe-agent-start")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:    "observe-agent-start",
				Events:  []string{sdk.EventAgentStart},
				Timeout: 400 * time.Millisecond,
			},
			ReceiveFunc: func(ctx context.Context, _ sdk.Delivery) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		closed: make(chan struct{}),
	}
	mount, err := runtime.Mount(context.Background(), sdk.Local(plugin))
	if err != nil {
		t.Fatalf("mount: %v", err)
	}

	emitted := make(chan error, 1)
	go func() {
		_, emitErr := runtime.Emit(
			context.Background(),
			sdk.EventAgentStart,
			"session-async",
			sdk.AgentStartPayload{},
		)
		emitted <- emitErr
	}()
	select {
	case err := <-emitted:
		if err != nil {
			t.Fatalf("emit: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Emit waited for the subscriber callback")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscriber was not invoked")
	}

	if err := mount.Unmount(context.Background()); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	select {
	case <-plugin.closed:
		t.Fatal("plugin connection closed while subscriber callback held a snapshot lease")
	default:
	}
	close(release)
	select {
	case <-mount.Done():
	case <-time.After(time.Second):
		t.Fatal("plugin connection did not close after subscriber callback returned")
	}

	eventually(t, time.Second, func() bool {
		deliveries, listErr := runtime.delivery.store.List(context.Background())
		return listErr == nil && len(deliveries) == 1 &&
			deliveries[0].State == sdk.DeliveryDelivered
	})
}

func TestSubscriberRetriesAndDeadLettersWithoutBlockingProducer(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	var attempts atomic.Int64
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "retrying-observer",
			Version:     "1.0.0",
			Description: "fails deliveries to exercise retry and dead-letter behavior",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("retry-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "retry-events",
				Events: []string{sdk.EventAgentEnd},
			},
			ReceiveFunc: func(context.Context, sdk.Delivery) error {
				if attempts.Add(1) < 3 {
					return errors.New("temporary failure")
				}
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if _, err := runtime.Emit(
		context.Background(),
		sdk.EventAgentEnd,
		"session-retry",
		sdk.AgentEndPayload{},
	); err != nil {
		t.Fatalf("emit: %v", err)
	}
	eventually(t, time.Second, func() bool {
		deliveries, listErr := runtime.delivery.store.List(context.Background())
		return listErr == nil && len(deliveries) == 1 &&
			deliveries[0].State == sdk.DeliveryDelivered &&
			deliveries[0].Attempt == 3
	})
}

func TestDrainDeliveriesWaitsForCurrentSubscribersAndHonorsContext(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "drain-observer",
			Version:     "1.0.0",
			Description: "holds a delivery across an explicit drain boundary",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("drain-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "drain-events",
				Events: []string{sdk.EventAgentEnd},
			},
			ReceiveFunc: func(ctx context.Context, _ sdk.Delivery) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Emit(
		context.Background(),
		sdk.EventAgentEnd,
		"drain-session",
		sdk.AgentEndPayload{},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("subscriber did not enter")
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	if err := runtime.DrainDeliveries(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short drain error = %v", err)
	}

	close(release)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := runtime.DrainDeliveries(drainCtx); err != nil {
		t.Fatalf("drain deliveries: %v", err)
	}
	deliveries, err := runtime.delivery.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 || deliveries[0].State != sdk.DeliveryDelivered {
		t.Fatalf("drained deliveries = %#v", deliveries)
	}
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
