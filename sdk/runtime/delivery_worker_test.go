package runtime

// Delivery tests cover outbox worker retry and acknowledgement.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/deliveryworker"
	"github.com/lincyaw/ag/sdk"
)

type subscriberTestPlugin struct {
	manifest   sdk.Manifest
	hook       sdk.Hook
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
	if plugin.hook != nil {
		if err := registrar.RegisterHook(plugin.hook); err != nil {
			return err
		}
	}
	if plugin.subscriber != nil {
		return registrar.RegisterSubscriber(plugin.subscriber)
	}
	return nil
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
	unrelated := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "unrelated-observer",
			Version:     "1.0.0",
			Description: "should not be leased by another subscriber delivery",
			APIVersion:  sdk.APIVersion,
		},
		closed: make(chan struct{}),
	}
	unrelatedMount, err := runtime.Mount(context.Background(), sdk.Local(unrelated))
	if err != nil {
		t.Fatalf("mount unrelated: %v", err)
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

	if err := unrelatedMount.Unmount(context.Background()); err != nil {
		t.Fatalf("unmount unrelated: %v", err)
	}
	select {
	case <-unrelated.closed:
	case <-time.After(time.Second):
		t.Fatal("unrelated plugin stayed leased during subscriber callback")
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

func TestSubscriberDeliveryTargetMatchesCurrentAndLegacyDeliveries(t *testing.T) {
	t.Parallel()
	target := deliveryworker.Target{
		Plugin:           "observer",
		PluginVersion:    "2.0.0",
		Subscription:     "events",
		ResourceRevision: "revision-new",
	}
	cases := []struct {
		name     string
		delivery sdk.Delivery
		match    bool
	}{
		{
			name: "current target",
			delivery: sdk.Delivery{
				Plugin:           "observer",
				PluginVersion:    "2.0.0",
				Subscription:     "events",
				ResourceRevision: "revision-new",
			},
			match: true,
		},
		{
			name: "legacy delivery without versioned target",
			delivery: sdk.Delivery{
				Plugin:       "observer",
				Subscription: "events",
			},
			match: true,
		},
		{
			name: "stale plugin version",
			delivery: sdk.Delivery{
				Plugin:           "observer",
				PluginVersion:    "1.0.0",
				Subscription:     "events",
				ResourceRevision: "revision-new",
			},
		},
		{
			name: "stale resource revision",
			delivery: sdk.Delivery{
				Plugin:           "observer",
				PluginVersion:    "2.0.0",
				Subscription:     "events",
				ResourceRevision: "revision-old",
			},
		},
		{
			name: "other plugin",
			delivery: sdk.Delivery{
				Plugin:           "other",
				PluginVersion:    "2.0.0",
				Subscription:     "events",
				ResourceRevision: "revision-new",
			},
		},
		{
			name: "other subscription",
			delivery: sdk.Delivery{
				Plugin:           "observer",
				PluginVersion:    "2.0.0",
				Subscription:     "other",
				ResourceRevision: "revision-new",
			},
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := target.Matches(testCase.delivery); got != testCase.match {
				t.Fatalf("Matches() = %v, want %v", got, testCase.match)
			}
		})
	}
}

func TestTrajectoryEventEnqueueSurvivesCallerCancellation(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	received := make(chan sdk.Delivery, 1)
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "trajectory-observer",
			Version:     "1.0.0",
			Description: "observes committed trajectory events",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.SubscriberResource("trajectory-events")},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "trajectory-events",
				Events: []string{sdk.EventTrajectoryAppend},
			},
			ReceiveFunc: func(_ context.Context, delivery sdk.Delivery) error {
				received <- delivery
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	dispatchSubscriberTestPostCommit(
		t,
		runtime,
		cancelled,
		sdk.EventTrajectoryAppend,
		"cancelled-after-commit",
		sdk.TrajectoryEventPayload{
			TrajectoryID: "cancelled-after-commit",
			EntryID:      "entry-after-commit",
			EntryKind:    sdk.TrajectoryKindCheckpoint,
		},
	)

	select {
	case delivery := <-received:
		if delivery.Event.SessionID != "cancelled-after-commit" {
			t.Fatalf("delivery event = %#v", delivery.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("trajectory event was not delivered")
	}
}

func TestPostCommitHookFailureDoesNotBlockSubscriberDelivery(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	received := make(chan sdk.Delivery, 1)
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "post-commit-observer",
			Version:     "1.0.0",
			Description: "tests post-commit hook failure isolation",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.HookResource("failing-trajectory-hook"),
				sdk.SubscriberResource("trajectory-events"),
			},
		},
		hook: sdk.HookFunc{
			HookSpec: sdk.HookSpec{
				Name:          "failing-trajectory-hook",
				Event:         sdk.EventTrajectoryAppend,
				FailurePolicy: sdk.FailurePolicyFailClosed,
			},
			HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
				return sdk.Effect{}, errors.New("hook failed after commit")
			},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "trajectory-events",
				Events: []string{sdk.EventTrajectoryAppend},
			},
			ReceiveFunc: func(_ context.Context, delivery sdk.Delivery) error {
				received <- delivery
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}

	dispatchSubscriberTestPostCommit(
		t,
		runtime,
		context.Background(),
		sdk.EventTrajectoryAppend,
		"post-commit-hook-failure",
		sdk.TrajectoryEventPayload{
			TrajectoryID: "post-commit-hook-failure",
			EntryID:      "post-commit-entry",
			EntryKind:    sdk.TrajectoryKindDecision,
		},
	)

	select {
	case delivery := <-received:
		if delivery.Event.SessionID != "post-commit-hook-failure" {
			t.Fatalf("delivery event = %#v", delivery.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber delivery was blocked by post-commit hook failure")
	}
}

func TestPostCommitHookPatchFailureDoesNotBlockSubscriberDelivery(t *testing.T) {
	t.Parallel()
	runtime := newSubscriberTestRuntime(t)
	received := make(chan sdk.Delivery, 1)
	plugin := &subscriberTestPlugin{
		manifest: sdk.Manifest{
			Name:        "post-commit-patch-observer",
			Version:     "1.0.0",
			Description: "tests post-commit patch failure isolation",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.HookResource("bad-post-commit-patch"),
				sdk.SubscriberResource("patch-failure-subscriber"),
			},
		},
		hook: sdk.HookFunc{
			HookSpec: sdk.HookSpec{
				Name:  "bad-post-commit-patch",
				Event: sdk.EventBeforeProvider,
			},
			HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
				return sdk.Effect{
					Patch: map[string]json.RawMessage{
						"provider": json.RawMessage(`{`),
					},
				}, nil
			},
		},
		subscriber: sdk.SubscriberFunc{
			SubscriberSpec: sdk.SubscriberSpec{
				Name:   "patch-failure-subscriber",
				Events: []string{sdk.EventBeforeProvider},
			},
			ReceiveFunc: func(_ context.Context, delivery sdk.Delivery) error {
				received <- delivery
				return nil
			},
		},
		closed: make(chan struct{}),
	}
	if _, err := runtime.Mount(context.Background(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	dispatchSubscriberTestPostCommit(
		t,
		runtime,
		context.Background(),
		sdk.EventBeforeProvider,
		"post-commit-patch-failure",
		sdk.BeforeProviderPayload{Provider: "gateway-test"},
	)

	select {
	case delivery := <-received:
		if delivery.Event.SessionID != "post-commit-patch-failure" {
			t.Fatalf("delivery event = %#v", delivery.Event)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber delivery was blocked by post-commit patch failure")
	}
}

func dispatchSubscriberTestPostCommit(
	t *testing.T,
	runtime *Runtime,
	ctx context.Context,
	eventName string,
	sessionID string,
	payload any,
) {
	t.Helper()
	plan, err := preparePostCommitEventPlan(
		runtime.current.Load(),
		eventName,
		postCommitSessionSubject(sessionID),
		payload,
		postCommitDeliveryBoundaryAfterDispatch,
	)
	if err != nil {
		t.Fatalf("prepare post-commit event: %v", err)
	}
	postCommitEventBundle{plan}.dispatchAfterCommit(ctx, runtime)
}

func TestPostCommitHostOutboxDeliveriesReturnsOwnedDeliveries(t *testing.T) {
	delivery := postCommitDelivery{
		hostOutbox: []sdk.Delivery{{
			ID: "delivery-1",
			Event: sdk.Event{
				ID:      "event-1",
				Name:    "example.event",
				Payload: json.RawMessage(`{"value":1}`),
			},
		}},
	}
	first := delivery.hostOutboxDeliveries()
	first[0].ID = "changed"
	first[0].Event.Payload[0] = '['

	second := delivery.hostOutboxDeliveries()
	if second[0].ID != "delivery-1" ||
		string(second[0].Event.Payload) != `{"value":1}` {
		t.Fatalf("host outbox delivery aliased internal delivery: %#v", second)
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
