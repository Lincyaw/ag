package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/deliveryworker"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
)

// deliveryRuntime hosts the durable subscriber outbox workers.
type deliveryRuntime struct {
	store       sdk.DeliveryStore
	workers     int
	lease       time.Duration
	poll        time.Duration
	timeout     time.Duration
	maxAttempts int
	context     context.Context
	cancel      context.CancelFunc
	once        sync.Once
	wait        sync.WaitGroup
}

type subscriberDeliveryBinding struct {
	subscriber sdk.Subscriber
	owner      *mountState
	timeout    time.Duration
}

type subscriberDeliveryInvocation struct {
	subscriber sdk.Subscriber
	timeout    time.Duration
	lease      *snapshotLease
}

func (delivery *deliveryRuntime) start(run func(int)) {
	delivery.once.Do(func() {
		for worker := range delivery.workers {
			delivery.wait.Add(1)
			go func(worker int) {
				defer delivery.wait.Done()
				run(worker)
			}(worker)
		}
	})
}

func (delivery *deliveryRuntime) stop() {
	if delivery.cancel != nil {
		delivery.cancel()
	}
}

func (delivery *deliveryRuntime) waitStopped() {
	delivery.wait.Wait()
}

func newSubscriberDeliveryTarget(
	name string,
	owned ownedResource[sdk.Subscriber, sdk.SubscriberSpec],
) deliveryworker.Target {
	return deliveryworker.Target{
		Plugin:        owned.owner.manifest.Name,
		PluginVersion: owned.owner.manifest.Version,
		Subscription:  name,
		ResourceRevision: owned.resourceRevision(
			sdk.ResourceKindSubscriber,
			name,
		),
	}
}

func (runtime *Runtime) enqueueSubscribers(
	ctx context.Context,
	snapshot *registrySnapshot,
	event sdk.Event,
) error {
	deliveries := planSubscriberDeliveries(snapshot, event, time.Now().UTC())
	if len(deliveries) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := runtime.delivery.store.Enqueue(ctx, deliveries...); err != nil {
		return fmt.Errorf(
			"persist subscriber deliveries for event %s: %w",
			event.ID,
			err,
		)
	}
	return nil
}

func planSubscriberDeliveries(
	snapshot *registrySnapshot,
	event sdk.Event,
	now time.Time,
) []sdk.Delivery {
	if snapshot == nil {
		return nil
	}
	if len(snapshot.subscribers) == 0 {
		return nil
	}
	names := make([]string, 0, len(snapshot.subscribers))
	for name, subscriber := range snapshot.subscribers {
		if slices.Contains(subscriber.spec.Events, event.Name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)
	deliveries := make([]sdk.Delivery, 0, len(names))
	for _, name := range names {
		subscriber := snapshot.subscribers[name]
		target := newSubscriberDeliveryTarget(name, subscriber)
		deliveries = append(deliveries, target.Record(event, now))
	}
	return deliveries
}

// startDeliveryWorkersLocked is called while runtime.mu prevents Close from
// waiting on delivery workers.
func (runtime *Runtime) startDeliveryWorkersLocked() {
	runtime.delivery.start(runtime.runDeliveryWorker)
}

// DrainDeliveries waits for deliveries addressed to subscribers in the current
// runtime snapshot to reach a terminal state. Producers remain asynchronous;
// callers opt into this synchronization only at an explicit lifecycle boundary.
func (runtime *Runtime) DrainDeliveries(ctx context.Context) error {
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return err
	}
	targets := make(deliveryworker.Targets, len(lease.snapshot.subscribers))
	for name, subscriber := range lease.snapshot.subscribers {
		targets[name] = newSubscriberDeliveryTarget(name, subscriber)
	}
	lease.release()
	if len(targets) == 0 {
		return nil
	}

	for {
		deliveries, err := runtime.delivery.store.ListNonTerminal(ctx)
		if err != nil {
			return fmt.Errorf("list non-terminal subscriber deliveries while draining: %w", err)
		}
		if !targets.MatchesAny(deliveries) {
			return nil
		}
		if !waitContext(ctx, runtime.delivery.poll) {
			return ctx.Err()
		}
	}
}

func (runtime *Runtime) runDeliveryWorker(worker int) {
	runner := deliveryworker.Runner{
		Store:       runtime.delivery.store,
		Logger:      runtime.logger,
		Context:     runtime.delivery.context,
		Queue:       "subscriber outbox",
		Lease:       runtime.delivery.lease,
		Poll:        runtime.delivery.poll,
		MaxAttempts: runtime.delivery.maxAttempts,
	}
	runner.Run(worker, runtime.handleSubscriberDelivery)
}

func (runtime *Runtime) handleSubscriberDelivery(
	ctx context.Context,
	delivery sdk.Delivery,
) error {
	target, err := runtime.acquireSubscriberDelivery(ctx, delivery)
	if err != nil {
		return err
	}
	defer target.release()
	return pluginpolicy.ReceiveSubscriber(
		ctx,
		target.subscriber,
		delivery,
		target.timeout,
	)
}

func (runtime *Runtime) acquireSubscriberDelivery(
	ctx context.Context,
	delivery sdk.Delivery,
) (subscriberDeliveryInvocation, error) {
	snapshotLease, err := runtime.acquireSnapshot()
	if err != nil {
		if ctx.Err() != nil {
			return subscriberDeliveryInvocation{}, ctx.Err()
		}
		return subscriberDeliveryInvocation{}, err
	}
	target, err := resolveSubscriberDeliveryBinding(
		snapshotLease.snapshot,
		delivery,
		runtime.delivery.timeout,
	)
	if err != nil {
		snapshotLease.release()
		return subscriberDeliveryInvocation{}, err
	}
	ownerLease, err := runtime.acquireMounts(target.owner)
	snapshotLease.release()
	if err != nil {
		return subscriberDeliveryInvocation{}, err
	}
	return subscriberDeliveryInvocation{
		subscriber: target.subscriber,
		timeout:    target.timeout,
		lease:      ownerLease,
	}, nil
}

func resolveSubscriberDeliveryBinding(
	snapshot *registrySnapshot,
	delivery sdk.Delivery,
	defaultTimeout time.Duration,
) (subscriberDeliveryBinding, error) {
	owned, exists := snapshot.subscribers[delivery.Subscription]
	if !exists || owned.owner.manifest.Name != delivery.Plugin {
		return subscriberDeliveryBinding{}, errors.New("subscriber is not mounted")
	}
	deliveryTarget := newSubscriberDeliveryTarget(delivery.Subscription, owned)
	if err := deliveryTarget.Validate(delivery); err != nil {
		return subscriberDeliveryBinding{}, deliveryworker.Permanent(err)
	}
	return subscriberDeliveryBinding{
		subscriber: owned.value,
		owner:      owned.owner,
		timeout: pluginpolicy.SubscriberTimeout(
			defaultTimeout,
			owned.spec.Timeout,
		),
	}, nil
}

func (target subscriberDeliveryInvocation) release() {
	target.lease.release()
}
