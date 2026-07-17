package runtime

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

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

func (runtime *Runtime) enqueueSubscribers(
	ctx context.Context,
	snapshot *registrySnapshot,
	event sdk.Event,
) error {
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
	now := time.Now().UTC()
	deliveries := make([]sdk.Delivery, 0, len(names))
	for _, name := range names {
		subscriber := snapshot.subscribers[name]
		revision := sdk.ResourceRevision(
			subscriber.owner.manifest,
			"subscriber",
			name,
			subscriber.spec,
		)
		partition := name
		if event.SessionID != "" {
			partition += "/" + event.SessionID
		}
		deliveries = append(deliveries, sdk.Delivery{
			ID:               event.ID + "." + name + "." + revision[:12],
			Plugin:           subscriber.owner.manifest.Name,
			PluginVersion:    subscriber.owner.manifest.Version,
			Subscription:     name,
			ResourceRevision: revision,
			Partition:        partition,
			Event:            sdk.CloneEvent(event),
			CreatedAt:        now,
		})
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

// startDeliveryWorkersLocked is called while runtime.mu prevents Close from
// waiting on delivery workers.
func (runtime *Runtime) startDeliveryWorkersLocked() {
	runtime.delivery.once.Do(func() {
		for worker := range runtime.delivery.workers {
			runtime.delivery.wait.Add(1)
			go runtime.deliveryLoop(worker)
		}
	})
}

// DrainDeliveries waits for deliveries addressed to subscribers in the current
// runtime snapshot to reach a terminal state. Producers remain asynchronous;
// callers opt into this synchronization only at an explicit lifecycle boundary.
func (runtime *Runtime) DrainDeliveries(ctx context.Context) error {
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return err
	}
	targets := make(map[string]string, len(lease.snapshot.subscribers))
	for name, subscriber := range lease.snapshot.subscribers {
		targets[name] = subscriber.owner.manifest.Name
	}
	lease.release()
	if len(targets) == 0 {
		return nil
	}

	for {
		deliveries, err := runtime.delivery.store.List(ctx)
		if err != nil {
			return fmt.Errorf("list subscriber deliveries while draining: %w", err)
		}
		pending := false
		for _, delivery := range deliveries {
			plugin, exists := targets[delivery.Subscription]
			if !exists || plugin != delivery.Plugin {
				continue
			}
			if delivery.State != sdk.DeliveryDelivered &&
				delivery.State != sdk.DeliveryDeadLetter {
				pending = true
				break
			}
		}
		if !pending {
			return nil
		}
		if !waitContext(ctx, runtime.delivery.poll) {
			return ctx.Err()
		}
	}
}

func (runtime *Runtime) deliveryLoop(worker int) {
	defer runtime.delivery.wait.Done()
	for {
		if err := runtime.delivery.context.Err(); err != nil {
			return
		}
		delivery, err := runtime.delivery.store.Lease(
			runtime.delivery.context,
			time.Now().UTC(),
			runtime.delivery.lease,
		)
		if errors.Is(err, sdk.ErrNoDelivery) {
			if !waitContext(runtime.delivery.context, runtime.delivery.poll) {
				return
			}
			continue
		}
		if err != nil {
			if runtime.delivery.context.Err() != nil {
				return
			}
			runtime.logger.WarnContext(
				runtime.delivery.context,
				"lease subscriber delivery",
				"worker",
				worker,
				"error",
				err,
			)
			if !waitContext(runtime.delivery.context, runtime.delivery.poll) {
				return
			}
			continue
		}
		runtime.deliver(delivery)
	}
}

func (runtime *Runtime) deliver(delivery sdk.Delivery) {
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return
	}
	owned, exists := lease.snapshot.subscribers[delivery.Subscription]
	if !exists || owned.owner.manifest.Name != delivery.Plugin {
		lease.release()
		runtime.retryDelivery(delivery, errors.New("subscriber is not mounted"))
		return
	}
	currentRevision := sdk.ResourceRevision(
		owned.owner.manifest,
		"subscriber",
		delivery.Subscription,
		owned.spec,
	)
	if (delivery.PluginVersion != "" &&
		delivery.PluginVersion != owned.owner.manifest.Version) ||
		(delivery.ResourceRevision != "" &&
			delivery.ResourceRevision != currentRevision) {
		lease.release()
		runtime.deadLetterDelivery(
			delivery,
			fmt.Errorf(
				"delivery targets plugin %s@%s resource revision %s; current target is %s@%s revision %s",
				delivery.Plugin,
				delivery.PluginVersion,
				delivery.ResourceRevision,
				owned.owner.manifest.Name,
				owned.owner.manifest.Version,
				currentRevision,
			),
		)
		return
	}
	timeout := runtime.delivery.timeout
	if owned.spec.Timeout > 0 && owned.spec.Timeout < timeout {
		timeout = owned.spec.Timeout
	}
	ctx, cancel := context.WithTimeout(runtime.delivery.context, timeout)
	err = receiveSubscriber(ctx, owned.value, sdk.CloneDelivery(delivery))
	cancel()
	lease.release()
	if err != nil {
		runtime.retryDelivery(delivery, err)
		return
	}
	if err := runtime.delivery.store.Ack(
		runtime.delivery.context,
		delivery.ID,
		delivery.LeaseToken,
		time.Now().UTC(),
	); err != nil && !errors.Is(err, context.Canceled) {
		runtime.logger.WarnContext(
			runtime.delivery.context,
			"ack subscriber delivery",
			"delivery_id",
			delivery.ID,
			"error",
			err,
		)
	}
}

func (runtime *Runtime) deadLetterDelivery(
	delivery sdk.Delivery,
	cause error,
) {
	err := runtime.delivery.store.DeadLetter(
		runtime.delivery.context,
		delivery.ID,
		delivery.LeaseToken,
		time.Now().UTC(),
		cause.Error(),
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		runtime.logger.WarnContext(
			runtime.delivery.context,
			"dead-letter subscriber delivery",
			"delivery_id",
			delivery.ID,
			"error",
			err,
		)
	}
}

func receiveSubscriber(
	ctx context.Context,
	subscriber sdk.Subscriber,
	delivery sdk.Delivery,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("subscriber panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return subscriber.Receive(ctx, delivery)
}

func (runtime *Runtime) retryDelivery(delivery sdk.Delivery, cause error) {
	if runtime.delivery.context.Err() != nil {
		return
	}
	now := time.Now().UTC()
	var err error
	if delivery.Attempt >= runtime.delivery.maxAttempts {
		err = runtime.delivery.store.DeadLetter(
			runtime.delivery.context,
			delivery.ID,
			delivery.LeaseToken,
			now,
			cause.Error(),
		)
	} else {
		err = runtime.delivery.store.Retry(
			runtime.delivery.context,
			delivery.ID,
			delivery.LeaseToken,
			now.Add(runtime.deliveryBackoff(delivery.Attempt)),
			cause.Error(),
		)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		runtime.logger.WarnContext(
			runtime.delivery.context,
			"reschedule subscriber delivery",
			"delivery_id",
			delivery.ID,
			"attempt",
			delivery.Attempt,
			"error",
			err,
		)
	}
}

func (runtime *Runtime) deliveryBackoff(attempt int) time.Duration {
	shift := min(max(attempt-1, 0), 10)
	delay := runtime.delivery.poll * time.Duration(1<<shift)
	return min(delay, 30*time.Second)
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
