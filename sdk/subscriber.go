package sdk

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"time"
)

func (runtime *Runtime) enqueueSubscribers(
	snapshot *registrySnapshot,
	event Event,
) {
	if len(snapshot.subscribers) == 0 {
		return
	}
	names := make([]string, 0, len(snapshot.subscribers))
	for name, subscriber := range snapshot.subscribers {
		if slices.Contains(subscriber.spec.Events, event.Name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return
	}
	slices.Sort(names)
	now := time.Now().UTC()
	deliveries := make([]Delivery, 0, len(names))
	for _, name := range names {
		subscriber := snapshot.subscribers[name]
		partition := name
		if event.SessionID != "" {
			partition += "/" + event.SessionID
		}
		deliveries = append(deliveries, Delivery{
			ID:           event.ID + "." + name,
			Plugin:       subscriber.owner.manifest.Name,
			Subscription: name,
			Partition:    partition,
			Event:        cloneEvent(event),
			CreatedAt:    now,
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.outbox.Enqueue(ctx, deliveries...); err != nil {
		runtime.logger.ErrorContext(
			ctx,
			"enqueue subscriber deliveries",
			"event",
			event.Name,
			"event_id",
			event.ID,
			"error",
			err,
		)
	}
}

func (runtime *Runtime) startDeliveryWorkers() {
	runtime.deliveryOnce.Do(func() {
		for worker := range runtime.deliveryWorkers {
			runtime.deliveryWait.Add(1)
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
		deliveries, err := runtime.outbox.List(ctx)
		if err != nil {
			return fmt.Errorf("list subscriber deliveries while draining: %w", err)
		}
		pending := false
		for _, delivery := range deliveries {
			plugin, exists := targets[delivery.Subscription]
			if !exists || plugin != delivery.Plugin {
				continue
			}
			if delivery.State != DeliveryDelivered &&
				delivery.State != DeliveryDeadLetter {
				pending = true
				break
			}
		}
		if !pending {
			return nil
		}
		if !waitContext(ctx, runtime.deliveryPoll) {
			return ctx.Err()
		}
	}
}

func (runtime *Runtime) deliveryLoop(worker int) {
	defer runtime.deliveryWait.Done()
	for {
		if err := runtime.deliveryContext.Err(); err != nil {
			return
		}
		delivery, err := runtime.outbox.Lease(
			runtime.deliveryContext,
			time.Now().UTC(),
			runtime.deliveryLease,
		)
		if errors.Is(err, ErrNoDelivery) {
			if !waitContext(runtime.deliveryContext, runtime.deliveryPoll) {
				return
			}
			continue
		}
		if err != nil {
			if runtime.deliveryContext.Err() != nil {
				return
			}
			runtime.logger.WarnContext(
				runtime.deliveryContext,
				"lease subscriber delivery",
				"worker",
				worker,
				"error",
				err,
			)
			if !waitContext(runtime.deliveryContext, runtime.deliveryPoll) {
				return
			}
			continue
		}
		runtime.deliver(delivery)
	}
}

func (runtime *Runtime) deliver(delivery Delivery) {
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
	timeout := runtime.deliveryTimeout
	if owned.spec.Timeout > 0 && owned.spec.Timeout < timeout {
		timeout = owned.spec.Timeout
	}
	ctx, cancel := context.WithTimeout(runtime.deliveryContext, timeout)
	err = receiveSubscriber(ctx, owned.subscriber, cloneDelivery(delivery))
	cancel()
	lease.release()
	if err != nil {
		runtime.retryDelivery(delivery, err)
		return
	}
	if err := runtime.outbox.Ack(
		runtime.deliveryContext,
		delivery.ID,
		delivery.LeaseToken,
		time.Now().UTC(),
	); err != nil && !errors.Is(err, context.Canceled) {
		runtime.logger.WarnContext(
			runtime.deliveryContext,
			"ack subscriber delivery",
			"delivery_id",
			delivery.ID,
			"error",
			err,
		)
	}
}

func receiveSubscriber(
	ctx context.Context,
	subscriber Subscriber,
	delivery Delivery,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("subscriber panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return subscriber.Receive(ctx, delivery)
}

func (runtime *Runtime) retryDelivery(delivery Delivery, cause error) {
	if runtime.deliveryContext.Err() != nil {
		return
	}
	now := time.Now().UTC()
	var err error
	if delivery.Attempt >= runtime.deliveryMaxAttempts {
		err = runtime.outbox.DeadLetter(
			runtime.deliveryContext,
			delivery.ID,
			delivery.LeaseToken,
			now,
			cause.Error(),
		)
	} else {
		err = runtime.outbox.Retry(
			runtime.deliveryContext,
			delivery.ID,
			delivery.LeaseToken,
			now.Add(runtime.deliveryBackoff(delivery.Attempt)),
			cause.Error(),
		)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		runtime.logger.WarnContext(
			runtime.deliveryContext,
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
	delay := runtime.deliveryPoll * time.Duration(1<<shift)
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
