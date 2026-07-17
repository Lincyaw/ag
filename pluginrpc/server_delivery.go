package pluginrpc

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func (server *server) inboxLoop(worker int) {
	defer server.wait.Done()
	for {
		if server.context.Err() != nil {
			return
		}
		delivery, err := server.inbox.Lease(server.context, time.Now().UTC(), server.inboxLease)
		if errors.Is(err, sdk.ErrNoDelivery) {
			if !wait(server.context, server.inboxPoll) {
				return
			}
			continue
		}
		if err != nil {
			server.logger.Warn("lease plugin inbox", "worker", worker, "error", err)
			if !wait(server.context, server.inboxPoll) {
				return
			}
			continue
		}
		server.receiveDelivery(delivery)
	}
}

func (server *server) receiveDelivery(delivery sdk.Delivery) {
	subscriber, exists := server.registrar.subscribers[delivery.Subscription]
	if !exists {
		server.retryDelivery(delivery, errors.New("subscriber disappeared"))
		return
	}
	expectedRevision := sdk.ResourceRevision(
		server.manifest,
		"subscriber",
		delivery.Subscription,
		subscriber.spec,
	)
	if delivery.Plugin != server.manifest.Name ||
		(delivery.PluginVersion != "" &&
			delivery.PluginVersion != server.manifest.Version) ||
		(delivery.ResourceRevision != "" &&
			delivery.ResourceRevision != expectedRevision) {
		err := server.inbox.DeadLetter(
			server.context,
			delivery.ID,
			delivery.LeaseToken,
			time.Now().UTC(),
			fmt.Sprintf(
				"delivery target %s@%s revision %s does not match current %s@%s revision %s",
				delivery.Plugin,
				delivery.PluginVersion,
				delivery.ResourceRevision,
				server.manifest.Name,
				server.manifest.Version,
				expectedRevision,
			),
		)
		if err != nil && !errors.Is(err, context.Canceled) {
			server.logger.Warn(
				"dead-letter stale plugin delivery",
				"delivery_id",
				delivery.ID,
				"error",
				err,
			)
		}
		return
	}
	timeout := server.subscriberTimeout
	if configured := subscriber.spec.Timeout; configured > 0 && configured < timeout {
		timeout = configured
	}
	ctx, cancel := context.WithTimeout(server.context, timeout)
	err := safeReceive(ctx, subscriber.value, delivery)
	cancel()
	if err != nil {
		server.retryDelivery(delivery, err)
		return
	}
	if err := server.inbox.Ack(server.context, delivery.ID, delivery.LeaseToken, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
		server.logger.Warn("ack plugin inbox", "delivery_id", delivery.ID, "error", err)
	}
}

func (server *server) retryDelivery(delivery sdk.Delivery, cause error) {
	if server.context.Err() != nil {
		return
	}
	now := time.Now().UTC()
	var err error
	if delivery.Attempt >= server.inboxMaxAttempts {
		err = server.inbox.DeadLetter(server.context, delivery.ID, delivery.LeaseToken, now, cause.Error())
	} else {
		shift := min(max(delivery.Attempt-1, 0), 10)
		delay := min(server.inboxPoll*time.Duration(1<<shift), 30*time.Second)
		err = server.inbox.Retry(server.context, delivery.ID, delivery.LeaseToken, now.Add(delay), cause.Error())
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		server.logger.Warn("reschedule plugin inbox", "delivery_id", delivery.ID, "error", err)
	}
}

func safeReceive(ctx context.Context, subscriber sdk.Subscriber, delivery sdk.Delivery) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("subscriber panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return subscriber.Receive(ctx, delivery)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
