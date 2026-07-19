package pluginrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/lincyaw/ag/internal/deliveryworker"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
)

type serverDeliveryTargetState uint8

const (
	serverDeliveryTargetReady serverDeliveryTargetState = iota
	serverDeliveryTargetWrongPlugin
	serverDeliveryTargetMissing
	serverDeliveryTargetStale
)

type serverDeliveryTarget struct {
	state      serverDeliveryTargetState
	cause      error
	subscriber sdk.Subscriber
	timeout    time.Duration
}

func (server *server) inboxLoop(worker int) {
	defer server.wait.Done()
	runner := deliveryworker.Runner{
		Store:       server.inbox,
		Logger:      server.logger,
		Context:     server.context,
		Queue:       "plugin inbox",
		Lease:       server.inboxLease,
		Poll:        server.inboxPoll,
		MaxAttempts: server.inboxMaxAttempts,
	}
	runner.Run(worker, server.receiveDelivery)
}

func (server *server) receiveDelivery(
	ctx context.Context,
	delivery sdk.Delivery,
) error {
	target := server.resolveDeliveryTarget(delivery)
	switch target.state {
	case serverDeliveryTargetReady:
	default:
		return deliveryworker.Permanent(target.cause)
	}
	return pluginpolicy.ReceiveSubscriber(
		ctx,
		target.subscriber,
		delivery,
		target.timeout,
	)
}

func (server *server) resolveDeliveryTarget(
	delivery sdk.Delivery,
) serverDeliveryTarget {
	if delivery.Plugin != server.manifest.Name {
		return serverDeliveryTarget{
			state: serverDeliveryTargetWrongPlugin,
			cause: fmt.Errorf(
				"delivery targets plugin %q",
				delivery.Plugin,
			),
		}
	}
	subscriber, exists := server.registrar.Subscribers[delivery.Subscription]
	if !exists {
		return serverDeliveryTarget{
			state: serverDeliveryTargetMissing,
			cause: fmt.Errorf(
				"subscriber %q not found",
				delivery.Subscription,
			),
		}
	}
	if err := server.subscriberDeliveryTarget(
		delivery.Subscription,
	).Validate(delivery); err != nil {
		return serverDeliveryTarget{
			state: serverDeliveryTargetStale,
			cause: err,
		}
	}
	return serverDeliveryTarget{
		state:      serverDeliveryTargetReady,
		subscriber: subscriber.Value,
		timeout: pluginpolicy.SubscriberTimeout(
			server.subscriberTimeout,
			subscriber.Spec.Timeout,
		),
	}
}

func (server *server) subscriberDeliveryTarget(
	subscription string,
) deliveryworker.Target {
	return deliveryworker.Target{
		Plugin:        server.manifest.Name,
		PluginVersion: server.manifest.Version,
		Subscription:  subscription,
		ResourceRevision: server.registrar.ResourceRevision(
			server.manifest,
			sdk.ResourceKindSubscriber,
			subscription,
		),
	}
}
