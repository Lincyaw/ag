package pluginrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/lincyaw/ag/internal/deliveryworker"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverDeliveryTarget struct {
	cause      error
	rpcCode    codes.Code
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
	if err := target.permanentRejection(); err != nil {
		return deliveryworker.Permanent(err)
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
		return rejectServerDeliveryTarget(
			codes.InvalidArgument,
			fmt.Errorf(
				"delivery targets plugin %q",
				delivery.Plugin,
			),
		)
	}
	subscriber, exists := server.registrar.Subscribers[delivery.Subscription]
	if !exists {
		return rejectServerDeliveryTarget(
			codes.NotFound,
			fmt.Errorf(
				"subscriber %q not found",
				delivery.Subscription,
			),
		)
	}
	if err := server.subscriberDeliveryTarget(
		delivery.Subscription,
	).Validate(delivery); err != nil {
		return rejectServerDeliveryTarget(codes.FailedPrecondition, err)
	}
	return serverDeliveryTarget{
		subscriber: subscriber.Value,
		timeout: pluginpolicy.SubscriberTimeout(
			server.subscriberTimeout,
			subscriber.Spec.Timeout,
		),
	}
}

func rejectServerDeliveryTarget(
	rpcCode codes.Code,
	cause error,
) serverDeliveryTarget {
	return serverDeliveryTarget{
		cause:   cause,
		rpcCode: rpcCode,
	}
}

func (target serverDeliveryTarget) permanentRejection() error {
	return target.cause
}

func (target serverDeliveryTarget) rpcRejection() error {
	if target.cause == nil {
		return nil
	}
	return status.Error(target.rpcCode, target.cause.Error())
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
