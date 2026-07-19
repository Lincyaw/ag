package deliveryworker

import (
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

// Target is the stable subscriber address for a durable delivery. Empty
// delivery version or revision fields are treated as legacy-compatible.
type Target struct {
	Plugin           string
	PluginVersion    string
	Subscription     string
	ResourceRevision string
}

// Targets is a snapshot of stable subscriber delivery addresses keyed by
// subscription name.
type Targets map[string]Target

func (targets Targets) Matches(delivery sdk.Delivery) bool {
	target, exists := targets[delivery.Subscription]
	return exists && target.Matches(delivery)
}

func (targets Targets) MatchesAny(deliveries []sdk.Delivery) bool {
	for _, delivery := range deliveries {
		if targets.Matches(delivery) {
			return true
		}
	}
	return false
}

func (target Target) Record(event sdk.Event, now time.Time) sdk.Delivery {
	return sdk.Delivery{
		ID:               target.deliveryID(event),
		Plugin:           target.Plugin,
		PluginVersion:    target.PluginVersion,
		Subscription:     target.Subscription,
		ResourceRevision: target.ResourceRevision,
		Partition:        target.partition(event),
		Event:            sdk.CloneEvent(event),
		CreatedAt:        now,
	}
}

func (target Target) Matches(delivery sdk.Delivery) bool {
	return target.Validate(delivery) == nil
}

func (target Target) Validate(delivery sdk.Delivery) error {
	if delivery.Plugin != target.Plugin ||
		delivery.Subscription != target.Subscription {
		return fmt.Errorf(
			"delivery targets plugin %s subscriber %s; current target is plugin %s subscriber %s",
			delivery.Plugin,
			delivery.Subscription,
			target.Plugin,
			target.Subscription,
		)
	}
	if delivery.PluginVersion != "" &&
		delivery.PluginVersion != target.PluginVersion {
		return fmt.Errorf(
			"delivery targets plugin version %q, current version is %q",
			delivery.PluginVersion,
			target.PluginVersion,
		)
	}
	if delivery.ResourceRevision != "" &&
		delivery.ResourceRevision != target.ResourceRevision {
		return fmt.Errorf(
			"delivery resource revision %q does not match current revision %q",
			delivery.ResourceRevision,
			target.ResourceRevision,
		)
	}
	return nil
}

func (target Target) deliveryID(event sdk.Event) string {
	id := event.ID + "." + target.Subscription
	if target.ResourceRevision == "" {
		return id
	}
	return id + "." + target.ResourceRevision
}

func (target Target) partition(event sdk.Event) string {
	partition := target.Subscription
	if event.SessionID != "" {
		partition += "/" + event.SessionID
	}
	return partition
}
