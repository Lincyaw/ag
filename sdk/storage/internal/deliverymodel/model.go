package deliverymodel

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

func ValidateNew(delivery sdk.Delivery) error {
	if delivery.ID == "" {
		return errors.New("delivery ID is empty")
	}
	if err := sdk.ValidateResourceName("plugin", delivery.Plugin); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName(
		"subscription",
		delivery.Subscription,
	); err != nil {
		return err
	}
	if delivery.Event.ID == "" || delivery.Event.Name == "" {
		return errors.New("delivery event ID and name are required")
	}
	if !json.Valid(delivery.Event.Payload) {
		return errors.New("delivery event payload is invalid JSON")
	}
	return nil
}

func SameIdentity(left, right sdk.Delivery) bool {
	return left.ID == right.ID &&
		left.Plugin == right.Plugin &&
		left.PluginVersion == right.PluginVersion &&
		left.Subscription == right.Subscription &&
		left.ResourceRevision == right.ResourceRevision &&
		left.Event.ID == right.Event.ID
}

func Compare(left, right sdk.Delivery) int {
	if left.Sequence != 0 || right.Sequence != 0 {
		if left.Sequence < right.Sequence {
			return -1
		}
		if left.Sequence > right.Sequence {
			return 1
		}
	}
	if order := left.AvailableAt.Compare(right.AvailableAt); order != 0 {
		return order
	}
	if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
		return order
	}
	if left.ID < right.ID {
		return -1
	}
	if left.ID > right.ID {
		return 1
	}
	return 0
}

func Partition(delivery sdk.Delivery) string {
	if delivery.Partition != "" {
		return delivery.Partition
	}
	return delivery.Plugin + "/" + delivery.Subscription
}

func ValidateLoaded(delivery sdk.Delivery) error {
	if err := ValidateNew(delivery); err != nil {
		return err
	}
	if delivery.Attempt < 0 {
		return errors.New("delivery attempt cannot be negative")
	}
	switch delivery.State {
	case sdk.DeliveryLeased:
		if delivery.Attempt == 0 ||
			delivery.LeaseToken == "" ||
			delivery.LeaseExpiresAt.IsZero() {
			return errors.New("leased delivery has an invalid lease")
		}
	case sdk.DeliveryPending,
		sdk.DeliveryDelivered,
		sdk.DeliveryDeadLetter:
		if delivery.LeaseToken != "" ||
			!delivery.LeaseExpiresAt.IsZero() {
			return errors.New("unleased delivery contains a lease")
		}
	default:
		return fmt.Errorf(
			"delivery has invalid state %q",
			delivery.State,
		)
	}
	return nil
}
