package deliverymodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

func PrepareNewBatch(
	deliveries []sdk.Delivery,
	now time.Time,
) ([]sdk.Delivery, error) {
	prepared := make([]sdk.Delivery, len(deliveries))
	byID := make(map[string]sdk.Delivery, len(deliveries))
	for index, delivery := range deliveries {
		normalized, err := PrepareNew(delivery, now)
		if err != nil {
			return nil, err
		}
		if existing, duplicate := byID[normalized.ID]; duplicate &&
			!SameIdentity(existing, normalized) {
			return nil, fmt.Errorf(
				"delivery %q appears more than once with different identity",
				normalized.ID,
			)
		}
		byID[normalized.ID] = normalized
		prepared[index] = normalized
	}
	return prepared, nil
}

func PrepareNew(delivery sdk.Delivery, now time.Time) (sdk.Delivery, error) {
	if err := ValidateNew(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	now = NormalizeMutationTime(now)
	createdAt := delivery.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = now
	}
	availableAt := delivery.AvailableAt.UTC()
	if availableAt.IsZero() {
		availableAt = createdAt
	}
	delivery.State = sdk.DeliveryPending
	delivery.Attempt = 0
	delivery.AvailableAt = availableAt
	delivery.LeaseToken = ""
	delivery.LeaseExpiresAt = time.Time{}
	delivery.Partition = Partition(delivery)
	delivery.CreatedAt = createdAt
	delivery.UpdatedAt = createdAt
	delivery.Event = sdk.CloneEvent(delivery.Event)
	return delivery, nil
}

func SameIdentity(left, right sdk.Delivery) bool {
	return left.ID == right.ID &&
		left.Plugin == right.Plugin &&
		left.PluginVersion == right.PluginVersion &&
		left.Subscription == right.Subscription &&
		left.ResourceRevision == right.ResourceRevision &&
		left.Partition == right.Partition &&
		left.Event.ID == right.Event.ID &&
		left.Event.Name == right.Event.Name &&
		left.Event.SessionID == right.Event.SessionID &&
		left.Event.Generation == right.Event.Generation &&
		bytes.Equal(left.Event.Payload, right.Event.Payload)
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

func LeaseAvailable(delivery sdk.Delivery, now time.Time) bool {
	now = NormalizeMutationTime(now)
	switch delivery.State {
	case sdk.DeliveryPending:
		return !delivery.AvailableAt.After(now)
	case sdk.DeliveryLeased:
		return !delivery.LeaseExpiresAt.After(now)
	default:
		return false
	}
}

func Lease(
	delivery sdk.Delivery,
	token string,
	now time.Time,
	duration time.Duration,
) (sdk.Delivery, error) {
	if err := ValidateLeaseDuration(duration); err != nil {
		return sdk.Delivery{}, err
	}
	if token == "" {
		return sdk.Delivery{}, errors.New("delivery lease token is empty")
	}
	now = NormalizeMutationTime(now)
	if !LeaseAvailable(delivery, now) {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	delivery.State = sdk.DeliveryLeased
	delivery.Attempt++
	delivery.LeaseToken = token
	delivery.LeaseExpiresAt = now.Add(duration)
	delivery.UpdatedAt = now
	if err := ValidateLoaded(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	return sdk.CloneDelivery(delivery), nil
}

func FinishLease(
	delivery sdk.Delivery,
	token string,
	now time.Time,
	state sdk.DeliveryState,
	availableAt time.Time,
	lastError string,
) (sdk.Delivery, error) {
	now = NormalizeMutationTime(now)
	if delivery.State != sdk.DeliveryLeased || delivery.LeaseToken != token {
		return sdk.Delivery{}, fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, delivery.ID)
	}
	switch state {
	case sdk.DeliveryPending,
		sdk.DeliveryDelivered,
		sdk.DeliveryDeadLetter:
	default:
		return sdk.Delivery{}, fmt.Errorf(
			"leased delivery cannot transition to %q",
			state,
		)
	}
	delivery.State = state
	delivery.AvailableAt = availableAt.UTC()
	delivery.LeaseToken = ""
	delivery.LeaseExpiresAt = time.Time{}
	if state != sdk.DeliveryDelivered || lastError != "" {
		delivery.LastError = lastError
	}
	delivery.UpdatedAt = now
	if err := ValidateLoaded(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	return sdk.CloneDelivery(delivery), nil
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

func ValidateLeaseDuration(duration time.Duration) error {
	if duration <= 0 {
		return errors.New("delivery lease duration must be positive")
	}
	return nil
}

func NormalizeMutationTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}
