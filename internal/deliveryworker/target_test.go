package deliveryworker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestTargetRecordSnapshotsEventAndAddressesSubscriber(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	target := Target{
		Plugin:           "observer",
		PluginVersion:    "1.0.0",
		Subscription:     "watch",
		ResourceRevision: "1234567890abcdef",
	}
	event := sdk.Event{
		ID:        "event-1",
		Name:      sdk.EventAgentEnd,
		SessionID: "session-a",
		Payload:   json.RawMessage(`{"done":true}`),
	}

	delivery := target.Record(event, now)
	event.Payload[0] = '['

	if delivery.ID != "event-1.watch.1234567890abcdef" ||
		delivery.Partition != "watch/session-a" ||
		delivery.Plugin != target.Plugin ||
		delivery.PluginVersion != target.PluginVersion ||
		delivery.Subscription != target.Subscription ||
		delivery.ResourceRevision != target.ResourceRevision ||
		!delivery.CreatedAt.Equal(now) ||
		string(delivery.Event.Payload) != `{"done":true}` {
		t.Fatalf("delivery = %#v", delivery)
	}
	if err := target.Validate(delivery); err != nil {
		t.Fatalf("target does not validate delivery: %v", err)
	}
}
