package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultEventPageSize      = 100
	maxEventPageSize          = 1000
	maxEncodedEventPageBytes  = 4 << 20
	eventPageEnvelopeOverhead = 64
)

// AgentEvent is the gateway-owned, reconnectable projection of one runtime
// event. Sequence is scoped to SessionID and is the only stream cursor clients
// need to persist.
type AgentEvent struct {
	Sequence   uint64          `json:"sequence"`
	SessionID  string          `json:"session_id"`
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Generation uint64          `json:"generation"`
	Payload    json.RawMessage `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
}

type EventQuery struct {
	After uint64 `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type EventPage struct {
	Items []AgentEvent `json:"items"`
	Next  uint64       `json:"next,omitempty"`
}

type EventCursor struct {
	Sequence uint64 `json:"sequence"`
}

// EventStore is the durable handoff between background runtimes and clients.
// Append is idempotent by runtime event ID. Wait returns immediately when a
// matching event already exists and otherwise blocks until append, close, or
// caller cancellation.
type EventStore interface {
	Append(context.Context, string, sdk.Event) (AgentEvent, error)
	Latest(context.Context, string) (uint64, error)
	List(context.Context, string, EventQuery) (EventPage, error)
	Wait(context.Context, string, EventQuery) (EventPage, error)
	Close(context.Context) error
}

type eventStream struct {
	NextSequence uint64       `json:"next_sequence"`
	Events       []AgentEvent `json:"events"`
	byID         map[string]int
}

func normalizeEventQuery(query EventQuery) (EventQuery, error) {
	if query.Limit == 0 {
		query.Limit = defaultEventPageSize
	}
	if query.Limit < 1 || query.Limit > maxEventPageSize {
		return EventQuery{}, fmt.Errorf(
			"gateway event page limit must be between 1 and %d",
			maxEventPageSize,
		)
	}
	return query, nil
}

func normalizeEventSessionID(sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if err := sdk.ValidateResourceName("gateway event session", sessionID); err != nil {
		return "", err
	}
	return sessionID, nil
}

func normalizeRuntimeEvent(event sdk.Event) (sdk.Event, error) {
	event.ID = strings.TrimSpace(event.ID)
	event.Name = strings.TrimSpace(event.Name)
	if err := sdk.ValidateResourceName("gateway runtime event", event.ID); err != nil {
		return sdk.Event{}, err
	}
	if event.Name == "" {
		return sdk.Event{}, errors.New("gateway runtime event name is empty")
	}
	if !json.Valid(event.Payload) {
		return sdk.Event{}, fmt.Errorf(
			"gateway runtime event %q payload is invalid JSON",
			event.ID,
		)
	}
	event = sdk.CloneEvent(event)
	if err := projectRuntimeEventPayload(&event); err != nil {
		return sdk.Event{}, fmt.Errorf(
			"project gateway runtime event %q: %w",
			event.Name,
			err,
		)
	}
	return event, nil
}

// projectRuntimeEventPayload retains the incremental fields needed by a view
// without duplicating the full conversation in the reconnect cursor log.
// Historical messages are loaded from the trajectory conversation projection.
func projectRuntimeEventPayload(event *sdk.Event) error {
	switch event.Name {
	case sdk.EventBeforeAgentStart:
		payload := sdk.BeforeAgentStartPayload{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		payload.Messages = nil
		payload.System = ""
		return remarshalRuntimeEventPayload(event, payload)
	case sdk.EventAgentStart:
		payload := sdk.AgentStartPayload{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		payload.Messages = latestGatewayUserMessage(payload.Messages)
		payload.System = ""
		return remarshalRuntimeEventPayload(event, payload)
	case sdk.EventBeforeProvider:
		payload := sdk.BeforeProviderPayload{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		payload.Messages = nil
		payload.System = ""
		payload.Tools = nil
		return remarshalRuntimeEventPayload(event, payload)
	case sdk.EventTurnEnd:
		payload := sdk.TurnEndPayload{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		payload.Messages = nil
		return remarshalRuntimeEventPayload(event, payload)
	case sdk.EventAgentEnd:
		payload := sdk.AgentEndPayload{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		payload.Messages = nil
		payload.ContextInjections = nil
		return remarshalRuntimeEventPayload(event, payload)
	default:
		return nil
	}
}

func remarshalRuntimeEventPayload(event *sdk.Event, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	event.Payload = raw
	return nil
}

func latestGatewayUserMessage(messages []sdk.Message) []sdk.Message {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == sdk.RoleUser {
			return sdk.CloneMessages(messages[index : index+1])
		}
	}
	return nil
}

func appendAgentEvent(
	stream eventStream,
	sessionID string,
	event sdk.Event,
	now time.Time,
) (eventStream, AgentEvent, bool, error) {
	if index, exists := stream.byID[event.ID]; exists {
		return stream, cloneAgentEvent(stream.Events[index]), false, nil
	}
	if stream.byID == nil {
		stream.byID = make(map[string]int, len(stream.Events)+1)
		for index, current := range stream.Events {
			stream.byID[current.ID] = index
			if current.ID == event.ID {
				return stream, cloneAgentEvent(current), false, nil
			}
		}
	}
	if stream.NextSequence == 0 {
		stream.NextSequence = 1
	}
	if stream.NextSequence == math.MaxUint64 {
		return eventStream{}, AgentEvent{}, false, errors.New(
			"gateway event sequence is exhausted",
		)
	}
	created := AgentEvent{
		Sequence:   stream.NextSequence,
		SessionID:  sessionID,
		ID:         event.ID,
		Name:       event.Name,
		Generation: event.Generation,
		Payload:    append(json.RawMessage(nil), event.Payload...),
		CreatedAt:  now.UTC(),
	}
	stream.NextSequence++
	stream.Events = append(stream.Events, cloneAgentEvent(created))
	stream.byID[created.ID] = len(stream.Events) - 1
	return stream, created, true, nil
}

func listAgentEvents(stream eventStream, query EventQuery) EventPage {
	page := EventPage{Items: make([]AgentEvent, 0, query.Limit)}
	encodedBytes := eventPageEnvelopeOverhead
	start := sort.Search(len(stream.Events), func(index int) bool {
		return stream.Events[index].Sequence > query.After
	})
	for _, event := range stream.Events[start:] {
		if !appendEventPageItem(&page, event, &encodedBytes) {
			break
		}
		if len(page.Items) == query.Limit {
			break
		}
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page
}

func appendEventPageItem(
	page *EventPage,
	event AgentEvent,
	encodedBytes *int,
) bool {
	raw, err := json.Marshal(event)
	if err != nil {
		return false
	}
	itemBytes := len(raw) + 1
	if len(page.Items) > 0 &&
		*encodedBytes+itemBytes > maxEncodedEventPageBytes {
		return false
	}
	page.Items = append(page.Items, cloneAgentEvent(event))
	*encodedBytes += itemBytes
	return true
}

func latestAgentEventSequence(stream eventStream) uint64 {
	if len(stream.Events) == 0 {
		return 0
	}
	return stream.Events[len(stream.Events)-1].Sequence
}

func cloneAgentEvent(event AgentEvent) AgentEvent {
	event.Payload = append(json.RawMessage(nil), event.Payload...)
	return event
}

func cloneEventStream(stream eventStream) eventStream {
	result := eventStream{
		NextSequence: stream.NextSequence,
		Events:       make([]AgentEvent, len(stream.Events)),
		byID:         make(map[string]int, len(stream.Events)),
	}
	for index := range stream.Events {
		result.Events[index] = cloneAgentEvent(stream.Events[index])
		result.byID[stream.Events[index].ID] = index
	}
	return result
}

func validateStoredEventStream(
	sessionID string,
	stream eventStream,
) (eventStream, error) {
	if stream.NextSequence == 0 {
		stream.NextSequence = 1
	}
	var previous uint64
	seen := make(map[string]struct{}, len(stream.Events))
	for index, event := range stream.Events {
		if event.SessionID != sessionID || event.Sequence == 0 ||
			event.Sequence <= previous || event.CreatedAt.IsZero() {
			return eventStream{}, fmt.Errorf(
				"gateway event stream %q contains invalid event at index %d",
				sessionID,
				index,
			)
		}
		if _, duplicate := seen[event.ID]; duplicate {
			return eventStream{}, fmt.Errorf(
				"gateway event stream %q contains duplicate event %q",
				sessionID,
				event.ID,
			)
		}
		seen[event.ID] = struct{}{}
		if _, err := normalizeRuntimeEvent(sdk.Event{
			ID: event.ID, Name: event.Name, SessionID: event.SessionID,
			Generation: event.Generation, Payload: event.Payload,
		}); err != nil {
			return eventStream{}, fmt.Errorf(
				"validate gateway event stream %q: %w",
				sessionID,
				err,
			)
		}
		event.CreatedAt = event.CreatedAt.UTC()
		stream.Events[index] = cloneAgentEvent(event)
		previous = event.Sequence
	}
	if previous >= stream.NextSequence {
		return eventStream{}, fmt.Errorf(
			"gateway event stream %q next sequence %d does not follow %d",
			sessionID,
			stream.NextSequence,
			previous,
		)
	}
	stream.byID = make(map[string]int, len(stream.Events))
	for index, event := range stream.Events {
		stream.byID[event.ID] = index
	}
	return cloneEventStream(stream), nil
}
