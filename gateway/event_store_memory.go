package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type memoryEventStore struct {
	mu      sync.Mutex
	streams map[string]eventStream
	notify  map[string]chan struct{}
	clock   func() time.Time
	closed  bool
}

func NewMemoryEventStore() EventStore {
	return &memoryEventStore{
		streams: make(map[string]eventStream),
		notify:  make(map[string]chan struct{}),
		clock:   func() time.Time { return time.Now().UTC() },
	}
}

func (store *memoryEventStore) Append(
	ctx context.Context,
	sessionID string,
	event sdk.Event,
) (AgentEvent, error) {
	if err := ctx.Err(); err != nil {
		return AgentEvent{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AgentEvent{}, err
	}
	event, err = normalizeRuntimeEvent(event)
	if err != nil {
		return AgentEvent{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentEvent{}, ErrStoreClosed
	}
	stream, created, changed, err := appendAgentEvent(
		store.streams[sessionID],
		sessionID,
		event,
		store.clock(),
	)
	if err != nil {
		return AgentEvent{}, err
	}
	if !changed {
		return created, nil
	}
	store.streams[sessionID] = stream
	store.signalLocked(sessionID)
	return cloneAgentEvent(created), nil
}

func (store *memoryEventStore) List(
	ctx context.Context,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	if err := ctx.Err(); err != nil {
		return EventPage{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return EventPage{}, err
	}
	query, err = normalizeEventQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return EventPage{}, ErrStoreClosed
	}
	return listAgentEvents(store.streams[sessionID], query), nil
}

func (store *memoryEventStore) Latest(
	ctx context.Context,
	sessionID string,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return 0, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrStoreClosed
	}
	return latestAgentEventSequence(store.streams[sessionID]), nil
}

func (store *memoryEventStore) Wait(
	ctx context.Context,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return EventPage{}, err
	}
	query, err = normalizeEventQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	for {
		store.mu.Lock()
		if store.closed {
			store.mu.Unlock()
			return EventPage{}, ErrStoreClosed
		}
		page := listAgentEvents(store.streams[sessionID], query)
		if len(page.Items) > 0 {
			store.mu.Unlock()
			return page, nil
		}
		notify := store.notifyLocked(sessionID)
		store.mu.Unlock()
		select {
		case <-ctx.Done():
			return EventPage{}, ctx.Err()
		case <-notify:
		}
	}
}

func (store *memoryEventStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	for sessionID, notify := range store.notify {
		close(notify)
		delete(store.notify, sessionID)
	}
	return nil
}

func (store *memoryEventStore) notifyLocked(sessionID string) chan struct{} {
	notify := store.notify[sessionID]
	if notify == nil {
		notify = make(chan struct{})
		store.notify[sessionID] = notify
	}
	return notify
}

func (store *memoryEventStore) signalLocked(sessionID string) {
	notify := store.notifyLocked(sessionID)
	close(notify)
	store.notify[sessionID] = make(chan struct{})
}
