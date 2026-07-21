package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type memoryInteractionStore struct {
	mu      sync.Mutex
	streams map[string]interactionStream
	notify  map[string]chan struct{}
	clock   func() time.Time
	closed  bool
}

func NewMemoryInteractionStore() InteractionStore {
	return &memoryInteractionStore{
		streams: make(map[string]interactionStream),
		notify:  make(map[string]chan struct{}),
		clock:   func() time.Time { return time.Now().UTC() },
	}
}

func (store *memoryInteractionStore) Create(
	ctx context.Context,
	value Interaction,
) (Interaction, error) {
	if err := ctx.Err(); err != nil {
		return Interaction{}, err
	}
	value, err := normalizeInteraction(value)
	if err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Interaction{}, ErrStoreClosed
	}
	stream, created, changed, err := createInteraction(
		store.streams[value.SessionID], value, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		store.streams[value.SessionID] = stream
		store.signalLocked(value.SessionID)
	}
	return created, nil
}

func (store *memoryInteractionStore) Get(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	if err := ctx.Err(); err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Interaction{}, ErrStoreClosed
	}
	return getInteraction(store.streams[sessionID], id)
}

func (store *memoryInteractionStore) List(
	ctx context.Context,
	sessionID string,
	query InteractionQuery,
) (InteractionPage, error) {
	if err := ctx.Err(); err != nil {
		return InteractionPage{}, err
	}
	query, err := normalizeInteractionQuery(query)
	if err != nil {
		return InteractionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return InteractionPage{}, ErrStoreClosed
	}
	return listInteractions(store.streams[sessionID], query), nil
}

func (store *memoryInteractionStore) Wait(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	for {
		store.mu.Lock()
		if store.closed {
			store.mu.Unlock()
			return Interaction{}, ErrStoreClosed
		}
		item, err := getInteraction(store.streams[sessionID], id)
		if err == nil && item.State.Terminal() {
			store.mu.Unlock()
			return item, nil
		}
		if err != nil {
			store.mu.Unlock()
			return Interaction{}, err
		}
		notify := store.notifyLocked(sessionID)
		store.mu.Unlock()
		select {
		case <-ctx.Done():
			return Interaction{}, ctx.Err()
		case <-notify:
		}
	}
}

func (store *memoryInteractionStore) Resolve(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
	answer InteractionAnswer,
) (Interaction, error) {
	if err := ctx.Err(); err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Interaction{}, ErrStoreClosed
	}
	stream, resolved, changed, err := resolveInteraction(
		store.streams[sessionID], id, expectedRevision, answer, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		store.streams[sessionID] = stream
		store.signalLocked(sessionID)
	}
	return resolved, nil
}

func (store *memoryInteractionStore) Cancel(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
) (Interaction, error) {
	if err := ctx.Err(); err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Interaction{}, ErrStoreClosed
	}
	stream, cancelled, changed, err := cancelInteraction(
		store.streams[sessionID], id, expectedRevision, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		store.streams[sessionID] = stream
		store.signalLocked(sessionID)
	}
	return cancelled, nil
}

func (store *memoryInteractionStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	for id, notify := range store.notify {
		close(notify)
		delete(store.notify, id)
	}
	return nil
}

func getInteraction(stream interactionStream, id string) (Interaction, error) {
	for _, item := range stream.Items {
		if item.ID == id {
			return cloneInteraction(item), nil
		}
	}
	return Interaction{}, fmt.Errorf("%w: %s", ErrInteractionNotFound, id)
}

func (store *memoryInteractionStore) notifyLocked(sessionID string) chan struct{} {
	if store.notify[sessionID] == nil {
		store.notify[sessionID] = make(chan struct{})
	}
	return store.notify[sessionID]
}

func (store *memoryInteractionStore) signalLocked(sessionID string) {
	close(store.notifyLocked(sessionID))
	store.notify[sessionID] = make(chan struct{})
}
