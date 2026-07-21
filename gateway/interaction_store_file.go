package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
)

const fileInteractionSchemaVersion uint32 = 1

type fileInteractionState struct {
	SchemaVersion uint32                       `json:"schema_version"`
	Streams       map[string]interactionStream `json:"streams"`
}

type fileInteractionStore struct {
	mu        sync.Mutex
	directory string
	statePath string
	notify    map[string]chan struct{}
	clock     func() time.Time
	closed    bool
}

func NewFileInteractionStore(directory string) (InteractionStore, error) {
	absolute, err := filestate.PrepareDirectory(
		"gateway interaction store",
		directory,
	)
	if err != nil {
		return nil, err
	}
	store := &fileInteractionStore{
		directory: absolute,
		statePath: filepath.Join(absolute, "interactions.json"),
		notify:    make(map[string]chan struct{}),
		clock:     func() time.Time { return time.Now().UTC() },
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.readLocked(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *fileInteractionStore) Create(
	ctx context.Context,
	value Interaction,
) (Interaction, error) {
	value, err := normalizeInteraction(value)
	if err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return Interaction{}, err
	}
	stream, created, changed, err := createInteraction(
		state.Streams[value.SessionID], value, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		state.Streams[value.SessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return Interaction{}, err
		}
		store.signalLocked(value.SessionID)
	}
	return created, nil
}

func (store *fileInteractionStore) Get(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return Interaction{}, err
	}
	return getInteraction(state.Streams[sessionID], id)
}

func (store *fileInteractionStore) List(
	ctx context.Context,
	sessionID string,
	query InteractionQuery,
) (InteractionPage, error) {
	query, err := normalizeInteractionQuery(query)
	if err != nil {
		return InteractionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return InteractionPage{}, err
	}
	return listInteractions(state.Streams[sessionID], query), nil
}

func (store *fileInteractionStore) Wait(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	for {
		store.mu.Lock()
		state, err := store.stateLocked(ctx)
		if err != nil {
			store.mu.Unlock()
			return Interaction{}, err
		}
		item, err := getInteraction(state.Streams[sessionID], id)
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

func (store *fileInteractionStore) Resolve(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
	answer InteractionAnswer,
) (Interaction, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return Interaction{}, err
	}
	stream, resolved, changed, err := resolveInteraction(
		state.Streams[sessionID], id, expectedRevision, answer, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return Interaction{}, err
		}
		store.signalLocked(sessionID)
	}
	return resolved, nil
}

func (store *fileInteractionStore) Cancel(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
) (Interaction, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return Interaction{}, err
	}
	stream, cancelled, changed, err := cancelInteraction(
		state.Streams[sessionID], id, expectedRevision, store.clock(),
	)
	if err != nil {
		return Interaction{}, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return Interaction{}, err
		}
		store.signalLocked(sessionID)
	}
	return cancelled, nil
}

func (store *fileInteractionStore) Close(context.Context) error {
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

func (store *fileInteractionStore) stateLocked(
	ctx context.Context,
) (fileInteractionState, error) {
	if err := ctx.Err(); err != nil {
		return fileInteractionState{}, err
	}
	if store.closed {
		return fileInteractionState{}, ErrStoreClosed
	}
	return store.readLocked()
}

func (store *fileInteractionStore) readLocked() (fileInteractionState, error) {
	raw, err := os.ReadFile(store.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return fileInteractionState{
			SchemaVersion: fileInteractionSchemaVersion,
			Streams:       make(map[string]interactionStream),
		}, nil
	}
	if err != nil {
		return fileInteractionState{}, fmt.Errorf("read gateway interactions: %w", err)
	}
	var state fileInteractionState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fileInteractionState{}, fmt.Errorf("decode gateway interactions: %w", err)
	}
	if state.SchemaVersion != fileInteractionSchemaVersion {
		return fileInteractionState{}, fmt.Errorf(
			"unsupported gateway interaction schema version %d",
			state.SchemaVersion,
		)
	}
	if state.Streams == nil {
		state.Streams = make(map[string]interactionStream)
	}
	for sessionID, stream := range state.Streams {
		if stream.NextSequence == 0 {
			stream.NextSequence = 1
		}
		var previous uint64
		for index, item := range stream.Items {
			normalized, err := normalizeInteraction(item)
			if err != nil || normalized.SessionID != sessionID ||
				item.Sequence == 0 || item.Sequence <= previous ||
				item.Revision == 0 || item.CreatedAt.IsZero() ||
				item.UpdatedAt.IsZero() {
				return fileInteractionState{}, fmt.Errorf(
					"gateway interaction stream %q contains invalid item at index %d: %v",
					sessionID,
					index,
					err,
				)
			}
			switch item.State {
			case InteractionPending, InteractionResolved, InteractionCancelled:
			default:
				return fileInteractionState{}, fmt.Errorf(
					"gateway interaction %q has invalid state %q",
					item.ID,
					item.State,
				)
			}
			previous = item.Sequence
		}
		if previous >= stream.NextSequence {
			return fileInteractionState{}, fmt.Errorf(
				"gateway interaction stream %q has invalid next sequence",
				sessionID,
			)
		}
		state.Streams[sessionID] = stream
	}
	return state, nil
}

func (store *fileInteractionStore) writeLocked(
	ctx context.Context,
	state fileInteractionState,
) error {
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.statePath,
		"gateway interactions",
		state,
	)
}

func (store *fileInteractionStore) notifyLocked(sessionID string) chan struct{} {
	if store.notify[sessionID] == nil {
		store.notify[sessionID] = make(chan struct{})
	}
	return store.notify[sessionID]
}

func (store *fileInteractionStore) signalLocked(sessionID string) {
	close(store.notifyLocked(sessionID))
	store.notify[sessionID] = make(chan struct{})
}
