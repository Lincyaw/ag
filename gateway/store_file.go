package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
	"github.com/lincyaw/ag/sdk"
)

const fileSessionSchemaVersion uint32 = 1

type fileSessionState struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Sessions      map[string]storedSession `json:"sessions"`
}

// storedSession keeps private execution configuration in the control-plane
// store while Session's normal JSON representation remains safe for clients.
type storedSession struct {
	Session
	RuntimeConfig json.RawMessage `json:"runtime_config,omitempty"`
}

type fileSessionStore struct {
	mu        sync.Mutex
	directory string
	statePath string
	clock     func() time.Time
	closed    bool
}

func NewFileSessionStore(directory string) (SessionStore, error) {
	absolute, err := filestate.PrepareDirectory(
		"gateway session store",
		directory,
	)
	if err != nil {
		return nil, err
	}
	store := &fileSessionStore{
		directory: absolute,
		statePath: filepath.Join(absolute, "sessions.json"),
		clock:     func() time.Time { return time.Now().UTC() },
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.readLocked(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *fileSessionStore) Create(
	ctx context.Context,
	session Session,
) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	session, err := normalizeSession(session)
	if err != nil {
		return Session{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Session{}, ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return Session{}, err
	}
	if _, exists := state.Sessions[session.ID]; exists {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionExists, session.ID)
	}
	now := store.clock().UTC()
	session.Revision = 1
	session.CreatedAt = now
	session.UpdatedAt = now
	state.Sessions[session.ID] = storeSession(session)
	if err := store.writeLocked(ctx, state); err != nil {
		return Session{}, err
	}
	return cloneSession(session), nil
}

func (store *fileSessionStore) Get(
	ctx context.Context,
	id string,
) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Session{}, ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return Session{}, err
	}
	stored, exists := state.Sessions[strings.TrimSpace(id)]
	if !exists {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return cloneSession(stored.Session), nil
}

func (store *fileSessionStore) List(
	ctx context.Context,
	request sdk.PageRequest,
) (SessionPage, error) {
	if err := ctx.Err(); err != nil {
		return SessionPage{}, err
	}
	request, err := validatePage(request)
	if err != nil {
		return SessionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return SessionPage{}, ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return SessionPage{}, err
	}
	return listSessions(loadSessions(state.Sessions), request), nil
}

func (store *fileSessionStore) ListByUser(
	ctx context.Context,
	userID string,
	request sdk.PageRequest,
) (SessionPage, error) {
	if err := ctx.Err(); err != nil {
		return SessionPage{}, err
	}
	userID, err := normalizeUserID(userID)
	if err != nil {
		return SessionPage{}, err
	}
	request, err = validatePage(request)
	if err != nil {
		return SessionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return SessionPage{}, ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return SessionPage{}, err
	}
	return listSessionsByUser(loadSessions(state.Sessions), userID, request), nil
}

func (store *fileSessionStore) Save(
	ctx context.Context,
	session Session,
	expectedRevision uint64,
) (Session, error) {
	if err := ctx.Err(); err != nil {
		return Session{}, err
	}
	session, err := normalizeSession(session)
	if err != nil {
		return Session{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return Session{}, ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return Session{}, err
	}
	stored, exists := state.Sessions[session.ID]
	if !exists {
		return Session{}, fmt.Errorf(
			"%w: %s",
			ErrSessionNotFound,
			session.ID,
		)
	}
	current := stored.Session
	session, err = prepareSessionUpdate(
		current,
		session,
		expectedRevision,
		store.clock(),
	)
	if err != nil {
		return Session{}, err
	}
	state.Sessions[session.ID] = storeSession(session)
	if err := store.writeLocked(ctx, state); err != nil {
		return Session{}, err
	}
	return cloneSession(session), nil
}

func (store *fileSessionStore) Delete(
	ctx context.Context,
	id string,
	expectedRevision uint64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrStoreClosed
	}
	state, err := store.readLocked()
	if err != nil {
		return err
	}
	stored, exists := state.Sessions[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	current := stored.Session
	if current.Revision != expectedRevision {
		return fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict,
			id,
			current.Revision,
			expectedRevision,
		)
	}
	delete(state.Sessions, id)
	return store.writeLocked(ctx, state)
}

func (*fileSessionStore) Capabilities() StoreCapabilities {
	return StoreCapabilities{Durable: true}
}

func (store *fileSessionStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}

func (store *fileSessionStore) readLocked() (fileSessionState, error) {
	raw, err := os.ReadFile(store.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return fileSessionState{
			SchemaVersion: fileSessionSchemaVersion,
			Sessions:      make(map[string]storedSession),
		}, nil
	}
	if err != nil {
		return fileSessionState{}, fmt.Errorf(
			"read gateway sessions: %w",
			err,
		)
	}
	var state fileSessionState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fileSessionState{}, fmt.Errorf(
			"decode gateway sessions: %w",
			err,
		)
	}
	if state.SchemaVersion != fileSessionSchemaVersion {
		return fileSessionState{}, fmt.Errorf(
			"unsupported gateway session schema version %d",
			state.SchemaVersion,
		)
	}
	if state.Sessions == nil {
		state.Sessions = make(map[string]storedSession)
	}
	for id, stored := range state.Sessions {
		session := stored.Session
		session.RuntimeConfig = append(
			json.RawMessage(nil), stored.RuntimeConfig...,
		)
		normalized, err := normalizeSession(session)
		if err != nil {
			return fileSessionState{}, fmt.Errorf(
				"validate gateway session %q: %w",
				id,
				err,
			)
		}
		if id != normalized.ID || normalized.Revision == 0 ||
			normalized.CreatedAt.IsZero() ||
			normalized.UpdatedAt.IsZero() {
			return fileSessionState{}, fmt.Errorf(
				"gateway session %q has invalid stored metadata",
				id,
			)
		}
		state.Sessions[id] = storeSession(normalized)
	}
	return state, nil
}

func storeSession(session Session) storedSession {
	cloned := cloneSession(session)
	return storedSession{
		Session: cloned,
		RuntimeConfig: append(
			json.RawMessage(nil), cloned.RuntimeConfig...,
		),
	}
}

func loadSessions(stored map[string]storedSession) map[string]Session {
	sessions := make(map[string]Session, len(stored))
	for id, value := range stored {
		sessions[id] = cloneSession(value.Session)
	}
	return sessions
}

func (store *fileSessionStore) writeLocked(
	ctx context.Context,
	state fileSessionState,
) error {
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.statePath,
		"gateway sessions",
		state,
	)
}
