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

	"github.com/lincyaw/ag/sdk"
)

const fileSessionSchemaVersion uint32 = 1

type fileSessionState struct {
	SchemaVersion uint32             `json:"schema_version"`
	Sessions      map[string]Session `json:"sessions"`
}

type fileSessionStore struct {
	mu        sync.Mutex
	directory string
	statePath string
	clock     func() time.Time
	closed    bool
}

func NewFileSessionStore(directory string) (SessionStore, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, errors.New("gateway session store directory is empty")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve gateway session store directory: %w",
			err,
		)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf(
			"create gateway session store directory: %w",
			err,
		)
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
	state.Sessions[session.ID] = cloneSession(session)
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
	session, exists := state.Sessions[strings.TrimSpace(id)]
	if !exists {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return cloneSession(session), nil
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
	return listSessions(state.Sessions, request), nil
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
	current, exists := state.Sessions[session.ID]
	if !exists {
		return Session{}, fmt.Errorf(
			"%w: %s",
			ErrSessionNotFound,
			session.ID,
		)
	}
	if current.Revision != expectedRevision {
		return Session{}, fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict,
			session.ID,
			current.Revision,
			expectedRevision,
		)
	}
	if current.UserID != session.UserID {
		return Session{}, fmt.Errorf(
			"gateway session %s user ID is immutable",
			session.ID,
		)
	}
	session.Revision = current.Revision + 1
	session.CreatedAt = current.CreatedAt
	session.UpdatedAt = store.clock().UTC()
	state.Sessions[session.ID] = cloneSession(session)
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
	current, exists := state.Sessions[id]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
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
			Sessions:      make(map[string]Session),
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
		state.Sessions = make(map[string]Session)
	}
	for id, session := range state.Sessions {
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
		state.Sessions[id] = normalized
	}
	return state, nil
}

func (store *fileSessionStore) writeLocked(
	ctx context.Context,
	state fileSessionState,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode gateway sessions: %w", err)
	}
	temporary, err := os.CreateTemp(store.directory, ".sessions-*.tmp")
	if err != nil {
		return fmt.Errorf("create gateway session temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure gateway session temporary file: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write gateway sessions: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync gateway sessions: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close gateway session temporary file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.statePath); err != nil {
		return fmt.Errorf("publish gateway sessions: %w", err)
	}
	removeTemporary = false
	directory, err := os.Open(store.directory)
	if err != nil {
		return fmt.Errorf("open gateway session directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync gateway session directory: %w", err)
	}
	return nil
}
