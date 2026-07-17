package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type memorySessionStore struct {
	mu       sync.Mutex
	sessions map[string]Session
	clock    func() time.Time
	closed   bool
}

func NewMemorySessionStore() SessionStore {
	return &memorySessionStore{
		sessions: make(map[string]Session),
		clock:    func() time.Time { return time.Now().UTC() },
	}
}

func (store *memorySessionStore) Create(
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
	if _, exists := store.sessions[session.ID]; exists {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionExists, session.ID)
	}
	now := store.clock().UTC()
	session.Revision = 1
	session.CreatedAt = now
	session.UpdatedAt = now
	store.sessions[session.ID] = cloneSession(session)
	return cloneSession(session), nil
}

func (store *memorySessionStore) Get(
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
	session, exists := store.sessions[strings.TrimSpace(id)]
	if !exists {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	return cloneSession(session), nil
}

func (store *memorySessionStore) List(
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
	return listSessions(store.sessions, request), nil
}

func (store *memorySessionStore) Save(
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
	current, exists := store.sessions[session.ID]
	if !exists {
		return Session{}, fmt.Errorf(
			"%w: %s",
			ErrSessionNotFound,
			session.ID,
		)
	}
	session, err = prepareSessionUpdate(
		current,
		session,
		expectedRevision,
		store.clock(),
	)
	if err != nil {
		return Session{}, err
	}
	store.sessions[session.ID] = cloneSession(session)
	return cloneSession(session), nil
}

func (store *memorySessionStore) Delete(
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
	current, exists := store.sessions[id]
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
	delete(store.sessions, id)
	return nil
}

func (*memorySessionStore) Capabilities() StoreCapabilities {
	return StoreCapabilities{}
}

func (store *memorySessionStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}
