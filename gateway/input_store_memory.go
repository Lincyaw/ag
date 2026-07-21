package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type memoryInputStore struct {
	mu      sync.Mutex
	streams map[string]inputStream
	clock   func() time.Time
	closed  bool
}

func NewMemoryInputStore() InputStore {
	return &memoryInputStore{
		streams: make(map[string]inputStream),
		clock:   func() time.Time { return time.Now().UTC() },
	}
}

func (store *memoryInputStore) Enqueue(
	ctx context.Context,
	input AgentInput,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	input, err := normalizeAgentInput(input)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentInput{}, ErrStoreClosed
	}
	stream, created, _, err := enqueueAgentInput(
		store.streams[input.SessionID], input, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	store.streams[input.SessionID] = stream
	return created, nil
}

func (store *memoryInputStore) Get(
	ctx context.Context,
	sessionID string,
	inputID string,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentInput{}, ErrStoreClosed
	}
	for _, input := range store.streams[strings.TrimSpace(sessionID)].Inputs {
		if input.ID == strings.TrimSpace(inputID) {
			return input, nil
		}
	}
	return AgentInput{}, fmt.Errorf("%w: %s", ErrInputNotFound, inputID)
}

func (store *memoryInputStore) List(
	ctx context.Context,
	sessionID string,
	query InputQuery,
) (InputPage, error) {
	if err := ctx.Err(); err != nil {
		return InputPage{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return InputPage{}, err
	}
	query, err = normalizeInputQuery(query)
	if err != nil {
		return InputPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return InputPage{}, ErrStoreClosed
	}
	return listAgentInputs(store.streams[sessionID], query), nil
}

func (store *memoryInputStore) AcquireNext(
	ctx context.Context,
	sessionID string,
) (AcquiredInput, bool, error) {
	if err := ctx.Err(); err != nil {
		return AcquiredInput{}, false, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AcquiredInput{}, false, ErrStoreClosed
	}
	stream, acquired, ok, changed, err := acquireAgentInput(
		store.streams[sessionID], store.clock(),
	)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	if changed {
		store.streams[sessionID] = stream
	}
	return acquired, ok, nil
}

func (store *memoryInputStore) BindExecution(
	ctx context.Context,
	sessionID string,
	inputID string,
	executionID string,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentInput{}, ErrStoreClosed
	}
	stream, input, changed, err := bindAgentInputExecution(
		store.streams[sessionID], inputID, executionID, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		store.streams[sessionID] = stream
	}
	return input, nil
}

func (store *memoryInputStore) Complete(
	ctx context.Context,
	sessionID string,
	inputID string,
	state AgentInputState,
	lastError string,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentInput{}, ErrStoreClosed
	}
	stream, input, changed, err := completeAgentInput(
		store.streams[sessionID], inputID, state, lastError, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		store.streams[sessionID] = stream
	}
	return input, nil
}

func (store *memoryInputStore) CancelQueued(
	ctx context.Context,
	sessionID string,
	inputID string,
	expectedRevision uint64,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentInput{}, ErrStoreClosed
	}
	stream, input, changed, err := cancelQueuedAgentInput(
		store.streams[sessionID], inputID, expectedRevision, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		store.streams[sessionID] = stream
	}
	return input, nil
}

func (store *memoryInputStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}
