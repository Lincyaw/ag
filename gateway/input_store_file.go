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

const fileInputSchemaVersion uint32 = 1

type fileInputState struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Streams       map[string]inputStream `json:"streams"`
}

type fileInputStore struct {
	mu        sync.Mutex
	directory string
	statePath string
	clock     func() time.Time
	closed    bool
}

func NewFileInputStore(directory string) (InputStore, error) {
	absolute, err := filestate.PrepareDirectory("gateway input store", directory)
	if err != nil {
		return nil, err
	}
	store := &fileInputStore{
		directory: absolute,
		statePath: filepath.Join(absolute, "inputs.json"),
		clock:     func() time.Time { return time.Now().UTC() },
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, err := store.readLocked(); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *fileInputStore) Enqueue(
	ctx context.Context,
	input AgentInput,
) (AgentInput, error) {
	input, err := normalizeAgentInput(input)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AgentInput{}, err
	}
	stream, created, changed, err := enqueueAgentInput(
		state.Streams[input.SessionID], input, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		state.Streams[input.SessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return AgentInput{}, err
		}
	}
	return created, nil
}

func (store *fileInputStore) Get(
	ctx context.Context,
	sessionID string,
	inputID string,
) (AgentInput, error) {
	if err := ctx.Err(); err != nil {
		return AgentInput{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AgentInput{}, err
	}
	for _, input := range state.Streams[sessionID].Inputs {
		if input.ID == inputID {
			return input, nil
		}
	}
	return AgentInput{}, fmt.Errorf("%w: %s", ErrInputNotFound, inputID)
}

func (store *fileInputStore) List(
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
	state, err := store.stateLocked(ctx)
	if err != nil {
		return InputPage{}, err
	}
	return listAgentInputs(state.Streams[sessionID], query), nil
}

func (store *fileInputStore) AcquireNext(
	ctx context.Context,
	sessionID string,
) (AcquiredInput, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	stream, acquired, ok, changed, err := acquireAgentInput(
		state.Streams[sessionID], store.clock(),
	)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return AcquiredInput{}, false, err
		}
	}
	return acquired, ok, nil
}

func (store *fileInputStore) BindExecution(
	ctx context.Context,
	sessionID string,
	inputID string,
	executionID string,
) (AgentInput, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AgentInput{}, err
	}
	stream, input, changed, err := bindAgentInputExecution(
		state.Streams[sessionID], inputID, executionID, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return AgentInput{}, err
		}
	}
	return input, nil
}

func (store *fileInputStore) Complete(
	ctx context.Context,
	sessionID string,
	inputID string,
	stateValue AgentInputState,
	lastError string,
) (AgentInput, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AgentInput{}, err
	}
	stream, input, changed, err := completeAgentInput(
		state.Streams[sessionID], inputID, stateValue, lastError, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return AgentInput{}, err
		}
	}
	return input, nil
}

func (store *fileInputStore) CancelQueued(
	ctx context.Context,
	sessionID string,
	inputID string,
	expectedRevision uint64,
) (AgentInput, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.stateLocked(ctx)
	if err != nil {
		return AgentInput{}, err
	}
	stream, input, changed, err := cancelQueuedAgentInput(
		state.Streams[sessionID], inputID, expectedRevision, store.clock(),
	)
	if err != nil {
		return AgentInput{}, err
	}
	if changed {
		state.Streams[sessionID] = stream
		if err := store.writeLocked(ctx, state); err != nil {
			return AgentInput{}, err
		}
	}
	return input, nil
}

func (store *fileInputStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}

func (store *fileInputStore) stateLocked(ctx context.Context) (fileInputState, error) {
	if err := ctx.Err(); err != nil {
		return fileInputState{}, err
	}
	if store.closed {
		return fileInputState{}, ErrStoreClosed
	}
	return store.readLocked()
}

func (store *fileInputStore) readLocked() (fileInputState, error) {
	raw, err := os.ReadFile(store.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return fileInputState{
			SchemaVersion: fileInputSchemaVersion,
			Streams:       make(map[string]inputStream),
		}, nil
	}
	if err != nil {
		return fileInputState{}, fmt.Errorf("read gateway inputs: %w", err)
	}
	var state fileInputState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fileInputState{}, fmt.Errorf("decode gateway inputs: %w", err)
	}
	if state.SchemaVersion != fileInputSchemaVersion {
		return fileInputState{}, fmt.Errorf(
			"unsupported gateway input schema version %d",
			state.SchemaVersion,
		)
	}
	if state.Streams == nil {
		state.Streams = make(map[string]inputStream)
	}
	for sessionID, stream := range state.Streams {
		validated, err := validateInputStream(sessionID, stream)
		if err != nil {
			return fileInputState{}, err
		}
		state.Streams[sessionID] = validated
	}
	return state, nil
}

func (store *fileInputStore) writeLocked(
	ctx context.Context,
	state fileInputState,
) error {
	return filestate.WriteJSON(
		ctx,
		store.directory,
		store.statePath,
		"gateway inputs",
		state,
	)
}
