package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/filestate"
	"github.com/lincyaw/ag/sdk"
)

const fileEventSchemaVersion uint32 = 1

const (
	fileEventJournalCompactBytes   = 16 << 20
	fileEventJournalCompactEntries = 10_000
)

type fileEventState struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Streams       map[string]eventStream `json:"streams"`
}

type fileEventStore struct {
	mu             sync.Mutex
	directory      string
	statePath      string
	snapshotPath   string
	journalPath    string
	state          fileEventState
	journalState   fileEventState
	journalEntries int
	notify         map[string]chan struct{}
	latest         map[string]uint64
	clock          func() time.Time
	closed         bool
}

func NewFileEventStore(directory string) (EventStore, error) {
	absolute, err := filestate.PrepareDirectory(
		"gateway event store",
		directory,
	)
	if err != nil {
		return nil, err
	}
	store := &fileEventStore{
		directory:    absolute,
		statePath:    filepath.Join(absolute, "events.json"),
		snapshotPath: filepath.Join(absolute, "events.snapshot.json"),
		journalPath:  filepath.Join(absolute, "events.journal.jsonl"),
		notify:       make(map[string]chan struct{}),
		latest:       make(map[string]uint64),
		clock:        func() time.Time { return time.Now().UTC() },
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	state, err := store.readLocked()
	if err != nil {
		return nil, err
	}
	journalState, err := store.readSnapshotLocked()
	if err != nil {
		return nil, err
	}
	mergeEventState(&state, journalState)
	if err := store.loadJournalLocked(&state, &journalState); err != nil {
		return nil, err
	}
	store.state = state
	store.journalState = journalState
	for sessionID, stream := range state.Streams {
		store.latest[sessionID] = latestAgentEventSequence(stream)
	}
	return store, nil
}

func (store *fileEventStore) Append(
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
		store.state.Streams[sessionID],
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
	if err := store.appendJournalLocked(created); err != nil {
		return AgentEvent{}, err
	}
	store.state.Streams[sessionID] = stream
	store.journalState.Streams[sessionID] = appendJournalEvent(
		store.journalState.Streams[sessionID],
		created,
	)
	store.journalEntries++
	store.latest[sessionID] = created.Sequence
	store.signalLocked(sessionID)
	if err := store.compactJournalLocked(ctx, false); err != nil {
		return AgentEvent{}, err
	}
	return cloneAgentEvent(created), nil
}

func (store *fileEventStore) Latest(
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
	return store.latest[sessionID], nil
}

func (store *fileEventStore) List(
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
	return listAgentEvents(store.state.Streams[sessionID], query), nil
}

func (store *fileEventStore) Wait(
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
		page := listAgentEvents(store.state.Streams[sessionID], query)
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

func (store *fileEventStore) Close(context.Context) error {
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

func (store *fileEventStore) readLocked() (fileEventState, error) {
	raw, err := os.ReadFile(store.statePath)
	if errors.Is(err, fs.ErrNotExist) {
		return fileEventState{
			SchemaVersion: fileEventSchemaVersion,
			Streams:       make(map[string]eventStream),
		}, nil
	}
	if err != nil {
		return fileEventState{}, fmt.Errorf("read gateway events: %w", err)
	}
	var state fileEventState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fileEventState{}, fmt.Errorf("decode gateway events: %w", err)
	}
	if state.SchemaVersion != fileEventSchemaVersion {
		return fileEventState{}, fmt.Errorf(
			"unsupported gateway event schema version %d",
			state.SchemaVersion,
		)
	}
	if state.Streams == nil {
		state.Streams = make(map[string]eventStream)
	}
	for sessionID, stream := range state.Streams {
		normalizedID, err := normalizeEventSessionID(sessionID)
		if err != nil || normalizedID != sessionID {
			return fileEventState{}, fmt.Errorf(
				"validate gateway event stream %q: %w",
				sessionID,
				err,
			)
		}
		stream, err = validateStoredEventStream(sessionID, stream)
		if err != nil {
			return fileEventState{}, err
		}
		state.Streams[sessionID] = stream
	}
	return state, nil
}

func (store *fileEventStore) readSnapshotLocked() (fileEventState, error) {
	raw, err := os.ReadFile(store.snapshotPath)
	if errors.Is(err, fs.ErrNotExist) {
		return fileEventState{
			SchemaVersion: fileEventSchemaVersion,
			Streams:       make(map[string]eventStream),
		}, nil
	}
	if err != nil {
		return fileEventState{}, fmt.Errorf("read gateway event snapshot: %w", err)
	}
	var state fileEventState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fileEventState{}, fmt.Errorf("decode gateway event snapshot: %w", err)
	}
	if state.SchemaVersion != fileEventSchemaVersion {
		return fileEventState{}, fmt.Errorf(
			"unsupported gateway event snapshot schema version %d",
			state.SchemaVersion,
		)
	}
	if state.Streams == nil {
		state.Streams = make(map[string]eventStream)
	}
	for sessionID, stream := range state.Streams {
		validated, err := validateStoredEventStream(sessionID, stream)
		if err != nil {
			return fileEventState{}, err
		}
		state.Streams[sessionID] = validated
	}
	return state, nil
}

func mergeEventState(target *fileEventState, source fileEventState) {
	for sessionID, stream := range source.Streams {
		for _, event := range stream.Events {
			target.Streams[sessionID] = appendJournalEvent(
				target.Streams[sessionID],
				event,
			)
		}
	}
}

func (store *fileEventStore) loadJournalLocked(
	state *fileEventState,
	journalState *fileEventState,
) error {
	file, err := os.OpenFile(store.journalPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open gateway event journal: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var offset int64
	for {
		line, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			if len(line) != 0 {
				if err := file.Truncate(offset); err != nil {
					return fmt.Errorf("truncate partial gateway event journal: %w", err)
				}
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read gateway event journal: %w", readErr)
		}
		var event AgentEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("decode gateway event journal at byte %d: %w", offset, err)
		}
		state.Streams[event.SessionID] = appendJournalEvent(
			state.Streams[event.SessionID],
			event,
		)
		journalState.Streams[event.SessionID] = appendJournalEvent(
			journalState.Streams[event.SessionID],
			event,
		)
		store.journalEntries++
		offset += int64(len(line))
	}
	for sessionID, stream := range state.Streams {
		validated, err := validateStoredEventStream(sessionID, stream)
		if err != nil {
			return err
		}
		state.Streams[sessionID] = validated
	}
	return nil
}

func (store *fileEventStore) appendJournalLocked(event AgentEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode gateway event journal: %w", err)
	}
	raw = append(raw, '\n')
	file, err := os.OpenFile(store.journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open gateway event journal: %w", err)
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return fmt.Errorf("append gateway event journal: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync gateway event journal: %w", err)
	}
	return file.Close()
}

func appendJournalEvent(stream eventStream, event AgentEvent) eventStream {
	if _, exists := stream.byID[event.ID]; exists {
		return stream
	}
	if stream.byID == nil {
		stream.byID = make(map[string]int, len(stream.Events)+1)
		for index, current := range stream.Events {
			stream.byID[current.ID] = index
			if current.ID == event.ID {
				return stream
			}
		}
	}
	stream.Events = append(stream.Events, cloneAgentEvent(event))
	stream.byID[event.ID] = len(stream.Events) - 1
	if event.Sequence >= stream.NextSequence {
		stream.NextSequence = event.Sequence + 1
	}
	return stream
}

func (store *fileEventStore) compactJournalLocked(
	ctx context.Context,
	force bool,
) error {
	if !force && store.journalEntries < fileEventJournalCompactEntries {
		if store.journalEntries%256 != 0 {
			return nil
		}
		info, err := os.Stat(store.journalPath)
		if err != nil {
			return fmt.Errorf("stat gateway event journal: %w", err)
		}
		if info.Size() < fileEventJournalCompactBytes {
			return nil
		}
	}
	if err := filestate.WriteJSON(
		ctx,
		store.directory,
		store.snapshotPath,
		"gateway event snapshot",
		store.journalState,
	); err != nil {
		return err
	}
	file, err := os.OpenFile(store.journalPath, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("truncate gateway event journal after snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync compacted gateway event journal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close compacted gateway event journal: %w", err)
	}
	store.journalEntries = 0
	return nil
}

func (store *fileEventStore) notifyLocked(sessionID string) chan struct{} {
	notify := store.notify[sessionID]
	if notify == nil {
		notify = make(chan struct{})
		store.notify[sessionID] = notify
	}
	return notify
}

func (store *fileEventStore) signalLocked(sessionID string) {
	notify := store.notifyLocked(sessionID)
	close(notify)
	store.notify[sessionID] = make(chan struct{})
}
