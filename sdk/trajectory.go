package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrTrajectoryNotFound = errors.New("trajectory not found")
	ErrTrajectoryExists   = errors.New("trajectory already exists")
	ErrTrajectoryConflict = errors.New("trajectory head conflict")
)

const (
	TrajectoryKindUserMessage      = "user_message"
	TrajectoryKindAgentStart       = "agent_start"
	TrajectoryKindProviderRequest  = "provider_request"
	TrajectoryKindProviderResponse = "provider_response"
	TrajectoryKindToolCall         = "tool_call"
	TrajectoryKindToolResult       = "tool_result"
	TrajectoryKindDecision         = "decision"
	TrajectoryKindCheckpoint       = "checkpoint"
	TrajectoryKindTerminal         = "terminal"
	TrajectoryKindRestore          = "restore"
	TrajectoryKindRollback         = "rollback"
)

type TrajectoryEntry struct {
	ID         string            `json:"id"`
	ParentID   string            `json:"parent_id,omitempty"`
	Kind       string            `json:"kind"`
	Timestamp  time.Time         `json:"timestamp"`
	Generation uint64            `json:"generation,omitempty"`
	Payload    json.RawMessage   `json:"payload"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type Trajectory struct {
	ID            string            `json:"id"`
	ParentID      string            `json:"parent_id,omitempty"`
	ParentEntryID string            `json:"parent_entry_id,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Head          string            `json:"head,omitempty"`
	Entries       []TrajectoryEntry `json:"entries"`
}

type TrajectorySummary struct {
	ID            string    `json:"id"`
	ParentID      string    `json:"parent_id,omitempty"`
	ParentEntryID string    `json:"parent_entry_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Head          string    `json:"head,omitempty"`
	EntryCount    int       `json:"entry_count"`
}

func (trajectory Trajectory) Branch(head string) ([]TrajectoryEntry, error) {
	if head == "" {
		return nil, nil
	}
	index := make(map[string]TrajectoryEntry, len(trajectory.Entries))
	for _, entry := range trajectory.Entries {
		index[entry.ID] = entry
	}
	result := make([]TrajectoryEntry, 0, len(trajectory.Entries))
	seen := make(map[string]struct{})
	for cursor := head; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return nil, fmt.Errorf("trajectory %q contains a cycle at %q", trajectory.ID, cursor)
		}
		seen[cursor] = struct{}{}
		entry, exists := index[cursor]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory %q branch references unknown entry %q",
				trajectory.ID,
				cursor,
			)
		}
		result = append(result, cloneTrajectoryEntry(entry))
		cursor = entry.ParentID
	}
	slices.Reverse(result)
	return result, nil
}

type TrajectoryStore interface {
	Create(context.Context, Trajectory) error
	Append(
		context.Context,
		string,
		string,
		...TrajectoryEntry,
	) (string, error)
	Load(context.Context, string) (Trajectory, error)
	List(context.Context) ([]TrajectorySummary, error)
}

type MemoryTrajectoryStore struct {
	mu           sync.RWMutex
	trajectories map[string]Trajectory
}

func NewMemoryTrajectoryStore() *MemoryTrajectoryStore {
	return &MemoryTrajectoryStore{
		trajectories: make(map[string]Trajectory),
	}
}

func (store *MemoryTrajectoryStore) Create(
	ctx context.Context,
	trajectory Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateNewTrajectory(trajectory); err != nil {
		return err
	}
	now := time.Now().UTC()
	if trajectory.CreatedAt.IsZero() {
		trajectory.CreatedAt = now
	}
	if trajectory.UpdatedAt.IsZero() {
		trajectory.UpdatedAt = trajectory.CreatedAt
	}
	trajectory.Entries = []TrajectoryEntry{}

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.trajectories[trajectory.ID]; exists {
		return fmt.Errorf("%w: %s", ErrTrajectoryExists, trajectory.ID)
	}
	store.trajectories[trajectory.ID] = cloneTrajectory(trajectory)
	return nil
}

func (store *MemoryTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...TrajectoryEntry,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, exists := store.trajectories[id]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrTrajectoryNotFound, id)
	}
	if trajectory.Head != expectedHead {
		return "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			ErrTrajectoryConflict,
			id,
			trajectory.Head,
			expectedHead,
		)
	}
	next, err := appendTrajectoryEntries(trajectory, entries)
	if err != nil {
		return "", err
	}
	store.trajectories[id] = next
	return next.Head, nil
}

func (store *MemoryTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (Trajectory, error) {
	if err := ctx.Err(); err != nil {
		return Trajectory{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	trajectory, exists := store.trajectories[id]
	if !exists {
		return Trajectory{}, fmt.Errorf("%w: %s", ErrTrajectoryNotFound, id)
	}
	return cloneTrajectory(trajectory), nil
}

func (store *MemoryTrajectoryStore) List(
	ctx context.Context,
) ([]TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	result := make([]TrajectorySummary, 0, len(store.trajectories))
	for _, trajectory := range store.trajectories {
		result = append(result, summarizeTrajectory(trajectory))
	}
	slices.SortFunc(result, func(left, right TrajectorySummary) int {
		if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})
	return result, nil
}

func validateNewTrajectory(trajectory Trajectory) error {
	if err := validateResourceName("trajectory", trajectory.ID); err != nil {
		return err
	}
	if trajectory.Head != "" || len(trajectory.Entries) != 0 {
		return errors.New("new trajectory must not contain entries or a head")
	}
	if (trajectory.ParentID == "") != (trajectory.ParentEntryID == "") {
		return errors.New(
			"trajectory parent_id and parent_entry_id must be set together",
		)
	}
	return nil
}

func appendTrajectoryEntries(
	trajectory Trajectory,
	entries []TrajectoryEntry,
) (Trajectory, error) {
	if len(entries) == 0 {
		return Trajectory{}, errors.New("trajectory append contains no entries")
	}
	known := make(map[string]struct{}, len(trajectory.Entries)+len(entries))
	for _, entry := range trajectory.Entries {
		known[entry.ID] = struct{}{}
	}
	for index := range entries {
		entry := &entries[index]
		if err := validateResourceName("trajectory entry", entry.ID); err != nil {
			return Trajectory{}, err
		}
		if _, duplicate := known[entry.ID]; duplicate {
			return Trajectory{}, fmt.Errorf(
				"trajectory entry %q already exists",
				entry.ID,
			)
		}
		if strings.TrimSpace(entry.Kind) == "" {
			return Trajectory{}, fmt.Errorf(
				"trajectory entry %q kind is empty",
				entry.ID,
			)
		}
		if !json.Valid(entry.Payload) {
			return Trajectory{}, fmt.Errorf(
				"trajectory entry %q payload is invalid JSON",
				entry.ID,
			)
		}
		if entry.ParentID == "" {
			if len(known) != 0 && entry.Kind != TrajectoryKindRestore {
				return Trajectory{}, fmt.Errorf(
					"trajectory entry %q has no parent in a non-empty trajectory",
					entry.ID,
				)
			}
		} else if _, exists := known[entry.ParentID]; !exists {
			return Trajectory{}, fmt.Errorf(
				"trajectory entry %q has unknown parent %q",
				entry.ID,
				entry.ParentID,
			)
		}
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}
		entry.Payload = append(json.RawMessage(nil), entry.Payload...)
		entry.Attributes = maps.Clone(entry.Attributes)
		known[entry.ID] = struct{}{}
	}
	next := cloneTrajectory(trajectory)
	for _, entry := range entries {
		next.Entries = append(next.Entries, cloneTrajectoryEntry(entry))
	}
	next.Head = entries[len(entries)-1].ID
	next.UpdatedAt = entries[len(entries)-1].Timestamp
	return next, nil
}

func summarizeTrajectory(trajectory Trajectory) TrajectorySummary {
	return TrajectorySummary{
		ID:            trajectory.ID,
		ParentID:      trajectory.ParentID,
		ParentEntryID: trajectory.ParentEntryID,
		CreatedAt:     trajectory.CreatedAt,
		UpdatedAt:     trajectory.UpdatedAt,
		Head:          trajectory.Head,
		EntryCount:    len(trajectory.Entries),
	}
}

func cloneTrajectory(source Trajectory) Trajectory {
	result := source
	result.Entries = make([]TrajectoryEntry, len(source.Entries))
	for index, entry := range source.Entries {
		result.Entries[index] = cloneTrajectoryEntry(entry)
	}
	return result
}

func cloneTrajectoryEntry(source TrajectoryEntry) TrajectoryEntry {
	result := source
	result.Payload = append(json.RawMessage(nil), source.Payload...)
	result.Attributes = maps.Clone(source.Attributes)
	return result
}
