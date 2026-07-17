package storage

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

	sdk "github.com/lincyaw/ag/sdk"
)

type memoryTrajectoryStore struct {
	mu           sync.RWMutex
	trajectories map[string]sdk.Trajectory
}

func NewMemoryTrajectoryStore() sdk.TrajectoryStore {
	return newMemoryTrajectoryStore()
}

func newMemoryTrajectoryStore() *memoryTrajectoryStore {
	return &memoryTrajectoryStore{
		trajectories: make(map[string]sdk.Trajectory),
	}
}

func (store *memoryTrajectoryStore) Create(
	ctx context.Context,
	trajectory sdk.Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalizeTrajectory(&trajectory)
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
	trajectory.Entries = []sdk.TrajectoryEntry{}

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.trajectories[trajectory.ID]; exists {
		return fmt.Errorf("%w: %s", sdk.ErrTrajectoryExists, trajectory.ID)
	}
	store.trajectories[trajectory.ID] = cloneTrajectory(trajectory)
	return nil
}

func (store *memoryTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, exists := store.trajectories[id]
	if !exists {
		return "", fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	if trajectory.Head != expectedHead {
		return "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
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

func (store *memoryTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	trajectory, exists := store.trajectories[id]
	if !exists {
		return sdk.Trajectory{}, fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	return cloneTrajectory(trajectory), nil
}

func (store *memoryTrajectoryStore) List(
	ctx context.Context,
) ([]sdk.TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	result := make([]sdk.TrajectorySummary, 0, len(store.trajectories))
	for _, trajectory := range store.trajectories {
		result = append(result, summarizeTrajectory(trajectory))
	}
	slices.SortFunc(result, func(left, right sdk.TrajectorySummary) int {
		if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})
	return result, nil
}

func (store *memoryTrajectoryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.TrajectoryPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	page, next, err := pageWindow(
		items,
		request,
		func(item sdk.TrajectorySummary) string { return item.ID },
	)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	return sdk.TrajectoryPage{Items: page, Next: next}, nil
}

func (store *memoryTrajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.trajectories[id]; !exists {
		return fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	delete(store.trajectories, id)
	return nil
}

func validateNewTrajectory(trajectory sdk.Trajectory) error {
	if trajectory.SchemaVersion > sdk.TrajectorySchemaVersion {
		return fmt.Errorf(
			"%w: got %d, maximum supported is %d",
			sdk.ErrTrajectoryVersion,
			trajectory.SchemaVersion,
			sdk.TrajectorySchemaVersion,
		)
	}
	if err := sdk.ValidateResourceName("trajectory", trajectory.ID); err != nil {
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
	trajectory sdk.Trajectory,
	entries []sdk.TrajectoryEntry,
) (sdk.Trajectory, error) {
	if len(entries) == 0 {
		return sdk.Trajectory{}, errors.New("trajectory append contains no entries")
	}
	known := make(map[string]struct{}, len(trajectory.Entries)+len(entries))
	for _, entry := range trajectory.Entries {
		known[entry.ID] = struct{}{}
	}
	for index := range entries {
		entry := &entries[index]
		if err := sdk.ValidateResourceName("trajectory entry", entry.ID); err != nil {
			return sdk.Trajectory{}, err
		}
		if _, duplicate := known[entry.ID]; duplicate {
			return sdk.Trajectory{}, fmt.Errorf(
				"trajectory entry %q already exists",
				entry.ID,
			)
		}
		if strings.TrimSpace(entry.Kind) == "" {
			return sdk.Trajectory{}, fmt.Errorf(
				"trajectory entry %q kind is empty",
				entry.ID,
			)
		}
		if !json.Valid(entry.Payload) {
			return sdk.Trajectory{}, fmt.Errorf(
				"trajectory entry %q payload is invalid JSON",
				entry.ID,
			)
		}
		if entry.ParentID == "" {
			if len(known) != 0 && entry.Kind != sdk.TrajectoryKindRestore {
				return sdk.Trajectory{}, fmt.Errorf(
					"trajectory entry %q has no parent in a non-empty trajectory",
					entry.ID,
				)
			}
		} else if _, exists := known[entry.ParentID]; !exists {
			return sdk.Trajectory{}, fmt.Errorf(
				"trajectory entry %q has unknown parent %q",
				entry.ID,
				entry.ParentID,
			)
		}
		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}
		if entry.PayloadVersion == 0 {
			entry.PayloadVersion = sdk.TrajectoryPayloadVersion
		}
		if entry.PayloadVersion > sdk.TrajectoryPayloadVersion {
			return sdk.Trajectory{}, fmt.Errorf(
				"%w: entry %q payload version %d, maximum supported is %d",
				sdk.ErrTrajectoryVersion,
				entry.ID,
				entry.PayloadVersion,
				sdk.TrajectoryPayloadVersion,
			)
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

func summarizeTrajectory(trajectory sdk.Trajectory) sdk.TrajectorySummary {
	return sdk.TrajectorySummary{
		SchemaVersion: trajectory.SchemaVersion,
		ID:            trajectory.ID,
		ParentID:      trajectory.ParentID,
		ParentEntryID: trajectory.ParentEntryID,
		CreatedAt:     trajectory.CreatedAt,
		UpdatedAt:     trajectory.UpdatedAt,
		Head:          trajectory.Head,
		EntryCount:    len(trajectory.Entries),
	}
}

func cloneTrajectory(source sdk.Trajectory) sdk.Trajectory {
	result := source
	result.Environment = cloneTrajectoryEnvironment(source.Environment)
	result.Entries = make([]sdk.TrajectoryEntry, len(source.Entries))
	for index, entry := range source.Entries {
		result.Entries[index] = cloneTrajectoryEntry(entry)
	}
	return result
}

func normalizeTrajectory(trajectory *sdk.Trajectory) {
	if trajectory.SchemaVersion == 0 {
		trajectory.SchemaVersion = sdk.TrajectorySchemaVersion
	}
	for index := range trajectory.Entries {
		if trajectory.Entries[index].PayloadVersion == 0 {
			trajectory.Entries[index].PayloadVersion = sdk.TrajectoryPayloadVersion
		}
	}
}

func validateLoadedTrajectory(trajectory *sdk.Trajectory) error {
	if trajectory.SchemaVersion > sdk.TrajectorySchemaVersion {
		return fmt.Errorf(
			"%w: got %d, maximum supported is %d",
			sdk.ErrTrajectoryVersion,
			trajectory.SchemaVersion,
			sdk.TrajectorySchemaVersion,
		)
	}
	normalizeTrajectory(trajectory)
	for _, entry := range trajectory.Entries {
		if entry.PayloadVersion > sdk.TrajectoryPayloadVersion {
			return fmt.Errorf(
				"%w: entry %q payload version %d, maximum supported is %d",
				sdk.ErrTrajectoryVersion,
				entry.ID,
				entry.PayloadVersion,
				sdk.TrajectoryPayloadVersion,
			)
		}
	}
	return nil
}

func cloneTrajectoryEnvironment(source sdk.TrajectoryEnvironment) sdk.TrajectoryEnvironment {
	result := source
	result.Plugins = make([]sdk.TrajectoryPlugin, len(source.Plugins))
	for index, plugin := range source.Plugins {
		result.Plugins[index] = plugin
		result.Plugins[index].Registers = append([]string(nil), plugin.Registers...)
	}
	result.Providers = append([]sdk.ProviderSpec(nil), source.Providers...)
	result.Tools = make([]sdk.ToolSpec, len(source.Tools))
	for index, spec := range source.Tools {
		result.Tools[index] = spec
		result.Tools[index].Parameters = maps.Clone(spec.Parameters)
	}
	result.Hooks = append([]sdk.HookSpec(nil), source.Hooks...)
	result.Subscribers = make([]sdk.SubscriberSpec, len(source.Subscribers))
	for index, spec := range source.Subscribers {
		result.Subscribers[index] = spec
		result.Subscribers[index].Events = append([]string(nil), spec.Events...)
	}
	result.Capabilities = make([]sdk.CapabilitySpec, len(source.Capabilities))
	for index, spec := range source.Capabilities {
		result.Capabilities[index] = spec
		result.Capabilities[index].InputSchema = maps.Clone(spec.InputSchema)
		result.Capabilities[index].OutputSchema = maps.Clone(spec.OutputSchema)
	}
	result.Events = make([]sdk.EventContract, len(source.Events))
	for index, contract := range source.Events {
		result.Events[index] = contract
		result.Events[index].MutableFields = append(
			[]string(nil),
			contract.MutableFields...,
		)
	}
	return result
}

func cloneTrajectoryEntry(source sdk.TrajectoryEntry) sdk.TrajectoryEntry {
	result := source
	result.Payload = append(json.RawMessage(nil), source.Payload...)
	result.Attributes = maps.Clone(source.Attributes)
	return result
}
