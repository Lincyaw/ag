package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type memoryTrajectory struct {
	trajectory     sdk.Trajectory
	entries        map[string]sdk.TrajectoryEntry
	order          []string
	inheritedCount int
}

type memoryTrajectoryStore struct {
	mu           sync.RWMutex
	trajectories map[string]*memoryTrajectory
}

func NewMemoryTrajectoryStore() sdk.TrajectoryStore {
	return newMemoryTrajectoryStore()
}

func newMemoryTrajectoryStore() *memoryTrajectoryStore {
	return &memoryTrajectoryStore{
		trajectories: make(map[string]*memoryTrajectory),
	}
}

func (store *memoryTrajectoryStore) trajectoryLocked(
	id string,
) (*memoryTrajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return nil, err
	}
	trajectory, exists := store.trajectories[id]
	if !exists {
		return nil, fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	return trajectory, nil
}

func (store *memoryTrajectoryStore) Create(
	ctx context.Context,
	trajectory sdk.Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewTrajectory(trajectory, time.Now().UTC())
	if err != nil {
		return err
	}
	trajectory = prepared.Trajectory

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.trajectories[trajectory.ID]; exists {
		return fmt.Errorf("%w: %s", sdk.ErrTrajectoryExists, trajectory.ID)
	}
	if trajectory.ParentID != "" {
		branch, err := store.branchLocked(
			trajectory.ParentID,
			trajectory.ParentEntryID,
		)
		if err != nil {
			return fmt.Errorf(
				"resolve trajectory %q fork point: %w",
				trajectory.ID,
				err,
			)
		}
		prepared, err = prepareNewTrajectoryFork(prepared, branch)
		if err != nil {
			return err
		}
		trajectory = prepared.Trajectory
	}
	store.trajectories[trajectory.ID] = &memoryTrajectory{
		trajectory:     cloneTrajectory(trajectory),
		entries:        make(map[string]sdk.TrajectoryEntry),
		inheritedCount: int(prepared.InheritedEntryCount),
	}
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
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return "", err
	}
	if trajectory.trajectory.Execution != nil &&
		!trajectory.trajectory.Execution.Terminal() {
		return "", fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			id,
			trajectory.trajectory.Execution.ID,
		)
	}
	return store.appendLocked(trajectory, id, expectedHead, entries)
}

func (store *memoryTrajectoryStore) BeginExecution(
	ctx context.Context,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if input.Kind != sdk.TrajectoryKindUserMessage {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution input must be a user_message entry",
		)
	}
	now := time.Now().UTC()
	execution, err := prepareTrajectoryExecutionStart(
		start,
		expectedHead,
		input.ID,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	bound, err := bindTrajectoryExecutionEntries(
		execution.ID,
		[]sdk.TrajectoryEntry{input},
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	input = bound[0]
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.trajectory.Execution != nil &&
		!trajectory.trajectory.Execution.Terminal() {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			id,
			trajectory.trajectory.Execution.ID,
		)
	}
	if _, err := store.appendLocked(
		trajectory,
		id,
		expectedHead,
		[]sdk.TrajectoryEntry{input},
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
	return trajectoryMetadata(
		trajectory.trajectory,
		trajectory.inheritedCount+len(trajectory.order),
		len(trajectory.order),
	), nil
}

func (store *memoryTrajectoryStore) ClaimExecution(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if trajectory.trajectory.Execution == nil {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			id,
		)
	}
	execution, err := claimTrajectoryExecution(
		*trajectory.trajectory.Execution,
		owner,
		now,
		ttl,
	)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
	trajectory.trajectory.UpdatedAt = now
	return execution, nil
}

func (store *memoryTrajectoryStore) RenewExecution(
	ctx context.Context,
	id string,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if trajectory.trajectory.Execution == nil {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			id,
		)
	}
	execution, err := renewTrajectoryExecution(
		*trajectory.trajectory.Execution,
		executionID,
		token,
		now,
		ttl,
	)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
	trajectory.trajectory.UpdatedAt = now
	return execution, nil
}

func (store *memoryTrajectoryStore) CommitExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if commit.TrajectoryID == "" {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution commit has no trajectory ID",
		)
	}
	if len(commit.Entries) == 0 && commit.State == "" {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution commit has no mutation",
		)
	}
	entries, err := bindTrajectoryExecutionEntries(
		commit.ExecutionID,
		commit.Entries,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	commit.Entries = entries
	now := time.Now().UTC()
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(commit.TrajectoryID)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.trajectory.Head != commit.ExpectedHead {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			commit.TrajectoryID,
			trajectory.trajectory.Head,
			commit.ExpectedHead,
		)
	}
	if trajectory.trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, err := commitTrajectoryExecution(
		*trajectory.trajectory.Execution,
		commit,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if len(commit.Entries) > 0 {
		if _, err := store.appendLocked(
			trajectory,
			commit.TrajectoryID,
			commit.ExpectedHead,
			commit.Entries,
		); err != nil {
			return sdk.TrajectoryMetadata{}, err
		}
	}
	trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
	trajectory.trajectory.UpdatedAt = now
	return trajectoryMetadata(
		trajectory.trajectory,
		trajectory.inheritedCount+len(trajectory.order),
		len(trajectory.order),
	), nil
}

func (store *memoryTrajectoryStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	result := make([]sdk.TrajectoryMetadata, 0)
	for _, trajectory := range store.trajectories {
		execution := trajectory.trajectory.Execution
		if execution == nil || !execution.RecoverableAt(now) {
			continue
		}
		result = append(result, trajectoryMetadata(
			trajectory.trajectory,
			trajectory.inheritedCount+len(trajectory.order),
			len(trajectory.order),
		))
	}
	slices.SortFunc(result, func(left, right sdk.TrajectoryMetadata) int {
		if order := left.Execution.CreatedAt.Compare(
			right.Execution.CreatedAt,
		); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})
	return result, nil
}

func (store *memoryTrajectoryStore) appendLocked(
	trajectory *memoryTrajectory,
	id string,
	expectedHead string,
	entries []sdk.TrajectoryEntry,
) (string, error) {
	if trajectory.trajectory.Head != expectedHead {
		return "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			id,
			trajectory.trajectory.Head,
			expectedHead,
		)
	}
	prepared, err := prepareTrajectoryEntries(
		id,
		uint64(len(trajectory.order)),
		trajectory.trajectory.Head != "",
		entries,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.entryLocked(id, entryID)
		},
	)
	if err != nil {
		return "", err
	}
	preparedIndex := make(map[string]sdk.TrajectoryEntry, len(prepared))
	for _, entry := range prepared {
		preparedIndex[entry.ID] = entry
	}
	last := prepared[len(prepared)-1]
	checkpointID, err := latestCheckpointAfterAppend(
		trajectory.trajectory.Head,
		trajectory.trajectory.Checkpoint,
		last.ID,
		preparedIndex,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.entryLocked(id, entryID)
		},
	)
	if err != nil {
		return "", err
	}
	for _, entry := range prepared {
		trajectory.entries[entry.ID] = entry
		trajectory.order = append(trajectory.order, entry.ID)
	}
	trajectory.trajectory.Head = last.ID
	trajectory.trajectory.UpdatedAt = last.Timestamp
	trajectory.trajectory.Checkpoint = checkpointID
	return last.ID, nil
}

func (store *memoryTrajectoryStore) LoadMetadata(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadata(
		trajectory.trajectory,
		trajectory.inheritedCount+len(trajectory.order),
		len(trajectory.order),
	), nil
}

func (store *memoryTrajectoryStore) LoadEntry(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := sdk.ValidateResourceName("trajectory entry", entryID); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	entry, found, err := store.entryLocked(id, entryID)
	if err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if !found {
		return sdk.TrajectoryEntry{}, fmt.Errorf(
			"%w: trajectory %s entry %s",
			sdk.ErrTrajectoryEntryNotFound,
			id,
			entryID,
		)
	}
	return cloneTrajectoryEntry(entry), nil
}

func (store *memoryTrajectoryStore) LoadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.branchLocked(id, head)
}

func (store *memoryTrajectoryStore) LoadBranchView(
	ctx context.Context,
	id string,
	head string,
) (sdk.Trajectory, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	branch, err := store.branchLocked(id, head)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	return projectTrajectoryBranch(
		trajectoryMetadata(
			trajectory.trajectory,
			trajectory.inheritedCount+len(trajectory.order),
			len(trajectory.order),
		),
		head,
		branch,
	), nil
}

func (store *memoryTrajectoryStore) FindLatest(
	ctx context.Context,
	id string,
	head string,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if err := validateTrajectoryKind(kind); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.findLatestLocked(id, head, kind)
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
	return store.materializeLocked(id)
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
		result = append(result, summarizeTrajectory(
			trajectory.trajectory,
			trajectory.inheritedCount+len(trajectory.order),
			len(trajectory.order),
		))
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
	store.mu.Lock()
	defer store.mu.Unlock()
	target, err := store.trajectoryLocked(id)
	if err != nil {
		return err
	}
	if target.trajectory.Execution != nil &&
		!target.trajectory.Execution.Terminal() {
		return fmt.Errorf(
			"%w: trajectory %s execution %s is active",
			sdk.ErrTrajectoryExecution,
			id,
			target.trajectory.Execution.ID,
		)
	}
	for _, trajectory := range store.trajectories {
		if trajectory.trajectory.ParentID == id {
			return fmt.Errorf(
				"%w: trajectory %s is parent of %s",
				sdk.ErrTrajectoryReferenced,
				id,
				trajectory.trajectory.ID,
			)
		}
	}
	delete(store.trajectories, id)
	return nil
}

func (store *memoryTrajectoryStore) entryLocked(
	id string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	trajectory, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if entry, found := trajectory.entries[entryID]; found {
		return cloneTrajectoryEntry(entry), true, nil
	}
	if trajectory.trajectory.SchemaVersion < 2 ||
		trajectory.trajectory.ParentID == "" {
		return sdk.TrajectoryEntry{}, false, nil
	}
	return store.entryOnBranchLocked(
		trajectory.trajectory.ParentID,
		trajectory.trajectory.ParentEntryID,
		entryID,
	)
}

func (store *memoryTrajectoryStore) entryOnBranchLocked(
	id string,
	head string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	return findEntryOnBranch(id, head, entryID, func(
		cursor string,
	) (sdk.TrajectoryEntry, bool, error) {
		return store.entryLocked(id, cursor)
	})
}

func (store *memoryTrajectoryStore) branchLocked(
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if _, err := store.trajectoryLocked(id); err != nil {
		return nil, err
	}
	return resolveBranch(id, head, func(
		cursor string,
	) (sdk.TrajectoryEntry, bool, error) {
		return store.entryLocked(id, cursor)
	})
}

func (store *memoryTrajectoryStore) findLatestLocked(
	id string,
	head string,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool, error) {
	if _, err := store.trajectoryLocked(id); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	return latestEntry(
		head,
		kind,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.entryLocked(id, entryID)
		},
	)
}

func (store *memoryTrajectoryStore) materializeLocked(
	id string,
) (sdk.Trajectory, error) {
	stored, err := store.trajectoryLocked(id)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory := cloneTrajectory(stored.trajectory)
	trajectory.Entries = make(
		[]sdk.TrajectoryEntry,
		0,
		stored.inheritedCount+len(stored.order),
	)
	if trajectory.SchemaVersion >= 2 && trajectory.ParentID != "" {
		inherited, err := store.branchLocked(
			trajectory.ParentID,
			trajectory.ParentEntryID,
		)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(trajectory.Entries, inherited...)
	}
	for _, entryID := range stored.order {
		trajectory.Entries = append(
			trajectory.Entries,
			cloneTrajectoryEntry(stored.entries[entryID]),
		)
	}
	return trajectory, nil
}
