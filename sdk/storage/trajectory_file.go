package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type fileTrajectoryStore struct {
	directory string
	lockPath  string
}

func NewFileTrajectoryStore(directory string) (sdk.TrajectoryStore, error) {
	absolute, err := prepareDirectory("trajectory", directory)
	if err != nil {
		return nil, err
	}
	return &fileTrajectoryStore{
		directory: absolute,
		lockPath:  filepath.Join(absolute, ".trajectories.lock"),
	}, nil
}

func (store *fileTrajectoryStore) Create(
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

	return withFileLock(store.lockPath, true, func() error {
		path := store.path(trajectory.ID)
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%w: %s", sdk.ErrTrajectoryExists, trajectory.ID)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat trajectory %q: %w", trajectory.ID, err)
		}
		if trajectory.ParentID != "" {
			source, readErr := store.materializeLocked(trajectory.ParentID)
			if readErr != nil {
				return fmt.Errorf(
					"resolve trajectory %q fork point: %w",
					trajectory.ID,
					readErr,
				)
			}
			branch, branchErr := source.Branch(trajectory.ParentEntryID)
			if branchErr != nil {
				return fmt.Errorf(
					"resolve trajectory %q fork point: %w",
					trajectory.ID,
					branchErr,
				)
			}
			trajectory.Head = trajectory.ParentEntryID
			if checkpoint, found := findLatestInBranch(
				branch,
				sdk.TrajectoryKindCheckpoint,
			); found {
				trajectory.Checkpoint = checkpoint.ID
			}
		}
		return store.writeLocked(ctx, trajectory)
	})
}

func (store *fileTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var head string
	err := withFileLock(store.lockPath, true, func() error {
		stored, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		if stored.Execution != nil && !stored.Execution.Terminal() {
			return fmt.Errorf(
				"%w: trajectory %s has active execution %s",
				sdk.ErrTrajectoryExecution,
				id,
				stored.Execution.ID,
			)
		}
		var appendErr error
		stored, head, appendErr = store.appendStoredLocked(
			stored,
			id,
			entries,
			expectedHead,
		)
		if appendErr != nil {
			return appendErr
		}
		if writeErr := store.writeLocked(ctx, stored); writeErr != nil {
			return writeErr
		}
		return nil
	})
	return head, err
}

func (store *fileTrajectoryStore) BeginExecution(
	ctx context.Context,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
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
	var metadata sdk.TrajectoryMetadata
	err = withFileLock(store.lockPath, true, func() error {
		stored, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		if stored.Execution != nil && !stored.Execution.Terminal() {
			return fmt.Errorf(
				"%w: trajectory %s has active execution %s",
				sdk.ErrTrajectoryExecution,
				id,
				stored.Execution.ID,
			)
		}
		stored, _, readErr = store.appendStoredLocked(
			stored,
			id,
			[]sdk.TrajectoryEntry{input},
			expectedHead,
		)
		if readErr != nil {
			return readErr
		}
		stored.Execution = cloneTrajectoryExecution(&execution)
		materialized, materializeErr := store.materializeStoredLocked(stored)
		if materializeErr != nil {
			return materializeErr
		}
		candidate := trajectoryMetadata(
			stored,
			len(materialized.Entries),
			len(stored.Entries),
		)
		if writeErr := store.writeLocked(ctx, stored); writeErr != nil {
			return writeErr
		}
		metadata = candidate
		return nil
	})
	return metadata, err
}

func (store *fileTrajectoryStore) ClaimExecution(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var result sdk.TrajectoryExecution
	err := withFileLock(store.lockPath, true, func() error {
		stored, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		if stored.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				id,
			)
		}
		result, readErr = claimTrajectoryExecution(
			*stored.Execution,
			owner,
			now,
			ttl,
		)
		if readErr != nil {
			return readErr
		}
		stored.Execution = cloneTrajectoryExecution(&result)
		stored.UpdatedAt = now
		return store.writeLocked(ctx, stored)
	})
	return result, err
}

func (store *fileTrajectoryStore) RenewExecution(
	ctx context.Context,
	id string,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var result sdk.TrajectoryExecution
	err := withFileLock(store.lockPath, true, func() error {
		stored, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		if stored.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				id,
			)
		}
		result, readErr = renewTrajectoryExecution(
			*stored.Execution,
			executionID,
			token,
			now,
			ttl,
		)
		if readErr != nil {
			return readErr
		}
		stored.Execution = cloneTrajectoryExecution(&result)
		stored.UpdatedAt = now
		return store.writeLocked(ctx, stored)
	})
	return result, err
}

func (store *fileTrajectoryStore) CommitExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
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
	var metadata sdk.TrajectoryMetadata
	err = withFileLock(store.lockPath, true, func() error {
		stored, readErr := store.readStoredLocked(commit.TrajectoryID)
		if readErr != nil {
			return readErr
		}
		if stored.Head != commit.ExpectedHead {
			return fmt.Errorf(
				"%w: trajectory %s has head %q, expected %q",
				sdk.ErrTrajectoryConflict,
				commit.TrajectoryID,
				stored.Head,
				commit.ExpectedHead,
			)
		}
		if stored.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				commit.TrajectoryID,
			)
		}
		execution, executionErr := commitTrajectoryExecution(
			*stored.Execution,
			commit,
			now,
		)
		if executionErr != nil {
			return executionErr
		}
		if len(commit.Entries) > 0 {
			stored, _, readErr = store.appendStoredLocked(
				stored,
				commit.TrajectoryID,
				commit.Entries,
				commit.ExpectedHead,
			)
			if readErr != nil {
				return readErr
			}
		}
		stored.Execution = cloneTrajectoryExecution(&execution)
		stored.UpdatedAt = now
		materialized, materializeErr := store.materializeStoredLocked(stored)
		if materializeErr != nil {
			return materializeErr
		}
		candidate := trajectoryMetadata(
			stored,
			len(materialized.Entries),
			len(stored.Entries),
		)
		if writeErr := store.writeLocked(ctx, stored); writeErr != nil {
			return writeErr
		}
		metadata = candidate
		return nil
	})
	return metadata, err
}

func (store *fileTrajectoryStore) ListRecoverable(
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
	var result []sdk.TrajectoryMetadata
	err := withFileLock(store.lockPath, false, func() error {
		paths, globErr := filepath.Glob(filepath.Join(store.directory, "*.json"))
		if globErr != nil {
			return fmt.Errorf("list recoverable trajectories: %w", globErr)
		}
		for _, path := range paths {
			if err := ctx.Err(); err != nil {
				return err
			}
			id := strings.TrimSuffix(filepath.Base(path), ".json")
			stored, readErr := store.readStoredLocked(id)
			if readErr != nil {
				return readErr
			}
			execution := stored.Execution
			if execution == nil ||
				execution.Terminal() ||
				(execution.State == sdk.TrajectoryExecutionRunning &&
					execution.LeaseExpiresAt.After(now)) {
				continue
			}
			materialized, materializeErr := store.materializeStoredLocked(stored)
			if materializeErr != nil {
				return materializeErr
			}
			result = append(result, trajectoryMetadata(
				stored,
				len(materialized.Entries),
				len(stored.Entries),
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
		return nil
	})
	return result, err
}

func (store *fileTrajectoryStore) LoadMetadata(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	var metadata sdk.TrajectoryMetadata
	err := withFileLock(store.lockPath, false, func() error {
		stored, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		materialized, materializeErr := store.materializeStoredLocked(stored)
		if materializeErr != nil {
			return materializeErr
		}
		if stored.Checkpoint == "" && stored.Head != "" {
			branch, branchErr := materialized.Branch(stored.Head)
			if branchErr != nil {
				return branchErr
			}
			if checkpoint, found := findLatestInBranch(
				branch,
				sdk.TrajectoryKindCheckpoint,
			); found {
				stored.Checkpoint = checkpoint.ID
			}
		}
		metadata = trajectoryMetadata(
			stored,
			len(materialized.Entries),
			len(stored.Entries),
		)
		return nil
	})
	return metadata, err
}

func (store *fileTrajectoryStore) LoadEntry(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := sdk.ValidateResourceName("trajectory entry", entryID); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	var result sdk.TrajectoryEntry
	err := withFileLock(store.lockPath, false, func() error {
		trajectory, readErr := store.materializeLocked(id)
		if readErr != nil {
			return readErr
		}
		for _, entry := range trajectory.Entries {
			if entry.ID == entryID {
				result = cloneTrajectoryEntry(entry)
				return nil
			}
		}
		return fmt.Errorf(
			"%w: trajectory %s entry %s",
			sdk.ErrTrajectoryEntryNotFound,
			id,
			entryID,
		)
	})
	return result, err
}

func (store *fileTrajectoryStore) LoadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var branch []sdk.TrajectoryEntry
	err := withFileLock(store.lockPath, false, func() error {
		trajectory, readErr := store.materializeLocked(id)
		if readErr != nil {
			return readErr
		}
		var branchErr error
		branch, branchErr = trajectory.Branch(head)
		return branchErr
	})
	return branch, err
}

func (store *fileTrajectoryStore) FindLatest(
	ctx context.Context,
	id string,
	head string,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool, error) {
	if err := validateTrajectoryKind(kind); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	branch, err := store.LoadBranch(ctx, id, head)
	if err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	entry, found := findLatestInBranch(branch, kind)
	return entry, found, nil
}

func (store *fileTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	var trajectory sdk.Trajectory
	err := withFileLock(store.lockPath, false, func() error {
		var readErr error
		trajectory, readErr = store.materializeLocked(id)
		return readErr
	})
	return trajectory, err
}

func (store *fileTrajectoryStore) List(
	ctx context.Context,
) ([]sdk.TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []sdk.TrajectorySummary
	err := withFileLock(store.lockPath, false, func() error {
		paths, globErr := filepath.Glob(filepath.Join(store.directory, "*.json"))
		if globErr != nil {
			return fmt.Errorf("list trajectories: %w", globErr)
		}
		result = make([]sdk.TrajectorySummary, 0, len(paths))
		for _, path := range paths {
			if err := ctx.Err(); err != nil {
				return err
			}
			id := strings.TrimSuffix(filepath.Base(path), ".json")
			stored, readErr := store.readStoredLocked(id)
			if readErr != nil {
				return readErr
			}
			trajectory, materializeErr := store.materializeStoredLocked(stored)
			if materializeErr != nil {
				return materializeErr
			}
			result = append(result, summarizeTrajectory(
				stored,
				len(trajectory.Entries),
				len(stored.Entries),
			))
		}
		slices.SortFunc(result, func(left, right sdk.TrajectorySummary) int {
			if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
				return order
			}
			return strings.Compare(left.ID, right.ID)
		})
		return nil
	})
	return result, err
}

func (store *fileTrajectoryStore) ListPage(
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

func (store *fileTrajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return withFileLock(store.lockPath, true, func() error {
		target, readErr := store.readStoredLocked(id)
		if readErr != nil {
			return readErr
		}
		if target.Execution != nil && !target.Execution.Terminal() {
			return fmt.Errorf(
				"%w: trajectory %s execution %s is active",
				sdk.ErrTrajectoryExecution,
				id,
				target.Execution.ID,
			)
		}
		paths, globErr := filepath.Glob(filepath.Join(store.directory, "*.json"))
		if globErr != nil {
			return fmt.Errorf("list trajectories before delete: %w", globErr)
		}
		for _, path := range paths {
			childID := strings.TrimSuffix(filepath.Base(path), ".json")
			if childID == id {
				continue
			}
			child, childErr := store.readStoredLocked(childID)
			if childErr != nil {
				return childErr
			}
			if child.ParentID == id {
				return fmt.Errorf(
					"%w: trajectory %s is parent of %s",
					sdk.ErrTrajectoryReferenced,
					id,
					childID,
				)
			}
		}
		err := os.Remove(store.path(id))
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
		}
		if err != nil {
			return fmt.Errorf("delete trajectory %q: %w", id, err)
		}
		return syncDirectory(store.directory)
	})
}

func (store *fileTrajectoryStore) path(id string) string {
	return filepath.Join(store.directory, id+".json")
}

func (store *fileTrajectoryStore) readStoredLocked(
	id string,
) (sdk.Trajectory, error) {
	raw, err := os.ReadFile(store.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return sdk.Trajectory{}, fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
	}
	if err != nil {
		return sdk.Trajectory{}, fmt.Errorf("read trajectory %q: %w", id, err)
	}
	var trajectory sdk.Trajectory
	if err := json.Unmarshal(raw, &trajectory); err != nil {
		return sdk.Trajectory{}, fmt.Errorf("decode trajectory %q: %w", id, err)
	}
	if trajectory.ID != id {
		return sdk.Trajectory{}, fmt.Errorf(
			"trajectory file %q contains id %q",
			id,
			trajectory.ID,
		)
	}
	if err := validateLoadedTrajectory(&trajectory); err != nil {
		return sdk.Trajectory{}, fmt.Errorf("validate trajectory %q: %w", id, err)
	}
	return cloneTrajectory(trajectory), nil
}

func (store *fileTrajectoryStore) materializeLocked(
	id string,
) (sdk.Trajectory, error) {
	stored, err := store.readStoredLocked(id)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	return store.materializeStoredLocked(stored)
}

func (store *fileTrajectoryStore) materializeStoredLocked(
	stored sdk.Trajectory,
) (sdk.Trajectory, error) {
	trajectory := cloneTrajectory(stored)
	if stored.SchemaVersion >= 2 && stored.ParentID != "" {
		parent, err := store.materializeLocked(stored.ParentID)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		inherited, err := parent.Branch(stored.ParentEntryID)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = make(
			[]sdk.TrajectoryEntry,
			0,
			len(inherited)+len(stored.Entries),
		)
		trajectory.Entries = append(trajectory.Entries, inherited...)
		for _, entry := range stored.Entries {
			trajectory.Entries = append(
				trajectory.Entries,
				cloneTrajectoryEntry(entry),
			)
		}
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	if stored.SchemaVersion >= 2 {
		checkpoint := ""
		if entry, found := findLatestInBranch(
			branch,
			sdk.TrajectoryKindCheckpoint,
		); found {
			checkpoint = entry.ID
		}
		if checkpoint != stored.Checkpoint {
			return sdk.Trajectory{}, fmt.Errorf(
				"trajectory %q checkpoint is %q, active branch requires %q",
				stored.ID,
				stored.Checkpoint,
				checkpoint,
			)
		}
	}
	return trajectory, nil
}

func (store *fileTrajectoryStore) appendStoredLocked(
	stored sdk.Trajectory,
	id string,
	entries []sdk.TrajectoryEntry,
	expectedHead string,
) (sdk.Trajectory, string, error) {
	if stored.Head != expectedHead {
		return sdk.Trajectory{}, "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			id,
			stored.Head,
			expectedHead,
		)
	}
	materialized, err := store.materializeStoredLocked(stored)
	if err != nil {
		return sdk.Trajectory{}, "", err
	}
	index := make(map[string]sdk.TrajectoryEntry, len(materialized.Entries))
	for _, entry := range materialized.Entries {
		index[entry.ID] = entry
	}
	prepared, err := prepareTrajectoryEntries(
		id,
		uint64(len(stored.Entries)),
		stored.Head != "",
		entries,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			entry, found := index[entryID]
			return entry, found, nil
		},
	)
	if err != nil {
		return sdk.Trajectory{}, "", err
	}
	currentHead := stored.Head
	currentCheckpoint := stored.Checkpoint
	preparedIndex := make(map[string]sdk.TrajectoryEntry, len(prepared))
	for _, entry := range prepared {
		preparedIndex[entry.ID] = entry
	}
	stored.Entries = append(stored.Entries, prepared...)
	last := prepared[len(prepared)-1]
	stored.Head = last.ID
	stored.UpdatedAt = last.Timestamp
	for _, entry := range prepared {
		index[entry.ID] = entry
	}
	stored.Checkpoint, err = latestCheckpointAfterAppend(
		currentHead,
		currentCheckpoint,
		stored.Head,
		preparedIndex,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			entry, found := index[entryID]
			return entry, found, nil
		},
	)
	if err != nil {
		return sdk.Trajectory{}, "", err
	}
	return stored, last.ID, nil
}

func (store *fileTrajectoryStore) writeLocked(
	ctx context.Context,
	trajectory sdk.Trajectory,
) error {
	return writeJSONAtomic(
		ctx,
		store.directory,
		store.path(trajectory.ID),
		".trajectory-*.tmp",
		fmt.Sprintf("trajectory %q", trajectory.ID),
		trajectory,
	)
}
