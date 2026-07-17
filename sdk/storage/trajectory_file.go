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
		trajectory, readErr := store.readLocked(id)
		if readErr != nil {
			return readErr
		}
		if trajectory.Head != expectedHead {
			return fmt.Errorf(
				"%w: trajectory %s has head %q, expected %q",
				sdk.ErrTrajectoryConflict,
				id,
				trajectory.Head,
				expectedHead,
			)
		}
		next, appendErr := appendTrajectoryEntries(trajectory, entries)
		if appendErr != nil {
			return appendErr
		}
		if writeErr := store.writeLocked(ctx, next); writeErr != nil {
			return writeErr
		}
		head = next.Head
		return nil
	})
	return head, err
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
		trajectory, readErr = store.readLocked(id)
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
			trajectory, readErr := store.readLocked(id)
			if readErr != nil {
				return readErr
			}
			result = append(result, summarizeTrajectory(trajectory))
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

func (store *fileTrajectoryStore) readLocked(id string) (sdk.Trajectory, error) {
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
