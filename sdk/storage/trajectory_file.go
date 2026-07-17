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
	"sync"
	"time"

	. "github.com/lincyaw/ag/sdk"
)

var trajectoryDirectoryLocks sync.Map

type FileTrajectoryStore struct {
	directory string
	lockPath  string
	mu        *sync.RWMutex
}

func NewFileTrajectoryStore(directory string) (*FileTrajectoryStore, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(directory))
	if err != nil {
		return nil, fmt.Errorf("resolve trajectory directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create trajectory directory: %w", err)
	}
	value, _ := trajectoryDirectoryLocks.LoadOrStore(
		absolute,
		&sync.RWMutex{},
	)
	return &FileTrajectoryStore{
		directory: absolute,
		lockPath:  filepath.Join(absolute, ".trajectories.lock"),
		mu:        value.(*sync.RWMutex),
	}, nil
}

func (store *FileTrajectoryStore) Directory() string {
	if store == nil {
		return ""
	}
	return store.directory
}

func (store *FileTrajectoryStore) Create(
	ctx context.Context,
	trajectory Trajectory,
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
	trajectory.Entries = []TrajectoryEntry{}

	store.mu.Lock()
	defer store.mu.Unlock()
	return WithFileLock(store.lockPath, true, func() error {
		path := store.path(trajectory.ID)
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%w: %s", ErrTrajectoryExists, trajectory.ID)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("stat trajectory %q: %w", trajectory.ID, err)
		}
		return store.writeLocked(ctx, trajectory)
	})
}

func (store *FileTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...TrajectoryEntry,
) (string, error) {
	if err := ValidateResourceName("trajectory", id); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	var head string
	err := WithFileLock(store.lockPath, true, func() error {
		trajectory, readErr := store.readLocked(id)
		if readErr != nil {
			return readErr
		}
		if trajectory.Head != expectedHead {
			return fmt.Errorf(
				"%w: trajectory %s has head %q, expected %q",
				ErrTrajectoryConflict,
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

func (store *FileTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (Trajectory, error) {
	if err := ValidateResourceName("trajectory", id); err != nil {
		return Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return Trajectory{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	var trajectory Trajectory
	err := WithFileLock(store.lockPath, false, func() error {
		var readErr error
		trajectory, readErr = store.readLocked(id)
		return readErr
	})
	return trajectory, err
}

func (store *FileTrajectoryStore) List(
	ctx context.Context,
) ([]TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	var result []TrajectorySummary
	err := WithFileLock(store.lockPath, false, func() error {
		paths, globErr := filepath.Glob(filepath.Join(store.directory, "*.json"))
		if globErr != nil {
			return fmt.Errorf("list trajectories: %w", globErr)
		}
		result = make([]TrajectorySummary, 0, len(paths))
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
		slices.SortFunc(result, func(left, right TrajectorySummary) int {
			if order := left.CreatedAt.Compare(right.CreatedAt); order != 0 {
				return order
			}
			return strings.Compare(left.ID, right.ID)
		})
		return nil
	})
	return result, err
}

func (store *FileTrajectoryStore) ListPage(
	ctx context.Context,
	request PageRequest,
) (TrajectoryPage, error) {
	items, err := store.List(ctx)
	if err != nil {
		return TrajectoryPage{}, err
	}
	page, next, err := PageWindow(
		items,
		request,
		func(item TrajectorySummary) string { return item.ID },
	)
	if err != nil {
		return TrajectoryPage{}, err
	}
	return TrajectoryPage{Items: page, Next: next}, nil
}

func (store *FileTrajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return WithFileLock(store.lockPath, true, func() error {
		err := os.Remove(store.path(id))
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrTrajectoryNotFound, id)
		}
		if err != nil {
			return fmt.Errorf("delete trajectory %q: %w", id, err)
		}
		return SyncDirectory(store.directory)
	})
}

func (store *FileTrajectoryStore) path(id string) string {
	return filepath.Join(store.directory, id+".json")
}

func (store *FileTrajectoryStore) readLocked(id string) (Trajectory, error) {
	raw, err := os.ReadFile(store.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return Trajectory{}, fmt.Errorf("%w: %s", ErrTrajectoryNotFound, id)
	}
	if err != nil {
		return Trajectory{}, fmt.Errorf("read trajectory %q: %w", id, err)
	}
	var trajectory Trajectory
	if err := json.Unmarshal(raw, &trajectory); err != nil {
		return Trajectory{}, fmt.Errorf("decode trajectory %q: %w", id, err)
	}
	if trajectory.ID != id {
		return Trajectory{}, fmt.Errorf(
			"trajectory file %q contains id %q",
			id,
			trajectory.ID,
		)
	}
	if err := validateLoadedTrajectory(&trajectory); err != nil {
		return Trajectory{}, fmt.Errorf("validate trajectory %q: %w", id, err)
	}
	return cloneTrajectory(trajectory), nil
}

func (store *FileTrajectoryStore) writeLocked(
	ctx context.Context,
	trajectory Trajectory,
) error {
	return WriteJSONAtomic(
		ctx,
		store.directory,
		store.path(trajectory.ID),
		".trajectory-*.tmp",
		fmt.Sprintf("trajectory %q", trajectory.ID),
		trajectory,
	)
}
