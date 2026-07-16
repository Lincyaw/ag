package sdk

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
)

var trajectoryDirectoryLocks sync.Map

type FileTrajectoryStore struct {
	directory string
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
	path := store.path(trajectory.ID)
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%w: %s", ErrTrajectoryExists, trajectory.ID)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat trajectory %q: %w", trajectory.ID, err)
	}
	return store.writeLocked(ctx, trajectory)
}

func (store *FileTrajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...TrajectoryEntry,
) (string, error) {
	if err := validateResourceName("trajectory", id); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.readLocked(id)
	if err != nil {
		return "", err
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
	if err := store.writeLocked(ctx, next); err != nil {
		return "", err
	}
	return next.Head, nil
}

func (store *FileTrajectoryStore) Load(
	ctx context.Context,
	id string,
) (Trajectory, error) {
	if err := validateResourceName("trajectory", id); err != nil {
		return Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return Trajectory{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.readLocked(id)
}

func (store *FileTrajectoryStore) List(
	ctx context.Context,
) ([]TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	paths, err := filepath.Glob(filepath.Join(store.directory, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("list trajectories: %w", err)
	}
	result := make([]TrajectorySummary, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		trajectory, err := store.readLocked(id)
		if err != nil {
			return nil, err
		}
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
	return cloneTrajectory(trajectory), nil
}

func (store *FileTrajectoryStore) writeLocked(
	ctx context.Context,
	trajectory Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(trajectory)
	if err != nil {
		return fmt.Errorf("encode trajectory %q: %w", trajectory.ID, err)
	}
	temporary, err := os.CreateTemp(store.directory, ".trajectory-*.tmp")
	if err != nil {
		return fmt.Errorf("create trajectory temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure trajectory temporary file: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write trajectory %q: %w", trajectory.ID, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync trajectory %q: %w", trajectory.ID, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close trajectory %q: %w", trajectory.ID, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path(trajectory.ID)); err != nil {
		return fmt.Errorf("publish trajectory %q: %w", trajectory.ID, err)
	}
	removeTemporary = false
	directory, err := os.Open(store.directory)
	if err != nil {
		return fmt.Errorf("open trajectory directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync trajectory directory: %w", err)
	}
	return nil
}
