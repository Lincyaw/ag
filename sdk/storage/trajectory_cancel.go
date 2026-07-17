package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func (store *memoryTrajectoryStore) CancelExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	now = normalizedMutationTime(now)
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(trajectoryID)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			trajectoryID,
		)
	}
	execution, changed, err := cancelTrajectoryExecution(
		*trajectory.trajectory.Execution,
		executionID,
		reason,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if changed {
		trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
		trajectory.trajectory.UpdatedAt = now
	}
	return trajectoryMetadata(
		trajectory.trajectory,
		trajectory.inheritedCount+len(trajectory.order),
		len(trajectory.order),
	), nil
}

func (store *fileTrajectoryStore) CancelExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		trajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	now = normalizedMutationTime(now)
	var metadata sdk.TrajectoryMetadata
	err := withFileLock(store.lockPath, true, func() error {
		stored, err := store.readStoredLocked(trajectoryID)
		if err != nil {
			return err
		}
		if stored.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				trajectoryID,
			)
		}
		execution, changed, err := cancelTrajectoryExecution(
			*stored.Execution,
			executionID,
			reason,
			now,
		)
		if err != nil {
			return err
		}
		if changed {
			stored.Execution = cloneTrajectoryExecution(&execution)
			stored.UpdatedAt = now
			if err := store.writeLocked(ctx, stored); err != nil {
				return err
			}
		}
		materialized, err := store.materializeStoredLocked(stored)
		if err != nil {
			return err
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
