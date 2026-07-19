package storage

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/internal/filestate"
	"github.com/lincyaw/ag/sdk"
)

func (store *memoryTrajectoryStore) CancelExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
) (sdk.TrajectoryExecutionCancelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	now := normalizedMutationTime(commit.At)
	entries, err := bindTrajectoryExecutionEntries(
		commit.ExecutionID,
		commit.Entries,
	)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	trajectory, err := store.trajectoryLocked(commit.TrajectoryID)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	if trajectory.trajectory.Execution == nil {
		return sdk.TrajectoryExecutionCancelResult{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, changed, err := cancelTrajectoryExecution(
		*trajectory.trajectory.Execution,
		commit.ExecutionID,
		commit.Reason,
		now,
	)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	if changed {
		if len(entries) > 0 {
			if _, err := store.appendLocked(
				trajectory,
				commit.TrajectoryID,
				commit.ExpectedHead,
				entries,
			); err != nil {
				return sdk.TrajectoryExecutionCancelResult{}, err
			}
		}
		trajectory.trajectory.Execution = cloneTrajectoryExecution(&execution)
		trajectory.trajectory.UpdatedAt = now
	}
	return sdk.TrajectoryExecutionCancelResult{
		Trajectory: trajectoryMetadata(
			trajectory.trajectory,
			trajectory.inheritedCount+len(trajectory.order),
			len(trajectory.order),
		),
		Changed: changed,
	}, nil
}

func (store *fileTrajectoryStore) CancelExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
) (sdk.TrajectoryExecutionCancelResult, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	now := normalizedMutationTime(commit.At)
	entries, err := bindTrajectoryExecutionEntries(
		commit.ExecutionID,
		commit.Entries,
	)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	var result sdk.TrajectoryExecutionCancelResult
	err = filestate.WithExclusiveLock(store.lockPath, func() error {
		stored, err := store.readStoredLocked(commit.TrajectoryID)
		if err != nil {
			return err
		}
		if stored.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				commit.TrajectoryID,
			)
		}
		execution, changed, err := cancelTrajectoryExecution(
			*stored.Execution,
			commit.ExecutionID,
			commit.Reason,
			now,
		)
		if err != nil {
			return err
		}
		if changed {
			if len(entries) > 0 {
				stored, _, err = store.appendStoredLocked(
					stored,
					commit.TrajectoryID,
					entries,
					commit.ExpectedHead,
				)
				if err != nil {
					return err
				}
			}
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
		result = sdk.TrajectoryExecutionCancelResult{
			Trajectory: trajectoryMetadata(
				stored,
				len(materialized.Entries),
				len(stored.Entries),
			),
			Changed: changed,
		}
		return nil
	})
	return result, err
}
