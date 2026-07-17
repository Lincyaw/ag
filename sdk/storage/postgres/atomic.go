package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lincyaw/ag/sdk"
)

func (backend *Backend) CommitExecutionStep(
	ctx context.Context,
	commit sdk.ExecutionStepCommit,
) (sdk.ExecutionStepResult, error) {
	if commit.Trajectory.TrajectoryID == "" {
		return sdk.ExecutionStepResult{}, errors.New(
			"execution-step commit has no trajectory",
		)
	}
	if commit.InboxAck != nil {
		if err := validateExecutionStepQueue(
			commit.InboxAck.Queue,
		); err != nil {
			return sdk.ExecutionStepResult{}, err
		}
	}
	preparedOutbox := make(
		[][]sdk.Delivery,
		len(commit.Outbox),
	)
	for index, group := range commit.Outbox {
		if err := validateExecutionStepQueue(group.Queue); err != nil {
			return sdk.ExecutionStepResult{}, err
		}
		prepared, err := prepareDeliveries(group.Deliveries)
		if err != nil {
			return sdk.ExecutionStepResult{}, err
		}
		preparedOutbox[index] = prepared
	}
	now := time.Now().UTC()
	tx, err := backend.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.ExecutionStepResult{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	metadata, err := backend.trajectories.commitExecutionInTx(
		ctx,
		tx,
		commit.Trajectory,
		now,
	)
	if err != nil {
		return sdk.ExecutionStepResult{}, err
	}
	result := sdk.ExecutionStepResult{Trajectory: metadata}
	if commit.Operation != nil {
		record, err := backend.operations.completeInTx(
			ctx,
			tx,
			commit.Operation.ID,
			commit.Operation.LeaseToken,
			commit.Operation.State,
			commit.Operation.Output,
			commit.Operation.Error,
			now,
		)
		if err != nil {
			return sdk.ExecutionStepResult{}, err
		}
		result.Operation = &record
	}
	if commit.InboxAck != nil {
		at := commit.InboxAck.At.UTC()
		if at.IsZero() {
			at = now
		}
		inbox := newDeliveryStore(
			backend.pool,
			backend.namespace,
			commit.InboxAck.Queue,
		)
		if err := inbox.ackInTx(
			ctx,
			tx,
			commit.InboxAck.ID,
			commit.InboxAck.LeaseToken,
			at,
		); err != nil {
			return sdk.ExecutionStepResult{}, err
		}
	}
	for index, group := range commit.Outbox {
		outbox := newDeliveryStore(
			backend.pool,
			backend.namespace,
			group.Queue,
		)
		if err := outbox.enqueueInTx(
			ctx,
			tx,
			preparedOutbox[index],
		); err != nil {
			return sdk.ExecutionStepResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.ExecutionStepResult{}, err
	}
	return result, nil
}

var _ sdk.AtomicStateBackend = (*Backend)(nil)
