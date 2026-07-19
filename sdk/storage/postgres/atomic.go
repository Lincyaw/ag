package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lincyaw/ag/sdk"
)

func (backend *Backend) StartExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
) (sdk.ExecutionMutationResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.ExecutionMutationResult{}, errors.New(
			"execution-start commit has no trajectory",
		)
	}
	now := time.Now().UTC()
	var metadata sdk.TrajectoryMetadata
	if err := backend.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var err error
			metadata, err = backend.trajectories.beginExecutionInTx(
				ctx,
				tx,
				commit.TrajectoryID,
				commit.ExpectedHead,
				commit.Start,
				commit.Input,
				now,
			)
			return true, err
		},
	); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (backend *Backend) AppendTrajectory(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.TrajectoryAppendResult{}, errors.New(
			"trajectory-append commit has no trajectory",
		)
	}
	var metadata sdk.TrajectoryMetadata
	if err := backend.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var err error
			metadata, err = backend.trajectories.appendTrajectoryInTx(
				ctx,
				tx,
				commit,
			)
			return true, err
		},
	); err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	return sdk.TrajectoryAppendResult{Trajectory: metadata}, nil
}

func (backend *Backend) CommitExecution(
	ctx context.Context,
	commit sdk.ExecutionMutationCommit,
) (sdk.ExecutionMutationResult, error) {
	if commit.Trajectory.TrajectoryID == "" {
		return sdk.ExecutionMutationResult{}, errors.New(
			"execution mutation commit has no trajectory",
		)
	}
	now := time.Now().UTC()
	var metadata sdk.TrajectoryMetadata
	if err := backend.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var err error
			metadata, err = backend.trajectories.commitExecutionInTx(
				ctx,
				tx,
				commit.Trajectory,
				now,
			)
			return true, err
		},
	); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (backend *Backend) CancelExecution(
	ctx context.Context,
	commit sdk.ExecutionCancelCommit,
) (sdk.ExecutionCancelResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.ExecutionCancelResult{}, errors.New(
			"execution-cancel commit has no trajectory",
		)
	}
	if commit.ExecutionID == "" {
		return sdk.ExecutionCancelResult{}, errors.New(
			"execution-cancel commit has no execution",
		)
	}
	now := commit.At.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var metadata sdk.TrajectoryMetadata
	var changed bool
	if err := backend.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx pgx.Tx) (bool, error) {
			var err error
			metadata, changed, err = backend.trajectories.cancelExecutionInTx(
				ctx,
				tx,
				commit.TrajectoryCommit(),
				now,
			)
			return changed, err
		},
	); err != nil {
		return sdk.ExecutionCancelResult{}, err
	}
	return sdk.ExecutionCancelResult{
		Trajectory: metadata,
		Changed:    changed,
	}, nil
}

var _ sdk.AtomicStateBackend = (*Backend)(nil)

func (backend *Backend) commitAtomicStateMutation(
	ctx context.Context,
	outbox []sdk.StateMutationDeliveries,
	mutate func(context.Context, pgx.Tx) (bool, error),
) error {
	preparedOutbox, err := prepareStateMutationOutbox(outbox)
	if err != nil {
		return err
	}
	tx, err := backend.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	enqueueOutbox, err := mutate(ctx, tx)
	if err != nil {
		return err
	}
	if enqueueOutbox {
		if err := backend.enqueueStateMutationOutbox(
			ctx,
			tx,
			outbox,
			preparedOutbox,
		); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func prepareStateMutationOutbox(
	groups []sdk.StateMutationDeliveries,
) ([][]sdk.Delivery, error) {
	preparedOutbox := make([][]sdk.Delivery, len(groups))
	for index, group := range groups {
		if err := validateAtomicDeliveryQueue(group.Queue); err != nil {
			return nil, err
		}
		prepared, err := prepareDeliveries(group.Deliveries)
		if err != nil {
			return nil, err
		}
		preparedOutbox[index] = prepared
	}
	return preparedOutbox, nil
}

func (backend *Backend) enqueueStateMutationOutbox(
	ctx context.Context,
	tx pgx.Tx,
	groups []sdk.StateMutationDeliveries,
	prepared [][]sdk.Delivery,
) error {
	for index, group := range groups {
		outbox := newDeliveryStore(
			backend.pool,
			backend.namespace,
			group.Queue,
		)
		if err := outbox.enqueueInTx(ctx, tx, prepared[index]); err != nil {
			return err
		}
	}
	return nil
}
