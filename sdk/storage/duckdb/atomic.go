package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func (store *duckDBTrajectoryStore) AppendTrajectoryCommit(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.TrajectoryAppendResult{}, errors.New(
			"trajectory-append commit has no trajectory",
		)
	}
	var metadata sdk.TrajectoryMetadata
	if err := store.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx *sql.Tx) (bool, error) {
			var err error
			metadata, err = store.appendTrajectoryInTx(ctx, tx, commit)
			return true, err
		},
	); err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	return sdk.TrajectoryAppendResult{Trajectory: metadata}, nil
}

func (store *duckDBTrajectoryStore) StartExecutionCommit(
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
	if err := store.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx *sql.Tx) (bool, error) {
			var err error
			metadata, err = store.beginExecutionInTx(
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

func (store *duckDBTrajectoryStore) CommitExecutionMutation(
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
	if err := store.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx *sql.Tx) (bool, error) {
			var err error
			metadata, err = store.commitExecutionInTx(
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

func (store *duckDBTrajectoryStore) CancelExecutionCommit(
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
	if err := store.commitAtomicStateMutation(
		ctx,
		commit.Outbox,
		func(ctx context.Context, tx *sql.Tx) (bool, error) {
			var err error
			metadata, changed, err = store.cancelExecutionInTx(
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

func (store *duckDBTrajectoryStore) commitAtomicStateMutation(
	ctx context.Context,
	outbox []sdk.StateMutationDeliveries,
	mutate func(context.Context, *sql.Tx) (bool, error),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	preparedOutbox, err := prepareDuckDBStateMutationOutbox(outbox)
	if err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	enqueueOutbox, err := mutate(ctx, tx)
	if err != nil {
		return err
	}
	if enqueueOutbox {
		if err := store.enqueueStateMutationOutbox(
			ctx,
			tx,
			outbox,
			preparedOutbox,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return mapDuckDBTrajectoryWriteError(err)
	}
	return nil
}

func prepareDuckDBStateMutationOutbox(
	groups []sdk.StateMutationDeliveries,
) ([][]sdk.Delivery, error) {
	preparedOutbox := make([][]sdk.Delivery, len(groups))
	now := time.Now().UTC()
	for index, group := range groups {
		if err := validateDuckDBAtomicDeliveryQueue(group.Queue); err != nil {
			return nil, err
		}
		prepared, err := prepareNewDeliveries(group.Deliveries, now)
		if err != nil {
			return nil, err
		}
		preparedOutbox[index] = prepared
	}
	return preparedOutbox, nil
}

func (store *duckDBTrajectoryStore) enqueueStateMutationOutbox(
	ctx context.Context,
	tx *sql.Tx,
	groups []sdk.StateMutationDeliveries,
	prepared [][]sdk.Delivery,
) error {
	for index, group := range groups {
		outbox := &DeliveryStore{
			db:        store.db,
			writeMu:   &store.writeMu,
			namespace: store.namespace,
			queue:     group.Queue,
		}
		if err := outbox.enqueueInTx(ctx, tx, prepared[index]); err != nil {
			return err
		}
	}
	return nil
}

func validateDuckDBAtomicDeliveryQueue(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("atomic delivery queue is empty")
	}
	return sdk.ValidateResourceName("delivery queue", name)
}
