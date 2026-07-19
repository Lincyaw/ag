package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lincyaw/ag/sdk"
)

func (store *TrajectoryStore) appendEntries(
	ctx context.Context,
	tx pgx.Tx,
	trajectory sdk.Trajectory,
	ownedCount uint64,
	expectedHead string,
	entries []sdk.TrajectoryEntry,
) (string, error) {
	if trajectory.Head != expectedHead {
		return "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			trajectory.ID,
			trajectory.Head,
			expectedHead,
		)
	}
	prepared, err := prepareTrajectoryEntries(
		trajectory.ID,
		ownedCount,
		trajectory.Head != "",
		entries,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.loadEntry(
				ctx,
				tx,
				trajectory.ID,
				entryID,
			)
		},
	)
	if err != nil {
		return "", err
	}
	for _, entry := range prepared {
		if err := store.insertEntry(ctx, tx, entry); err != nil {
			return "", err
		}
	}
	last := prepared[len(prepared)-1]
	preparedIndex := make(
		map[string]sdk.TrajectoryEntry,
		len(prepared),
	)
	for _, entry := range prepared {
		preparedIndex[entry.ID] = entry
	}
	checkpoint, err := latestCheckpointAfterAppend(
		trajectory.Head,
		trajectory.Checkpoint,
		last.ID,
		preparedIndex,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return store.loadEntry(
				ctx,
				tx,
				trajectory.ID,
				entryID,
			)
		},
	)
	if err != nil {
		return "", err
	}
	tag, err := tx.Exec(
		ctx,
		`UPDATE ag_trajectories
		 SET head = $1,
		     checkpoint = $2,
		     updated_at = $3,
		     owned_entry_count = owned_entry_count + $4
		 WHERE namespace = $5
		   AND id = $6
		   AND head = $7`,
		last.ID,
		checkpoint,
		last.Timestamp.UTC(),
		len(prepared),
		store.namespace,
		trajectory.ID,
		expectedHead,
	)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() != 1 {
		return "", fmt.Errorf(
			"%w: trajectory %s head changed while appending",
			sdk.ErrTrajectoryConflict,
			trajectory.ID,
		)
	}
	return last.ID, nil
}

func (store *TrajectoryStore) insertEntry(
	ctx context.Context,
	tx pgx.Tx,
	entry sdk.TrajectoryEntry,
) error {
	attributesJSON, err := trajectoryAttributesJSON(entry.Attributes)
	if err != nil {
		return fmt.Errorf(
			"encode trajectory entry %q attributes: %w",
			entry.ID,
			err,
		)
	}
	auditJSON, err := trajectoryAuditJSON(entry.Audit)
	if err != nil {
		return fmt.Errorf(
			"encode trajectory entry %q audit: %w",
			entry.ID,
			err,
		)
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO ag_trajectory_entries (
			namespace,
			trajectory_id,
			entry_id,
			parent_id,
			ordinal,
			depth,
			kind,
			recorded_at,
			generation,
			execution_id,
			operation_key,
			turn,
			correlation_id,
			provider,
			model,
			tool_name,
			tool_call_id,
			finish_reason,
			input_tokens,
			output_tokens,
			is_error,
			cause_code,
			action_kind,
			payload_version,
			payload,
			attributes,
			audit
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19,
			$20, $21, $22, $23, $24, $25, $26,
			$27
		)`,
		store.namespace,
		entry.TrajectoryID,
		entry.ID,
		entry.ParentID,
		entry.Ordinal,
		entry.Depth,
		string(entry.Kind),
		entry.Timestamp.UTC(),
		entry.Generation,
		entry.Fields.ExecutionID,
		entry.Fields.OperationKey,
		nullableInt(entry.Fields.Turn),
		entry.Fields.CorrelationID,
		entry.Fields.Provider,
		entry.Fields.Model,
		entry.Fields.ToolName,
		entry.Fields.ToolCallID,
		entry.Fields.FinishReason,
		entry.Fields.InputTokens,
		entry.Fields.OutputTokens,
		nullableBool(entry.Fields.IsError),
		entry.Fields.CauseCode,
		string(entry.Fields.ActionKind),
		entry.PayloadVersion,
		[]byte(entry.Payload),
		attributesJSON,
		auditJSON,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf(
				"trajectory entry %q already exists: %w",
				entry.ID,
				sdk.ErrTrajectoryConflict,
			)
		}
		return err
	}
	return nil
}

func (store *TrajectoryStore) replaceExecution(
	ctx context.Context,
	tx pgx.Tx,
	trajectoryID string,
	execution sdk.TrajectoryExecution,
) error {
	_, err := tx.Exec(
		ctx,
		`INSERT INTO ag_trajectory_executions (
			namespace,
			trajectory_id,
			execution_id,
			state,
			revision,
			base_head,
			input_entry_id,
			provider,
			system_prompt,
			max_turns,
			owner,
			lease_token,
			lease_expires_at,
			last_error,
			created_at,
			updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16
		)
		ON CONFLICT (namespace, trajectory_id) DO UPDATE
		SET execution_id = EXCLUDED.execution_id,
		    state = EXCLUDED.state,
		    revision = EXCLUDED.revision,
		    base_head = EXCLUDED.base_head,
		    input_entry_id = EXCLUDED.input_entry_id,
		    provider = EXCLUDED.provider,
		    system_prompt = EXCLUDED.system_prompt,
		    max_turns = EXCLUDED.max_turns,
		    owner = EXCLUDED.owner,
		    lease_token = EXCLUDED.lease_token,
		    lease_expires_at = EXCLUDED.lease_expires_at,
		    last_error = EXCLUDED.last_error,
		    created_at = EXCLUDED.created_at,
		    updated_at = EXCLUDED.updated_at`,
		store.namespace,
		trajectoryID,
		execution.ID,
		string(execution.State),
		execution.Revision,
		execution.BaseHead,
		execution.InputEntryID,
		execution.Provider,
		execution.System,
		execution.MaxTurns,
		execution.Owner,
		execution.LeaseToken,
		nullableTime(execution.LeaseExpiresAt),
		execution.LastError,
		execution.CreatedAt.UTC(),
		execution.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf(
			"store PostgreSQL trajectory %q execution: %w",
			trajectoryID,
			err,
		)
	}
	return nil
}

func (store *TrajectoryStore) metadataInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
) (sdk.TrajectoryMetadata, error) {
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, tx, id, false)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadata(
		trajectory,
		int(inheritedCount+ownedCount),
		int(ownedCount),
	), nil
}

func (store *TrajectoryStore) BeginExecution(
	ctx context.Context,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryMetadata, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	metadata, err := store.beginExecutionInTx(
		ctx,
		tx,
		id,
		expectedHead,
		start,
		input,
		time.Now().UTC(),
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return metadata, nil
}

func (store *TrajectoryStore) beginExecutionInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if input.Kind != sdk.TrajectoryKindUserMessage {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution input must be a user_message entry",
		)
	}
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
	trajectory, _, ownedCount, err := store.loadStoredTrajectory(
		ctx,
		tx,
		id,
		true,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			id,
			trajectory.Execution.ID,
		)
	}
	if _, err := store.appendEntries(
		ctx,
		tx,
		trajectory,
		ownedCount,
		expectedHead,
		bound,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := store.replaceExecution(ctx, tx, id, execution); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	metadata, err := store.metadataInTx(ctx, tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return metadata, nil
}

func (store *TrajectoryStore) ClaimExecution(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = normalizedMutationTime(now)
	return store.mutateExecution(
		ctx,
		id,
		func(
			execution sdk.TrajectoryExecution,
		) (sdk.TrajectoryExecution, error) {
			return claimTrajectoryExecution(
				execution,
				owner,
				now,
				ttl,
			)
		},
	)
}

func (store *TrajectoryStore) RenewExecution(
	ctx context.Context,
	id string,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = normalizedMutationTime(now)
	return store.mutateExecution(
		ctx,
		id,
		func(
			execution sdk.TrajectoryExecution,
		) (sdk.TrajectoryExecution, error) {
			return renewTrajectoryExecution(
				execution,
				executionID,
				token,
				now,
				ttl,
			)
		},
	)
}

func (store *TrajectoryStore) mutateExecution(
	ctx context.Context,
	id string,
	mutation func(
		sdk.TrajectoryExecution,
	) (sdk.TrajectoryExecution, error),
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	trajectory, _, _, err := store.loadStoredTrajectory(
		ctx,
		tx,
		id,
		true,
	)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if trajectory.Execution == nil {
		return sdk.TrajectoryExecution{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			id,
		)
	}
	execution, err := mutation(*trajectory.Execution)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := store.replaceExecution(
		ctx,
		tx,
		id,
		execution,
	); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE ag_trajectories
		 SET updated_at = $1
		 WHERE namespace = $2 AND id = $3`,
		execution.UpdatedAt.UTC(),
		store.namespace,
		id,
	); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	return execution, nil
}

func (store *TrajectoryStore) CommitExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	metadata, err := store.commitExecutionInTx(
		ctx,
		tx,
		commit,
		time.Now().UTC(),
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return metadata, nil
}

func (store *TrajectoryStore) commitExecutionInTx(
	ctx context.Context,
	tx pgx.Tx,
	commit sdk.TrajectoryExecutionCommit,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
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
	trajectory, _, ownedCount, err := store.loadStoredTrajectory(
		ctx,
		tx,
		commit.TrajectoryID,
		true,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Head != commit.ExpectedHead {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			commit.TrajectoryID,
			trajectory.Head,
			commit.ExpectedHead,
		)
	}
	if trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, err := commitTrajectoryExecution(
		*trajectory.Execution,
		commit,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if len(commit.Entries) > 0 {
		if _, err := store.appendEntries(
			ctx,
			tx,
			trajectory,
			ownedCount,
			commit.ExpectedHead,
			commit.Entries,
		); err != nil {
			return sdk.TrajectoryMetadata{}, err
		}
	}
	if err := store.replaceExecution(
		ctx,
		tx,
		commit.TrajectoryID,
		execution,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE ag_trajectories
		 SET updated_at = $1
		 WHERE namespace = $2 AND id = $3`,
		now,
		store.namespace,
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return store.metadataInTx(ctx, tx, commit.TrajectoryID)
}

func (store *TrajectoryStore) CancelExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
) (sdk.TrajectoryExecutionCancelResult, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	now := normalizedMutationTime(commit.At)
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	metadata, changed, err := store.cancelExecutionInTx(
		ctx,
		tx,
		commit,
		now,
	)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	return sdk.TrajectoryExecutionCancelResult{
		Trajectory: metadata,
		Changed:    changed,
	}, nil
}

func (store *TrajectoryStore) cancelExecutionInTx(
	ctx context.Context,
	tx pgx.Tx,
	commit sdk.TrajectoryExecutionCancelCommit,
	now time.Time,
) (sdk.TrajectoryMetadata, bool, error) {
	if err := sdk.ValidateResourceName(
		"trajectory",
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	entries, err := bindTrajectoryExecutionEntries(
		commit.ExecutionID,
		commit.Entries,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	trajectory, _, ownedCount, err := store.loadStoredTrajectory(
		ctx,
		tx,
		commit.TrajectoryID,
		true,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	if trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, false, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, changed, err := cancelTrajectoryExecution(
		*trajectory.Execution,
		commit.ExecutionID,
		commit.Reason,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	if changed {
		if len(entries) > 0 && trajectory.Head != commit.ExpectedHead {
			return sdk.TrajectoryMetadata{}, false, fmt.Errorf(
				"%w: trajectory %s has head %q, expected %q",
				sdk.ErrTrajectoryConflict,
				commit.TrajectoryID,
				trajectory.Head,
				commit.ExpectedHead,
			)
		}
		if len(entries) > 0 {
			if _, err := store.appendEntries(
				ctx,
				tx,
				trajectory,
				ownedCount,
				commit.ExpectedHead,
				entries,
			); err != nil {
				return sdk.TrajectoryMetadata{}, false, err
			}
		}
		if err := store.replaceExecution(
			ctx,
			tx,
			commit.TrajectoryID,
			execution,
		); err != nil {
			return sdk.TrajectoryMetadata{}, false, err
		}
		if len(entries) == 0 {
			if _, err := tx.Exec(
				ctx,
				`UPDATE ag_trajectories
				 SET updated_at = $1
				 WHERE namespace = $2 AND id = $3`,
				now,
				store.namespace,
				commit.TrajectoryID,
			); err != nil {
				return sdk.TrajectoryMetadata{}, false, err
			}
		}
	}
	metadata, err := store.metadataInTx(ctx, tx, commit.TrajectoryID)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	return metadata, changed, nil
}

func (store *TrajectoryStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.TrajectoryMetadata, error) {
	now = normalizedMutationTime(now)
	rows, err := store.pool.Query(
		ctx,
		`SELECT trajectory_id
		 FROM ag_trajectory_executions
		 WHERE namespace = $1
		   AND (
		     state = $2
		     OR (
		       state = $3
		       AND lease_expires_at <= $4
		     )
		   )
		 ORDER BY created_at, trajectory_id`,
		store.namespace,
		string(sdk.TrajectoryExecutionPending),
		string(sdk.TrajectoryExecutionRunning),
		now,
	)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]sdk.TrajectoryMetadata, 0, len(ids))
	for _, id := range ids {
		metadata, err := store.LoadMetadata(ctx, id)
		if err != nil {
			return nil, err
		}
		result = append(result, metadata)
	}
	return result, nil
}

func (store *TrajectoryStore) AnalyzeEntries(
	ctx context.Context,
	query sdk.TrajectoryEntryQuery,
) ([]sdk.TrajectoryEntry, error) {
	where := `namespace = $1`
	args := []any{store.namespace}
	add := func(column string, value any) {
		args = append(args, value)
		where += fmt.Sprintf(
			` AND %s = $%d`,
			column,
			len(args),
		)
	}
	if query.TrajectoryID != "" {
		if err := sdk.ValidateResourceName(
			"trajectory",
			query.TrajectoryID,
		); err != nil {
			return nil, err
		}
		add("trajectory_id", query.TrajectoryID)
	}
	if query.ExecutionID != "" {
		add("execution_id", query.ExecutionID)
	}
	if query.OperationKey != "" {
		add("operation_key", query.OperationKey)
	}
	if query.Kind != "" {
		if err := validateTrajectoryKind(query.Kind); err != nil {
			return nil, err
		}
		add("kind", string(query.Kind))
	}
	if query.Provider != "" {
		add("provider", query.Provider)
	}
	if query.ToolName != "" {
		add("tool_name", query.ToolName)
	}
	if query.CorrelationID != "" {
		add("correlation_id", query.CorrelationID)
	}
	limit := query.Limit
	if limit == 0 {
		limit = sdk.DefaultPageSize
	}
	if limit < 0 || limit > sdk.MaxPageSize {
		return nil, fmt.Errorf(
			"trajectory analysis limit %d must be between 0 and %d",
			limit,
			sdk.MaxPageSize,
		)
	}
	args = append(args, limit)
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+trajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE `+where+`
		 ORDER BY recorded_at, trajectory_id, ordinal
		 LIMIT $`+fmt.Sprint(len(args)),
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]sdk.TrajectoryEntry, 0)
	for rows.Next() {
		entry, err := scanPostgresTrajectoryEntry(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}

func validateAtomicDeliveryQueue(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("atomic delivery queue is empty")
	}
	return sdk.ValidateResourceName("delivery queue", name)
}
