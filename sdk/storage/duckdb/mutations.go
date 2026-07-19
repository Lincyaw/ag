package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func (store *duckDBTrajectoryStore) appendEntries(
	ctx context.Context,
	tx *sql.Tx,
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
	preparedIndex := make(map[string]sdk.TrajectoryEntry, len(prepared))
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
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE ag_trajectories
		 SET head = ?,
		     checkpoint = ?,
		     updated_at = ?,
		     owned_entry_count = owned_entry_count + ?
		 WHERE namespace = ? AND id = ?`,
		last.ID,
		checkpoint,
		last.Timestamp.UTC(),
		len(prepared),
		store.namespace,
		trajectory.ID,
	); err != nil {
		return "", mapDuckDBTrajectoryWriteError(
			fmt.Errorf(
				"advance DuckDB trajectory %q head: %w",
				trajectory.ID,
				err,
			),
		)
	}
	return last.ID, nil
}

func (store *duckDBTrajectoryStore) insertEntry(
	ctx context.Context,
	tx *sql.Tx,
	entry sdk.TrajectoryEntry,
) error {
	attributesJSON, err := duckDBAttributesJSON(entry.Attributes)
	if err != nil {
		return fmt.Errorf(
			"encode trajectory entry %q attributes: %w",
			entry.ID,
			err,
		)
	}
	auditJSON, err := duckDBAuditJSON(entry.Audit)
	if err != nil {
		return fmt.Errorf(
			"encode trajectory entry %q audit: %w",
			entry.ID,
			err,
		)
	}
	_, err = tx.ExecContext(
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
			attributes_json,
			audit_json
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?
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
		duckDBNullableInt(entry.Fields.Turn),
		entry.Fields.CorrelationID,
		entry.Fields.Provider,
		entry.Fields.Model,
		entry.Fields.ToolName,
		entry.Fields.ToolCallID,
		entry.Fields.FinishReason,
		entry.Fields.InputTokens,
		entry.Fields.OutputTokens,
		duckDBNullableBool(entry.Fields.IsError),
		entry.Fields.CauseCode,
		string(entry.Fields.ActionKind),
		entry.PayloadVersion,
		[]byte(entry.Payload),
		attributesJSON,
		auditJSON,
	)
	if err != nil {
		return mapDuckDBTrajectoryWriteError(
			fmt.Errorf(
				"insert DuckDB trajectory %q entry %q: %w",
				entry.TrajectoryID,
				entry.ID,
				err,
			),
		)
	}
	return nil
}

func (store *duckDBTrajectoryStore) replaceExecution(
	ctx context.Context,
	tx *sql.Tx,
	trajectoryID string,
	execution sdk.TrajectoryExecution,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM ag_trajectory_executions
		 WHERE namespace = ? AND trajectory_id = ?`,
		store.namespace,
		trajectoryID,
	); err != nil {
		return mapDuckDBTrajectoryWriteError(err)
	}
	_, err := tx.ExecContext(
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
		duckDBNullableTime(execution.LeaseExpiresAt),
		execution.LastError,
		execution.CreatedAt.UTC(),
		execution.UpdatedAt.UTC(),
	)
	if err != nil {
		return mapDuckDBTrajectoryWriteError(
			fmt.Errorf(
				"store DuckDB trajectory %q execution: %w",
				trajectoryID,
				err,
			),
		)
	}
	return nil
}

func (store *duckDBTrajectoryStore) metadataInTransaction(
	ctx context.Context,
	tx *sql.Tx,
	id string,
) (sdk.TrajectoryMetadata, error) {
	trajectory, inheritedCount, ownedCount, err :=
		store.loadStoredTrajectory(ctx, tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadata(
		trajectory,
		int(inheritedCount+ownedCount),
		int(ownedCount),
	), nil
}

func (store *duckDBTrajectoryStore) BeginExecution(
	ctx context.Context,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	defer tx.Rollback()
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
	if err := tx.Commit(); err != nil {
		return sdk.TrajectoryMetadata{}, mapDuckDBTrajectoryWriteError(err)
	}
	return metadata, nil
}

func (store *duckDBTrajectoryStore) beginExecutionInTx(
	ctx context.Context,
	tx *sql.Tx,
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
	trajectory, _, ownedCount, err := store.loadStoredTrajectory(ctx, tx, id)
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
	metadata, err := store.metadataInTransaction(ctx, tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return metadata, nil
}

func (store *duckDBTrajectoryStore) ClaimExecution(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return store.mutateExecution(
		ctx,
		id,
		func(
			execution sdk.TrajectoryExecution,
		) (sdk.TrajectoryExecution, error) {
			return claimTrajectoryExecution(execution, owner, now, ttl)
		},
	)
}

func (store *duckDBTrajectoryStore) RenewExecution(
	ctx context.Context,
	id string,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
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

func (store *duckDBTrajectoryStore) mutateExecution(
	ctx context.Context,
	id string,
	mutation func(
		sdk.TrajectoryExecution,
	) (sdk.TrajectoryExecution, error),
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	defer tx.Rollback()
	trajectory, _, _, err := store.loadStoredTrajectory(ctx, tx, id)
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
	if err := store.replaceExecution(ctx, tx, id, execution); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE ag_trajectories
		 SET updated_at = ?
		 WHERE namespace = ? AND id = ?`,
		execution.UpdatedAt.UTC(),
		store.namespace,
		id,
	); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := tx.Commit(); err != nil {
		return sdk.TrajectoryExecution{}, mapDuckDBTrajectoryWriteError(err)
	}
	return execution, nil
}

func (store *duckDBTrajectoryStore) CommitExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	defer tx.Rollback()
	metadata, err := store.commitExecutionInTx(
		ctx,
		tx,
		commit,
		time.Now().UTC(),
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := tx.Commit(); err != nil {
		return sdk.TrajectoryMetadata{}, mapDuckDBTrajectoryWriteError(err)
	}
	return metadata, nil
}

func (store *duckDBTrajectoryStore) commitExecutionInTx(
	ctx context.Context,
	tx *sql.Tx,
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
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE ag_trajectories
		 SET updated_at = ?
		 WHERE namespace = ? AND id = ?`,
		now,
		store.namespace,
		commit.TrajectoryID,
	); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	metadata, err := store.metadataInTransaction(
		ctx,
		tx,
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return metadata, nil
}

func (store *duckDBTrajectoryStore) CancelExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
) (sdk.TrajectoryExecutionCancelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	defer tx.Rollback()
	metadata, changed, err := store.cancelExecutionInTx(
		ctx,
		tx,
		commit,
		normalizedMutationTime(commit.At),
	)
	if err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return sdk.TrajectoryExecutionCancelResult{},
			mapDuckDBTrajectoryWriteError(err)
	}
	return sdk.TrajectoryExecutionCancelResult{
		Trajectory: metadata,
		Changed:    changed,
	}, nil
}

func (store *duckDBTrajectoryStore) cancelExecutionInTx(
	ctx context.Context,
	tx *sql.Tx,
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
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE ag_trajectories
			 SET updated_at = ?
			 WHERE namespace = ? AND id = ?`,
			now,
			store.namespace,
			commit.TrajectoryID,
		); err != nil {
			return sdk.TrajectoryMetadata{}, false, err
		}
	}
	metadata, err := store.metadataInTransaction(ctx, tx, commit.TrajectoryID)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	return metadata, changed, nil
}

func (store *duckDBTrajectoryStore) ListRecoverable(
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
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT trajectory_id
		 FROM ag_trajectory_executions
		 WHERE namespace = ?
		   AND (
		     state = ?
		     OR (
		       state = ?
		       AND lease_expires_at <= ?
		     )
		   )
		 ORDER BY created_at, trajectory_id`,
		store.namespace,
		string(sdk.TrajectoryExecutionPending),
		string(sdk.TrajectoryExecutionRunning),
		now,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list recoverable DuckDB trajectory executions: %w",
			err,
		)
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
	if err := rows.Close(); err != nil {
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

func (store *duckDBTrajectoryStore) AnalyzeEntries(
	ctx context.Context,
	query sdk.TrajectoryEntryQuery,
) ([]sdk.TrajectoryEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := `namespace = ?`
	args := []any{store.namespace}
	if query.TrajectoryID != "" {
		if err := sdk.ValidateResourceName(
			"trajectory",
			query.TrajectoryID,
		); err != nil {
			return nil, err
		}
		where += ` AND trajectory_id = ?`
		args = append(args, query.TrajectoryID)
	}
	if query.ExecutionID != "" {
		where += ` AND execution_id = ?`
		args = append(args, query.ExecutionID)
	}
	if query.OperationKey != "" {
		where += ` AND operation_key = ?`
		args = append(args, query.OperationKey)
	}
	if query.Kind != "" {
		if err := validateTrajectoryKind(query.Kind); err != nil {
			return nil, err
		}
		where += ` AND kind = ?`
		args = append(args, string(query.Kind))
	}
	if query.Provider != "" {
		where += ` AND provider = ?`
		args = append(args, query.Provider)
	}
	if query.ToolName != "" {
		where += ` AND tool_name = ?`
		args = append(args, query.ToolName)
	}
	if query.CorrelationID != "" {
		where += ` AND correlation_id = ?`
		args = append(args, query.CorrelationID)
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
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBTrajectoryEntryColumns+`
		 FROM ag_trajectory_entries
		 WHERE `+where+`
		 ORDER BY recorded_at, trajectory_id, ordinal
		 LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("analyze DuckDB trajectory entries: %w", err)
	}
	defer rows.Close()
	result := make([]sdk.TrajectoryEntry, 0)
	for rows.Next() {
		entry, err := scanDuckDBTrajectoryEntry(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}
