package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/lincyaw/ag/sdk"
)

const trajectoryEntryColumns = `
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
	attributes`

func (store *TrajectoryStore) loadStoredTrajectory(
	ctx context.Context,
	query queryer,
	id string,
	lock bool,
) (sdk.Trajectory, uint64, uint64, error) {
	statement := `SELECT
		schema_version,
		id,
		parent_id,
		parent_entry_id,
		created_at,
		updated_at,
		head,
		checkpoint,
		environment,
		inherited_entry_count,
		owned_entry_count
	 FROM ag_trajectories
	 WHERE namespace = $1 AND id = $2`
	if lock {
		statement += ` FOR UPDATE`
	}
	var trajectory sdk.Trajectory
	var environmentJSON []byte
	var inheritedCount uint64
	var ownedCount uint64
	err := query.QueryRow(
		ctx,
		statement,
		store.namespace,
		id,
	).Scan(
		&trajectory.SchemaVersion,
		&trajectory.ID,
		&trajectory.ParentID,
		&trajectory.ParentEntryID,
		&trajectory.CreatedAt,
		&trajectory.UpdatedAt,
		&trajectory.Head,
		&trajectory.Checkpoint,
		&environmentJSON,
		&inheritedCount,
		&ownedCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"%w: %s",
			sdk.ErrTrajectoryNotFound,
			id,
		)
	}
	if err != nil {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"load PostgreSQL trajectory %q: %w",
			id,
			err,
		)
	}
	trajectory.CreatedAt = trajectory.CreatedAt.UTC()
	trajectory.UpdatedAt = trajectory.UpdatedAt.UTC()
	if err := json.Unmarshal(
		environmentJSON,
		&trajectory.Environment,
	); err != nil {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"decode PostgreSQL trajectory %q environment: %w",
			id,
			err,
		)
	}
	execution, err := store.loadExecution(ctx, query, id)
	if err != nil {
		return sdk.Trajectory{}, 0, 0, err
	}
	trajectory.Execution = execution
	trajectory.Entries = []sdk.TrajectoryEntry{}
	return trajectory, inheritedCount, ownedCount, nil
}

func (store *TrajectoryStore) loadExecution(
	ctx context.Context,
	query queryer,
	trajectoryID string,
) (*sdk.TrajectoryExecution, error) {
	var execution sdk.TrajectoryExecution
	var leaseExpiresAt sql.NullTime
	err := query.QueryRow(
		ctx,
		`SELECT
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
		 FROM ag_trajectory_executions
		 WHERE namespace = $1 AND trajectory_id = $2`,
		store.namespace,
		trajectoryID,
	).Scan(
		&execution.ID,
		&execution.State,
		&execution.Revision,
		&execution.BaseHead,
		&execution.InputEntryID,
		&execution.Provider,
		&execution.System,
		&execution.MaxTurns,
		&execution.Owner,
		&execution.LeaseToken,
		&leaseExpiresAt,
		&execution.LastError,
		&execution.CreatedAt,
		&execution.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"load PostgreSQL trajectory %q execution: %w",
			trajectoryID,
			err,
		)
	}
	if leaseExpiresAt.Valid {
		execution.LeaseExpiresAt = leaseExpiresAt.Time.UTC()
	}
	execution.CreatedAt = execution.CreatedAt.UTC()
	execution.UpdatedAt = execution.UpdatedAt.UTC()
	if err := validateTrajectoryExecution(execution); err != nil {
		return nil, fmt.Errorf(
			"validate PostgreSQL trajectory %q execution: %w",
			trajectoryID,
			err,
		)
	}
	return &execution, nil
}

func scanPostgresTrajectoryEntry(scanner interface {
	Scan(...any) error
}) (sdk.TrajectoryEntry, error) {
	var entry sdk.TrajectoryEntry
	var kind string
	var actionKind string
	var turn sql.NullInt64
	var isError sql.NullBool
	var attributesJSON []byte
	if err := scanner.Scan(
		&entry.TrajectoryID,
		&entry.ID,
		&entry.ParentID,
		&entry.Ordinal,
		&entry.Depth,
		&kind,
		&entry.Timestamp,
		&entry.Generation,
		&entry.Fields.ExecutionID,
		&entry.Fields.OperationKey,
		&turn,
		&entry.Fields.CorrelationID,
		&entry.Fields.Provider,
		&entry.Fields.Model,
		&entry.Fields.ToolName,
		&entry.Fields.ToolCallID,
		&entry.Fields.FinishReason,
		&entry.Fields.InputTokens,
		&entry.Fields.OutputTokens,
		&isError,
		&entry.Fields.CauseCode,
		&actionKind,
		&entry.PayloadVersion,
		&entry.Payload,
		&attributesJSON,
	); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	entry.Kind = sdk.TrajectoryKind(kind)
	entry.Fields.ActionKind = sdk.ActionKind(actionKind)
	entry.Timestamp = entry.Timestamp.UTC()
	if turn.Valid {
		value := int(turn.Int64)
		entry.Fields.Turn = &value
	}
	if isError.Valid {
		value := isError.Bool
		entry.Fields.IsError = &value
	}
	entry.Payload = append(json.RawMessage(nil), entry.Payload...)
	if len(attributesJSON) != 0 {
		if err := json.Unmarshal(
			attributesJSON,
			&entry.Attributes,
		); err != nil {
			return sdk.TrajectoryEntry{}, fmt.Errorf(
				"decode trajectory entry %q attributes: %w",
				entry.ID,
				err,
			)
		}
	}
	return entry, nil
}

func trajectoryAttributesJSON(
	attributes map[string]string,
) (any, error) {
	if attributes == nil {
		return nil, nil
	}
	raw, err := json.Marshal(attributes)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func trajectoryEnvironmentJSON(
	environment sdk.TrajectoryEnvironment,
) ([]byte, error) {
	return json.Marshal(environment)
}
