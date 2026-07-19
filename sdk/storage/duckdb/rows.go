package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const duckDBTrajectoryEntryColumns = `
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
	audit_json`

type duckDBScanner interface {
	Scan(...any) error
}

type duckDBQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type duckDBExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (store *duckDBTrajectoryStore) loadStoredTrajectory(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
) (sdk.Trajectory, uint64, uint64, error) {
	var trajectory sdk.Trajectory
	var schemaVersion uint32
	var environmentJSON string
	var inheritedCount uint64
	var ownedCount uint64
	err := queryer.QueryRowContext(
		ctx,
		`SELECT
			schema_version,
			id,
			parent_id,
			parent_entry_id,
			created_at,
			updated_at,
			head,
			checkpoint,
			environment_json,
			inherited_entry_count,
			owned_entry_count
		 FROM ag_trajectories
		 WHERE namespace = ? AND id = ?`,
		store.namespace,
		id,
	).Scan(
		&schemaVersion,
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
	if errors.Is(err, sql.ErrNoRows) {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"%w: %s",
			sdk.ErrTrajectoryNotFound,
			id,
		)
	}
	if err != nil {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"load DuckDB trajectory %q: %w",
			id,
			err,
		)
	}
	trajectory.SchemaVersion = schemaVersion
	if err := json.Unmarshal(
		[]byte(environmentJSON),
		&trajectory.Environment,
	); err != nil {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf(
			"decode DuckDB trajectory %q environment: %w",
			id,
			err,
		)
	}
	execution, err := store.loadExecution(ctx, queryer, id)
	if err != nil {
		return sdk.Trajectory{}, 0, 0, err
	}
	trajectory.Execution = execution
	trajectory.Entries = []sdk.TrajectoryEntry{}
	return trajectory, inheritedCount, ownedCount, nil
}

func (store *duckDBTrajectoryStore) loadExecution(
	ctx context.Context,
	queryer duckDBQueryer,
	trajectoryID string,
) (*sdk.TrajectoryExecution, error) {
	var execution sdk.TrajectoryExecution
	var leaseExpiresAt sql.NullTime
	err := queryer.QueryRowContext(
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
		 WHERE namespace = ? AND trajectory_id = ?`,
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
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"load DuckDB trajectory %q execution: %w",
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
			"validate DuckDB trajectory %q execution: %w",
			trajectoryID,
			err,
		)
	}
	return &execution, nil
}

func scanDuckDBTrajectoryEntry(
	scanner duckDBScanner,
) (sdk.TrajectoryEntry, error) {
	var entry sdk.TrajectoryEntry
	var kind string
	var actionKind string
	var turn sql.NullInt64
	var isError sql.NullBool
	var attributesJSON sql.NullString
	var auditJSON sql.NullString
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
		&auditJSON,
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
	if attributesJSON.Valid {
		if err := json.Unmarshal(
			[]byte(attributesJSON.String),
			&entry.Attributes,
		); err != nil {
			return sdk.TrajectoryEntry{}, fmt.Errorf(
				"decode trajectory entry %q attributes: %w",
				entry.ID,
				err,
			)
		}
	}
	if auditJSON.Valid {
		if err := json.Unmarshal(
			[]byte(auditJSON.String),
			&entry.Audit,
		); err != nil {
			return sdk.TrajectoryEntry{}, fmt.Errorf(
				"decode trajectory entry %q audit: %w",
				entry.ID,
				err,
			)
		}
		entry.Audit = sdk.CloneEventAudits(entry.Audit)
	}
	return entry, nil
}

func duckDBNullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func duckDBNullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func duckDBNullableBool(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func duckDBAttributesJSON(attributes map[string]string) (any, error) {
	if attributes == nil {
		return nil, nil
	}
	raw, err := json.Marshal(attributes)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

func duckDBAuditJSON(audits []sdk.EventAudit) (any, error) {
	if len(audits) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(audits)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

func duckDBEnvironmentJSON(
	environment sdk.TrajectoryEnvironment,
) (string, error) {
	raw, err := json.Marshal(environment)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
