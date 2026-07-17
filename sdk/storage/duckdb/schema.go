package duckdb

import (
	"context"
	"database/sql"
	"fmt"
)

const duckDBTrajectorySchemaVersion = 1

var duckDBTrajectorySchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS ag_storage_schema (
		component VARCHAR PRIMARY KEY,
		version INTEGER NOT NULL
	)`,
	`INSERT INTO ag_storage_schema (component, version)
		VALUES ('trajectory', 1)
		ON CONFLICT DO NOTHING`,
	`CREATE TABLE IF NOT EXISTS ag_trajectories (
		namespace VARCHAR NOT NULL,
		id VARCHAR NOT NULL,
		schema_version UINTEGER NOT NULL,
		parent_id VARCHAR NOT NULL,
		parent_entry_id VARCHAR NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		head VARCHAR NOT NULL,
		checkpoint VARCHAR NOT NULL,
		environment_json VARCHAR NOT NULL,
		inherited_entry_count UBIGINT NOT NULL,
		owned_entry_count UBIGINT NOT NULL,
		PRIMARY KEY (namespace, id)
	)`,
	`CREATE TABLE IF NOT EXISTS ag_trajectory_executions (
		namespace VARCHAR NOT NULL,
		trajectory_id VARCHAR NOT NULL,
		execution_id VARCHAR NOT NULL,
		state VARCHAR NOT NULL,
		revision UBIGINT NOT NULL,
		base_head VARCHAR NOT NULL,
		input_entry_id VARCHAR NOT NULL,
		provider VARCHAR NOT NULL,
		system_prompt VARCHAR NOT NULL,
		max_turns INTEGER NOT NULL,
		owner VARCHAR NOT NULL,
		lease_token VARCHAR NOT NULL,
		lease_expires_at TIMESTAMPTZ,
		last_error VARCHAR NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (namespace, trajectory_id)
	)`,
	`CREATE TABLE IF NOT EXISTS ag_trajectory_entries (
		namespace VARCHAR NOT NULL,
		trajectory_id VARCHAR NOT NULL,
		entry_id VARCHAR NOT NULL,
		parent_id VARCHAR NOT NULL,
		ordinal UBIGINT NOT NULL,
		depth UBIGINT NOT NULL,
		kind VARCHAR NOT NULL,
		recorded_at TIMESTAMPTZ NOT NULL,
		generation UBIGINT NOT NULL,
		execution_id VARCHAR NOT NULL,
		operation_key VARCHAR NOT NULL,
		turn INTEGER,
		correlation_id VARCHAR NOT NULL,
		provider VARCHAR NOT NULL,
		model VARCHAR NOT NULL,
		tool_name VARCHAR NOT NULL,
		tool_call_id VARCHAR NOT NULL,
		finish_reason VARCHAR NOT NULL,
		input_tokens BIGINT NOT NULL,
		output_tokens BIGINT NOT NULL,
		is_error BOOLEAN,
		cause_code VARCHAR NOT NULL,
		action_kind VARCHAR NOT NULL,
		payload_version UINTEGER NOT NULL,
		payload BLOB NOT NULL,
		attributes_json VARCHAR,
		PRIMARY KEY (namespace, trajectory_id, entry_id)
	)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_parent_idx
		ON ag_trajectory_entries (namespace, trajectory_id, parent_id)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_kind_idx
		ON ag_trajectory_entries (namespace, trajectory_id, kind, ordinal)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_execution_idx
		ON ag_trajectory_entries (namespace, execution_id, ordinal)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_operation_idx
		ON ag_trajectory_entries (namespace, operation_key)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_tool_idx
		ON ag_trajectory_entries (
			namespace,
			tool_name,
			tool_call_id
		)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_correlation_idx
		ON ag_trajectory_entries (namespace, correlation_id)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_time_idx
		ON ag_trajectory_entries (namespace, recorded_at)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectory_executions_recovery_idx
		ON ag_trajectory_executions (
			namespace,
			state,
			lease_expires_at
		)`,
	`CREATE INDEX IF NOT EXISTS ag_trajectories_updated_idx
		ON ag_trajectories (namespace, updated_at, id)`,
}

func initDuckDBTrajectorySchema(ctx context.Context, db *sql.DB) error {
	for _, statement := range duckDBTrajectorySchemaStatements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize DuckDB trajectory schema: %w", err)
		}
	}
	var version int
	if err := db.QueryRowContext(
		ctx,
		`SELECT version
		 FROM ag_storage_schema
		 WHERE component = 'trajectory'`,
	).Scan(&version); err != nil {
		return fmt.Errorf("read DuckDB trajectory schema version: %w", err)
	}
	if version > duckDBTrajectorySchemaVersion {
		return fmt.Errorf(
			"DuckDB trajectory schema version %d is newer than supported version %d",
			version,
			duckDBTrajectorySchemaVersion,
		)
	}
	if version < duckDBTrajectorySchemaVersion {
		return fmt.Errorf(
			"DuckDB trajectory schema version %d requires migration to version %d",
			version,
			duckDBTrajectorySchemaVersion,
		)
	}
	return nil
}
