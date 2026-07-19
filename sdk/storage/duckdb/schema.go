package duckdb

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	duckDBTrajectorySchemaVersion = 1
	duckDBDeliverySchemaVersion   = 1
	duckDBOperationSchemaVersion  = 1
	duckDBContextSchemaVersion    = 1
)

var duckDBTrajectorySchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS ag_storage_schema (
		component VARCHAR PRIMARY KEY,
		version INTEGER NOT NULL
	)`,
	`INSERT INTO ag_storage_schema (component, version)
			VALUES ('trajectory', 1)
			ON CONFLICT DO NOTHING`,
	`INSERT INTO ag_storage_schema (component, version)
			VALUES ('delivery', 1)
			ON CONFLICT DO NOTHING`,
	`INSERT INTO ag_storage_schema (component, version)
			VALUES ('operation', 1)
			ON CONFLICT DO NOTHING`,
	`INSERT INTO ag_storage_schema (component, version)
			VALUES ('context_injection', 1)
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
		audit_json VARCHAR,
		PRIMARY KEY (namespace, trajectory_id, entry_id)
	)`,
	`ALTER TABLE ag_trajectory_entries
			ADD COLUMN IF NOT EXISTS audit_json VARCHAR`,
	`CREATE TABLE IF NOT EXISTS ag_operations (
			namespace VARCHAR NOT NULL,
			id VARCHAR NOT NULL,
			idempotency_key VARCHAR NOT NULL,
			kind VARCHAR NOT NULL,
			resource VARCHAR NOT NULL,
			resource_revision VARCHAR NOT NULL,
			state VARCHAR NOT NULL,
			revision UBIGINT NOT NULL,
			input BLOB NOT NULL,
			invocation_json VARCHAR NOT NULL,
			invocation_root_id VARCHAR NOT NULL,
			invocation_parent_id VARCHAR NOT NULL,
			invocation_group_id VARCHAR NOT NULL,
			output BLOB,
			operation_error VARCHAR NOT NULL,
			submitted_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			lease_owner VARCHAR NOT NULL,
			lease_token VARCHAR NOT NULL,
			lease_expires_at TIMESTAMPTZ,
			PRIMARY KEY (namespace, id)
		)`,
	`CREATE TABLE IF NOT EXISTS ag_deliveries (
			namespace VARCHAR NOT NULL,
			queue VARCHAR NOT NULL,
			id VARCHAR NOT NULL,
			sequence UBIGINT NOT NULL,
			plugin VARCHAR NOT NULL,
			plugin_version VARCHAR NOT NULL,
			subscription VARCHAR NOT NULL,
			resource_revision VARCHAR NOT NULL,
			partition_key VARCHAR NOT NULL,
			event_id VARCHAR NOT NULL,
			event_name VARCHAR NOT NULL,
			event_session_id VARCHAR NOT NULL,
			event_generation UBIGINT NOT NULL,
			event_payload BLOB NOT NULL,
			state VARCHAR NOT NULL,
			attempt INTEGER NOT NULL,
			available_at TIMESTAMPTZ,
			lease_token VARCHAR NOT NULL,
			lease_expires_at TIMESTAMPTZ,
			last_error VARCHAR NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (namespace, queue, id)
		)`,
	`CREATE TABLE IF NOT EXISTS ag_context_injections (
			namespace VARCHAR NOT NULL,
			id VARCHAR NOT NULL,
			sequence UBIGINT NOT NULL,
			priority VARCHAR NOT NULL,
			mode VARCHAR NOT NULL,
			origin VARCHAR NOT NULL,
			target_session_id VARCHAR NOT NULL,
			target_execution_id VARCHAR NOT NULL,
			is_meta BOOLEAN NOT NULL,
			messages BLOB NOT NULL,
			attributes_json VARCHAR,
			created_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (namespace, id)
		)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ag_deliveries_sequence_idx
			ON ag_deliveries (namespace, queue, sequence)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ag_context_injections_sequence_idx
			ON ag_context_injections (namespace, sequence)`,
	`CREATE INDEX IF NOT EXISTS ag_context_injections_target_idx
			ON ag_context_injections (
				namespace,
				target_session_id,
				target_execution_id,
				sequence
			)`,
	`CREATE INDEX IF NOT EXISTS ag_deliveries_ready_idx
			ON ag_deliveries (
				namespace,
				queue,
				state,
				available_at,
				lease_expires_at,
				sequence
			)`,
	`CREATE INDEX IF NOT EXISTS ag_deliveries_partition_idx
			ON ag_deliveries (
				namespace,
				queue,
				partition_key,
				sequence
			)`,
	`CREATE INDEX IF NOT EXISTS ag_deliveries_updated_idx
			ON ag_deliveries (namespace, queue, updated_at, id)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ag_operations_idempotency_idx
			ON ag_operations (
				namespace,
				kind,
				resource,
				resource_revision,
				idempotency_key
			)`,
	`CREATE INDEX IF NOT EXISTS ag_operations_state_idx
			ON ag_operations (namespace, state, lease_expires_at, updated_at)`,
	`CREATE INDEX IF NOT EXISTS ag_operations_updated_idx
			ON ag_operations (namespace, updated_at, id)`,
	`CREATE INDEX IF NOT EXISTS ag_operations_invocation_root_idx
			ON ag_operations (namespace, invocation_root_id, submitted_at, id)`,
	`CREATE INDEX IF NOT EXISTS ag_operations_invocation_parent_idx
			ON ag_operations (namespace, invocation_parent_id, submitted_at, id)`,
	`CREATE INDEX IF NOT EXISTS ag_operations_invocation_group_idx
			ON ag_operations (namespace, invocation_group_id, submitted_at, id)`,
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
	if err := validateDuckDBSchemaComponent(
		ctx,
		db,
		"trajectory",
		duckDBTrajectorySchemaVersion,
	); err != nil {
		return err
	}
	if err := validateDuckDBSchemaComponent(
		ctx,
		db,
		"delivery",
		duckDBDeliverySchemaVersion,
	); err != nil {
		return err
	}
	if err := validateDuckDBSchemaComponent(
		ctx,
		db,
		"operation",
		duckDBOperationSchemaVersion,
	); err != nil {
		return err
	}
	if err := validateDuckDBSchemaComponent(
		ctx,
		db,
		"context_injection",
		duckDBContextSchemaVersion,
	); err != nil {
		return err
	}
	return nil
}

func validateDuckDBSchemaComponent(
	ctx context.Context,
	db *sql.DB,
	component string,
	supportedVersion int,
) error {
	var version int
	if err := db.QueryRowContext(
		ctx,
		`SELECT version
		 FROM ag_storage_schema
		 WHERE component = ?`,
		component,
	).Scan(&version); err != nil {
		return fmt.Errorf("read DuckDB %s schema version: %w", component, err)
	}
	if version > supportedVersion {
		return fmt.Errorf(
			"DuckDB %s schema version %d is newer than supported version %d",
			component,
			version,
			supportedVersion,
		)
	}
	if version < supportedVersion {
		return fmt.Errorf(
			"DuckDB %s schema version %d requires migration to version %d",
			component,
			version,
			supportedVersion,
		)
	}
	return nil
}
