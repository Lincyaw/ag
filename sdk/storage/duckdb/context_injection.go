package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
)

type ContextInjectionStore struct {
	trajectories *duckDBTrajectoryStore
}

func (store *duckDBTrajectoryStore) ContextInjectionStore() *ContextInjectionStore {
	return &ContextInjectionStore{trajectories: store}
}

func (store *ContextInjectionStore) Enqueue(
	ctx context.Context,
	injections ...sdk.ContextInjection,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.trajectories == nil {
		return errors.New("DuckDB context injection store is nil")
	}
	prepared, err := prepareContextInjections(
		injections,
		time.Now().UTC(),
	)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return nil
	}
	trajectoryStore := store.trajectories
	trajectoryStore.writeMu.Lock()
	defer trajectoryStore.writeMu.Unlock()
	tx, err := trajectoryStore.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin DuckDB context injection enqueue: %w", err)
	}
	defer tx.Rollback()
	var nextSequence uint64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(sequence), 0)
		 FROM ag_context_injections
		 WHERE namespace = ?`,
		trajectoryStore.namespace,
	).Scan(&nextSequence); err != nil {
		return fmt.Errorf("load DuckDB context injection sequence: %w", err)
	}
	for _, injection := range prepared {
		existing, loadErr := store.load(ctx, tx, injection.ID)
		if loadErr == nil {
			if !sameContextInjectionIdentity(
				existing.Injection,
				injection,
			) {
				return fmt.Errorf(
					"context injection %q already exists with different identity",
					injection.ID,
				)
			}
			continue
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		nextSequence++
		if err := store.insert(ctx, tx, nextSequence, injection); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DuckDB context injection enqueue: %w", err)
	}
	return nil
}

func (store *ContextInjectionStore) List(
	ctx context.Context,
	query sdk.ContextInjectionQuery,
) ([]sdk.ContextInjection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || store.trajectories == nil {
		return nil, errors.New("DuckDB context injection store is nil")
	}
	if err := validateContextQuery(query); err != nil {
		return nil, err
	}
	statement := `SELECT
			id,
			sequence,
			priority,
			mode,
			origin,
			target_session_id,
			target_execution_id,
			is_meta,
			messages,
			attributes_json,
			created_at
		FROM ag_context_injections
		WHERE namespace = ?
		  AND (? = '' OR target_session_id = '' OR target_session_id = ?)
		  AND (? = '' OR target_execution_id = '' OR target_execution_id = ?)
		ORDER BY sequence`
	args := []any{
		store.trajectories.namespace,
		query.TargetSessionID,
		query.TargetSessionID,
		query.TargetExecutionID,
		query.TargetExecutionID,
	}
	if query.Limit > 0 {
		statement += ` LIMIT ?`
		args = append(args, query.Limit)
	}
	rows, err := store.trajectories.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list DuckDB context injections: %w", err)
	}
	defer rows.Close()
	var result []sdk.ContextInjection
	for rows.Next() {
		record, err := scanDuckDBContextInjection(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, sdk.CloneContextInjection(record.Injection))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan DuckDB context injections: %w", err)
	}
	return result, nil
}

func (store *ContextInjectionStore) load(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
) (contextinjectionmodel.Record, error) {
	record, err := scanDuckDBContextInjection(queryer.QueryRowContext(
		ctx,
		`SELECT
			id,
			sequence,
			priority,
			mode,
			origin,
			target_session_id,
			target_execution_id,
			is_meta,
			messages,
			attributes_json,
			created_at
		 FROM ag_context_injections
		 WHERE namespace = ? AND id = ?`,
		store.trajectories.namespace,
		id,
	))
	if err != nil {
		return contextinjectionmodel.Record{}, err
	}
	return record, nil
}

func (store *ContextInjectionStore) insert(
	ctx context.Context,
	execer duckDBExecer,
	sequence uint64,
	injection sdk.ContextInjection,
) error {
	messages, err := json.Marshal(injection.Messages)
	if err != nil {
		return fmt.Errorf("encode context injection %q messages: %w", injection.ID, err)
	}
	attributes, err := duckDBAttributesJSON(injection.Attributes)
	if err != nil {
		return fmt.Errorf("encode context injection %q attributes: %w", injection.ID, err)
	}
	_, err = execer.ExecContext(
		ctx,
		`INSERT INTO ag_context_injections (
			namespace,
			id,
			sequence,
			priority,
			mode,
			origin,
			target_session_id,
			target_execution_id,
			is_meta,
			messages,
			attributes_json,
			created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		store.trajectories.namespace,
		injection.ID,
		sequence,
		string(injection.Priority),
		string(injection.Mode),
		injection.Origin,
		injection.TargetSessionID,
		injection.TargetExecutionID,
		injection.IsMeta,
		messages,
		attributes,
		injection.CreatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert DuckDB context injection %q: %w", injection.ID, err)
	}
	return nil
}

func scanDuckDBContextInjection(
	scanner duckDBScanner,
) (contextinjectionmodel.Record, error) {
	var record contextinjectionmodel.Record
	var priority string
	var mode string
	var messages []byte
	var attributes sql.NullString
	if err := scanner.Scan(
		&record.Injection.ID,
		&record.Sequence,
		&priority,
		&mode,
		&record.Injection.Origin,
		&record.Injection.TargetSessionID,
		&record.Injection.TargetExecutionID,
		&record.Injection.IsMeta,
		&messages,
		&attributes,
		&record.Injection.CreatedAt,
	); err != nil {
		return contextinjectionmodel.Record{}, err
	}
	record.Injection.Priority = sdk.ContextInjectionPriority(priority)
	record.Injection.Mode = sdk.ContextInjectionMode(mode)
	record.Injection.CreatedAt = record.Injection.CreatedAt.UTC()
	if err := json.Unmarshal(messages, &record.Injection.Messages); err != nil {
		return contextinjectionmodel.Record{}, fmt.Errorf(
			"decode context injection %q messages: %w",
			record.Injection.ID,
			err,
		)
	}
	if attributes.Valid {
		if err := json.Unmarshal(
			[]byte(attributes.String),
			&record.Injection.Attributes,
		); err != nil {
			return contextinjectionmodel.Record{}, fmt.Errorf(
				"decode context injection %q attributes: %w",
				record.Injection.ID,
				err,
			)
		}
	}
	if err := validateLoadedContextRecord(record); err != nil {
		return contextinjectionmodel.Record{}, err
	}
	return record, nil
}
