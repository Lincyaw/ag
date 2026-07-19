package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
)

const contextInjectionColumns = `
	injection.id,
	injection.sequence,
	injection.priority,
	injection.mode,
	injection.origin,
	injection.target_session_id,
	injection.target_execution_id,
	injection.is_meta,
	injection.messages,
	injection.attributes,
	injection.created_at`

type ContextInjectionStore struct {
	pool      *pgxpool.Pool
	namespace string
}

func newContextInjectionStore(
	pool *pgxpool.Pool,
	namespace string,
) *ContextInjectionStore {
	return &ContextInjectionStore{pool: pool, namespace: namespace}
}

func (store *ContextInjectionStore) Enqueue(
	ctx context.Context,
	injections ...sdk.ContextInjection,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.pool == nil {
		return errors.New("PostgreSQL context injection store is nil")
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
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, injection := range prepared {
		existing, loadErr := store.load(ctx, tx, injection.ID, true)
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
		if !errors.Is(loadErr, pgx.ErrNoRows) {
			return loadErr
		}
		if err := store.insert(ctx, tx, injection); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL context injection enqueue: %w", err)
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
	if store == nil || store.pool == nil {
		return nil, errors.New("PostgreSQL context injection store is nil")
	}
	if err := validateContextQuery(query); err != nil {
		return nil, err
	}
	statement := `SELECT ` + contextInjectionColumns + `
		FROM ag_context_injections injection
		WHERE injection.namespace = $1
		  AND ($2 = '' OR injection.target_session_id = '' OR injection.target_session_id = $2)
		  AND ($3 = '' OR injection.target_execution_id = '' OR injection.target_execution_id = $3)
		ORDER BY injection.sequence`
	args := []any{
		store.namespace,
		query.TargetSessionID,
		query.TargetExecutionID,
	}
	if query.Limit > 0 {
		statement += ` LIMIT $4`
		args = append(args, query.Limit)
	}
	rows, err := store.pool.Query(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list PostgreSQL context injections: %w", err)
	}
	defer rows.Close()
	var result []sdk.ContextInjection
	for rows.Next() {
		record, err := scanPostgresContextInjection(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, sdk.CloneContextInjection(record.Injection))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan PostgreSQL context injections: %w", err)
	}
	return result, nil
}

func (store *ContextInjectionStore) ConsumeContextInjections(
	ctx context.Context,
	ids ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil || store.pool == nil {
		return errors.New("PostgreSQL context injection store is nil")
	}
	if err := validateContextInjectionIDs(ids); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	for _, id := range ids {
		if _, err := tx.Exec(
			ctx,
			`DELETE FROM ag_context_injections
			 WHERE namespace = $1 AND id = $2`,
			store.namespace,
			id,
		); err != nil {
			return fmt.Errorf(
				"consume PostgreSQL context injection %q: %w",
				id,
				err,
			)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit PostgreSQL context injection consume: %w", err)
	}
	return nil
}

func (store *ContextInjectionStore) load(
	ctx context.Context,
	queryer queryer,
	id string,
	forUpdate bool,
) (contextinjectionmodel.Record, error) {
	statement := `SELECT ` + contextInjectionColumns + `
		FROM ag_context_injections injection
		WHERE injection.namespace = $1 AND injection.id = $2`
	if forUpdate {
		statement += ` FOR UPDATE`
	}
	return scanPostgresContextInjection(queryer.QueryRow(
		ctx,
		statement,
		store.namespace,
		id,
	))
}

func (store *ContextInjectionStore) insert(
	ctx context.Context,
	execer queryer,
	injection sdk.ContextInjection,
) error {
	messages, err := json.Marshal(injection.Messages)
	if err != nil {
		return fmt.Errorf("encode context injection %q messages: %w", injection.ID, err)
	}
	attributes, err := trajectoryAttributesJSON(injection.Attributes)
	if err != nil {
		return fmt.Errorf("encode context injection %q attributes: %w", injection.ID, err)
	}
	_, err = execer.Exec(
		ctx,
		`INSERT INTO ag_context_injections (
			namespace,
			id,
			priority,
			mode,
			origin,
			target_session_id,
			target_execution_id,
			is_meta,
			messages,
			attributes,
			created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11
		)`,
		store.namespace,
		injection.ID,
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
		return fmt.Errorf("insert PostgreSQL context injection %q: %w", injection.ID, err)
	}
	return nil
}

func scanPostgresContextInjection(
	row pgx.Row,
) (contextinjectionmodel.Record, error) {
	var record contextinjectionmodel.Record
	var priority string
	var mode string
	var messages []byte
	var attributes []byte
	if err := row.Scan(
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
	if len(attributes) > 0 {
		if err := json.Unmarshal(
			attributes,
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
