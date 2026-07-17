package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lincyaw/ag/sdk"
)

const operationColumns = `
	id,
	idempotency_key,
	state,
	revision,
	output,
	operation_error,
	submitted_at,
	updated_at,
	kind,
	resource,
	resource_revision,
	input,
	invocation,
	lease_owner,
	lease_token,
	lease_expires_at`

type OperationStore struct {
	pool      *pgxpool.Pool
	namespace string
}

func newOperationStore(
	pool *pgxpool.Pool,
	namespace string,
) *OperationStore {
	return &OperationStore{pool: pool, namespace: namespace}
}

func scanPostgresOperation(scanner interface {
	Scan(...any) error
}) (sdk.OperationRecord, error) {
	var record sdk.OperationRecord
	var output []byte
	var leaseOwner string
	var leaseToken string
	var leaseExpiresAt sql.NullTime
	var kind string
	var invocationJSON []byte
	if err := scanner.Scan(
		&record.Operation.ID,
		&record.Operation.IdempotencyKey,
		&record.Operation.State,
		&record.Operation.Revision,
		&output,
		&record.Operation.Error,
		&record.Operation.SubmittedAt,
		&record.Operation.UpdatedAt,
		&kind,
		&record.Resource,
		&record.ResourceRevision,
		&record.Input,
		&invocationJSON,
		&leaseOwner,
		&leaseToken,
		&leaseExpiresAt,
	); err != nil {
		return sdk.OperationRecord{}, err
	}
	record.Kind = sdk.OperationKind(kind)
	record.Operation.SubmittedAt = record.Operation.SubmittedAt.UTC()
	record.Operation.UpdatedAt = record.Operation.UpdatedAt.UTC()
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Input = append(json.RawMessage(nil), record.Input...)
	if len(invocationJSON) != 0 {
		if err := json.Unmarshal(
			invocationJSON,
			&record.Invocation,
		); err != nil {
			return sdk.OperationRecord{}, fmt.Errorf(
				"decode PostgreSQL operation invocation: %w",
				err,
			)
		}
	}
	if leaseOwner != "" || leaseToken != "" || leaseExpiresAt.Valid {
		if !leaseExpiresAt.Valid {
			return sdk.OperationRecord{}, errors.New(
				"PostgreSQL operation has an incomplete lease",
			)
		}
		record.Execution = &sdk.OperationLease{
			Owner:     leaseOwner,
			Token:     leaseToken,
			ExpiresAt: leaseExpiresAt.Time.UTC(),
		}
	}
	if err := validateNewOperationRecord(record); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	if record.Execution != nil &&
		(record.Operation.State != sdk.OperationRunning ||
			strings.TrimSpace(record.Execution.Owner) == "" ||
			record.Execution.Token == "") {
		return sdk.OperationRecord{}, errors.New(
			"PostgreSQL operation has an invalid lease",
		)
	}
	return record, nil
}

func (store *OperationStore) Submit(
	ctx context.Context,
	record sdk.OperationRecord,
) (sdk.OperationRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	if record.Operation.ID == "" {
		record.Operation.ID = sdk.NewID()
	}
	if err := validateNewOperationRecord(record); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	now := time.Now().UTC()
	if record.Operation.SubmittedAt.IsZero() {
		record.Operation.SubmittedAt = now
	} else {
		record.Operation.SubmittedAt =
			record.Operation.SubmittedAt.UTC()
	}
	record.Operation.UpdatedAt = record.Operation.SubmittedAt
	record.Operation.State = sdk.OperationPending
	record.Operation.Revision = 1
	record.Operation.Output = nil
	record.Operation.Error = ""
	record.Execution = nil
	invocationJSON, err := json.Marshal(record.Invocation)
	if err != nil {
		return sdk.OperationRecord{}, false, fmt.Errorf(
			"encode operation invocation: %w",
			err,
		)
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.OperationRecord{}, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	row := tx.QueryRow(
		ctx,
		`INSERT INTO ag_operations (
			namespace,
			id,
			idempotency_key,
			kind,
			resource,
			resource_revision,
			state,
			revision,
			input,
			invocation,
			output,
			operation_error,
			submitted_at,
			updated_at,
			lease_owner,
			lease_token,
			lease_expires_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, NULL, '', $11, $11, '', '', NULL
		)
		ON CONFLICT (
			namespace,
			kind,
			resource,
			resource_revision,
			idempotency_key
		) DO NOTHING
		RETURNING `+operationColumns,
		store.namespace,
		record.Operation.ID,
		record.Operation.IdempotencyKey,
		string(record.Kind),
		record.Resource,
		record.ResourceRevision,
		string(record.Operation.State),
		record.Operation.Revision,
		[]byte(record.Input),
		invocationJSON,
		record.Operation.SubmittedAt,
	)
	inserted, scanErr := scanPostgresOperation(row)
	created := scanErr == nil
	if errors.Is(scanErr, pgx.ErrNoRows) {
		inserted, scanErr = store.loadByIdempotency(
			ctx,
			tx,
			record,
			true,
		)
		if scanErr == nil && !sameOperationSubmission(inserted, record) {
			scanErr = fmt.Errorf(
				"operation idempotency key %q was reused with a different submission",
				record.Operation.IdempotencyKey,
			)
		}
	}
	if scanErr != nil {
		if isUniqueViolation(scanErr) {
			return sdk.OperationRecord{}, false, fmt.Errorf(
				"operation ID %q already exists",
				record.Operation.ID,
			)
		}
		return sdk.OperationRecord{}, false, scanErr
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	return cloneOperationRecord(inserted), created, nil
}

func (store *OperationStore) loadByIdempotency(
	ctx context.Context,
	query queryer,
	record sdk.OperationRecord,
	lock bool,
) (sdk.OperationRecord, error) {
	statement := `SELECT ` + operationColumns + `
		FROM ag_operations
		WHERE namespace = $1
		  AND kind = $2
		  AND resource = $3
		  AND resource_revision = $4
		  AND idempotency_key = $5`
	if lock {
		statement += ` FOR UPDATE`
	}
	return scanPostgresOperation(query.QueryRow(
		ctx,
		statement,
		store.namespace,
		string(record.Kind),
		record.Resource,
		record.ResourceRevision,
		record.Operation.IdempotencyKey,
	))
}

func (store *OperationStore) Get(
	ctx context.Context,
	id string,
) (sdk.OperationRecord, error) {
	record, err := store.load(ctx, store.pool, id, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			id,
		)
	}
	return record, err
}

func (store *OperationStore) load(
	ctx context.Context,
	query queryer,
	id string,
	lock bool,
) (sdk.OperationRecord, error) {
	statement := `SELECT ` + operationColumns + `
		FROM ag_operations
		WHERE namespace = $1 AND id = $2`
	if lock {
		statement += ` FOR UPDATE`
	}
	return scanPostgresOperation(
		query.QueryRow(ctx, statement, store.namespace, id),
	)
}

func (store *OperationStore) Transition(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
) (sdk.OperationRecord, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	record, err := store.load(ctx, tx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			id,
		)
	}
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if record.Operation.Revision != expectedRevision {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s has revision %d, expected %d",
			sdk.ErrOperationConflict,
			id,
			record.Operation.Revision,
			expectedRevision,
		)
	}
	if err := validateOperationTransition(
		record.Operation.State,
		state,
	); err != nil {
		return sdk.OperationRecord{}, err
	}
	record.Operation.State = state
	record.Operation.Revision++
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Operation.Error = operationError
	record.Operation.UpdatedAt = time.Now().UTC()
	if state != sdk.OperationRunning {
		record.Execution = nil
	}
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := store.replace(ctx, tx, record); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.OperationRecord{}, err
	}
	return cloneOperationRecord(record), nil
}

func (store *OperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if strings.TrimSpace(owner) == "" {
		return sdk.OperationRecord{}, errors.New(
			"operation lease owner is empty",
		)
	}
	if ttl <= 0 {
		return sdk.OperationRecord{}, errors.New(
			"operation lease TTL must be positive",
		)
	}
	now = normalizedMutationTime(now)
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			if record.Operation.Terminal() {
				return sdk.OperationRecord{}, fmt.Errorf(
					"terminal operation %q cannot be claimed",
					id,
				)
			}
			if record.Execution != nil &&
				record.Execution.ExpiresAt.After(now) {
				return sdk.OperationRecord{}, fmt.Errorf(
					"%w: operation %s is owned by %s until %s",
					sdk.ErrOperationClaimed,
					id,
					record.Execution.Owner,
					record.Execution.ExpiresAt.Format(
						time.RFC3339Nano,
					),
				)
			}
			record.Operation.State = sdk.OperationRunning
			record.Operation.Revision++
			record.Operation.UpdatedAt = now
			record.Execution = &sdk.OperationLease{
				Owner:     owner,
				Token:     sdk.NewID(),
				ExpiresAt: now.Add(ttl),
			}
			return record, nil
		},
	)
}

func (store *OperationStore) Renew(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if ttl <= 0 {
		return sdk.OperationRecord{}, errors.New(
			"operation lease TTL must be positive",
		)
	}
	now = normalizedMutationTime(now)
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			if record.Operation.State != sdk.OperationRunning ||
				record.Execution == nil ||
				record.Execution.Token != token ||
				!record.Execution.ExpiresAt.After(now) {
				return sdk.OperationRecord{}, fmt.Errorf(
					"%w: operation %s token is stale or expired",
					sdk.ErrOperationFence,
					id,
				)
			}
			record.Execution.ExpiresAt = now.Add(ttl)
			record.Operation.Revision++
			record.Operation.UpdatedAt = now
			return record, nil
		},
	)
}

func (store *OperationStore) Complete(
	ctx context.Context,
	id string,
	token string,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
) (sdk.OperationRecord, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	record, err := store.completeInTx(
		ctx,
		tx,
		id,
		token,
		state,
		output,
		operationError,
		time.Now().UTC(),
	)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.OperationRecord{}, err
	}
	return record, nil
}

func (store *OperationStore) completeInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	token string,
	state sdk.OperationState,
	output json.RawMessage,
	operationError string,
	now time.Time,
) (sdk.OperationRecord, error) {
	if state != sdk.OperationSucceeded && state != sdk.OperationFailed {
		return sdk.OperationRecord{}, fmt.Errorf(
			"claimed operation cannot complete as %q",
			state,
		)
	}
	record, err := store.load(ctx, tx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			id,
		)
	}
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if record.Operation.State != sdk.OperationRunning ||
		record.Execution == nil ||
		record.Execution.Token != token ||
		!record.Execution.ExpiresAt.After(now) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: operation %s token is stale or expired",
			sdk.ErrOperationFence,
			id,
		)
	}
	record.Operation.State = state
	record.Operation.Revision++
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Operation.Error = operationError
	record.Operation.UpdatedAt = now
	record.Execution = nil
	if err := sdk.ValidateOperation(record.Operation); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := store.replace(ctx, tx, record); err != nil {
		return sdk.OperationRecord{}, err
	}
	return cloneOperationRecord(record), nil
}

func (store *OperationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (sdk.OperationRecord, error) {
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			if record.Operation.State != sdk.OperationRunning ||
				record.Execution == nil ||
				record.Execution.Token != token {
				return sdk.OperationRecord{}, fmt.Errorf(
					"%w: operation %s token is stale",
					sdk.ErrOperationFence,
					id,
				)
			}
			record.Execution = nil
			record.Operation.Revision++
			record.Operation.UpdatedAt = time.Now().UTC()
			return record, nil
		},
	)
}

func (store *OperationStore) mutate(
	ctx context.Context,
	id string,
	mutation func(
		sdk.OperationRecord,
	) (sdk.OperationRecord, error),
) (sdk.OperationRecord, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	record, err := store.load(ctx, tx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			id,
		)
	}
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	record, err = mutation(record)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := store.replace(ctx, tx, record); err != nil {
		return sdk.OperationRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.OperationRecord{}, err
	}
	return cloneOperationRecord(record), nil
}

func (store *OperationStore) replace(
	ctx context.Context,
	query queryer,
	record sdk.OperationRecord,
) error {
	var output any
	if len(record.Operation.Output) != 0 {
		output = []byte(record.Operation.Output)
	}
	leaseOwner := ""
	leaseToken := ""
	var leaseExpiresAt any
	if record.Execution != nil {
		leaseOwner = record.Execution.Owner
		leaseToken = record.Execution.Token
		leaseExpiresAt = record.Execution.ExpiresAt.UTC()
	}
	tag, err := query.Exec(
		ctx,
		`UPDATE ag_operations
		 SET state = $1,
		     revision = $2,
		     output = $3,
		     operation_error = $4,
		     updated_at = $5,
		     lease_owner = $6,
		     lease_token = $7,
		     lease_expires_at = $8
		 WHERE namespace = $9 AND id = $10`,
		string(record.Operation.State),
		record.Operation.Revision,
		output,
		record.Operation.Error,
		record.Operation.UpdatedAt.UTC(),
		leaseOwner,
		leaseToken,
		leaseExpiresAt,
		store.namespace,
		record.Operation.ID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			record.Operation.ID,
		)
	}
	return nil
}

func (store *OperationStore) List(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+operationColumns+`
		 FROM ag_operations
		 WHERE namespace = $1
		 ORDER BY submitted_at, id`,
		store.namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]sdk.OperationRecord, 0)
	for rows.Next() {
		record, err := scanPostgresOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (store *OperationStore) ListByInvocationRoot(
	ctx context.Context,
	rootID string,
) ([]sdk.OperationRecord, error) {
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+operationColumns+`
		 FROM ag_operations
		 WHERE namespace = $1
		   AND invocation ->> 'root_id' = $2
		 ORDER BY submitted_at, id`,
		store.namespace,
		rootID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]sdk.OperationRecord, 0)
	for rows.Next() {
		record, scanErr := scanPostgresOperation(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, cloneOperationRecord(record))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *OperationStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.OperationPage, error) {
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	statement := `SELECT ` + operationColumns + `
		FROM ag_operations
		WHERE namespace = $1`
	args := []any{store.namespace}
	if request.After != "" {
		var submittedAt time.Time
		if err := store.pool.QueryRow(
			ctx,
			`SELECT submitted_at
			 FROM ag_operations
			 WHERE namespace = $1 AND id = $2`,
			store.namespace,
			request.After,
		).Scan(&submittedAt); errors.Is(err, pgx.ErrNoRows) {
			return sdk.OperationPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.OperationPage{}, err
		}
		statement += ` AND (
			submitted_at > $2
			OR (submitted_at = $2 AND id > $3)
		)`
		args = append(args, submittedAt.UTC(), request.After)
	}
	statement += fmt.Sprintf(
		` ORDER BY submitted_at, id LIMIT $%d`,
		len(args)+1,
	)
	args = append(args, request.Limit+1)
	rows, err := store.pool.Query(ctx, statement, args...)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	defer rows.Close()
	items := make([]sdk.OperationRecord, 0, request.Limit+1)
	for rows.Next() {
		record, err := scanPostgresOperation(rows)
		if err != nil {
			return sdk.OperationPage{}, err
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return sdk.OperationPage{}, err
	}
	next := ""
	if len(items) > request.Limit {
		items = items[:request.Limit]
		next = items[len(items)-1].Operation.ID
	}
	return sdk.OperationPage{Items: items, Next: next}, nil
}

func (store *OperationStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if before.IsZero() {
		return 0, errors.New("operation purge cutoff is required")
	}
	tag, err := store.pool.Exec(
		ctx,
		`DELETE FROM ag_operations
		 WHERE namespace = $1
		   AND state IN ($2, $3, $4)
		   AND updated_at < $5`,
		store.namespace,
		string(sdk.OperationSucceeded),
		string(sdk.OperationFailed),
		string(sdk.OperationCancelled),
		before.UTC(),
	)
	return int(tag.RowsAffected()), err
}
