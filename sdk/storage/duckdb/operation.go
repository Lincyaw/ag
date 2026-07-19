package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const duckDBOperationColumns = `
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
	invocation_json,
	lease_owner,
	lease_token,
	lease_expires_at`

// OperationStore is the DuckDB implementation of sdk.OperationStore.
type OperationStore struct {
	db        *sql.DB
	writeMu   *sync.RWMutex
	namespace string
}

func (store *duckDBTrajectoryStore) OperationStore() *OperationStore {
	return &OperationStore{
		db:        store.db,
		writeMu:   &store.writeMu,
		namespace: store.namespace,
	}
}

func scanDuckDBOperation(
	scanner duckDBScanner,
) (sdk.OperationRecord, error) {
	var record sdk.OperationRecord
	var kind string
	var state string
	var output []byte
	var input []byte
	var invocationJSON string
	var leaseOwner string
	var leaseToken string
	var leaseExpiresAt sql.NullTime
	if err := scanner.Scan(
		&record.Operation.ID,
		&record.Operation.IdempotencyKey,
		&state,
		&record.Operation.Revision,
		&output,
		&record.Operation.Error,
		&record.Operation.SubmittedAt,
		&record.Operation.UpdatedAt,
		&kind,
		&record.Resource,
		&record.ResourceRevision,
		&input,
		&invocationJSON,
		&leaseOwner,
		&leaseToken,
		&leaseExpiresAt,
	); err != nil {
		return sdk.OperationRecord{}, err
	}
	record.Kind = sdk.OperationKind(kind)
	record.Operation.State = sdk.OperationState(state)
	record.Operation.Output = append(json.RawMessage(nil), output...)
	record.Input = append(json.RawMessage(nil), input...)
	record.Operation.SubmittedAt = record.Operation.SubmittedAt.UTC()
	record.Operation.UpdatedAt = record.Operation.UpdatedAt.UTC()
	if invocationJSON != "" {
		if err := json.Unmarshal(
			[]byte(invocationJSON),
			&record.Invocation,
		); err != nil {
			return sdk.OperationRecord{}, fmt.Errorf(
				"decode DuckDB operation invocation: %w",
				err,
			)
		}
	}
	if leaseOwner != "" || leaseToken != "" || leaseExpiresAt.Valid {
		if !leaseExpiresAt.Valid {
			return sdk.OperationRecord{}, errors.New(
				"DuckDB operation has an incomplete lease",
			)
		}
		record.Execution = &sdk.OperationLease{
			Owner:     leaseOwner,
			Token:     leaseToken,
			ExpiresAt: leaseExpiresAt.Time.UTC(),
		}
	}
	if err := validateLoadedOperationRecord(record); err != nil {
		return sdk.OperationRecord{}, err
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
	record, err := prepareNewOperationRecord(record, time.Now().UTC())
	if err != nil {
		return sdk.OperationRecord{}, false, err
	}
	invocationJSON, err := json.Marshal(record.Invocation)
	if err != nil {
		return sdk.OperationRecord{}, false, fmt.Errorf(
			"encode operation invocation: %w",
			err,
		)
	}

	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.OperationRecord{}, false, err
	}
	defer tx.Rollback()

	existing, err := store.loadByIdempotency(ctx, tx, record)
	if err == nil {
		if !sameOperationSubmission(existing, record) {
			return sdk.OperationRecord{}, false, fmt.Errorf(
				"operation idempotency key %q was reused with a different submission",
				record.Operation.IdempotencyKey,
			)
		}
		if err := tx.Commit(); err != nil {
			return sdk.OperationRecord{}, false, err
		}
		return cloneOperationRecord(existing), false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return sdk.OperationRecord{}, false, err
	}
	if _, err := store.load(ctx, tx, record.Operation.ID); err == nil {
		return sdk.OperationRecord{}, false, fmt.Errorf(
			"operation ID %q already exists",
			record.Operation.ID,
		)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return sdk.OperationRecord{}, false, err
	}

	if err := store.insert(ctx, tx, record, string(invocationJSON)); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return sdk.OperationRecord{}, false, err
	}
	return cloneOperationRecord(record), true, nil
}

func (store *OperationStore) Import(
	ctx context.Context,
	records ...sdk.OperationRecord,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	prepared := make([]sdk.OperationRecord, 0, len(records))
	invocations := make([]string, 0, len(records))
	for _, record := range records {
		record = cloneOperationRecord(record)
		record.Operation.SubmittedAt = record.Operation.SubmittedAt.UTC()
		record.Operation.UpdatedAt = record.Operation.UpdatedAt.UTC()
		if record.Execution != nil {
			record.Execution.ExpiresAt = record.Execution.ExpiresAt.UTC()
		}
		if err := validateLoadedOperationRecord(record); err != nil {
			return 0, err
		}
		invocationJSON, err := json.Marshal(record.Invocation)
		if err != nil {
			return 0, fmt.Errorf("encode operation invocation: %w", err)
		}
		prepared = append(prepared, record)
		invocations = append(invocations, string(invocationJSON))
	}

	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	imported := 0
	for index, record := range prepared {
		if _, err := store.load(ctx, tx, record.Operation.ID); err == nil {
			return 0, fmt.Errorf(
				"operation ID %q already exists",
				record.Operation.ID,
			)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if _, err := store.loadByIdempotency(ctx, tx, record); err == nil {
			return 0, fmt.Errorf(
				"operation idempotency key %q already exists",
				record.Operation.IdempotencyKey,
			)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if err := store.insert(ctx, tx, record, invocations[index]); err != nil {
			return 0, err
		}
		imported++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return imported, nil
}

func (store *OperationStore) insert(
	ctx context.Context,
	queryer duckDBExecer,
	record sdk.OperationRecord,
	invocationJSON string,
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
	_, err := queryer.ExecContext(
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
			invocation_json,
			invocation_root_id,
			invocation_parent_id,
			invocation_group_id,
			output,
			operation_error,
			submitted_at,
			updated_at,
			lease_owner,
			lease_token,
			lease_expires_at
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?
		)`,
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
		record.Invocation.RootID,
		record.Invocation.ParentID,
		record.Invocation.GroupID,
		output,
		record.Operation.Error,
		record.Operation.SubmittedAt.UTC(),
		record.Operation.UpdatedAt.UTC(),
		leaseOwner,
		leaseToken,
		leaseExpiresAt,
	)
	return err
}

func (store *OperationStore) loadByIdempotency(
	ctx context.Context,
	queryer duckDBQueryer,
	record sdk.OperationRecord,
) (sdk.OperationRecord, error) {
	return scanDuckDBOperation(queryer.QueryRowContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ?
		   AND kind = ?
		   AND resource = ?
		   AND resource_revision = ?
		   AND idempotency_key = ?`,
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
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	record, err := store.load(ctx, store.db, id)
	if errors.Is(err, sql.ErrNoRows) {
		return sdk.OperationRecord{}, fmt.Errorf(
			"%w: %s",
			sdk.ErrOperationNotFound,
			id,
		)
	}
	return cloneOperationRecord(record), err
}

func (store *OperationStore) load(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
) (sdk.OperationRecord, error) {
	return scanDuckDBOperation(queryer.QueryRowContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ? AND id = ?`,
		store.namespace,
		id,
	))
}

func (store *OperationStore) Cancel(
	ctx context.Context,
	id string,
	expectedRevision uint64,
) (sdk.OperationRecord, error) {
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			return cancelOperation(
				record,
				expectedRevision,
				time.Now().UTC(),
			)
		},
	)
}

func (store *OperationStore) Fail(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	operationError string,
) (sdk.OperationRecord, error) {
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			return failOperation(
				record,
				expectedRevision,
				operationError,
				time.Now().UTC(),
			)
		},
	)
}

func (store *OperationStore) Claim(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.OperationRecord, error) {
	if err := validateOperationClaim(owner, ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = normalizeOperationMutationTime(now)
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			return claimOperation(record, owner, now, ttl)
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
	if err := validateOperationLeaseDuration(ttl); err != nil {
		return sdk.OperationRecord{}, err
	}
	now = normalizeOperationMutationTime(now)
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			return renewOperation(record, token, now, ttl)
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
	if err := validateOperationCompletion(state); err != nil {
		return sdk.OperationRecord{}, err
	}
	return store.mutate(
		ctx,
		id,
		func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
			return completeOperation(
				record,
				token,
				state,
				output,
				operationError,
				time.Now().UTC(),
			)
		},
	)
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
			return releaseOperation(record, token, time.Now().UTC())
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
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	defer tx.Rollback()
	record, err := store.load(ctx, tx, id)
	if errors.Is(err, sql.ErrNoRows) {
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
	if err := tx.Commit(); err != nil {
		return sdk.OperationRecord{}, err
	}
	return cloneOperationRecord(record), nil
}

func (store *OperationStore) replace(
	ctx context.Context,
	queryer duckDBExecer,
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
	result, err := queryer.ExecContext(
		ctx,
		`UPDATE ag_operations
		 SET state = ?,
		     revision = ?,
		     output = ?,
		     operation_error = ?,
		     updated_at = ?,
		     lease_owner = ?,
		     lease_token = ?,
		     lease_expires_at = ?
		 WHERE namespace = ? AND id = ?`,
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
	return requireDuckDBRows(
		result,
		1,
		fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, record.Operation.ID),
	)
}

func (store *OperationStore) List(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ?
		 ORDER BY submitted_at, id`,
		store.namespace,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBOperations(rows)
}

func (store *OperationStore) ListByInvocationRoot(
	ctx context.Context,
	rootID string,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := sdk.ValidateResourceName("invocation root", rootID); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ?
		   AND invocation_root_id = ?
		 ORDER BY submitted_at, id`,
		store.namespace,
		rootID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBOperations(rows)
}

func (store *OperationStore) ListNonTerminal(
	ctx context.Context,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ?
		   AND state NOT IN (?, ?, ?)
		 ORDER BY submitted_at, id`,
		store.namespace,
		string(sdk.OperationSucceeded),
		string(sdk.OperationFailed),
		string(sdk.OperationCancelled),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBOperations(rows)
}

func (store *OperationStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now = normalizeOperationMutationTime(now)
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBOperationColumns+`
		 FROM ag_operations
		 WHERE namespace = ?
		   AND state NOT IN (?, ?, ?)
		   AND (
		     lease_expires_at IS NULL
		     OR lease_expires_at <= ?
		   )
		 ORDER BY submitted_at, id`,
		store.namespace,
		string(sdk.OperationSucceeded),
		string(sdk.OperationFailed),
		string(sdk.OperationCancelled),
		now,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBOperations(rows)
}

func scanDuckDBOperations(rows *sql.Rows) ([]sdk.OperationRecord, error) {
	result := make([]sdk.OperationRecord, 0)
	for rows.Next() {
		record, err := scanDuckDBOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, cloneOperationRecord(record))
	}
	return result, rows.Err()
}

func (store *OperationStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.OperationPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationPage{}, err
	}
	request, err := normalizeDuckDBPageRequest(request)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	query := `SELECT ` + duckDBOperationColumns + `
		FROM ag_operations
		WHERE namespace = ?`
	args := []any{store.namespace}
	if request.After != "" {
		var submittedAt time.Time
		if err := store.db.QueryRowContext(
			ctx,
			`SELECT submitted_at
			 FROM ag_operations
			 WHERE namespace = ? AND id = ?`,
			store.namespace,
			request.After,
		).Scan(&submittedAt); errors.Is(err, sql.ErrNoRows) {
			return sdk.OperationPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.OperationPage{}, err
		}
		query += ` AND (
			submitted_at > ?
			OR (submitted_at = ? AND id > ?)
		)`
		args = append(args, submittedAt.UTC(), submittedAt.UTC(), request.After)
	}
	query += ` ORDER BY submitted_at, id LIMIT ?`
	args = append(args, request.Limit+1)
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	defer rows.Close()
	items, err := scanDuckDBOperations(rows)
	if err != nil {
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
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("operation purge cutoff is required")
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	result, err := store.db.ExecContext(
		ctx,
		`DELETE FROM ag_operations
		 WHERE namespace = ?
		   AND state IN (?, ?, ?)
		   AND updated_at < ?`,
		store.namespace,
		string(sdk.OperationSucceeded),
		string(sdk.OperationFailed),
		string(sdk.OperationCancelled),
		before.UTC(),
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}
