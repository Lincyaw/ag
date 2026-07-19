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

const duckDBDeliveryColumns = `
	id,
	sequence,
	plugin,
	plugin_version,
	subscription,
	resource_revision,
	partition_key,
	event_id,
	event_name,
	event_session_id,
	event_generation,
	event_payload,
	state,
	attempt,
	available_at,
	lease_token,
	lease_expires_at,
	last_error,
	created_at,
	updated_at`

const qualifiedDuckDBDeliveryColumns = `
	delivery.id,
	delivery.sequence,
	delivery.plugin,
	delivery.plugin_version,
	delivery.subscription,
	delivery.resource_revision,
	delivery.partition_key,
	delivery.event_id,
	delivery.event_name,
	delivery.event_session_id,
	delivery.event_generation,
	delivery.event_payload,
	delivery.state,
	delivery.attempt,
	delivery.available_at,
	delivery.lease_token,
	delivery.lease_expires_at,
	delivery.last_error,
	delivery.created_at,
	delivery.updated_at`

// DeliveryStore is the DuckDB implementation of sdk.DeliveryStore.
type DeliveryStore struct {
	db        *sql.DB
	writeMu   *sync.RWMutex
	namespace string
	queue     string
}

func (store *duckDBTrajectoryStore) DeliveryStore(queue string) (*DeliveryStore, error) {
	if store == nil || store.db == nil {
		return nil, errors.New("DuckDB store is not initialized")
	}
	if err := sdk.ValidateResourceName("delivery queue", queue); err != nil {
		return nil, err
	}
	return &DeliveryStore{
		db:        store.db,
		writeMu:   &store.writeMu,
		namespace: store.namespace,
		queue:     queue,
	}, nil
}

func scanDuckDBDelivery(scanner duckDBScanner) (sdk.Delivery, error) {
	var delivery sdk.Delivery
	var payload []byte
	var state string
	var availableAt sql.NullTime
	var leaseExpiresAt sql.NullTime
	if err := scanner.Scan(
		&delivery.ID,
		&delivery.Sequence,
		&delivery.Plugin,
		&delivery.PluginVersion,
		&delivery.Subscription,
		&delivery.ResourceRevision,
		&delivery.Partition,
		&delivery.Event.ID,
		&delivery.Event.Name,
		&delivery.Event.SessionID,
		&delivery.Event.Generation,
		&payload,
		&state,
		&delivery.Attempt,
		&availableAt,
		&delivery.LeaseToken,
		&leaseExpiresAt,
		&delivery.LastError,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	); err != nil {
		return sdk.Delivery{}, err
	}
	delivery.Event.Payload = append(json.RawMessage(nil), payload...)
	delivery.State = sdk.DeliveryState(state)
	if availableAt.Valid {
		delivery.AvailableAt = availableAt.Time.UTC()
	}
	if leaseExpiresAt.Valid {
		delivery.LeaseExpiresAt = leaseExpiresAt.Time.UTC()
	}
	delivery.CreatedAt = delivery.CreatedAt.UTC()
	delivery.UpdatedAt = delivery.UpdatedAt.UTC()
	if err := validateLoadedDelivery(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (store *DeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...sdk.Delivery,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewDeliveries(deliveries, time.Now().UTC())
	if err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := store.enqueueInTx(ctx, tx, prepared); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *DeliveryStore) enqueueInTx(
	ctx context.Context,
	tx *sql.Tx,
	prepared []sdk.Delivery,
) error {
	newDeliveries := make([]sdk.Delivery, 0, len(prepared))
	staged := make(map[string]struct{}, len(prepared))
	for _, delivery := range prepared {
		if _, exists := staged[delivery.ID]; exists {
			continue
		}
		existing, loadErr := store.load(ctx, tx, delivery.ID)
		if loadErr == nil {
			if !sameDeliveryIdentity(existing, delivery) {
				return fmt.Errorf(
					"delivery %q already exists with different identity",
					delivery.ID,
				)
			}
			continue
		}
		if !errors.Is(loadErr, sql.ErrNoRows) {
			return loadErr
		}
		staged[delivery.ID] = struct{}{}
		newDeliveries = append(newDeliveries, delivery)
	}
	if len(newDeliveries) == 0 {
		return nil
	}

	var nextSequence uint64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		 FROM ag_deliveries
		 WHERE namespace = ? AND queue = ?`,
		store.namespace,
		store.queue,
	).Scan(&nextSequence); err != nil {
		return err
	}
	for _, delivery := range newDeliveries {
		delivery.Sequence = nextSequence
		nextSequence++
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO ag_deliveries (
				namespace,
				queue,
				id,
				sequence,
				plugin,
				plugin_version,
				subscription,
				resource_revision,
				partition_key,
				event_id,
				event_name,
				event_session_id,
				event_generation,
				event_payload,
				state,
				attempt,
				available_at,
				lease_token,
				lease_expires_at,
				last_error,
				created_at,
				updated_at
			) VALUES (
				?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
				?, ?, ?, ?, '', NULL, '', ?, ?
			)`,
			store.namespace,
			store.queue,
			delivery.ID,
			delivery.Sequence,
			delivery.Plugin,
			delivery.PluginVersion,
			delivery.Subscription,
			delivery.ResourceRevision,
			delivery.Partition,
			delivery.Event.ID,
			delivery.Event.Name,
			delivery.Event.SessionID,
			delivery.Event.Generation,
			[]byte(delivery.Event.Payload),
			string(delivery.State),
			delivery.Attempt,
			duckDBNullableTime(delivery.AvailableAt),
			delivery.CreatedAt.UTC(),
			delivery.UpdatedAt.UTC(),
		); err != nil {
			return err
		}
	}
	return nil
}

func (store *DeliveryStore) load(
	ctx context.Context,
	queryer duckDBQueryer,
	id string,
) (sdk.Delivery, error) {
	return scanDuckDBDelivery(queryer.QueryRowContext(
		ctx,
		`SELECT `+duckDBDeliveryColumns+`
		 FROM ag_deliveries
		 WHERE namespace = ? AND queue = ? AND id = ?`,
		store.namespace,
		store.queue,
		id,
	))
}

func (store *DeliveryStore) Lease(
	ctx context.Context,
	now time.Time,
	duration time.Duration,
) (sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return sdk.Delivery{}, err
	}
	if err := validateDeliveryLeaseDuration(duration); err != nil {
		return sdk.Delivery{}, err
	}
	now = normalizeDeliveryMutationTime(now)
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return sdk.Delivery{}, err
	}
	defer tx.Rollback()

	delivery, err := scanDuckDBDelivery(tx.QueryRowContext(
		ctx,
		`WITH heads AS (
			SELECT partition_key, MIN(sequence) AS sequence
			FROM ag_deliveries
			WHERE namespace = ?
			  AND queue = ?
			  AND state NOT IN (?, ?)
			GROUP BY partition_key
		)
		SELECT `+qualifiedDuckDBDeliveryColumns+`
		FROM ag_deliveries delivery
		JOIN heads
		  ON heads.partition_key = delivery.partition_key
		 AND heads.sequence = delivery.sequence
		WHERE delivery.namespace = ?
		  AND delivery.queue = ?
		  AND (
		    (delivery.state = ? AND delivery.available_at <= ?)
		    OR (delivery.state = ? AND delivery.lease_expires_at <= ?)
		  )
		ORDER BY delivery.sequence
		LIMIT 1`,
		store.namespace,
		store.queue,
		string(sdk.DeliveryDelivered),
		string(sdk.DeliveryDeadLetter),
		store.namespace,
		store.queue,
		string(sdk.DeliveryPending),
		now,
		string(sdk.DeliveryLeased),
		now,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	if err != nil {
		return sdk.Delivery{}, err
	}
	delivery, err = leaseDelivery(delivery, sdk.NewID(), now, duration)
	if err != nil {
		return sdk.Delivery{}, err
	}
	if err := store.updateLease(ctx, tx, delivery); err != nil {
		return sdk.Delivery{}, err
	}
	if err := tx.Commit(); err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (store *DeliveryStore) updateLease(
	ctx context.Context,
	tx *sql.Tx,
	delivery sdk.Delivery,
) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE ag_deliveries
		 SET state = ?,
		     attempt = ?,
		     lease_token = ?,
		     lease_expires_at = ?,
		     updated_at = ?
		 WHERE namespace = ?
		   AND queue = ?
		   AND id = ?`,
		string(delivery.State),
		delivery.Attempt,
		delivery.LeaseToken,
		delivery.LeaseExpiresAt.UTC(),
		delivery.UpdatedAt.UTC(),
		store.namespace,
		store.queue,
		delivery.ID,
	)
	if err != nil {
		return err
	}
	return requireDuckDBRows(result, 1, fmt.Errorf("%w: %s", sdk.ErrNoDelivery, delivery.ID))
}

func (store *DeliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return store.transition(ctx, id, token, now, sdk.DeliveryDelivered, time.Time{}, "")
}

func (store *DeliveryStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return store.transition(
		ctx,
		id,
		token,
		time.Now().UTC(),
		sdk.DeliveryPending,
		availableAt.UTC(),
		lastError,
	)
}

func (store *DeliveryStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return store.transition(
		ctx,
		id,
		token,
		now,
		sdk.DeliveryDeadLetter,
		time.Time{},
		lastError,
	)
}

func (store *DeliveryStore) transition(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	state sdk.DeliveryState,
	availableAt time.Time,
	lastError string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	delivery, err := store.load(ctx, tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
	}
	if err != nil {
		return err
	}
	delivery, err = finishDeliveryLease(
		delivery,
		token,
		now,
		state,
		availableAt,
		lastError,
	)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE ag_deliveries
		 SET state = ?,
		     available_at = ?,
		     lease_token = '',
		     lease_expires_at = NULL,
		     last_error = ?,
		     updated_at = ?
		 WHERE namespace = ?
		   AND queue = ?
		   AND id = ?`,
		string(delivery.State),
		duckDBNullableTime(delivery.AvailableAt),
		delivery.LastError,
		delivery.UpdatedAt.UTC(),
		store.namespace,
		store.queue,
		delivery.ID,
	)
	if err != nil {
		return err
	}
	if err := requireDuckDBRows(result, 1, fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)); err != nil {
		return err
	}
	return tx.Commit()
}

func (store *DeliveryStore) List(ctx context.Context) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBDeliveryColumns+`
		 FROM ag_deliveries
		 WHERE namespace = ? AND queue = ?
		 ORDER BY sequence`,
		store.namespace,
		store.queue,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBDeliveries(rows)
}

func (store *DeliveryStore) ListNonTerminal(ctx context.Context) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(
		ctx,
		`SELECT `+duckDBDeliveryColumns+`
		 FROM ag_deliveries
		 WHERE namespace = ?
		   AND queue = ?
		   AND state NOT IN (?, ?)
		 ORDER BY sequence`,
		store.namespace,
		store.queue,
		string(sdk.DeliveryDelivered),
		string(sdk.DeliveryDeadLetter),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDuckDBDeliveries(rows)
}

func scanDuckDBDeliveries(rows *sql.Rows) ([]sdk.Delivery, error) {
	result := make([]sdk.Delivery, 0)
	for rows.Next() {
		delivery, err := scanDuckDBDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	return result, rows.Err()
}

func (store *DeliveryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.DeliveryPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.DeliveryPage{}, err
	}
	request, err := normalizeDuckDBPageRequest(request)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	query := `SELECT ` + duckDBDeliveryColumns + `
		FROM ag_deliveries
		WHERE namespace = ? AND queue = ?`
	args := []any{store.namespace, store.queue}
	if request.After != "" {
		var sequence uint64
		if err := store.db.QueryRowContext(
			ctx,
			`SELECT sequence
			 FROM ag_deliveries
			 WHERE namespace = ? AND queue = ? AND id = ?`,
			store.namespace,
			store.queue,
			request.After,
		).Scan(&sequence); errors.Is(err, sql.ErrNoRows) {
			return sdk.DeliveryPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.DeliveryPage{}, err
		}
		query += ` AND sequence > ?`
		args = append(args, sequence)
	}
	query += ` ORDER BY sequence LIMIT ?`
	args = append(args, request.Limit+1)
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	defer rows.Close()
	items, err := scanDuckDBDeliveries(rows)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	next := ""
	if len(items) > request.Limit {
		items = items[:request.Limit]
		next = items[len(items)-1].ID
	}
	return sdk.DeliveryPage{Items: items, Next: next}, nil
}

func (store *DeliveryStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("delivery purge cutoff is required")
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	result, err := store.db.ExecContext(
		ctx,
		`DELETE FROM ag_deliveries
		 WHERE namespace = ?
		   AND queue = ?
		   AND state IN (?, ?)
		   AND updated_at < ?`,
		store.namespace,
		store.queue,
		string(sdk.DeliveryDelivered),
		string(sdk.DeliveryDeadLetter),
		before.UTC(),
	)
	if err != nil {
		return 0, err
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}

func normalizeDuckDBPageRequest(
	request sdk.PageRequest,
) (sdk.PageRequest, error) {
	if request.Limit == 0 {
		request.Limit = sdk.DefaultPageSize
	}
	if request.Limit < 0 {
		return sdk.PageRequest{}, errors.New("page limit cannot be negative")
	}
	if request.Limit > sdk.MaxPageSize {
		return sdk.PageRequest{}, fmt.Errorf(
			"page limit %d exceeds maximum %d",
			request.Limit,
			sdk.MaxPageSize,
		)
	}
	return request, nil
}

func requireDuckDBRows(result sql.Result, want int64, err error) error {
	rows, rowsErr := result.RowsAffected()
	if rowsErr != nil {
		return rowsErr
	}
	if rows != want {
		return err
	}
	return nil
}
