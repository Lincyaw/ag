package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lincyaw/ag/sdk"
)

const deliveryColumns = `
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

const qualifiedDeliveryColumns = `
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

type DeliveryStore struct {
	pool      *pgxpool.Pool
	namespace string
	queue     string
}

func newDeliveryStore(
	pool *pgxpool.Pool,
	namespace string,
	queue string,
) *DeliveryStore {
	return &DeliveryStore{
		pool:      pool,
		namespace: namespace,
		queue:     queue,
	}
}

func scanPostgresDelivery(scanner interface {
	Scan(...any) error
}) (sdk.Delivery, error) {
	var delivery sdk.Delivery
	var payload []byte
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
		&delivery.State,
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
	if availableAt.Valid {
		delivery.AvailableAt = availableAt.Time.UTC()
	}
	delivery.CreatedAt = delivery.CreatedAt.UTC()
	delivery.UpdatedAt = delivery.UpdatedAt.UTC()
	if leaseExpiresAt.Valid {
		delivery.LeaseExpiresAt = leaseExpiresAt.Time.UTC()
	}
	if err := validateLoadedDelivery(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func prepareDeliveries(
	deliveries []sdk.Delivery,
) ([]sdk.Delivery, error) {
	return prepareNewDeliveries(deliveries, time.Now().UTC())
}

func (store *DeliveryStore) Enqueue(
	ctx context.Context,
	deliveries ...sdk.Delivery,
) error {
	prepared, err := prepareDeliveries(deliveries)
	if err != nil {
		return err
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := store.enqueueInTx(ctx, tx, prepared); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *DeliveryStore) enqueueInTx(
	ctx context.Context,
	tx pgx.Tx,
	deliveries []sdk.Delivery,
) error {
	for _, delivery := range deliveries {
		inserted, err := scanPostgresDelivery(tx.QueryRow(
			ctx,
			`INSERT INTO ag_deliveries (
				namespace,
				queue,
				id,
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
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10, $11, $12, $13, $14,
				$15, $16, '', NULL, '', $17, $17
			)
			ON CONFLICT (namespace, queue, id) DO NOTHING
			RETURNING `+deliveryColumns,
			store.namespace,
			store.queue,
			delivery.ID,
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
			delivery.AvailableAt.UTC(),
			delivery.CreatedAt.UTC(),
		))
		if err == nil {
			_ = inserted
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		existing, err := store.load(ctx, tx, delivery.ID, true)
		if err != nil {
			return err
		}
		if !sameDeliveryIdentity(existing, delivery) {
			return fmt.Errorf(
				"delivery %q already exists with different identity",
				delivery.ID,
			)
		}
	}
	return nil
}

func (store *DeliveryStore) load(
	ctx context.Context,
	query queryer,
	id string,
	lock bool,
) (sdk.Delivery, error) {
	statement := `SELECT ` + deliveryColumns + `
		FROM ag_deliveries
		WHERE namespace = $1 AND queue = $2 AND id = $3`
	if lock {
		statement += ` FOR UPDATE`
	}
	return scanPostgresDelivery(query.QueryRow(
		ctx,
		statement,
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
	if err := validateDeliveryLeaseDuration(duration); err != nil {
		return sdk.Delivery{}, err
	}
	now = normalizeDeliveryMutationTime(now)
	token := sdk.NewID()
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sdk.Delivery{}, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	delivery, err := scanPostgresDelivery(tx.QueryRow(
		ctx,
		`WITH heads AS MATERIALIZED (
			SELECT DISTINCT ON (partition_key)
				id,
				sequence
			FROM ag_deliveries
			WHERE namespace = $1
			  AND queue = $2
			  AND state NOT IN ($3, $4)
			ORDER BY partition_key, sequence
		),
		candidate AS (
			SELECT delivery.id
			FROM ag_deliveries delivery
			JOIN heads
			  ON heads.id = delivery.id
			 AND heads.sequence = delivery.sequence
			WHERE delivery.namespace = $1
			  AND delivery.queue = $2
			  AND (
			    (
			      delivery.state = $5
			      AND delivery.available_at <= $7
			    )
			    OR (
			      delivery.state = $6
			      AND delivery.lease_expires_at <= $7
			    )
			  )
			ORDER BY delivery.sequence
			LIMIT 1
			FOR UPDATE OF delivery SKIP LOCKED
		)
		SELECT `+qualifiedDeliveryColumns+`
		FROM ag_deliveries delivery
		JOIN candidate ON candidate.id = delivery.id
		WHERE delivery.namespace = $1
		  AND delivery.queue = $2
		  AND delivery.id = candidate.id`,
		store.namespace,
		store.queue,
		string(sdk.DeliveryDelivered),
		string(sdk.DeliveryDeadLetter),
		string(sdk.DeliveryPending),
		string(sdk.DeliveryLeased),
		now,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	if err != nil {
		return sdk.Delivery{}, err
	}
	delivery, err = leaseDelivery(delivery, token, now, duration)
	if err != nil {
		return sdk.Delivery{}, err
	}
	tag, err := tx.Exec(
		ctx,
		`UPDATE ag_deliveries
		 SET state = $1,
		     attempt = $2,
		     lease_token = $3,
		     lease_expires_at = $4,
		     updated_at = $5
		 WHERE namespace = $6
		   AND queue = $7
		   AND id = $8`,
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
		return sdk.Delivery{}, err
	}
	if tag.RowsAffected() != 1 {
		return sdk.Delivery{}, sdk.ErrNoDelivery
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (store *DeliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := store.ackInTx(
		ctx,
		tx,
		id,
		token,
		normalizeDeliveryMutationTime(now),
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *DeliveryStore) ackInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	token string,
	now time.Time,
) error {
	return store.transitionInTx(
		ctx,
		tx,
		id,
		token,
		now,
		sdk.DeliveryDelivered,
		time.Time{},
		"",
	)
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
		normalizeDeliveryMutationTime(now),
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
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := store.transitionInTx(
		ctx,
		tx,
		id,
		token,
		now,
		state,
		availableAt,
		lastError,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (store *DeliveryStore) transitionInTx(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	token string,
	now time.Time,
	state sdk.DeliveryState,
	availableAt time.Time,
	lastError string,
) error {
	delivery, err := store.load(ctx, tx, id, true)
	if errors.Is(err, pgx.ErrNoRows) {
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
	tag, err := tx.Exec(
		ctx,
		`UPDATE ag_deliveries
		 SET state = $1,
		     available_at = $2,
		     lease_token = '',
		     lease_expires_at = NULL,
		     last_error = $3,
		     updated_at = $4
		 WHERE namespace = $5
		   AND queue = $6
		   AND id = $7`,
		string(delivery.State),
		nullableTime(delivery.AvailableAt),
		delivery.LastError,
		delivery.UpdatedAt.UTC(),
		store.namespace,
		store.queue,
		delivery.ID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
	}
	return nil
}

func (store *DeliveryStore) List(
	ctx context.Context,
) ([]sdk.Delivery, error) {
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+deliveryColumns+`
		 FROM ag_deliveries
		 WHERE namespace = $1 AND queue = $2
		 ORDER BY sequence`,
		store.namespace,
		store.queue,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]sdk.Delivery, 0)
	for rows.Next() {
		delivery, err := scanPostgresDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	return result, rows.Err()
}

func (store *DeliveryStore) ListNonTerminal(
	ctx context.Context,
) ([]sdk.Delivery, error) {
	rows, err := store.pool.Query(
		ctx,
		`SELECT `+deliveryColumns+`
		 FROM ag_deliveries
		 WHERE namespace = $1
		   AND queue = $2
		   AND state NOT IN ($3, $4)
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
	result := make([]sdk.Delivery, 0)
	for rows.Next() {
		delivery, scanErr := scanPostgresDelivery(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, delivery)
	}
	return result, rows.Err()
}

func (store *DeliveryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.DeliveryPage, error) {
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	statement := `SELECT ` + deliveryColumns + `
		FROM ag_deliveries
		WHERE namespace = $1 AND queue = $2`
	args := []any{store.namespace, store.queue}
	if request.After != "" {
		var sequence uint64
		if err := store.pool.QueryRow(
			ctx,
			`SELECT sequence
			 FROM ag_deliveries
			 WHERE namespace = $1
			   AND queue = $2
			   AND id = $3`,
			store.namespace,
			store.queue,
			request.After,
		).Scan(&sequence); errors.Is(err, pgx.ErrNoRows) {
			return sdk.DeliveryPage{}, fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		} else if err != nil {
			return sdk.DeliveryPage{}, err
		}
		statement += ` AND sequence > $3`
		args = append(args, sequence)
	}
	statement += fmt.Sprintf(
		` ORDER BY sequence LIMIT $%d`,
		len(args)+1,
	)
	args = append(args, request.Limit+1)
	rows, err := store.pool.Query(ctx, statement, args...)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	defer rows.Close()
	items := make([]sdk.Delivery, 0, request.Limit+1)
	for rows.Next() {
		delivery, err := scanPostgresDelivery(rows)
		if err != nil {
			return sdk.DeliveryPage{}, err
		}
		items = append(items, delivery)
	}
	if err := rows.Err(); err != nil {
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
	if before.IsZero() {
		return 0, errors.New("delivery purge cutoff is required")
	}
	tag, err := store.pool.Exec(
		ctx,
		`DELETE FROM ag_deliveries
		 WHERE namespace = $1
		   AND queue = $2
		   AND state IN ($3, $4)
		   AND updated_at < $5`,
		store.namespace,
		store.queue,
		string(sdk.DeliveryDelivered),
		string(sdk.DeliveryDeadLetter),
		before.UTC(),
	)
	return int(tag.RowsAffected()), err
}
