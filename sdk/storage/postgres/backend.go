package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lincyaw/ag/sdk"
)

type Config struct {
	ConnectionString string
	Namespace        string
	DisplayURI       string
}

// Backend stores trajectories, operations, inboxes, and outboxes in one
// PostgreSQL database and one transaction domain.
type Backend struct {
	pool         *pgxpool.Pool
	trajectories *TrajectoryStore
	operations   *OperationStore
	namespace    string
	displayURI   string

	deliveryMu sync.Mutex
	deliveries map[string]*DeliveryStore
	closeOnce  sync.Once
}

func NewStateBackend(
	ctx context.Context,
	connectionString string,
) (*Backend, error) {
	return Open(ctx, Config{
		ConnectionString: connectionString,
		Namespace:        "default",
	})
}

func Open(ctx context.Context, config Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(config.ConnectionString) == "" {
		return nil, errors.New("PostgreSQL connection string is empty")
	}
	namespace := strings.TrimSpace(config.Namespace)
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName(
		"storage namespace",
		namespace,
	); err != nil {
		return nil, err
	}
	poolConfig, err := pgxpool.ParseConfig(config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection string: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL state backend: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL state backend: %w", err)
	}
	if err := initPostgresStateSchema(ctx, pool); err != nil {
		return nil, err
	}
	displayURI := strings.TrimSpace(config.DisplayURI)
	if displayURI == "" {
		displayURI = redactedPostgresURI(config.ConnectionString, namespace)
	}
	backend := &Backend{
		pool:       pool,
		namespace:  namespace,
		displayURI: displayURI,
		deliveries: make(map[string]*DeliveryStore),
	}
	backend.trajectories = newTrajectoryStore(pool, namespace)
	backend.operations = newOperationStore(pool, namespace)
	cleanup = false
	return backend, nil
}

func redactedPostgresURI(connectionString, namespace string) string {
	parsed, err := url.Parse(connectionString)
	if err == nil &&
		(parsed.Scheme == "postgres" || parsed.Scheme == "postgresql") {
		query := parsed.Query()
		query.Set("namespace", namespace)
		parsed.RawQuery = query.Encode()
		return parsed.Redacted()
	}
	return "postgres://<configured>?namespace=" +
		url.QueryEscape(namespace)
}

func (backend *Backend) Trajectories() sdk.TrajectoryStore {
	return backend.trajectories
}

func (backend *Backend) Operations() sdk.OperationStore {
	return backend.operations
}

func (backend *Backend) Deliveries(
	name string,
) (sdk.DeliveryStore, error) {
	if err := sdk.ValidateResourceName("delivery queue", name); err != nil {
		return nil, err
	}
	backend.deliveryMu.Lock()
	defer backend.deliveryMu.Unlock()
	if existing := backend.deliveries[name]; existing != nil {
		return existing, nil
	}
	store := newDeliveryStore(backend.pool, backend.namespace, name)
	backend.deliveries[name] = store
	return store, nil
}

func (*Backend) Capabilities() sdk.StorageCapabilities {
	return sdk.StorageCapabilities{
		Durable:            true,
		MultiProcessSafe:   true,
		AtomicState:        true,
		Pagination:         true,
		Maintenance:        true,
		OperationFencing:   true,
		NamedQueues:        true,
		NamespaceIsolation: true,
	}
}

func (backend *Backend) Namespace() string {
	return backend.namespace
}

func (backend *Backend) Health(ctx context.Context) error {
	if backend == nil || backend.pool == nil {
		return errors.New("PostgreSQL state backend is not initialized")
	}
	return backend.pool.Ping(ctx)
}

func (backend *Backend) Close(_ context.Context) error {
	if backend == nil || backend.pool == nil {
		return nil
	}
	backend.closeOnce.Do(backend.pool.Close)
	return nil
}

func (backend *Backend) String() string {
	return backend.displayURI
}

func (backend *Backend) Prune(
	ctx context.Context,
	policy sdk.RetentionPolicy,
) (sdk.PruneResult, error) {
	var result sdk.PruneResult
	tx, err := backend.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if !policy.OperationsBefore.IsZero() {
		var activeTrajectory string
		err := tx.QueryRow(
			ctx,
			`SELECT trajectory_id
			 FROM ag_trajectory_executions
			 WHERE namespace = $1
			   AND state IN ($2, $3)
			 ORDER BY created_at, trajectory_id
			 LIMIT 1`,
			backend.namespace,
			string(sdk.TrajectoryExecutionPending),
			string(sdk.TrajectoryExecutionRunning),
		).Scan(&activeTrajectory)
		if err == nil {
			return result, fmt.Errorf(
				"%w: cannot prune operations while trajectory %s is active",
				sdk.ErrTrajectoryExecution,
				activeTrajectory,
			)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
		tag, err := tx.Exec(
			ctx,
			`DELETE FROM ag_operations
			 WHERE namespace = $1
			   AND state IN ($2, $3, $4)
			   AND updated_at < $5`,
			backend.namespace,
			string(sdk.OperationSucceeded),
			string(sdk.OperationFailed),
			string(sdk.OperationCancelled),
			policy.OperationsBefore.UTC(),
		)
		if err != nil {
			return result, err
		}
		result.Operations = int(tag.RowsAffected())
	}
	if !policy.DeliveriesBefore.IsZero() {
		tag, err := tx.Exec(
			ctx,
			`DELETE FROM ag_deliveries
			 WHERE namespace = $1
			   AND state IN ($2, $3)
			   AND updated_at < $4`,
			backend.namespace,
			string(sdk.DeliveryDelivered),
			string(sdk.DeliveryDeadLetter),
			policy.DeliveriesBefore.UTC(),
		)
		if err != nil {
			return result, err
		}
		result.Deliveries = int(tag.RowsAffected())
	}
	if !policy.TrajectoriesBefore.IsZero() {
		var blockedID string
		err := tx.QueryRow(
			ctx,
			`SELECT t.id
			 FROM ag_trajectories t
			 JOIN ag_trajectory_executions e
			   ON e.namespace = t.namespace
			  AND e.trajectory_id = t.id
			 WHERE t.namespace = $1
			   AND t.updated_at < $2
			   AND e.state IN ($3, $4)
			 ORDER BY t.updated_at, t.id
			 LIMIT 1`,
			backend.namespace,
			policy.TrajectoriesBefore.UTC(),
			string(sdk.TrajectoryExecutionPending),
			string(sdk.TrajectoryExecutionRunning),
		).Scan(&blockedID)
		if err == nil {
			return result, fmt.Errorf(
				"%w: trajectory %s has an active execution",
				sdk.ErrTrajectoryExecution,
				blockedID,
			)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
		err = tx.QueryRow(
			ctx,
			`SELECT parent.id
			 FROM ag_trajectories parent
			 JOIN ag_trajectories child
			   ON child.namespace = parent.namespace
			  AND child.parent_id = parent.id
			 WHERE parent.namespace = $1
			   AND parent.updated_at < $2
			 ORDER BY parent.updated_at, parent.id
			 LIMIT 1`,
			backend.namespace,
			policy.TrajectoriesBefore.UTC(),
		).Scan(&blockedID)
		if err == nil {
			return result, fmt.Errorf(
				"%w: trajectory %s has live forks",
				sdk.ErrTrajectoryReferenced,
				blockedID,
			)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return result, err
		}
		tag, err := tx.Exec(
			ctx,
			`DELETE FROM ag_trajectories
			 WHERE namespace = $1
			   AND updated_at < $2`,
			backend.namespace,
			policy.TrajectoriesBefore.UTC(),
		)
		if err != nil {
			return result, err
		}
		result.Trajectories = int(tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return sdk.PruneResult{}, err
	}
	return result, nil
}
