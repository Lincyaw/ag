package gormstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/sqlitecoord"
	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
	deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"
	operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"
	trajectorymodel "github.com/lincyaw/ag/sdk/storage/internal/trajectorymodel"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Aliases for the internal model helpers reused across sub-stores.
var (
	validateNewTrajectory           = trajectorymodel.ValidateNewTrajectory
	prepareNewTrajectory            = trajectorymodel.PrepareNewTrajectory
	prepareNewTrajectoryFork        = trajectorymodel.PrepareNewTrajectoryFork
	prepareTrajectoryEntries        = trajectorymodel.PrepareTrajectoryEntries
	trajectoryMetadataFn            = trajectorymodel.TrajectoryMetadata
	projectTrajectoryBranch         = trajectorymodel.ProjectTrajectoryBranch
	findEntryOnBranch               = trajectorymodel.FindEntryOnBranch
	resolveBranch                   = trajectorymodel.ResolveBranch
	latestEntry                     = trajectorymodel.LatestEntry
	latestCheckpointAfterAppend     = trajectorymodel.LatestCheckpointAfterAppend
	bindTrajectoryExecutionEntries  = trajectorymodel.BindTrajectoryExecutionEntries
	validateTrajectoryExecution     = trajectorymodel.ValidateTrajectoryExecution
	prepareTrajectoryExecutionStart = trajectorymodel.PrepareTrajectoryExecutionStart
	claimTrajectoryExecution        = trajectorymodel.ClaimTrajectoryExecution
	renewTrajectoryExecution        = trajectorymodel.RenewTrajectoryExecution
	commitTrajectoryExecution       = trajectorymodel.CommitTrajectoryExecution
	cancelTrajectoryExecution       = trajectorymodel.CancelTrajectoryExecution
	normalizedMutationTime          = trajectorymodel.NormalizeMutationTime
	validateTrajectoryKind          = trajectorymodel.ValidateTrajectoryKind

	prepareContextInjections     = contextinjectionmodel.PrepareBatch
	sameContextInjectionIdentity = contextinjectionmodel.SameIdentity
	validateLoadedContextRecord  = contextinjectionmodel.ValidateLoadedRecord
	validateContextQuery         = contextinjectionmodel.ValidateQuery
	validateContextInjectionIDs  = contextinjectionmodel.ValidateConsumeIDs

	prepareNewDeliveries          = deliverymodel.PrepareNewBatch
	sameDeliveryIdentity          = deliverymodel.SameIdentity
	leaseDelivery                 = deliverymodel.Lease
	finishDeliveryLease           = deliverymodel.FinishLease
	validateDeliveryLeaseDuration = deliverymodel.ValidateLeaseDuration
	normalizeDeliveryMutationTime = deliverymodel.NormalizeMutationTime
	validateLoadedDelivery        = deliverymodel.ValidateLoaded

	prepareNewOperationRecord      = operationmodel.PrepareNewRecord
	validateOperationClaim         = operationmodel.ValidateClaim
	validateOperationLeaseDuration = operationmodel.ValidateLeaseDuration
	validateOperationCompletion    = operationmodel.ValidateCompletionState
	normalizeOperationMutationTime = operationmodel.NormalizeMutationTime
	cancelOperation                = operationmodel.Cancel
	failOperation                  = operationmodel.Fail
	claimOperation                 = operationmodel.Claim
	renewOperation                 = operationmodel.Renew
	completeOperation              = operationmodel.Complete
	releaseOperation               = operationmodel.Release
	cloneOperationRecord           = operationmodel.CloneRecord
	sameOperationSubmission        = operationmodel.SameSubmission
	validateLoadedOperationRecord  = operationmodel.ValidateLoadedRecord
)

// Store is the shared GORM implementation of sdk.AtomicStateBackend.
type Store struct {
	db         *gorm.DB
	sqlDB      *sql.DB
	namespace  string
	dialect    string
	displayURI string
	writeMu    *sync.Mutex
	closeOnce  sync.Once
	closeErr   error
}

// Open opens a GORM+SQLite state backend at the given DSN (file path).
func Open(dsn string, namespace string) (*Store, error) {
	db, err := openSQLite(dsn)
	if err != nil {
		return nil, err
	}
	return openStoreWithWriteMutex(
		db,
		namespace,
		"sqlite://?namespace="+namespace,
		sqlitecoord.WriteMutex(dsn),
	)
}

func openStore(
	db *gorm.DB,
	namespace string,
	displayURI string,
) (*Store, error) {
	return openStoreWithWriteMutex(
		db,
		namespace,
		displayURI,
		new(sync.Mutex),
	)
}

func openStoreWithWriteMutex(
	db *gorm.DB,
	namespace string,
	displayURI string,
	writeMu *sync.Mutex,
) (*Store, error) {
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying sql.DB: %w", err)
	}

	writeMu.Lock()
	err = migrateStateSchema(db)
	writeMu.Unlock()
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}

	return &Store{
		db:         db,
		sqlDB:      sqlDB,
		namespace:  namespace,
		dialect:    db.Dialector.Name(),
		displayURI: displayURI,
		writeMu:    writeMu,
	}, nil
}

const postgresSchemaMigrationLockID int64 = 0x41674753746f7265

func migrateStateSchema(db *gorm.DB) error {
	migrate := func(tx *gorm.DB) error {
		if err := tx.AutoMigrate(
			&Trajectory{},
			&TrajectoryExecution{},
			&TrajectoryEntry{},
			&Operation{},
			&Delivery{},
			&ContextInjection{},
		); err != nil {
			return fmt.Errorf(
				"auto-migrate %s schema: %w",
				tx.Dialector.Name(),
				err,
			)
		}
		return ensureStateIndexes(tx)
	}

	if db.Dialector.Name() != "postgres" {
		return migrate(db)
	}

	// Multiple gateway processes can open the same PostgreSQL database at the
	// same time. PostgreSQL DDL is transactional, so serialize the entire GORM
	// migration on a transaction-scoped advisory lock instead of maintaining a
	// second, hand-written schema implementation in the PostgreSQL adapter.
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"SELECT pg_advisory_xact_lock(?)",
			postgresSchemaMigrationLockID,
		).Error; err != nil {
			return fmt.Errorf("lock postgres state schema migration: %w", err)
		}
		return migrate(tx)
	})
}

func ensureStateIndexes(db *gorm.DB) error {
	statements := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_trajectory_executions_id_idx
			ON ag_trajectory_executions (namespace, execution_id)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_executions_recovery_idx
			ON ag_trajectory_executions (namespace, state, lease_expires_at, created_at)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectories_updated_idx
			ON ag_trajectories (namespace, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectories_parent_idx
			ON ag_trajectories (namespace, parent_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_trajectory_entries_ordinal_idx
			ON ag_trajectory_entries (namespace, trajectory_id, ordinal)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_parent_idx
			ON ag_trajectory_entries (namespace, trajectory_id, parent_id)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_kind_idx
			ON ag_trajectory_entries (namespace, trajectory_id, kind, ordinal)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_execution_idx
			ON ag_trajectory_entries (namespace, execution_id, ordinal)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_operation_idx
			ON ag_trajectory_entries (namespace, operation_key)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_tool_idx
			ON ag_trajectory_entries (namespace, tool_name, tool_call_id)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_correlation_idx
			ON ag_trajectory_entries (namespace, correlation_id)`,
		`CREATE INDEX IF NOT EXISTS ag_trajectory_entries_time_idx
			ON ag_trajectory_entries (namespace, recorded_at)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_operations_idempotency_idx
			ON ag_operations (namespace, kind, resource, resource_revision, idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS ag_operations_state_idx
			ON ag_operations (namespace, state, lease_expires_at, updated_at)`,
		`CREATE INDEX IF NOT EXISTS ag_operations_updated_idx
			ON ag_operations (namespace, updated_at, id)`,
		`CREATE INDEX IF NOT EXISTS ag_operations_invocation_root_idx
			ON ag_operations (namespace, invocation_root_id)`,
		`CREATE INDEX IF NOT EXISTS ag_operations_invocation_parent_idx
			ON ag_operations (namespace, invocation_parent_id)`,
		`CREATE INDEX IF NOT EXISTS ag_operations_invocation_group_idx
			ON ag_operations (namespace, invocation_group_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_deliveries_sequence_idx
			ON ag_deliveries (namespace, queue, sequence)`,
		`CREATE INDEX IF NOT EXISTS ag_deliveries_ready_idx
			ON ag_deliveries (namespace, queue, state, available_at, lease_expires_at, sequence)`,
		`CREATE INDEX IF NOT EXISTS ag_deliveries_partition_idx
			ON ag_deliveries (namespace, queue, partition_key, sequence)`,
		`CREATE INDEX IF NOT EXISTS ag_deliveries_updated_idx
			ON ag_deliveries (namespace, updated_at, id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_context_injections_sequence_idx
			ON ag_context_injections (namespace, sequence)`,
		`CREATE INDEX IF NOT EXISTS ag_context_injections_target_idx
			ON ag_context_injections (namespace, target_session_id, target_execution_id, sequence)`,
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			return fmt.Errorf("initialize %s state index: %w", db.Dialector.Name(), err)
		}
	}
	return nil
}

func (s *Store) Trajectories() sdk.TrajectoryStore {
	return &trajectoryStore{store: s}
}

func (s *Store) Operations() sdk.OperationStore {
	return &operationStore{store: s}
}

func (s *Store) ContextInjections() sdk.ContextInjectionStore {
	return &contextInjectionStore{store: s}
}

func (s *Store) Deliveries(name string) (sdk.DeliveryStore, error) {
	if err := sdk.ValidateResourceName("delivery queue", name); err != nil {
		return nil, err
	}
	return &deliveryStore{store: s, queue: name}, nil
}

func (s *Store) Capabilities() sdk.StorageCapabilities {
	return sdk.StorageCapabilities{
		Durable:            true,
		MultiProcessSafe:   true,
		AtomicState:        true,
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (s *Store) Namespace() string { return s.namespace }

func (s *Store) Health(ctx context.Context) error {
	return s.sqlDB.PingContext(ctx)
}

func (s *Store) Close(_ context.Context) error {
	s.closeOnce.Do(func() {
		s.closeErr = s.sqlDB.Close()
	})
	return s.closeErr
}

func (s *Store) String() string {
	return s.displayURI
}

func (s *Store) forUpdate(db *gorm.DB) *gorm.DB {
	if s.dialect != "postgres" {
		return db
	}
	// Clauses returns an initialized chainable DB. Give callers a fresh session
	// so a query executed through it cannot leak its table or WHERE clauses into
	// the next model query in the same transaction.
	return db.Clauses(clause.Locking{Strength: "UPDATE"}).Session(&gorm.Session{})
}

func (s *Store) lockMutationResource(db *gorm.DB, resource string) error {
	if s.dialect != "postgres" {
		return nil
	}
	if err := db.Exec(
		"SELECT pg_advisory_xact_lock(hashtextextended(?::text, 0))",
		resource,
	).Error; err != nil {
		return fmt.Errorf("lock postgres mutation resource: %w", err)
	}
	return nil
}

func (s *Store) Prune(
	ctx context.Context,
	policy sdk.RetentionPolicy,
) (sdk.PruneResult, error) {
	var result sdk.PruneResult

	if !policy.OperationsBefore.IsZero() {
		items, err := s.Trajectories().List(ctx)
		if err != nil {
			return result, err
		}
		for _, item := range items {
			if item.ExecutionState == sdk.TrajectoryExecutionPending ||
				item.ExecutionState == sdk.TrajectoryExecutionRunning {
				return result, fmt.Errorf(
					"%w: cannot prune operations while trajectory %s execution %s is active",
					sdk.ErrTrajectoryExecution,
					item.ID,
					item.ExecutionID,
				)
			}
		}
		ops := &operationStore{store: s}
		count, err := ops.PurgeTerminal(ctx, policy.OperationsBefore)
		if err != nil {
			return result, err
		}
		result.Operations = count
	}

	if !policy.DeliveriesBefore.IsZero() {
		// Gather all known queues.
		var queues []string
		if err := s.db.WithContext(ctx).
			Model(&Delivery{}).
			Where("namespace = ?", s.namespace).
			Distinct("queue").
			Pluck("queue", &queues).Error; err != nil {
			return result, err
		}
		for _, q := range queues {
			ds := &deliveryStore{store: s, queue: q}
			removed, err := ds.PurgeTerminal(ctx, policy.DeliveriesBefore)
			result.Deliveries += removed
			if err != nil {
				return result, err
			}
		}
	}

	if !policy.TrajectoriesBefore.IsZero() {
		items, err := s.Trajectories().List(ctx)
		if err != nil {
			return result, err
		}
		ts := &trajectoryStore{store: s}
		for i := len(items) - 1; i >= 0; i-- {
			item := items[i]
			if item.UpdatedAt.Before(policy.TrajectoriesBefore) {
				if err := ts.Delete(ctx, item.ID); err != nil {
					if errors.Is(err, sdk.ErrTrajectoryReferenced) ||
						errors.Is(err, sdk.ErrTrajectoryExecution) {
						continue
					}
					return result, err
				}
				result.Trajectories++
			}
		}
	}

	return result, nil
}

// Helper: nullableTime converts a time.Time to *time.Time, returning nil for zero times.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	utc := t.UTC()
	return &utc
}

// Helper: attributesJSON encodes attributes map to a nullable string.
func attributesJSON(attrs map[string]string) (*string, error) {
	if attrs == nil {
		return nil, nil
	}
	raw, err := json.Marshal(attrs)
	if err != nil {
		return nil, err
	}
	s := string(raw)
	return &s, nil
}

// Helper: auditJSON encodes event audits to a nullable string.
func auditJSON(audits []sdk.EventAudit) (*string, error) {
	if len(audits) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(audits)
	if err != nil {
		return nil, err
	}
	s := string(raw)
	return &s, nil
}

// Helper: environmentJSON encodes environment to a string.
func environmentJSON(env sdk.TrajectoryEnvironment) (string, error) {
	raw, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

var _ sdk.AtomicStateBackend = (*Store)(nil)
