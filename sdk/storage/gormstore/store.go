package gormstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
	deliverymodel "github.com/lincyaw/ag/sdk/storage/internal/deliverymodel"
	operationmodel "github.com/lincyaw/ag/sdk/storage/internal/operationmodel"
	trajectorymodel "github.com/lincyaw/ag/sdk/storage/internal/trajectorymodel"
	"gorm.io/gorm"
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

// Store is the GORM+SQLite implementation of sdk.AtomicStateBackend.
type Store struct {
	db        *gorm.DB
	sqlDB     *sql.DB
	namespace string
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Open opens a GORM+SQLite state backend at the given DSN (file path).
func Open(dsn string, namespace string) (*Store, error) {
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}

	db, err := openSQLite(dsn)
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get underlying sql.DB: %w", err)
	}

	if err := db.AutoMigrate(
		&Trajectory{},
		&TrajectoryExecution{},
		&TrajectoryEntry{},
		&Operation{},
		&Delivery{},
		&ContextInjection{},
	); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("auto-migrate SQLite schema: %w", err)
	}

	return &Store{
		db:        db,
		sqlDB:     sqlDB,
		namespace: namespace,
	}, nil
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
	return fmt.Sprintf("sqlite://?namespace=%s", s.namespace)
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
