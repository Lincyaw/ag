package gormstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
	"gorm.io/gorm"
)

func (s *Store) AppendTrajectory(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.TrajectoryAppendResult{}, errors.New(
			"trajectory-append commit has no trajectory",
		)
	}
	preparedOutbox, err := prepareStateMutationOutbox(commit.Outbox)
	if err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ts := &trajectoryStore{store: s}
	var metadata sdk.TrajectoryMetadata
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, err = ts.appendInTx(tx, commit)
		if err != nil {
			return err
		}
		return enqueueStateMutationOutbox(tx, s.namespace, commit.Outbox, preparedOutbox)
	}); err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	return sdk.TrajectoryAppendResult{Trajectory: metadata}, nil
}

func (s *Store) StartExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
) (sdk.ExecutionMutationResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.ExecutionMutationResult{}, errors.New(
			"execution-start commit has no trajectory",
		)
	}
	preparedOutbox, err := prepareStateMutationOutbox(commit.Outbox)
	if err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	now := time.Now().UTC()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ts := &trajectoryStore{store: s}
	var metadata sdk.TrajectoryMetadata
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, err = ts.beginExecutionInTx(
			tx,
			commit.TrajectoryID,
			commit.ExpectedHead,
			commit.Start,
			commit.Input,
			now,
		)
		if err != nil {
			return err
		}
		return enqueueStateMutationOutbox(tx, s.namespace, commit.Outbox, preparedOutbox)
	}); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (s *Store) CommitExecution(
	ctx context.Context,
	commit sdk.ExecutionMutationCommit,
) (sdk.ExecutionMutationResult, error) {
	if commit.Trajectory.TrajectoryID == "" {
		return sdk.ExecutionMutationResult{}, errors.New(
			"execution mutation commit has no trajectory",
		)
	}
	preparedOutbox, err := prepareStateMutationOutbox(commit.Outbox)
	if err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	now := time.Now().UTC()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ts := &trajectoryStore{store: s}
	var metadata sdk.TrajectoryMetadata
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, err = ts.commitExecutionInTx(tx, commit.Trajectory, now)
		if err != nil {
			return err
		}
		return enqueueStateMutationOutbox(tx, s.namespace, commit.Outbox, preparedOutbox)
	}); err != nil {
		return sdk.ExecutionMutationResult{}, err
	}
	return sdk.ExecutionMutationResult{Trajectory: metadata}, nil
}

func (s *Store) CancelExecution(
	ctx context.Context,
	commit sdk.ExecutionCancelCommit,
) (sdk.ExecutionCancelResult, error) {
	if commit.TrajectoryID == "" {
		return sdk.ExecutionCancelResult{}, errors.New(
			"execution-cancel commit has no trajectory",
		)
	}
	if commit.ExecutionID == "" {
		return sdk.ExecutionCancelResult{}, errors.New(
			"execution-cancel commit has no execution",
		)
	}
	preparedOutbox, err := prepareStateMutationOutbox(commit.Outbox)
	if err != nil {
		return sdk.ExecutionCancelResult{}, err
	}
	now := commit.At.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	ts := &trajectoryStore{store: s}
	var metadata sdk.TrajectoryMetadata
	var changed bool
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, changed, err = ts.cancelExecutionInTx(tx, commit.TrajectoryCommit(), now)
		if err != nil {
			return err
		}
		if changed {
			return enqueueStateMutationOutbox(tx, s.namespace, commit.Outbox, preparedOutbox)
		}
		return nil
	}); err != nil {
		return sdk.ExecutionCancelResult{}, err
	}
	return sdk.ExecutionCancelResult{
		Trajectory: metadata,
		Changed:    changed,
	}, nil
}

// --- outbox helpers ---

func prepareStateMutationOutbox(
	groups []sdk.StateMutationDeliveries,
) ([][]sdk.Delivery, error) {
	preparedOutbox := make([][]sdk.Delivery, len(groups))
	now := time.Now().UTC()
	for index, group := range groups {
		if err := validateAtomicDeliveryQueue(group.Queue); err != nil {
			return nil, err
		}
		prepared, err := prepareNewDeliveries(group.Deliveries, now)
		if err != nil {
			return nil, err
		}
		preparedOutbox[index] = prepared
	}
	return preparedOutbox, nil
}

func enqueueStateMutationOutbox(
	tx *gorm.DB,
	namespace string,
	groups []sdk.StateMutationDeliveries,
	prepared [][]sdk.Delivery,
) error {
	for index, group := range groups {
		ds := &deliveryStore{
			store: &Store{db: tx, namespace: namespace},
			queue: group.Queue,
		}
		if err := ds.enqueueInTx(tx, prepared[index]); err != nil {
			return err
		}
	}
	return nil
}

func validateAtomicDeliveryQueue(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("atomic delivery queue is empty")
	}
	return sdk.ValidateResourceName("delivery queue", name)
}

// Verify that ContextInjections returns a consumer as well.
var _ sdk.ContextInjectionConsumer = (*contextInjectionStore)(nil)

// ContextInjectionConsumer returns the context injection store cast to the
// consumer interface, mirroring DuckDB's dual-role store.
func (s *Store) ContextInjectionConsumer() sdk.ContextInjectionConsumer {
	return &contextInjectionStore{store: s}
}

// Prune is declared on Store (satisfies StateBackend), already defined in store.go.
// This file avoids re-declaring it. The SDK router calls it via the interface.

// Ensure Store implements the full AtomicStateBackend interface.
var _ sdk.AtomicStateBackend = (*Store)(nil)

// Unused but useful for documentation: helpers to keep GORM error mapping similar.
func mapWriteError(err error) error {
	if err == nil {
		return nil
	}
	lower := strings.ToLower(err.Error())
	if strings.Contains(lower, "unique constraint") ||
		strings.Contains(lower, "constraint failed") {
		return fmt.Errorf("%w: %v", sdk.ErrTrajectoryConflict, err)
	}
	return err
}
