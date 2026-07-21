package gormstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
	"gorm.io/gorm"
)

type trajectoryStore struct {
	store *Store
}

type trajectoryEntryInspectionRow struct {
	TrajectoryID   string
	EntryID        string
	ParentID       string
	Ordinal        uint64
	Depth          uint64
	Kind           string
	RecordedAt     time.Time
	Generation     uint64
	ExecutionID    string
	OperationKey   string
	Turn           *int
	CorrelationID  string
	Provider       string
	Model          string
	ToolName       string
	ToolCallID     string
	FinishReason   string
	InputTokens    int64
	OutputTokens   int64
	IsError        *bool
	CauseCode      string
	ActionKind     string
	PayloadVersion uint32
	PayloadBytes   int64
	AttributesJSON *string
	AuditJSON      *string
}

func (ts *trajectoryStore) Create(
	ctx context.Context,
	trajectory sdk.Trajectory,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewTrajectory(trajectory, time.Now().UTC())
	if err != nil {
		return err
	}
	trajectory = prepared.Trajectory
	envJSON, err := environmentJSON(trajectory.Environment)
	if err != nil {
		return fmt.Errorf("encode trajectory environment: %w", err)
	}

	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	return ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ts.store.lockMutationResource(
			tx,
			"trajectory:create:"+ts.store.namespace+":"+trajectory.ID,
		); err != nil {
			return err
		}
		// Check existence
		var count int64
		if err := tx.Model(&Trajectory{}).
			Where("namespace = ? AND id = ?", ts.store.namespace, trajectory.ID).
			Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return fmt.Errorf("%w: %s", sdk.ErrTrajectoryExists, trajectory.ID)
		}

		var inheritedCount uint64
		if trajectory.ParentID != "" {
			branch, err := ts.loadBranchInTx(tx, trajectory.ParentID, trajectory.ParentEntryID)
			if err != nil {
				return fmt.Errorf(
					"resolve trajectory %q fork point: %w",
					trajectory.ID,
					err,
				)
			}
			prepared, err = prepareNewTrajectoryFork(prepared, branch)
			if err != nil {
				return err
			}
			trajectory = prepared.Trajectory
			inheritedCount = prepared.InheritedEntryCount
		}

		row := Trajectory{
			Namespace:           ts.store.namespace,
			ID:                  trajectory.ID,
			SchemaVersion:       trajectory.SchemaVersion,
			ParentID:            trajectory.ParentID,
			ParentEntryID:       trajectory.ParentEntryID,
			CreatedAt:           trajectory.CreatedAt.UTC(),
			UpdatedAt:           trajectory.UpdatedAt.UTC(),
			Head:                trajectory.Head,
			Checkpoint:          trajectory.Checkpoint,
			EnvironmentJSON:     envJSON,
			InheritedEntryCount: inheritedCount,
			OwnedEntryCount:     0,
		}
		return tx.Create(&row).Error
	})
}

func (ts *trajectoryStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	var head string
	err := ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		metadata, err := ts.appendInTx(tx, sdk.TrajectoryAppendCommit{
			TrajectoryID: id,
			ExpectedHead: expectedHead,
			Entries:      entries,
		})
		if err != nil {
			return err
		}
		head = metadata.Head
		return nil
	})
	return head, err
}

func (ts *trajectoryStore) appendInTx(
	tx *gorm.DB,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", commit.TrajectoryID); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, _, ownedCount, err := ts.loadStoredTrajectoryTx(
		ts.store.forUpdate(tx),
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
			trajectory.Execution.ID,
		)
	}
	if _, err := ts.appendEntries(tx, trajectory, ownedCount, commit.ExpectedHead, commit.Entries); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return ts.metadataInTx(tx, commit.TrajectoryID)
}

func (ts *trajectoryStore) appendEntries(
	tx *gorm.DB,
	trajectory sdk.Trajectory,
	ownedCount uint64,
	expectedHead string,
	entries []sdk.TrajectoryEntry,
) (string, error) {
	if trajectory.Head != expectedHead {
		return "", fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			trajectory.ID,
			trajectory.Head,
			expectedHead,
		)
	}
	prepared, err := prepareTrajectoryEntries(
		trajectory.ID,
		ownedCount,
		trajectory.Head != "",
		entries,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return ts.loadEntryTx(tx, trajectory.ID, entryID)
		},
	)
	if err != nil {
		return "", err
	}
	for _, entry := range prepared {
		if err := ts.insertEntry(tx, entry); err != nil {
			return "", err
		}
	}
	last := prepared[len(prepared)-1]
	preparedIndex := make(map[string]sdk.TrajectoryEntry, len(prepared))
	for _, entry := range prepared {
		preparedIndex[entry.ID] = entry
	}
	checkpoint, err := latestCheckpointAfterAppend(
		trajectory.Head,
		trajectory.Checkpoint,
		last.ID,
		preparedIndex,
		func(entryID string) (sdk.TrajectoryEntry, bool, error) {
			return ts.loadEntryTx(tx, trajectory.ID, entryID)
		},
	)
	if err != nil {
		return "", err
	}
	if err := tx.Model(&Trajectory{}).
		Where("namespace = ? AND id = ?", ts.store.namespace, trajectory.ID).
		Updates(map[string]any{
			"head":              last.ID,
			"checkpoint":        checkpoint,
			"updated_at":        last.Timestamp.UTC(),
			"owned_entry_count": gorm.Expr("owned_entry_count + ?", len(prepared)),
		}).Error; err != nil {
		return "", err
	}
	return last.ID, nil
}

func (ts *trajectoryStore) insertEntry(tx *gorm.DB, entry sdk.TrajectoryEntry) error {
	attrsJSON, err := attributesJSON(entry.Attributes)
	if err != nil {
		return fmt.Errorf("encode trajectory entry %q attributes: %w", entry.ID, err)
	}
	adtJSON, err := auditJSON(entry.Audit)
	if err != nil {
		return fmt.Errorf("encode trajectory entry %q audit: %w", entry.ID, err)
	}
	row := TrajectoryEntry{
		Namespace:      ts.store.namespace,
		TrajectoryID:   entry.TrajectoryID,
		EntryID:        entry.ID,
		ParentID:       entry.ParentID,
		Ordinal:        entry.Ordinal,
		Depth:          entry.Depth,
		Kind:           string(entry.Kind),
		RecordedAt:     entry.Timestamp.UTC(),
		Generation:     entry.Generation,
		ExecutionID:    entry.Fields.ExecutionID,
		OperationKey:   entry.Fields.OperationKey,
		Turn:           entry.Fields.Turn,
		CorrelationID:  entry.Fields.CorrelationID,
		Provider:       entry.Fields.Provider,
		Model:          entry.Fields.Model,
		ToolName:       entry.Fields.ToolName,
		ToolCallID:     entry.Fields.ToolCallID,
		FinishReason:   entry.Fields.FinishReason,
		InputTokens:    entry.Fields.InputTokens,
		OutputTokens:   entry.Fields.OutputTokens,
		IsError:        entry.Fields.IsError,
		CauseCode:      entry.Fields.CauseCode,
		ActionKind:     string(entry.Fields.ActionKind),
		PayloadVersion: entry.PayloadVersion,
		Payload:        []byte(entry.Payload),
		AttributesJSON: attrsJSON,
		AuditJSON:      adtJSON,
	}
	return tx.Create(&row).Error
}

func (ts *trajectoryStore) BeginExecution(
	ctx context.Context,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	var metadata sdk.TrajectoryMetadata
	err := ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, err = ts.beginExecutionInTx(tx, id, expectedHead, start, input, time.Now().UTC())
		return err
	})
	return metadata, err
}

func (ts *trajectoryStore) beginExecutionInTx(
	tx *gorm.DB,
	id string,
	expectedHead string,
	start sdk.TrajectoryExecutionStart,
	input sdk.TrajectoryEntry,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if input.Kind != sdk.TrajectoryKindUserMessage {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution input must be a user_message entry",
		)
	}
	execution, err := prepareTrajectoryExecutionStart(start, expectedHead, input.ID, now)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	bound, err := bindTrajectoryExecutionEntries(execution.ID, []sdk.TrajectoryEntry{input})
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, _, ownedCount, err := ts.loadStoredTrajectoryTx(
		ts.store.forUpdate(tx),
		id,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has active execution %s",
			sdk.ErrTrajectoryExecution,
			id,
			trajectory.Execution.ID,
		)
	}
	if _, err := ts.appendEntries(tx, trajectory, ownedCount, expectedHead, bound); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ts.replaceExecution(tx, id, execution); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return ts.metadataInTx(tx, id)
}

func (ts *trajectoryStore) ClaimExecution(
	ctx context.Context,
	id string,
	owner string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return ts.mutateExecution(ctx, id, func(exec sdk.TrajectoryExecution) (sdk.TrajectoryExecution, error) {
		return claimTrajectoryExecution(exec, owner, now, ttl)
	})
}

func (ts *trajectoryStore) RenewExecution(
	ctx context.Context,
	id string,
	executionID string,
	token string,
	now time.Time,
	ttl time.Duration,
) (sdk.TrajectoryExecution, error) {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return ts.mutateExecution(ctx, id, func(exec sdk.TrajectoryExecution) (sdk.TrajectoryExecution, error) {
		return renewTrajectoryExecution(exec, executionID, token, now, ttl)
	})
}

func (ts *trajectoryStore) mutateExecution(
	ctx context.Context,
	id string,
	mutation func(sdk.TrajectoryExecution) (sdk.TrajectoryExecution, error),
) (sdk.TrajectoryExecution, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecution{}, err
	}
	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	var execution sdk.TrajectoryExecution
	err := ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		trajectory, _, _, err := ts.loadStoredTrajectoryTx(
			ts.store.forUpdate(tx),
			id,
		)
		if err != nil {
			return err
		}
		if trajectory.Execution == nil {
			return fmt.Errorf(
				"%w: trajectory %s has no execution",
				sdk.ErrTrajectoryExecution,
				id,
			)
		}
		execution, err = mutation(*trajectory.Execution)
		if err != nil {
			return err
		}
		if err := ts.replaceExecution(tx, id, execution); err != nil {
			return err
		}
		return tx.Model(&Trajectory{}).
			Where("namespace = ? AND id = ?", ts.store.namespace, id).
			Update("updated_at", execution.UpdatedAt.UTC()).Error
	})
	return execution, err
}

func (ts *trajectoryStore) CommitExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	var metadata sdk.TrajectoryMetadata
	err := ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		metadata, err = ts.commitExecutionInTx(tx, commit, time.Now().UTC())
		return err
	})
	return metadata, err
}

func (ts *trajectoryStore) commitExecutionInTx(
	tx *gorm.DB,
	commit sdk.TrajectoryExecutionCommit,
	now time.Time,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", commit.TrajectoryID); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if len(commit.Entries) == 0 && commit.State == "" {
		return sdk.TrajectoryMetadata{}, errors.New(
			"trajectory execution commit has no mutation",
		)
	}
	entries, err := bindTrajectoryExecutionEntries(commit.ExecutionID, commit.Entries)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	commit.Entries = entries

	trajectory, _, ownedCount, err := ts.loadStoredTrajectoryTx(
		ts.store.forUpdate(tx),
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if trajectory.Head != commit.ExpectedHead {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has head %q, expected %q",
			sdk.ErrTrajectoryConflict,
			commit.TrajectoryID,
			trajectory.Head,
			commit.ExpectedHead,
		)
	}
	if trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, err := commitTrajectoryExecution(*trajectory.Execution, commit, now)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if len(commit.Entries) > 0 {
		if _, err := ts.appendEntries(tx, trajectory, ownedCount, commit.ExpectedHead, commit.Entries); err != nil {
			return sdk.TrajectoryMetadata{}, err
		}
	}
	if err := ts.replaceExecution(tx, commit.TrajectoryID, execution); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := tx.Model(&Trajectory{}).
		Where("namespace = ? AND id = ?", ts.store.namespace, commit.TrajectoryID).
		Update("updated_at", now).Error; err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return ts.metadataInTx(tx, commit.TrajectoryID)
}

func (ts *trajectoryStore) CancelExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
) (sdk.TrajectoryExecutionCancelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryExecutionCancelResult{}, err
	}
	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	var result sdk.TrajectoryExecutionCancelResult
	err := ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		metadata, changed, err := ts.cancelExecutionInTx(tx, commit, normalizedMutationTime(commit.At))
		if err != nil {
			return err
		}
		result = sdk.TrajectoryExecutionCancelResult{
			Trajectory: metadata,
			Changed:    changed,
		}
		return nil
	})
	return result, err
}

func (ts *trajectoryStore) cancelExecutionInTx(
	tx *gorm.DB,
	commit sdk.TrajectoryExecutionCancelCommit,
	now time.Time,
) (sdk.TrajectoryMetadata, bool, error) {
	if err := sdk.ValidateResourceName("trajectory", commit.TrajectoryID); err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	entries, err := bindTrajectoryExecutionEntries(commit.ExecutionID, commit.Entries)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	trajectory, _, ownedCount, err := ts.loadStoredTrajectoryTx(
		ts.store.forUpdate(tx),
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	if trajectory.Execution == nil {
		return sdk.TrajectoryMetadata{}, false, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			commit.TrajectoryID,
		)
	}
	execution, changed, err := cancelTrajectoryExecution(
		*trajectory.Execution,
		commit.ExecutionID,
		commit.Reason,
		now,
	)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	if changed {
		if len(entries) > 0 {
			if _, err := ts.appendEntries(tx, trajectory, ownedCount, commit.ExpectedHead, entries); err != nil {
				return sdk.TrajectoryMetadata{}, false, err
			}
		}
		if err := ts.replaceExecution(tx, commit.TrajectoryID, execution); err != nil {
			return sdk.TrajectoryMetadata{}, false, err
		}
		if err := tx.Model(&Trajectory{}).
			Where("namespace = ? AND id = ?", ts.store.namespace, commit.TrajectoryID).
			Update("updated_at", now).Error; err != nil {
			return sdk.TrajectoryMetadata{}, false, err
		}
	}
	metadata, err := ts.metadataInTx(tx, commit.TrajectoryID)
	if err != nil {
		return sdk.TrajectoryMetadata{}, false, err
	}
	return metadata, changed, nil
}

func (ts *trajectoryStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.TrajectoryMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var rows []TrajectoryExecution
	if err := ts.store.db.WithContext(ctx).
		Where(
			"namespace = ? AND (state = ? OR (state = ? AND lease_expires_at <= ?))",
			ts.store.namespace,
			string(sdk.TrajectoryExecutionPending),
			string(sdk.TrajectoryExecutionRunning),
			now,
		).
		Order("created_at, trajectory_id").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]sdk.TrajectoryMetadata, 0, len(rows))
	for _, row := range rows {
		metadata, err := ts.LoadMetadata(ctx, row.TrajectoryID)
		if err != nil {
			return nil, err
		}
		result = append(result, metadata)
	}
	return result, nil
}

func (ts *trajectoryStore) LoadMetadata(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	trajectory, inheritedCount, ownedCount, err := ts.loadStoredTrajectory(ctx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadataFn(trajectory, int(inheritedCount+ownedCount), int(ownedCount)), nil
}

func (ts *trajectoryStore) LoadEntry(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := sdk.ValidateResourceName("trajectory entry", entryID); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	entry, found, err := ts.loadEntryCtx(ctx, id, entryID)
	if err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	if !found {
		return sdk.TrajectoryEntry{}, fmt.Errorf(
			"%w: trajectory %s entry %s",
			sdk.ErrTrajectoryEntryNotFound,
			id,
			entryID,
		)
	}
	return entry, nil
}

func (ts *trajectoryStore) LoadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ts.loadBranch(ctx, id, head)
}

func (ts *trajectoryStore) LoadBranchView(
	ctx context.Context,
	id string,
	head string,
) (sdk.Trajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory, inheritedCount, ownedCount, err := ts.loadStoredTrajectory(ctx, id)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	branch, err := ts.loadBranch(ctx, id, head)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	return projectTrajectoryBranch(
		trajectoryMetadataFn(trajectory, int(inheritedCount+ownedCount), int(ownedCount)),
		head,
		branch,
	), nil
}

func (ts *trajectoryStore) InspectTrajectoryEntries(
	ctx context.Context,
	id string,
	head string,
) (sdk.TrajectoryMetadata, []sdk.TrajectoryEntryInspection, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryMetadata{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryMetadata{}, nil, err
	}
	return ts.inspectTrajectoryEntriesTx(
		ts.store.db.WithContext(ctx),
		id,
		head,
		make(map[string]struct{}),
	)
}

func (ts *trajectoryStore) inspectTrajectoryEntriesTx(
	tx *gorm.DB,
	id string,
	head string,
	trajectoryPath map[string]struct{},
) (sdk.TrajectoryMetadata, []sdk.TrajectoryEntryInspection, error) {
	if _, exists := trajectoryPath[id]; exists {
		return sdk.TrajectoryMetadata{}, nil, fmt.Errorf(
			"trajectory parent cycle contains %q",
			id,
		)
	}
	trajectoryPath[id] = struct{}{}
	defer delete(trajectoryPath, id)

	trajectory, inheritedCount, ownedCount, err := ts.loadStoredTrajectoryTx(tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, nil, err
	}
	metadata := trajectoryMetadataFn(
		trajectory,
		int(inheritedCount+ownedCount),
		int(ownedCount),
	)
	if head == "" {
		head = metadata.Head
	}

	candidates := make([]sdk.TrajectoryEntryInspection, 0, metadata.EntryCount)
	if trajectory.ParentID != "" {
		_, inherited, err := ts.inspectTrajectoryEntriesTx(
			tx,
			trajectory.ParentID,
			trajectory.ParentEntryID,
			trajectoryPath,
		)
		if err != nil {
			return sdk.TrajectoryMetadata{}, nil, err
		}
		candidates = append(candidates, inherited...)
	}
	owned, err := ts.loadOwnedEntryInspectionsTx(tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, nil, err
	}
	candidates = append(candidates, owned...)

	branch, err := resolveInspectionBranch(id, head, candidates)
	if err != nil {
		return sdk.TrajectoryMetadata{}, nil, err
	}
	metadata.Head = head
	metadata.EntryCount = len(branch)
	metadata.OwnedEntryCount = 0
	metadata.Checkpoint = ""
	for _, entry := range branch {
		if entry.TrajectoryID == id {
			metadata.OwnedEntryCount++
		}
		if entry.Kind == sdk.TrajectoryKindCheckpoint {
			metadata.Checkpoint = entry.ID
		}
	}
	return metadata, branch, nil
}

func (ts *trajectoryStore) loadOwnedEntryInspectionsTx(
	tx *gorm.DB,
	id string,
) ([]sdk.TrajectoryEntryInspection, error) {
	var rows []trajectoryEntryInspectionRow
	if err := tx.Table((TrajectoryEntry{}).TableName()).
		Select(`trajectory_id, entry_id, parent_id, ordinal, depth, kind,
			recorded_at, generation, execution_id, operation_key, turn,
			correlation_id, provider, model, tool_name, tool_call_id,
			finish_reason, input_tokens, output_tokens, is_error, cause_code,
			action_kind, payload_version, length(payload) AS payload_bytes,
			attributes_json, audit_json`).
		Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, id).
		Order("ordinal").
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	result := make([]sdk.TrajectoryEntryInspection, 0, len(rows))
	for _, row := range rows {
		entry, err := rowToTrajectoryEntryInspection(row)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func rowToTrajectoryEntryInspection(
	row trajectoryEntryInspectionRow,
) (sdk.TrajectoryEntryInspection, error) {
	kind := sdk.TrajectoryKind(row.Kind)
	if !kind.Valid() {
		return sdk.TrajectoryEntryInspection{}, fmt.Errorf(
			"trajectory entry %q has invalid kind %q",
			row.EntryID,
			row.Kind,
		)
	}
	if row.PayloadBytes < 0 || uint64(row.PayloadBytes) > uint64(^uint(0)>>1) {
		return sdk.TrajectoryEntryInspection{}, fmt.Errorf(
			"trajectory entry %q payload size %d is not representable",
			row.EntryID,
			row.PayloadBytes,
		)
	}
	entry := sdk.TrajectoryEntryInspection{
		ID:             row.EntryID,
		TrajectoryID:   row.TrajectoryID,
		ParentID:       row.ParentID,
		Ordinal:        row.Ordinal,
		Depth:          row.Depth,
		Kind:           kind,
		Timestamp:      row.RecordedAt.UTC(),
		Generation:     row.Generation,
		PayloadVersion: row.PayloadVersion,
		PayloadBytes:   int(row.PayloadBytes),
		Fields: sdk.TrajectoryEntryFields{
			ExecutionID:   row.ExecutionID,
			OperationKey:  row.OperationKey,
			Turn:          row.Turn,
			CorrelationID: row.CorrelationID,
			Provider:      row.Provider,
			Model:         row.Model,
			ToolName:      row.ToolName,
			ToolCallID:    row.ToolCallID,
			FinishReason:  row.FinishReason,
			InputTokens:   row.InputTokens,
			OutputTokens:  row.OutputTokens,
			IsError:       row.IsError,
			CauseCode:     row.CauseCode,
			ActionKind:    sdk.ActionKind(row.ActionKind),
		},
	}
	if row.AttributesJSON != nil {
		var attributes map[string]string
		if err := json.Unmarshal([]byte(*row.AttributesJSON), &attributes); err != nil {
			return sdk.TrajectoryEntryInspection{}, fmt.Errorf(
				"decode entry %q attributes: %w",
				row.EntryID,
				err,
			)
		}
		entry.AttributeCount = len(attributes)
	}
	if row.AuditJSON != nil {
		var audit []json.RawMessage
		if err := json.Unmarshal([]byte(*row.AuditJSON), &audit); err != nil {
			return sdk.TrajectoryEntryInspection{}, fmt.Errorf(
				"decode entry %q audit: %w",
				row.EntryID,
				err,
			)
		}
		entry.AuditCount = len(audit)
	}
	return entry, nil
}

func resolveInspectionBranch(
	trajectoryID string,
	head string,
	candidates []sdk.TrajectoryEntryInspection,
) ([]sdk.TrajectoryEntryInspection, error) {
	if head == "" {
		return []sdk.TrajectoryEntryInspection{}, nil
	}
	byID := make(map[string]sdk.TrajectoryEntryInspection, len(candidates))
	for _, entry := range candidates {
		byID[entry.ID] = entry
	}
	reversed := make([]sdk.TrajectoryEntryInspection, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for cursor := head; cursor != ""; {
		if _, exists := seen[cursor]; exists {
			return nil, fmt.Errorf(
				"trajectory %q branch cycle contains entry %q",
				trajectoryID,
				cursor,
			)
		}
		seen[cursor] = struct{}{}
		entry, exists := byID[cursor]
		if !exists {
			return nil, fmt.Errorf(
				"%w: trajectory %s entry %s",
				sdk.ErrTrajectoryEntryNotFound,
				trajectoryID,
				cursor,
			)
		}
		reversed = append(reversed, entry)
		cursor = entry.ParentID
	}
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return reversed, nil
}

func (ts *trajectoryStore) FindLatest(
	ctx context.Context,
	id string,
	head string,
	kind sdk.TrajectoryKind,
) (sdk.TrajectoryEntry, bool, error) {
	if err := validateTrajectoryKind(kind); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	if _, _, _, err := ts.loadStoredTrajectory(ctx, id); err != nil {
		return sdk.TrajectoryEntry{}, false, err
	}
	return latestEntry(head, kind, func(entryID string) (sdk.TrajectoryEntry, bool, error) {
		return ts.loadEntryCtx(ctx, id, entryID)
	})
}

func (ts *trajectoryStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return sdk.Trajectory{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory, inheritedCount, ownedCount, err := ts.loadStoredTrajectory(ctx, id)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	trajectory.Entries = make([]sdk.TrajectoryEntry, 0, int(inheritedCount+ownedCount))
	if trajectory.ParentID != "" {
		inherited, err := ts.loadBranch(ctx, trajectory.ParentID, trajectory.ParentEntryID)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(trajectory.Entries, inherited...)
	}
	var rows []TrajectoryEntry
	if err := ts.store.db.WithContext(ctx).
		Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, id).
		Order("ordinal").
		Find(&rows).Error; err != nil {
		return sdk.Trajectory{}, err
	}
	for _, row := range rows {
		entry, err := rowToTrajectoryEntry(row)
		if err != nil {
			return sdk.Trajectory{}, err
		}
		trajectory.Entries = append(trajectory.Entries, entry)
	}
	return trajectory, nil
}

func (ts *trajectoryStore) List(
	ctx context.Context,
) ([]sdk.TrajectorySummary, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var trajectories []Trajectory
	if err := ts.store.db.WithContext(ctx).
		Where("namespace = ?", ts.store.namespace).
		Order("created_at, id").
		Find(&trajectories).Error; err != nil {
		return nil, err
	}
	result := make([]sdk.TrajectorySummary, 0, len(trajectories))
	for _, t := range trajectories {
		var exec TrajectoryExecution
		execFound := ts.store.db.WithContext(ctx).
			Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, t.ID).
			First(&exec).Error == nil

		item := sdk.TrajectorySummary{
			SchemaVersion:   t.SchemaVersion,
			ID:              t.ID,
			ParentID:        t.ParentID,
			ParentEntryID:   t.ParentEntryID,
			CreatedAt:       t.CreatedAt.UTC(),
			UpdatedAt:       t.UpdatedAt.UTC(),
			Head:            t.Head,
			Checkpoint:      t.Checkpoint,
			EntryCount:      int(t.InheritedEntryCount + t.OwnedEntryCount),
			OwnedEntryCount: int(t.OwnedEntryCount),
		}
		if execFound {
			item.ExecutionID = exec.ExecutionID
			item.ExecutionState = sdk.TrajectoryExecutionState(exec.State)
		}
		result = append(result, item)
	}
	return result, nil
}

func (ts *trajectoryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.TrajectoryPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.TrajectoryPage{}, err
	}
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.TrajectoryPage{}, err
	}
	db := ts.store.db.WithContext(ctx).
		Model(&Trajectory{}).
		Where("namespace = ?", ts.store.namespace)
	if request.After != "" {
		var cursor Trajectory
		if err := ts.store.db.WithContext(ctx).
			Where("namespace = ? AND id = ?", ts.store.namespace, request.After).
			First(&cursor).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sdk.TrajectoryPage{}, fmt.Errorf(
					"pagination cursor %q was not found", request.After,
				)
			}
			return sdk.TrajectoryPage{}, err
		}
		db = db.Where(
			"(created_at > ? OR (created_at = ? AND id > ?))",
			cursor.CreatedAt.UTC(), cursor.CreatedAt.UTC(), request.After,
		)
	}
	var trajectories []Trajectory
	if err := db.Order("created_at, id").
		Limit(request.Limit + 1).
		Find(&trajectories).Error; err != nil {
		return sdk.TrajectoryPage{}, err
	}
	items := make([]sdk.TrajectorySummary, 0, len(trajectories))
	for _, t := range trajectories {
		var exec TrajectoryExecution
		execFound := ts.store.db.WithContext(ctx).
			Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, t.ID).
			First(&exec).Error == nil

		item := sdk.TrajectorySummary{
			SchemaVersion:   t.SchemaVersion,
			ID:              t.ID,
			ParentID:        t.ParentID,
			ParentEntryID:   t.ParentEntryID,
			CreatedAt:       t.CreatedAt.UTC(),
			UpdatedAt:       t.UpdatedAt.UTC(),
			Head:            t.Head,
			Checkpoint:      t.Checkpoint,
			EntryCount:      int(t.InheritedEntryCount + t.OwnedEntryCount),
			OwnedEntryCount: int(t.OwnedEntryCount),
		}
		if execFound {
			item.ExecutionID = exec.ExecutionID
			item.ExecutionState = sdk.TrajectoryExecutionState(exec.State)
		}
		items = append(items, item)
	}
	next := ""
	if len(items) > request.Limit {
		items = items[:request.Limit]
		next = items[len(items)-1].ID
	}
	return sdk.TrajectoryPage{Items: items, Next: next}, nil
}

func (ts *trajectoryStore) Delete(
	ctx context.Context,
	id string,
) error {
	if err := sdk.ValidateResourceName("trajectory", id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ts.store.writeMu.Lock()
	defer ts.store.writeMu.Unlock()

	return ts.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		trajectory, _, _, err := ts.loadStoredTrajectoryTx(
			ts.store.forUpdate(tx),
			id,
		)
		if err != nil {
			return err
		}
		if trajectory.Execution != nil && !trajectory.Execution.Terminal() {
			return fmt.Errorf(
				"%w: trajectory %s execution %s is active",
				sdk.ErrTrajectoryExecution,
				id,
				trajectory.Execution.ID,
			)
		}
		var children int64
		if err := tx.Model(&Trajectory{}).
			Where("namespace = ? AND parent_id = ?", ts.store.namespace, id).
			Count(&children).Error; err != nil {
			return err
		}
		if children > 0 {
			return fmt.Errorf("%w: trajectory %s has live forks", sdk.ErrTrajectoryReferenced, id)
		}
		if err := tx.Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, id).
			Delete(&TrajectoryEntry{}).Error; err != nil {
			return err
		}
		if err := tx.Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, id).
			Delete(&TrajectoryExecution{}).Error; err != nil {
			return err
		}
		return tx.Where("namespace = ? AND id = ?", ts.store.namespace, id).
			Delete(&Trajectory{}).Error
	})
}

// --- internal helpers ---

func (ts *trajectoryStore) loadStoredTrajectory(
	ctx context.Context,
	id string,
) (sdk.Trajectory, uint64, uint64, error) {
	return ts.loadStoredTrajectoryTx(ts.store.db.WithContext(ctx), id)
}

func (ts *trajectoryStore) loadStoredTrajectoryTx(
	tx *gorm.DB,
	id string,
) (sdk.Trajectory, uint64, uint64, error) {
	var row Trajectory
	if err := tx.Where("namespace = ? AND id = ?", ts.store.namespace, id).
		First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return sdk.Trajectory{}, 0, 0, fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
		}
		return sdk.Trajectory{}, 0, 0, err
	}
	var trajectory sdk.Trajectory
	trajectory.SchemaVersion = row.SchemaVersion
	trajectory.ID = row.ID
	trajectory.ParentID = row.ParentID
	trajectory.ParentEntryID = row.ParentEntryID
	trajectory.CreatedAt = row.CreatedAt.UTC()
	trajectory.UpdatedAt = row.UpdatedAt.UTC()
	trajectory.Head = row.Head
	trajectory.Checkpoint = row.Checkpoint
	if err := json.Unmarshal([]byte(row.EnvironmentJSON), &trajectory.Environment); err != nil {
		return sdk.Trajectory{}, 0, 0, fmt.Errorf("decode trajectory %q environment: %w", id, err)
	}
	execution, err := ts.loadExecutionTx(tx, id)
	if err != nil {
		return sdk.Trajectory{}, 0, 0, err
	}
	trajectory.Execution = execution
	trajectory.Entries = []sdk.TrajectoryEntry{}
	return trajectory, row.InheritedEntryCount, row.OwnedEntryCount, nil
}

func (ts *trajectoryStore) loadExecutionTx(
	tx *gorm.DB,
	trajectoryID string,
) (*sdk.TrajectoryExecution, error) {
	var row TrajectoryExecution
	if err := tx.Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, trajectoryID).
		First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	execution := sdk.TrajectoryExecution{
		ID:           row.ExecutionID,
		State:        sdk.TrajectoryExecutionState(row.State),
		Revision:     row.Revision,
		BaseHead:     row.BaseHead,
		InputEntryID: row.InputEntryID,
		Provider:     row.Provider,
		System:       row.SystemPrompt,
		MaxTurns:     row.MaxTurns,
		Owner:        row.Owner,
		LeaseToken:   row.LeaseToken,
		LastError:    row.LastError,
		CreatedAt:    row.CreatedAt.UTC(),
		UpdatedAt:    row.UpdatedAt.UTC(),
	}
	if row.LeaseExpiresAt != nil {
		execution.LeaseExpiresAt = row.LeaseExpiresAt.UTC()
	}
	if err := validateTrajectoryExecution(execution); err != nil {
		return nil, fmt.Errorf("validate trajectory %q execution: %w", trajectoryID, err)
	}
	return &execution, nil
}

func (ts *trajectoryStore) replaceExecution(
	tx *gorm.DB,
	trajectoryID string,
	execution sdk.TrajectoryExecution,
) error {
	if err := tx.Where("namespace = ? AND trajectory_id = ?", ts.store.namespace, trajectoryID).
		Delete(&TrajectoryExecution{}).Error; err != nil {
		return err
	}
	row := TrajectoryExecution{
		Namespace:      ts.store.namespace,
		TrajectoryID:   trajectoryID,
		ExecutionID:    execution.ID,
		State:          string(execution.State),
		Revision:       execution.Revision,
		BaseHead:       execution.BaseHead,
		InputEntryID:   execution.InputEntryID,
		Provider:       execution.Provider,
		SystemPrompt:   execution.System,
		MaxTurns:       execution.MaxTurns,
		Owner:          execution.Owner,
		LeaseToken:     execution.LeaseToken,
		LeaseExpiresAt: nullableTime(execution.LeaseExpiresAt),
		LastError:      execution.LastError,
		CreatedAt:      execution.CreatedAt.UTC(),
		UpdatedAt:      execution.UpdatedAt.UTC(),
	}
	return tx.Create(&row).Error
}

func (ts *trajectoryStore) metadataInTx(
	tx *gorm.DB,
	id string,
) (sdk.TrajectoryMetadata, error) {
	trajectory, inheritedCount, ownedCount, err := ts.loadStoredTrajectoryTx(tx, id)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	return trajectoryMetadataFn(trajectory, int(inheritedCount+ownedCount), int(ownedCount)), nil
}

func (ts *trajectoryStore) loadEntryCtx(
	ctx context.Context,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	return ts.loadEntryTx(ts.store.db.WithContext(ctx), id, entryID)
}

func (ts *trajectoryStore) loadEntryTx(
	tx *gorm.DB,
	id string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	var row TrajectoryEntry
	if err := tx.Where(
		"namespace = ? AND trajectory_id = ? AND entry_id = ?",
		ts.store.namespace, id, entryID,
	).First(&row).Error; err == nil {
		entry, parseErr := rowToTrajectoryEntry(row)
		return entry, true, parseErr
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return sdk.TrajectoryEntry{}, false, err
	}

	// Not found locally, check parent.
	var traj Trajectory
	if err := tx.Where("namespace = ? AND id = ?", ts.store.namespace, id).
		First(&traj).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return sdk.TrajectoryEntry{}, false, fmt.Errorf("%w: %s", sdk.ErrTrajectoryNotFound, id)
		}
		return sdk.TrajectoryEntry{}, false, err
	}
	if traj.ParentID == "" {
		return sdk.TrajectoryEntry{}, false, nil
	}
	return ts.entryOnBranch(tx, traj.ParentID, traj.ParentEntryID, entryID)
}

func (ts *trajectoryStore) entryOnBranch(
	tx *gorm.DB,
	id string,
	head string,
	entryID string,
) (sdk.TrajectoryEntry, bool, error) {
	return findEntryOnBranch(id, head, entryID, func(cursor string) (sdk.TrajectoryEntry, bool, error) {
		return ts.loadEntryTx(tx, id, cursor)
	})
}

func (ts *trajectoryStore) loadBranch(
	ctx context.Context,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	return ts.loadBranchInTx(ts.store.db.WithContext(ctx), id, head)
}

func (ts *trajectoryStore) loadBranchInTx(
	tx *gorm.DB,
	id string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	// Verify trajectory exists.
	if _, _, _, err := ts.loadStoredTrajectoryTx(tx, id); err != nil {
		return nil, err
	}
	return resolveBranch(id, head, func(cursor string) (sdk.TrajectoryEntry, bool, error) {
		return ts.loadEntryTx(tx, id, cursor)
	})
}

func rowToTrajectoryEntry(row TrajectoryEntry) (sdk.TrajectoryEntry, error) {
	entry := sdk.TrajectoryEntry{
		ID:             row.EntryID,
		TrajectoryID:   row.TrajectoryID,
		ParentID:       row.ParentID,
		Ordinal:        row.Ordinal,
		Depth:          row.Depth,
		Kind:           sdk.TrajectoryKind(row.Kind),
		Timestamp:      row.RecordedAt.UTC(),
		Generation:     row.Generation,
		PayloadVersion: row.PayloadVersion,
		Payload:        append(json.RawMessage(nil), row.Payload...),
		Fields: sdk.TrajectoryEntryFields{
			ExecutionID:   row.ExecutionID,
			OperationKey:  row.OperationKey,
			Turn:          row.Turn,
			CorrelationID: row.CorrelationID,
			Provider:      row.Provider,
			Model:         row.Model,
			ToolName:      row.ToolName,
			ToolCallID:    row.ToolCallID,
			FinishReason:  row.FinishReason,
			InputTokens:   row.InputTokens,
			OutputTokens:  row.OutputTokens,
			IsError:       row.IsError,
			CauseCode:     row.CauseCode,
			ActionKind:    sdk.ActionKind(row.ActionKind),
		},
	}
	if row.AttributesJSON != nil {
		if err := json.Unmarshal([]byte(*row.AttributesJSON), &entry.Attributes); err != nil {
			return sdk.TrajectoryEntry{}, fmt.Errorf("decode entry %q attributes: %w", row.EntryID, err)
		}
	}
	if row.AuditJSON != nil {
		if err := json.Unmarshal([]byte(*row.AuditJSON), &entry.Audit); err != nil {
			return sdk.TrajectoryEntry{}, fmt.Errorf("decode entry %q audit: %w", row.EntryID, err)
		}
		entry.Audit = sdk.CloneEventAudits(entry.Audit)
	}
	return entry, nil
}

func normalizePageRequest(request sdk.PageRequest) (sdk.PageRequest, error) {
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
