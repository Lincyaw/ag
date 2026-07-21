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

type operationStore struct {
	store *Store
}

func (os *operationStore) Submit(
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
		return sdk.OperationRecord{}, false, fmt.Errorf("encode operation invocation: %w", err)
	}

	os.store.writeMu.Lock()
	defer os.store.writeMu.Unlock()

	var result sdk.OperationRecord
	var created bool
	txErr := os.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := os.store.lockMutationResource(
			tx,
			"operation:submit:"+os.store.namespace+":"+
				string(record.Kind)+":"+record.Resource+":"+
				record.ResourceRevision+":"+record.Operation.IdempotencyKey,
		); err != nil {
			return err
		}
		existing, err := os.loadByIdempotencyTx(tx, record)
		if err == nil {
			if !sameOperationSubmission(existing, record) {
				return fmt.Errorf(
					"operation idempotency key %q was reused with a different submission",
					record.Operation.IdempotencyKey,
				)
			}
			result = cloneOperationRecord(existing)
			created = false
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if _, err := os.loadTx(tx, record.Operation.ID); err == nil {
			return fmt.Errorf("operation ID %q already exists", record.Operation.ID)
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := os.insertTx(tx, record, string(invocationJSON)); err != nil {
			return err
		}
		result = cloneOperationRecord(record)
		created = true
		return nil
	})
	return result, created, txErr
}

func (os *operationStore) Get(
	ctx context.Context,
	id string,
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	record, err := os.loadTx(os.store.db.WithContext(ctx), id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return sdk.OperationRecord{}, fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
	}
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	return cloneOperationRecord(record), nil
}

func (os *operationStore) Cancel(
	ctx context.Context,
	id string,
	expectedRevision uint64,
) (sdk.OperationRecord, error) {
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return cancelOperation(record, expectedRevision, time.Now().UTC())
	})
}

func (os *operationStore) Fail(
	ctx context.Context,
	id string,
	expectedRevision uint64,
	operationError string,
) (sdk.OperationRecord, error) {
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return failOperation(record, expectedRevision, operationError, time.Now().UTC())
	})
}

func (os *operationStore) Claim(
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
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return claimOperation(record, owner, now, ttl)
	})
}

func (os *operationStore) Renew(
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
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return renewOperation(record, token, now, ttl)
	})
}

func (os *operationStore) Complete(
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
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return completeOperation(record, token, state, output, operationError, time.Now().UTC())
	})
}

func (os *operationStore) Release(
	ctx context.Context,
	id string,
	token string,
) (sdk.OperationRecord, error) {
	return os.mutate(ctx, id, func(record sdk.OperationRecord) (sdk.OperationRecord, error) {
		return releaseOperation(record, token, time.Now().UTC())
	})
}

func (os *operationStore) mutate(
	ctx context.Context,
	id string,
	mutation func(sdk.OperationRecord) (sdk.OperationRecord, error),
) (sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationRecord{}, err
	}
	os.store.writeMu.Lock()
	defer os.store.writeMu.Unlock()

	var result sdk.OperationRecord
	err := os.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		record, err := os.loadTx(os.store.forUpdate(tx), id)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, id)
		}
		if err != nil {
			return err
		}
		record, err = mutation(record)
		if err != nil {
			return err
		}
		if err := os.replaceTx(tx, record); err != nil {
			return err
		}
		result = cloneOperationRecord(record)
		return nil
	})
	return result, err
}

func (os *operationStore) List(ctx context.Context) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rows []Operation
	if err := os.store.db.WithContext(ctx).
		Where("namespace = ?", os.store.namespace).
		Order("submitted_at, id").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return os.scanRows(rows)
}

func (os *operationStore) ListByInvocationRoot(
	ctx context.Context,
	rootID string,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := sdk.ValidateResourceName("invocation root", rootID); err != nil {
		return nil, err
	}
	var rows []Operation
	if err := os.store.db.WithContext(ctx).
		Where("namespace = ? AND invocation_root_id = ?", os.store.namespace, rootID).
		Order("submitted_at, id").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return os.scanRows(rows)
}

func (os *operationStore) ListNonTerminal(ctx context.Context) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rows []Operation
	if err := os.store.db.WithContext(ctx).
		Where("namespace = ? AND state NOT IN ?", os.store.namespace,
			[]string{string(sdk.OperationSucceeded), string(sdk.OperationFailed), string(sdk.OperationCancelled)}).
		Order("submitted_at, id").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return os.scanRows(rows)
}

func (os *operationStore) ListRecoverable(
	ctx context.Context,
	now time.Time,
) ([]sdk.OperationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now = normalizeOperationMutationTime(now)
	var rows []Operation
	if err := os.store.db.WithContext(ctx).
		Where(
			"namespace = ? AND state NOT IN ? AND (lease_expires_at IS NULL OR lease_expires_at <= ?)",
			os.store.namespace,
			[]string{string(sdk.OperationSucceeded), string(sdk.OperationFailed), string(sdk.OperationCancelled)},
			now,
		).
		Order("submitted_at, id").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return os.scanRows(rows)
}

func (os *operationStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.OperationPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.OperationPage{}, err
	}
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.OperationPage{}, err
	}
	db := os.store.db.WithContext(ctx).
		Model(&Operation{}).
		Where("namespace = ?", os.store.namespace)
	if request.After != "" {
		var cursor Operation
		if err := os.store.db.WithContext(ctx).
			Where("namespace = ? AND id = ?", os.store.namespace, request.After).
			First(&cursor).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sdk.OperationPage{}, fmt.Errorf("pagination cursor %q was not found", request.After)
			}
			return sdk.OperationPage{}, err
		}
		db = db.Where(
			"(submitted_at > ? OR (submitted_at = ? AND id > ?))",
			cursor.SubmittedAt.UTC(), cursor.SubmittedAt.UTC(), request.After,
		)
	}
	var rows []Operation
	if err := db.Order("submitted_at, id").Limit(request.Limit + 1).Find(&rows).Error; err != nil {
		return sdk.OperationPage{}, err
	}
	items, err := os.scanRows(rows)
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

func (os *operationStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("operation purge cutoff is required")
	}
	os.store.writeMu.Lock()
	defer os.store.writeMu.Unlock()
	result := os.store.db.WithContext(ctx).
		Where(
			"namespace = ? AND state IN ? AND updated_at < ?",
			os.store.namespace,
			[]string{string(sdk.OperationSucceeded), string(sdk.OperationFailed), string(sdk.OperationCancelled)},
			before.UTC(),
		).
		Delete(&Operation{})
	return int(result.RowsAffected), result.Error
}

// --- internal helpers ---

func (os *operationStore) loadTx(tx *gorm.DB, id string) (sdk.OperationRecord, error) {
	var row Operation
	if err := tx.Where("namespace = ? AND id = ?", os.store.namespace, id).
		First(&row).Error; err != nil {
		return sdk.OperationRecord{}, err
	}
	return os.rowToRecord(row)
}

func (os *operationStore) loadByIdempotencyTx(
	tx *gorm.DB,
	record sdk.OperationRecord,
) (sdk.OperationRecord, error) {
	var row Operation
	if err := tx.Where(
		"namespace = ? AND kind = ? AND resource = ? AND resource_revision = ? AND idempotency_key = ?",
		os.store.namespace,
		string(record.Kind),
		record.Resource,
		record.ResourceRevision,
		record.Operation.IdempotencyKey,
	).First(&row).Error; err != nil {
		return sdk.OperationRecord{}, err
	}
	return os.rowToRecord(row)
}

func (os *operationStore) insertTx(
	tx *gorm.DB,
	record sdk.OperationRecord,
	invocationJSON string,
) error {
	var output []byte
	if len(record.Operation.Output) != 0 {
		output = []byte(record.Operation.Output)
	}
	leaseOwner := ""
	leaseToken := ""
	var leaseExpiresAt *time.Time
	if record.Execution != nil {
		leaseOwner = record.Execution.Owner
		leaseToken = record.Execution.Token
		leaseExpiresAt = nullableTime(record.Execution.ExpiresAt)
	}
	row := Operation{
		Namespace:          os.store.namespace,
		ID:                 record.Operation.ID,
		IdempotencyKey:     record.Operation.IdempotencyKey,
		Kind:               string(record.Kind),
		Resource:           record.Resource,
		ResourceRevision:   record.ResourceRevision,
		State:              string(record.Operation.State),
		Revision:           record.Operation.Revision,
		Input:              []byte(record.Input),
		InvocationJSON:     invocationJSON,
		InvocationRootID:   record.Invocation.RootID,
		InvocationParentID: record.Invocation.ParentID,
		InvocationGroupID:  record.Invocation.GroupID,
		Output:             output,
		OperationError:     record.Operation.Error,
		SubmittedAt:        record.Operation.SubmittedAt.UTC(),
		UpdatedAt:          record.Operation.UpdatedAt.UTC(),
		LeaseOwner:         leaseOwner,
		LeaseToken:         leaseToken,
		LeaseExpiresAt:     leaseExpiresAt,
	}
	return tx.Create(&row).Error
}

func (os *operationStore) replaceTx(tx *gorm.DB, record sdk.OperationRecord) error {
	var output []byte
	if len(record.Operation.Output) != 0 {
		output = []byte(record.Operation.Output)
	}
	leaseOwner := ""
	leaseToken := ""
	var leaseExpiresAt *time.Time
	if record.Execution != nil {
		leaseOwner = record.Execution.Owner
		leaseToken = record.Execution.Token
		leaseExpiresAt = nullableTime(record.Execution.ExpiresAt)
	}
	result := tx.Model(&Operation{}).
		Where("namespace = ? AND id = ?", os.store.namespace, record.Operation.ID).
		Updates(map[string]any{
			"state":            string(record.Operation.State),
			"revision":         record.Operation.Revision,
			"output":           output,
			"operation_error":  record.Operation.Error,
			"updated_at":       record.Operation.UpdatedAt.UTC(),
			"lease_owner":      leaseOwner,
			"lease_token":      leaseToken,
			"lease_expires_at": leaseExpiresAt,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: %s", sdk.ErrOperationNotFound, record.Operation.ID)
	}
	return nil
}

func (os *operationStore) rowToRecord(row Operation) (sdk.OperationRecord, error) {
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             row.ID,
			IdempotencyKey: row.IdempotencyKey,
			State:          sdk.OperationState(row.State),
			Revision:       row.Revision,
			Output:         append(json.RawMessage(nil), row.Output...),
			Error:          row.OperationError,
			SubmittedAt:    row.SubmittedAt.UTC(),
			UpdatedAt:      row.UpdatedAt.UTC(),
		},
		Kind:             sdk.OperationKind(row.Kind),
		Resource:         row.Resource,
		ResourceRevision: row.ResourceRevision,
		Input:            append(json.RawMessage(nil), row.Input...),
	}
	if row.InvocationJSON != "" {
		if err := json.Unmarshal([]byte(row.InvocationJSON), &record.Invocation); err != nil {
			return sdk.OperationRecord{}, fmt.Errorf("decode operation invocation: %w", err)
		}
	}
	if row.LeaseOwner != "" || row.LeaseToken != "" || row.LeaseExpiresAt != nil {
		if row.LeaseExpiresAt == nil {
			return sdk.OperationRecord{}, errors.New("operation has an incomplete lease")
		}
		record.Execution = &sdk.OperationLease{
			Owner:     row.LeaseOwner,
			Token:     row.LeaseToken,
			ExpiresAt: row.LeaseExpiresAt.UTC(),
		}
	}
	if err := validateLoadedOperationRecord(record); err != nil {
		return sdk.OperationRecord{}, err
	}
	return record, nil
}

func (os *operationStore) scanRows(rows []Operation) ([]sdk.OperationRecord, error) {
	result := make([]sdk.OperationRecord, 0, len(rows))
	for _, row := range rows {
		record, err := os.rowToRecord(row)
		if err != nil {
			return nil, err
		}
		result = append(result, cloneOperationRecord(record))
	}
	return result, nil
}
