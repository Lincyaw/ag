package gormstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
	contextinjectionmodel "github.com/lincyaw/ag/sdk/storage/internal/contextinjectionmodel"
	"gorm.io/gorm"
)

type contextInjectionStore struct {
	store *Store
}

func (cs *contextInjectionStore) Enqueue(
	ctx context.Context,
	injections ...sdk.ContextInjection,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareContextInjections(injections, time.Now().UTC())
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return nil
	}

	cs.store.writeMu.Lock()
	defer cs.store.writeMu.Unlock()

	return cs.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := cs.store.lockMutationResource(
			tx,
			"context:enqueue:"+cs.store.namespace,
		); err != nil {
			return err
		}
		var maxSeq *uint64
		if err := tx.Model(&ContextInjection{}).
			Where("namespace = ?", cs.store.namespace).
			Select("MAX(sequence)").
			Scan(&maxSeq).Error; err != nil {
			return err
		}
		var nextSequence uint64
		if maxSeq != nil {
			nextSequence = *maxSeq
		}

		for _, injection := range prepared {
			existing, err := cs.loadTx(tx, injection.ID)
			if err == nil {
				if !sameContextInjectionIdentity(existing.Injection, injection) {
					return fmt.Errorf(
						"context injection %q already exists with different identity",
						injection.ID,
					)
				}
				continue
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			nextSequence++
			if err := cs.insertTx(tx, nextSequence, injection); err != nil {
				return err
			}
		}
		return nil
	})
}

func (cs *contextInjectionStore) List(
	ctx context.Context,
	query sdk.ContextInjectionQuery,
) ([]sdk.ContextInjection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateContextQuery(query); err != nil {
		return nil, err
	}

	db := cs.store.db.WithContext(ctx).
		Model(&ContextInjection{}).
		Where("namespace = ?", cs.store.namespace)

	if query.TargetSessionID != "" {
		db = db.Where("(target_session_id = '' OR target_session_id = ?)", query.TargetSessionID)
	}
	if query.TargetExecutionID != "" {
		db = db.Where("(target_execution_id = '' OR target_execution_id = ?)", query.TargetExecutionID)
	}
	db = db.Order("sequence")
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}

	var rows []ContextInjection
	if err := db.Find(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]sdk.ContextInjection, 0, len(rows))
	for _, row := range rows {
		record, err := rowToContextRecord(row)
		if err != nil {
			return nil, err
		}
		result = append(result, sdk.CloneContextInjection(record.Injection))
	}
	return result, nil
}

// ConsumeContextInjections implements sdk.ContextInjectionConsumer.
func (cs *contextInjectionStore) ConsumeContextInjections(
	ctx context.Context,
	ids ...string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateContextInjectionIDs(ids); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}

	cs.store.writeMu.Lock()
	defer cs.store.writeMu.Unlock()

	return cs.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, id := range ids {
			if err := tx.Where("namespace = ? AND id = ?", cs.store.namespace, id).
				Delete(&ContextInjection{}).Error; err != nil {
				return fmt.Errorf("consume context injection %q: %w", id, err)
			}
		}
		return nil
	})
}

// --- internal helpers ---

func (cs *contextInjectionStore) loadTx(
	tx *gorm.DB,
	id string,
) (contextinjectionmodel.Record, error) {
	var row ContextInjection
	if err := tx.Where("namespace = ? AND id = ?", cs.store.namespace, id).
		First(&row).Error; err != nil {
		return contextinjectionmodel.Record{}, err
	}
	return rowToContextRecord(row)
}

func (cs *contextInjectionStore) insertTx(
	tx *gorm.DB,
	sequence uint64,
	injection sdk.ContextInjection,
) error {
	messages, err := json.Marshal(injection.Messages)
	if err != nil {
		return fmt.Errorf("encode context injection %q messages: %w", injection.ID, err)
	}
	attrsJSON, err := attributesJSON(injection.Attributes)
	if err != nil {
		return fmt.Errorf("encode context injection %q attributes: %w", injection.ID, err)
	}
	row := ContextInjection{
		Namespace:         cs.store.namespace,
		ID:                injection.ID,
		Sequence:          sequence,
		Priority:          string(injection.Priority),
		Mode:              string(injection.Mode),
		Origin:            injection.Origin,
		TargetSessionID:   injection.TargetSessionID,
		TargetExecutionID: injection.TargetExecutionID,
		IsMeta:            injection.IsMeta,
		Messages:          messages,
		AttributesJSON:    attrsJSON,
		CreatedAt:         injection.CreatedAt.UTC(),
	}
	return tx.Create(&row).Error
}

func rowToContextRecord(row ContextInjection) (contextinjectionmodel.Record, error) {
	record := contextinjectionmodel.Record{
		Sequence: row.Sequence,
	}
	record.Injection.ID = row.ID
	record.Injection.Priority = sdk.ContextInjectionPriority(row.Priority)
	record.Injection.Mode = sdk.ContextInjectionMode(row.Mode)
	record.Injection.Origin = row.Origin
	record.Injection.TargetSessionID = row.TargetSessionID
	record.Injection.TargetExecutionID = row.TargetExecutionID
	record.Injection.IsMeta = row.IsMeta
	record.Injection.CreatedAt = row.CreatedAt.UTC()

	if err := json.Unmarshal(row.Messages, &record.Injection.Messages); err != nil {
		return contextinjectionmodel.Record{}, fmt.Errorf(
			"decode context injection %q messages: %w", row.ID, err,
		)
	}
	if row.AttributesJSON != nil {
		if err := json.Unmarshal([]byte(*row.AttributesJSON), &record.Injection.Attributes); err != nil {
			return contextinjectionmodel.Record{}, fmt.Errorf(
				"decode context injection %q attributes: %w", row.ID, err,
			)
		}
	}
	if err := validateLoadedContextRecord(record); err != nil {
		return contextinjectionmodel.Record{}, err
	}
	return record, nil
}
