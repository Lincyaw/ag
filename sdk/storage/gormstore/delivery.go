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

type deliveryStore struct {
	store *Store
	queue string
}

func (ds *deliveryStore) Enqueue(ctx context.Context, deliveries ...sdk.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	prepared, err := prepareNewDeliveries(deliveries, time.Now().UTC())
	if err != nil {
		return err
	}

	ds.store.writeMu.Lock()
	defer ds.store.writeMu.Unlock()

	return ds.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := ds.store.lockMutationResource(
			tx,
			"delivery:enqueue:"+ds.store.namespace+":"+ds.queue,
		); err != nil {
			return err
		}
		return ds.enqueueInTx(tx, prepared)
	})
}

func (ds *deliveryStore) enqueueInTx(tx *gorm.DB, prepared []sdk.Delivery) error {
	newDeliveries := make([]sdk.Delivery, 0, len(prepared))
	staged := make(map[string]struct{}, len(prepared))
	for _, delivery := range prepared {
		if _, exists := staged[delivery.ID]; exists {
			continue
		}
		existing, err := ds.loadTx(tx, delivery.ID)
		if err == nil {
			if !sameDeliveryIdentity(existing, delivery) {
				return fmt.Errorf(
					"delivery %q already exists with different identity",
					delivery.ID,
				)
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		staged[delivery.ID] = struct{}{}
		newDeliveries = append(newDeliveries, delivery)
	}
	if len(newDeliveries) == 0 {
		return nil
	}

	var nextSequence uint64
	var maxSeq *uint64
	if err := tx.Model(&Delivery{}).
		Where("namespace = ? AND queue = ?", ds.store.namespace, ds.queue).
		Select("MAX(sequence)").
		Scan(&maxSeq).Error; err != nil {
		return err
	}
	if maxSeq != nil {
		nextSequence = *maxSeq + 1
	} else {
		nextSequence = 1
	}

	for _, delivery := range newDeliveries {
		delivery.Sequence = nextSequence
		nextSequence++
		row := Delivery{
			Namespace:        ds.store.namespace,
			Queue:            ds.queue,
			ID:               delivery.ID,
			Sequence:         delivery.Sequence,
			Plugin:           delivery.Plugin,
			PluginVersion:    delivery.PluginVersion,
			Subscription:     delivery.Subscription,
			ResourceRevision: delivery.ResourceRevision,
			PartitionKey:     delivery.Partition,
			EventID:          delivery.Event.ID,
			EventName:        delivery.Event.Name,
			EventSessionID:   delivery.Event.SessionID,
			EventGeneration:  delivery.Event.Generation,
			EventPayload:     []byte(delivery.Event.Payload),
			State:            string(delivery.State),
			Attempt:          delivery.Attempt,
			AvailableAt:      nullableTime(delivery.AvailableAt),
			LeaseToken:       "",
			LeaseExpiresAt:   nil,
			LastError:        "",
			CreatedAt:        delivery.CreatedAt.UTC(),
			UpdatedAt:        delivery.UpdatedAt.UTC(),
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

func (ds *deliveryStore) Lease(
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

	ds.store.writeMu.Lock()
	defer ds.store.writeMu.Unlock()

	var delivery sdk.Delivery
	err := ds.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Head-of-line per partition: find the minimum sequence per partition
		// among non-terminal deliveries, then pick the first available one.
		query := `WITH heads AS (
				SELECT partition_key, MIN(sequence) AS sequence
				FROM ag_deliveries
				WHERE namespace = ?
				  AND queue = ?
				  AND state NOT IN (?, ?)
				GROUP BY partition_key
			)
			SELECT delivery.id
			FROM ag_deliveries delivery
			JOIN heads
			  ON heads.partition_key = delivery.partition_key
			 AND heads.sequence = delivery.sequence
			WHERE delivery.namespace = ?
			  AND delivery.queue = ?
			  AND (
			    (delivery.state = ? AND (delivery.available_at IS NULL OR delivery.available_at <= ?))
			    OR (delivery.state = ? AND delivery.lease_expires_at <= ?)
			  )
			ORDER BY delivery.sequence
			LIMIT 1`
		if ds.store.dialect == "postgres" {
			query += " FOR UPDATE OF delivery SKIP LOCKED"
		}
		var candidate struct {
			ID string
		}
		result := tx.Raw(
			query,
			ds.store.namespace,
			ds.queue,
			string(sdk.DeliveryDelivered),
			string(sdk.DeliveryDeadLetter),
			ds.store.namespace,
			ds.queue,
			string(sdk.DeliveryPending),
			now,
			string(sdk.DeliveryLeased),
			now,
		).Scan(&candidate)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return sdk.ErrNoDelivery
		}
		existing, err := ds.loadTx(tx, candidate.ID)
		if err != nil {
			return err
		}
		existing, err = leaseDelivery(existing, sdk.NewID(), now, duration)
		if err != nil {
			return err
		}
		leaseExpires := existing.LeaseExpiresAt.UTC()
		if err := tx.Model(&Delivery{}).
			Where("namespace = ? AND queue = ? AND id = ?", ds.store.namespace, ds.queue, existing.ID).
			Updates(map[string]any{
				"state":            string(existing.State),
				"attempt":          existing.Attempt,
				"lease_token":      existing.LeaseToken,
				"lease_expires_at": &leaseExpires,
				"updated_at":       existing.UpdatedAt.UTC(),
			}).Error; err != nil {
			return err
		}
		delivery = existing
		return nil
	})
	if err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (ds *deliveryStore) Ack(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
) error {
	return ds.transition(ctx, id, token, now, sdk.DeliveryDelivered, time.Time{}, "")
}

func (ds *deliveryStore) Retry(
	ctx context.Context,
	id string,
	token string,
	availableAt time.Time,
	lastError string,
) error {
	return ds.transition(ctx, id, token, time.Now().UTC(), sdk.DeliveryPending, availableAt.UTC(), lastError)
}

func (ds *deliveryStore) DeadLetter(
	ctx context.Context,
	id string,
	token string,
	now time.Time,
	lastError string,
) error {
	return ds.transition(ctx, id, token, now, sdk.DeliveryDeadLetter, time.Time{}, lastError)
}

func (ds *deliveryStore) transition(
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
	ds.store.writeMu.Lock()
	defer ds.store.writeMu.Unlock()

	return ds.store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		delivery, err := ds.loadTx(ds.store.forUpdate(tx), id)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
		}
		if err != nil {
			return err
		}
		delivery, err = finishDeliveryLease(delivery, token, now, state, availableAt, lastError)
		if err != nil {
			return err
		}
		result := tx.Model(&Delivery{}).
			Where("namespace = ? AND queue = ? AND id = ?", ds.store.namespace, ds.queue, delivery.ID).
			Updates(map[string]any{
				"state":            string(delivery.State),
				"available_at":     nullableTime(delivery.AvailableAt),
				"lease_token":      "",
				"lease_expires_at": nil,
				"last_error":       delivery.LastError,
				"updated_at":       delivery.UpdatedAt.UTC(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("%w: %s", sdk.ErrDeliveryLease, id)
		}
		return nil
	})
}

func (ds *deliveryStore) List(ctx context.Context) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rows []Delivery
	if err := ds.store.db.WithContext(ctx).
		Where("namespace = ? AND queue = ?", ds.store.namespace, ds.queue).
		Order("sequence").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return ds.scanRows(rows)
}

func (ds *deliveryStore) ListNonTerminal(ctx context.Context) ([]sdk.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var rows []Delivery
	if err := ds.store.db.WithContext(ctx).
		Where("namespace = ? AND queue = ? AND state NOT IN ?",
			ds.store.namespace, ds.queue,
			[]string{string(sdk.DeliveryDelivered), string(sdk.DeliveryDeadLetter)}).
		Order("sequence").
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return ds.scanRows(rows)
}

func (ds *deliveryStore) ListPage(
	ctx context.Context,
	request sdk.PageRequest,
) (sdk.DeliveryPage, error) {
	if err := ctx.Err(); err != nil {
		return sdk.DeliveryPage{}, err
	}
	request, err := normalizePageRequest(request)
	if err != nil {
		return sdk.DeliveryPage{}, err
	}
	db := ds.store.db.WithContext(ctx).
		Model(&Delivery{}).
		Where("namespace = ? AND queue = ?", ds.store.namespace, ds.queue)
	if request.After != "" {
		var cursor Delivery
		if err := ds.store.db.WithContext(ctx).
			Where("namespace = ? AND queue = ? AND id = ?", ds.store.namespace, ds.queue, request.After).
			First(&cursor).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sdk.DeliveryPage{}, fmt.Errorf("pagination cursor %q was not found", request.After)
			}
			return sdk.DeliveryPage{}, err
		}
		db = db.Where("sequence > ?", cursor.Sequence)
	}
	var rows []Delivery
	if err := db.Order("sequence").Limit(request.Limit + 1).Find(&rows).Error; err != nil {
		return sdk.DeliveryPage{}, err
	}
	items, err := ds.scanRows(rows)
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

func (ds *deliveryStore) PurgeTerminal(
	ctx context.Context,
	before time.Time,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if before.IsZero() {
		return 0, errors.New("delivery purge cutoff is required")
	}
	ds.store.writeMu.Lock()
	defer ds.store.writeMu.Unlock()
	result := ds.store.db.WithContext(ctx).
		Where(
			"namespace = ? AND queue = ? AND state IN ? AND updated_at < ?",
			ds.store.namespace, ds.queue,
			[]string{string(sdk.DeliveryDelivered), string(sdk.DeliveryDeadLetter)},
			before.UTC(),
		).
		Delete(&Delivery{})
	return int(result.RowsAffected), result.Error
}

// --- internal helpers ---

func (ds *deliveryStore) loadTx(tx *gorm.DB, id string) (sdk.Delivery, error) {
	var row Delivery
	if err := tx.Where(
		"namespace = ? AND queue = ? AND id = ?",
		ds.store.namespace, ds.queue, id,
	).First(&row).Error; err != nil {
		return sdk.Delivery{}, err
	}
	return rowToDelivery(row)
}

func rowToDelivery(row Delivery) (sdk.Delivery, error) {
	delivery := sdk.Delivery{
		ID:               row.ID,
		Sequence:         row.Sequence,
		Plugin:           row.Plugin,
		PluginVersion:    row.PluginVersion,
		Subscription:     row.Subscription,
		ResourceRevision: row.ResourceRevision,
		Partition:        row.PartitionKey,
		Event: sdk.Event{
			ID:         row.EventID,
			Name:       row.EventName,
			SessionID:  row.EventSessionID,
			Generation: row.EventGeneration,
			Payload:    append(json.RawMessage(nil), row.EventPayload...),
		},
		State:      sdk.DeliveryState(row.State),
		Attempt:    row.Attempt,
		LeaseToken: row.LeaseToken,
		LastError:  row.LastError,
		CreatedAt:  row.CreatedAt.UTC(),
		UpdatedAt:  row.UpdatedAt.UTC(),
	}
	if row.AvailableAt != nil {
		delivery.AvailableAt = row.AvailableAt.UTC()
	}
	if row.LeaseExpiresAt != nil {
		delivery.LeaseExpiresAt = row.LeaseExpiresAt.UTC()
	}
	if err := validateLoadedDelivery(delivery); err != nil {
		return sdk.Delivery{}, err
	}
	return delivery, nil
}

func (ds *deliveryStore) scanRows(rows []Delivery) ([]sdk.Delivery, error) {
	result := make([]sdk.Delivery, 0, len(rows))
	for _, row := range rows {
		delivery, err := rowToDelivery(row)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	return result, nil
}
