package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type gatewayInteractionRow struct {
	Namespace     string `gorm:"primaryKey;size:255"`
	SessionID     string `gorm:"primaryKey;size:255"`
	Sequence      uint64 `gorm:"primaryKey;autoIncrement:false"`
	InteractionID string `gorm:"size:255;not null"`
	State         string `gorm:"size:32;not null"`
	Revision      uint64 `gorm:"not null"`
	Payload       []byte `gorm:"not null"`
}

func (gatewayInteractionRow) TableName() string { return "ag_gateway_interactions" }

type gatewayInteractionCursorRow struct {
	Namespace    string `gorm:"primaryKey;size:255"`
	SessionID    string `gorm:"primaryKey;size:255"`
	NextSequence uint64 `gorm:"not null"`
}

func (gatewayInteractionCursorRow) TableName() string {
	return "ag_gateway_interaction_cursors"
}

type gormInteractionStore struct {
	*gatewayGORMCore
	notify map[string]chan struct{}
}

func NewGORMInteractionStore(
	ctx context.Context,
	rawURI string,
) (InteractionStore, error) {
	core, err := openGatewayGORMCore(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	core.writeMu.Lock()
	migrateErr := migrateGatewayInteractionSchema(core.db)
	core.writeMu.Unlock()
	if migrateErr != nil {
		_ = core.closeDB()
		return nil, migrateErr
	}
	return &gormInteractionStore{
		gatewayGORMCore: core,
		notify:          make(map[string]chan struct{}),
	}, nil
}

func migrateGatewayInteractionSchema(db *gorm.DB) error {
	if err := db.AutoMigrate(
		&gatewayInteractionRow{},
		&gatewayInteractionCursorRow{},
	); err != nil {
		return fmt.Errorf("auto-migrate gateway interaction schema: %w", err)
	}
	if err := db.Exec(
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_gateway_interaction_id_idx
			ON ag_gateway_interactions (namespace, session_id, interaction_id)`,
	).Error; err != nil {
		return fmt.Errorf("create gateway interaction ID index: %w", err)
	}
	return nil
}

func (store *gormInteractionStore) Create(
	ctx context.Context,
	value Interaction,
) (Interaction, error) {
	value, err := normalizeInteraction(value)
	if err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Interaction{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	stream, err := store.loadInteractionStreamLocked(ctx, value.SessionID)
	if err != nil {
		return Interaction{}, err
	}
	updated, created, changed, err := createInteraction(stream, value, store.clock())
	if err != nil || !changed {
		return created, err
	}
	row, err := gatewayInteractionRowFromInteraction(store.namespace, created)
	if err != nil {
		return Interaction{}, err
	}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		cursor := gatewayInteractionCursorRow{
			Namespace: store.namespace, SessionID: value.SessionID,
			NextSequence: updated.NextSequence,
		}
		var existing gatewayInteractionCursorRow
		result := tx.Where(
			"namespace = ? AND session_id = ?", store.namespace, value.SessionID,
		).First(&existing)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return tx.Create(&cursor).Error
		}
		if result.Error != nil {
			return result.Error
		}
		return tx.Model(&gatewayInteractionCursorRow{}).Where(
			"namespace = ? AND session_id = ?", store.namespace, value.SessionID,
		).Update("next_sequence", updated.NextSequence).Error
	})
	if err != nil {
		return Interaction{}, fmt.Errorf("create gateway interaction: %w", err)
	}
	store.signalInteractionLocked(value.SessionID)
	return created, nil
}

func (store *gormInteractionStore) Get(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Interaction{}, err
	}
	return store.getInteractionLocked(ctx, sessionID, id)
}

func (store *gormInteractionStore) getInteractionLocked(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	var row gatewayInteractionRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ? AND interaction_id = ?",
		store.namespace, sessionID, id,
	).First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Interaction{}, fmt.Errorf("%w: %s", ErrInteractionNotFound, id)
	}
	if result.Error != nil {
		return Interaction{}, result.Error
	}
	return interactionFromGatewayRow(row)
}

func (store *gormInteractionStore) List(
	ctx context.Context,
	sessionID string,
	query InteractionQuery,
) (InteractionPage, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return InteractionPage{}, err
	}
	query, err = normalizeInteractionQuery(query)
	if err != nil {
		return InteractionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return InteractionPage{}, err
	}
	dbQuery := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ? AND sequence > ?",
		store.namespace, sessionID, query.After,
	)
	if query.State != "" {
		dbQuery = dbQuery.Where("state = ?", string(query.State))
	}
	var rows []gatewayInteractionRow
	if err := dbQuery.Order("sequence ASC").Limit(query.Limit).Find(&rows).Error; err != nil {
		return InteractionPage{}, err
	}
	page := InteractionPage{Items: make([]Interaction, 0, len(rows))}
	for _, row := range rows {
		item, err := interactionFromGatewayRow(row)
		if err != nil {
			return InteractionPage{}, err
		}
		page.Items = append(page.Items, item)
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page, nil
}

func (store *gormInteractionStore) Wait(
	ctx context.Context,
	sessionID string,
	id string,
) (Interaction, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return Interaction{}, err
	}
	for {
		store.mu.Lock()
		if err := store.checkLocked(ctx); err != nil {
			store.mu.Unlock()
			return Interaction{}, err
		}
		item, err := store.getInteractionLocked(ctx, sessionID, id)
		if err != nil || item.State.Terminal() {
			store.mu.Unlock()
			return item, err
		}
		notify := store.interactionNotifyLocked(sessionID)
		store.mu.Unlock()
		select {
		case <-ctx.Done():
			return Interaction{}, ctx.Err()
		case <-notify:
		}
	}
}

func (store *gormInteractionStore) Resolve(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
	answer InteractionAnswer,
) (Interaction, error) {
	return store.mutateInteraction(ctx, sessionID, func(stream interactionStream) (Interaction, bool, error) {
		_, item, changed, err := resolveInteraction(
			stream, id, expectedRevision, answer, store.clock(),
		)
		return item, changed, err
	})
}

func (store *gormInteractionStore) Cancel(
	ctx context.Context,
	sessionID string,
	id string,
	expectedRevision uint64,
) (Interaction, error) {
	return store.mutateInteraction(ctx, sessionID, func(stream interactionStream) (Interaction, bool, error) {
		_, item, changed, err := cancelInteraction(
			stream, id, expectedRevision, store.clock(),
		)
		return item, changed, err
	})
}

func (store *gormInteractionStore) mutateInteraction(
	ctx context.Context,
	sessionID string,
	mutation func(interactionStream) (Interaction, bool, error),
) (Interaction, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return Interaction{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Interaction{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	stream, err := store.loadInteractionStreamLocked(ctx, sessionID)
	if err != nil {
		return Interaction{}, err
	}
	item, changed, err := mutation(stream)
	if err != nil || !changed {
		return item, err
	}
	row, err := gatewayInteractionRowFromInteraction(store.namespace, item)
	if err != nil {
		return Interaction{}, err
	}
	result := store.db.WithContext(ctx).Model(&gatewayInteractionRow{}).Where(
		"namespace = ? AND session_id = ? AND sequence = ? AND revision = ?",
		store.namespace, item.SessionID, item.Sequence, item.Revision-1,
	).Updates(map[string]any{
		"state": row.State, "revision": row.Revision, "payload": row.Payload,
	})
	if result.Error != nil {
		return Interaction{}, result.Error
	}
	if result.RowsAffected != 1 {
		return Interaction{}, fmt.Errorf(
			"%w: interaction %s revision changed", ErrInteractionConflict, item.ID,
		)
	}
	store.signalInteractionLocked(sessionID)
	return item, nil
}

func (store *gormInteractionStore) loadInteractionStreamLocked(
	ctx context.Context,
	sessionID string,
) (interactionStream, error) {
	var rows []gatewayInteractionRow
	if err := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ?", store.namespace, sessionID,
	).Order("sequence ASC").Find(&rows).Error; err != nil {
		return interactionStream{}, err
	}
	stream := interactionStream{NextSequence: 1, Items: make([]Interaction, 0, len(rows))}
	for _, row := range rows {
		item, err := interactionFromGatewayRow(row)
		if err != nil {
			return interactionStream{}, err
		}
		stream.Items = append(stream.Items, item)
	}
	var cursor gatewayInteractionCursorRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ?", store.namespace, sessionID,
	).First(&cursor)
	if result.Error == nil {
		stream.NextSequence = cursor.NextSequence
	} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return interactionStream{}, result.Error
	} else if len(stream.Items) > 0 {
		stream.NextSequence = stream.Items[len(stream.Items)-1].Sequence + 1
	}
	return stream, nil
}

func (store *gormInteractionStore) interactionNotifyLocked(sessionID string) chan struct{} {
	if store.notify[sessionID] == nil {
		store.notify[sessionID] = make(chan struct{})
	}
	return store.notify[sessionID]
}

func (store *gormInteractionStore) signalInteractionLocked(sessionID string) {
	close(store.interactionNotifyLocked(sessionID))
	store.notify[sessionID] = make(chan struct{})
}

func (store *gormInteractionStore) Close(context.Context) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	for sessionID, notify := range store.notify {
		close(notify)
		delete(store.notify, sessionID)
	}
	return store.closeDB()
}

func gatewayInteractionRowFromInteraction(
	namespace string,
	item Interaction,
) (gatewayInteractionRow, error) {
	payload, err := json.Marshal(item)
	if err != nil {
		return gatewayInteractionRow{}, fmt.Errorf("encode gateway interaction: %w", err)
	}
	return gatewayInteractionRow{
		Namespace: namespace, SessionID: item.SessionID, Sequence: item.Sequence,
		InteractionID: item.ID, State: string(item.State), Revision: item.Revision,
		Payload: payload,
	}, nil
}

func interactionFromGatewayRow(row gatewayInteractionRow) (Interaction, error) {
	var item Interaction
	if err := json.Unmarshal(row.Payload, &item); err != nil {
		return Interaction{}, fmt.Errorf(
			"decode gateway interaction %s: %w", row.InteractionID, err,
		)
	}
	if item.ID != row.InteractionID || item.SessionID != row.SessionID ||
		item.Sequence != row.Sequence || string(item.State) != row.State ||
		item.Revision != row.Revision {
		return Interaction{}, fmt.Errorf(
			"gateway interaction %s has inconsistent indexed metadata",
			row.InteractionID,
		)
	}
	return cloneInteraction(item), nil
}

var _ InteractionStore = (*gormInteractionStore)(nil)
