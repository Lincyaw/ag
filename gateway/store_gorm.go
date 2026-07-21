package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
	"gorm.io/gorm"
)

type gatewayGORMCore struct {
	mu        sync.Mutex
	writeMu   *sync.Mutex
	db        *gorm.DB
	closeDB   func() error
	namespace string
	clock     func() time.Time
	closed    bool
}

func openGatewayGORMCore(
	ctx context.Context,
	rawURI string,
) (*gatewayGORMCore, error) {
	db, namespace, closeDB, writeMu, err := openGatewayGORMDB(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	return &gatewayGORMCore{
		db: db, closeDB: closeDB, writeMu: writeMu, namespace: namespace,
		clock: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (core *gatewayGORMCore) checkLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if core.closed {
		return ErrStoreClosed
	}
	return nil
}

func (core *gatewayGORMCore) close() error {
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return nil
	}
	core.closed = true
	return core.closeDB()
}

type gatewaySessionRow struct {
	Namespace string    `gorm:"primaryKey;size:255"`
	ID        string    `gorm:"primaryKey;size:255"`
	UserID    string    `gorm:"size:256;not null;index:ag_gateway_session_user_idx,priority:2"`
	Revision  uint64    `gorm:"not null"`
	Payload   []byte    `gorm:"not null"`
	CreatedAt time.Time `gorm:"not null"`
	UpdatedAt time.Time `gorm:"not null"`
}

func (gatewaySessionRow) TableName() string { return "ag_gateway_sessions" }

type gormSessionStore struct{ *gatewayGORMCore }

func NewGORMSessionStore(
	ctx context.Context,
	rawURI string,
) (SessionStore, error) {
	core, err := openGatewayGORMCore(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	core.writeMu.Lock()
	migrateErr := migrateGatewaySessionSchema(core.db)
	core.writeMu.Unlock()
	if migrateErr != nil {
		_ = core.closeDB()
		return nil, migrateErr
	}
	return &gormSessionStore{gatewayGORMCore: core}, nil
}

func migrateGatewaySessionSchema(db *gorm.DB) error {
	if err := db.AutoMigrate(&gatewaySessionRow{}); err != nil {
		return fmt.Errorf("auto-migrate gateway session schema: %w", err)
	}
	return nil
}

func (store *gormSessionStore) Create(
	ctx context.Context,
	session Session,
) (Session, error) {
	session, err := normalizeSession(session)
	if err != nil {
		return Session{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Session{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	now := store.clock().UTC()
	session.Revision = 1
	session.CreatedAt = now
	session.UpdatedAt = now
	row, err := gatewaySessionRowFromSession(store.namespace, session)
	if err != nil {
		return Session{}, err
	}
	var existing gatewaySessionRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND id = ?", store.namespace, session.ID,
	).First(&existing)
	if result.Error == nil {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionExists, session.ID)
	}
	if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Session{}, result.Error
	}
	if err := store.db.WithContext(ctx).Create(&row).Error; err != nil {
		return Session{}, fmt.Errorf("create gateway session: %w", err)
	}
	return cloneSession(session), nil
}

func (store *gormSessionStore) Get(
	ctx context.Context,
	id string,
) (Session, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Session{}, err
	}
	return store.getLocked(ctx, strings.TrimSpace(id))
}

func (store *gormSessionStore) getLocked(
	ctx context.Context,
	id string,
) (Session, error) {
	var row gatewaySessionRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND id = ?", store.namespace, id,
	).First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return Session{}, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	if result.Error != nil {
		return Session{}, result.Error
	}
	return sessionFromGatewayRow(row)
}

func (store *gormSessionStore) List(
	ctx context.Context,
	request sdk.PageRequest,
) (SessionPage, error) {
	return store.list(ctx, "", request)
}

func (store *gormSessionStore) ListByUser(
	ctx context.Context,
	userID string,
	request sdk.PageRequest,
) (SessionPage, error) {
	userID, err := normalizeUserID(userID)
	if err != nil {
		return SessionPage{}, err
	}
	return store.list(ctx, userID, request)
}

func (store *gormSessionStore) list(
	ctx context.Context,
	userID string,
	request sdk.PageRequest,
) (SessionPage, error) {
	request, err := validatePage(request)
	if err != nil {
		return SessionPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return SessionPage{}, err
	}
	query := store.db.WithContext(ctx).Where(
		"namespace = ? AND id > ?", store.namespace, request.After,
	)
	if userID != "" {
		query = query.Where("user_id = ?", userID)
	}
	var rows []gatewaySessionRow
	if err := query.Order("id ASC").Limit(request.Limit + 1).Find(&rows).Error; err != nil {
		return SessionPage{}, err
	}
	page := SessionPage{Items: make([]Session, 0, min(len(rows), request.Limit))}
	for _, row := range rows[:min(len(rows), request.Limit)] {
		session, err := sessionFromGatewayRow(row)
		if err != nil {
			return SessionPage{}, err
		}
		page.Items = append(page.Items, session)
	}
	if len(rows) > request.Limit {
		page.Next = rows[request.Limit-1].ID
	}
	return page, nil
}

func (store *gormSessionStore) Save(
	ctx context.Context,
	session Session,
	expectedRevision uint64,
) (Session, error) {
	session, err := normalizeSession(session)
	if err != nil {
		return Session{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return Session{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	current, err := store.getLocked(ctx, session.ID)
	if err != nil {
		return Session{}, err
	}
	updated, err := prepareSessionUpdate(
		current, session, expectedRevision, store.clock(),
	)
	if err != nil {
		return Session{}, err
	}
	row, err := gatewaySessionRowFromSession(store.namespace, updated)
	if err != nil {
		return Session{}, err
	}
	result := store.db.WithContext(ctx).Model(&gatewaySessionRow{}).Where(
		"namespace = ? AND id = ? AND revision = ?",
		store.namespace, session.ID, expectedRevision,
	).Updates(map[string]any{
		"user_id": row.UserID, "revision": row.Revision,
		"payload": row.Payload, "updated_at": row.UpdatedAt,
	})
	if result.Error != nil {
		return Session{}, result.Error
	}
	if result.RowsAffected != 1 {
		return Session{}, fmt.Errorf(
			"%w: session %s revision changed during save",
			ErrSessionConflict, session.ID,
		)
	}
	return cloneSession(updated), nil
}

func (store *gormSessionStore) Delete(
	ctx context.Context,
	id string,
	expectedRevision uint64,
) error {
	id = strings.TrimSpace(id)
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	current, err := store.getLocked(ctx, id)
	if err != nil {
		return err
	}
	if current.Revision != expectedRevision {
		return fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict, id, current.Revision, expectedRevision,
		)
	}
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND id = ? AND revision = ?",
		store.namespace, id, expectedRevision,
	).Delete(&gatewaySessionRow{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: session %s revision changed during delete", ErrSessionConflict, id)
	}
	return nil
}

func (*gormSessionStore) Capabilities() StoreCapabilities {
	return StoreCapabilities{Durable: true, MultiProcessSafe: true}
}

func (store *gormSessionStore) Close(context.Context) error { return store.close() }

func gatewaySessionRowFromSession(
	namespace string,
	session Session,
) (gatewaySessionRow, error) {
	payload, err := json.Marshal(storeSession(session))
	if err != nil {
		return gatewaySessionRow{}, fmt.Errorf("encode gateway session: %w", err)
	}
	return gatewaySessionRow{
		Namespace: namespace, ID: session.ID, UserID: session.UserID,
		Revision: session.Revision, Payload: payload,
		CreatedAt: session.CreatedAt.UTC(), UpdatedAt: session.UpdatedAt.UTC(),
	}, nil
}

func sessionFromGatewayRow(row gatewaySessionRow) (Session, error) {
	var stored storedSession
	if err := json.Unmarshal(row.Payload, &stored); err != nil {
		return Session{}, fmt.Errorf("decode gateway session %s: %w", row.ID, err)
	}
	session := stored.Session
	session.RuntimeConfig = append(json.RawMessage(nil), stored.RuntimeConfig...)
	normalized, err := normalizeSession(session)
	if err != nil {
		return Session{}, fmt.Errorf("validate gateway session %s: %w", row.ID, err)
	}
	if normalized.ID != row.ID || normalized.UserID != row.UserID ||
		normalized.Revision != row.Revision ||
		!normalized.CreatedAt.Equal(row.CreatedAt) ||
		!normalized.UpdatedAt.Equal(row.UpdatedAt) {
		return Session{}, fmt.Errorf("gateway session %s has inconsistent indexed metadata", row.ID)
	}
	return cloneSession(normalized), nil
}

var _ SessionStore = (*gormSessionStore)(nil)
