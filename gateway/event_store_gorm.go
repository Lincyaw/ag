package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/lincyaw/ag/internal/sqlitecoord"
	"github.com/lincyaw/ag/sdk"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type gatewayEventRow struct {
	Namespace  string    `gorm:"primaryKey;size:255"`
	SessionID  string    `gorm:"primaryKey;size:255"`
	Sequence   uint64    `gorm:"primaryKey;autoIncrement:false"`
	EventID    string    `gorm:"size:255;not null"`
	Name       string    `gorm:"size:255;not null"`
	Generation uint64    `gorm:"not null"`
	Payload    []byte    `gorm:"not null"`
	CreatedAt  time.Time `gorm:"not null"`
}

func (gatewayEventRow) TableName() string { return "ag_gateway_events" }

type gatewayEventCursorRow struct {
	Namespace    string `gorm:"primaryKey;size:255"`
	SessionID    string `gorm:"primaryKey;size:255"`
	NextSequence uint64 `gorm:"not null"`
}

func (gatewayEventCursorRow) TableName() string { return "ag_gateway_event_cursors" }

type gormEventStore struct {
	mu        sync.Mutex
	writeMu   *sync.Mutex
	db        *gorm.DB
	closeDB   func() error
	namespace string
	notify    map[string]chan struct{}
	clock     func() time.Time
	closed    bool
}

func NewGORMEventStore(
	ctx context.Context,
	rawURI string,
) (EventStore, error) {
	db, namespace, closeDB, writeMu, err := openGatewayGORMDB(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	writeMu.Lock()
	migrateErr := migrateGatewayEventSchema(db)
	writeMu.Unlock()
	if migrateErr != nil {
		_ = closeDB()
		return nil, migrateErr
	}
	return &gormEventStore{
		db: db, closeDB: closeDB, writeMu: writeMu, namespace: namespace,
		notify: make(map[string]chan struct{}),
		clock:  func() time.Time { return time.Now().UTC() },
	}, nil
}

func (store *gormEventStore) Append(
	ctx context.Context,
	sessionID string,
	event sdk.Event,
) (AgentEvent, error) {
	if err := ctx.Err(); err != nil {
		return AgentEvent{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AgentEvent{}, err
	}
	event, err = normalizeRuntimeEvent(event)
	if err != nil {
		return AgentEvent{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return AgentEvent{}, ErrStoreClosed
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	var created AgentEvent
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var duplicate gatewayEventRow
		result := tx.Where(
			"namespace = ? AND session_id = ? AND event_id = ?",
			store.namespace,
			sessionID,
			event.ID,
		).First(&duplicate)
		if result.Error == nil {
			created = agentEventFromRow(duplicate)
			return nil
		}
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return result.Error
		}
		var cursor gatewayEventCursorRow
		result = tx.Where(
			"namespace = ? AND session_id = ?",
			store.namespace,
			sessionID,
		).First(&cursor)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			cursor = gatewayEventCursorRow{
				Namespace: store.namespace, SessionID: sessionID, NextSequence: 1,
			}
			if err := tx.Create(&cursor).Error; err != nil {
				return err
			}
		} else if result.Error != nil {
			return result.Error
		}
		if cursor.NextSequence == 0 {
			cursor.NextSequence = 1
		}
		if cursor.NextSequence == math.MaxUint64 {
			return errors.New("gateway event sequence is exhausted")
		}
		created = AgentEvent{
			Sequence: cursor.NextSequence, SessionID: sessionID,
			ID: event.ID, Name: event.Name, Generation: event.Generation,
			Payload:   append(json.RawMessage(nil), event.Payload...),
			CreatedAt: store.clock(),
		}
		if err := tx.Create(gatewayEventRowFromEvent(store.namespace, created)).Error; err != nil {
			return err
		}
		cursor.NextSequence++
		return tx.Model(&gatewayEventCursorRow{}).Where(
			"namespace = ? AND session_id = ?",
			store.namespace,
			sessionID,
		).Update("next_sequence", cursor.NextSequence).Error
	})
	if err != nil {
		return AgentEvent{}, fmt.Errorf("append gateway event: %w", err)
	}
	store.signalLocked(sessionID)
	return cloneAgentEvent(created), nil
}

func (store *gormEventStore) Latest(
	ctx context.Context,
	sessionID string,
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return 0, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrStoreClosed
	}
	var cursor gatewayEventCursorRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ?",
		store.namespace,
		sessionID,
	).First(&cursor)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if result.Error != nil {
		return 0, result.Error
	}
	if cursor.NextSequence <= 1 {
		return 0, nil
	}
	return cursor.NextSequence - 1, nil
}

func (store *gormEventStore) List(
	ctx context.Context,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	if err := ctx.Err(); err != nil {
		return EventPage{}, err
	}
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return EventPage{}, err
	}
	query, err = normalizeEventQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return EventPage{}, ErrStoreClosed
	}
	return store.listLocked(ctx, sessionID, query)
}

func (store *gormEventStore) Wait(
	ctx context.Context,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return EventPage{}, err
	}
	query, err = normalizeEventQuery(query)
	if err != nil {
		return EventPage{}, err
	}
	for {
		store.mu.Lock()
		if store.closed {
			store.mu.Unlock()
			return EventPage{}, ErrStoreClosed
		}
		page, err := store.listLocked(ctx, sessionID, query)
		if err != nil || len(page.Items) > 0 {
			store.mu.Unlock()
			return page, err
		}
		notify := store.notifyLocked(sessionID)
		store.mu.Unlock()
		select {
		case <-ctx.Done():
			return EventPage{}, ctx.Err()
		case <-notify:
		}
	}
}

func (store *gormEventStore) listLocked(
	ctx context.Context,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	var rows []gatewayEventRow
	if err := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ? AND sequence > ?",
		store.namespace,
		sessionID,
		query.After,
	).Order("sequence ASC").Limit(query.Limit).Find(&rows).Error; err != nil {
		return EventPage{}, err
	}
	page := EventPage{Items: make([]AgentEvent, len(rows))}
	page.Items = page.Items[:0]
	encodedBytes := eventPageEnvelopeOverhead
	for _, row := range rows {
		if !appendEventPageItem(
			&page,
			agentEventFromRow(row),
			&encodedBytes,
		) {
			break
		}
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page, nil
}

func (store *gormEventStore) Close(context.Context) error {
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

func (store *gormEventStore) notifyLocked(sessionID string) chan struct{} {
	notify := store.notify[sessionID]
	if notify == nil {
		notify = make(chan struct{})
		store.notify[sessionID] = notify
	}
	return notify
}

func (store *gormEventStore) signalLocked(sessionID string) {
	notify := store.notifyLocked(sessionID)
	close(notify)
	store.notify[sessionID] = make(chan struct{})
}

func gatewayEventRowFromEvent(namespace string, event AgentEvent) gatewayEventRow {
	return gatewayEventRow{
		Namespace: namespace, SessionID: event.SessionID,
		Sequence: event.Sequence, EventID: event.ID, Name: event.Name,
		Generation: event.Generation,
		Payload:    append([]byte(nil), event.Payload...), CreatedAt: event.CreatedAt,
	}
}

func agentEventFromRow(row gatewayEventRow) AgentEvent {
	return AgentEvent{
		Sequence: row.Sequence, SessionID: row.SessionID,
		ID: row.EventID, Name: row.Name, Generation: row.Generation,
		Payload:   append(json.RawMessage(nil), row.Payload...),
		CreatedAt: row.CreatedAt.UTC(),
	}
}

func openGatewayGORMDB(
	ctx context.Context,
	rawURI string,
) (*gorm.DB, string, func() error, *sync.Mutex, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", nil, nil, err
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("parse gateway storage URI: %w", err)
	}
	namespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("gateway event namespace", namespace); err != nil {
		return nil, "", nil, nil, err
	}
	config := &gorm.Config{Logger: logger.Discard}
	var db *gorm.DB
	writeMu := new(sync.Mutex)
	switch parsed.Scheme {
	case "sqlite":
		path := parsed.Path
		if parsed.Host != "" && parsed.Host != "localhost" {
			path = filepath.Join(string(filepath.Separator)+parsed.Host, parsed.Path)
		}
		if strings.TrimSpace(path) == "" {
			return nil, "", nil, nil, errors.New("gateway SQLite URI has no path")
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, "", nil, nil, fmt.Errorf(
				"resolve gateway SQLite path: %w",
				err,
			)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, "", nil, nil, err
		}
		writeMu = sqlitecoord.WriteMutex(path)
		openMu := sqlitecoord.OpenMutex()
		openMu.Lock()
		db, err = gorm.Open(sqlite.Open(path), config)
		openMu.Unlock()
	case "postgres", "postgresql":
		connection := *parsed
		query := connection.Query()
		query.Del("namespace")
		connection.RawQuery = query.Encode()
		db, err = gorm.Open(postgres.Open(connection.String()), config)
	default:
		return nil, "", nil, nil, fmt.Errorf(
			"gateway storage requires sqlite or PostgreSQL, got %q",
			parsed.Scheme,
		)
	}
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("open gateway database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, "", nil, nil, err
	}
	if parsed.Scheme == "sqlite" {
		for _, pragma := range []string{
			"PRAGMA journal_mode=WAL",
			"PRAGMA busy_timeout=5000",
			"PRAGMA synchronous=NORMAL",
			"PRAGMA foreign_keys=ON",
		} {
			if _, err := sqlDB.ExecContext(ctx, pragma); err != nil {
				_ = sqlDB.Close()
				return nil, "", nil, nil, fmt.Errorf("execute %s: %w", pragma, err)
			}
		}
	}
	return db, namespace, sqlDB.Close, writeMu, nil
}

const postgresGatewayEventMigrationLockID int64 = 0x41674745766e7473

func migrateGatewayEventSchema(db *gorm.DB) error {
	migrate := func(tx *gorm.DB) error {
		if err := tx.AutoMigrate(
			&gatewayEventRow{},
			&gatewayEventCursorRow{},
		); err != nil {
			return fmt.Errorf("auto-migrate gateway event schema: %w", err)
		}
		if err := tx.Exec(
			`CREATE UNIQUE INDEX IF NOT EXISTS ag_gateway_event_id_idx
				ON ag_gateway_events (namespace, session_id, event_id)`,
		).Error; err != nil {
			return fmt.Errorf("create gateway event ID index: %w", err)
		}
		return nil
	}
	if db.Dialector.Name() != "postgres" {
		return migrate(db)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			"SELECT pg_advisory_xact_lock(?)",
			postgresGatewayEventMigrationLockID,
		).Error; err != nil {
			return fmt.Errorf("lock gateway event schema migration: %w", err)
		}
		return migrate(tx)
	})
}

var _ EventStore = (*gormEventStore)(nil)
