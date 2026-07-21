package gateway

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type LegacyGatewayStoreDirectories struct {
	Sessions     string
	Inputs       string
	Interactions string
}

// GORMGatewayStores is the SQL-backed gateway control plane. Each interface
// owns its own GORM connection pool because their method names overlap, while
// every pool targets the exact same URI, namespace, and schema.
type GORMGatewayStores struct {
	Sessions     SessionStore
	Events       EventStore
	Inputs       InputStore
	Interactions InteractionStore
}

func NewGORMGatewayStores(
	ctx context.Context,
	rawURI string,
	legacy LegacyGatewayStoreDirectories,
) (*GORMGatewayStores, error) {
	sessions, err := NewGORMSessionStore(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	events, err := NewGORMEventStore(ctx, rawURI)
	if err != nil {
		_ = sessions.Close(context.Background())
		return nil, err
	}
	inputs, err := NewGORMInputStore(ctx, rawURI)
	if err != nil {
		_ = events.Close(context.Background())
		_ = sessions.Close(context.Background())
		return nil, err
	}
	interactions, err := NewGORMInteractionStore(ctx, rawURI)
	if err != nil {
		_ = inputs.Close(context.Background())
		_ = events.Close(context.Background())
		_ = sessions.Close(context.Background())
		return nil, err
	}
	stores := &GORMGatewayStores{
		Sessions: sessions, Events: events,
		Inputs: inputs, Interactions: interactions,
	}
	if err := stores.migrateLegacyFiles(ctx, legacy); err != nil {
		_ = stores.Close(context.Background())
		return nil, err
	}
	return stores, nil
}

func (stores *GORMGatewayStores) Close(ctx context.Context) error {
	if stores == nil {
		return nil
	}
	return errors.Join(
		stores.Interactions.Close(ctx),
		stores.Inputs.Close(ctx),
		stores.Events.Close(ctx),
		stores.Sessions.Close(ctx),
	)
}

func (stores *GORMGatewayStores) migrateLegacyFiles(
	ctx context.Context,
	legacy LegacyGatewayStoreDirectories,
) error {
	sessions := stores.Sessions.(*gormSessionStore)
	inputs := stores.Inputs.(*gormInputStore)
	interactions := stores.Interactions.(*gormInteractionStore)
	if legacy.Sessions != "" {
		state, err := readLegacySessionState(legacy.Sessions)
		if err != nil {
			return err
		}
		if err := sessions.importLegacy(ctx, state); err != nil {
			return err
		}
	}
	if legacy.Inputs != "" {
		state, err := readLegacyInputState(legacy.Inputs)
		if err != nil {
			return err
		}
		if err := inputs.importLegacy(ctx, state); err != nil {
			return err
		}
	}
	if legacy.Interactions != "" {
		state, err := readLegacyInteractionState(legacy.Interactions)
		if err != nil {
			return err
		}
		if err := interactions.importLegacy(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func readLegacySessionState(directory string) (fileSessionState, error) {
	store, err := NewFileSessionStore(filepath.Clean(directory))
	if err != nil {
		return fileSessionState{}, err
	}
	fileStore := store.(*fileSessionStore)
	fileStore.mu.Lock()
	state, readErr := fileStore.readLocked()
	fileStore.mu.Unlock()
	return state, errors.Join(readErr, fileStore.Close(context.Background()))
}

func readLegacyInputState(directory string) (fileInputState, error) {
	store, err := NewFileInputStore(filepath.Clean(directory))
	if err != nil {
		return fileInputState{}, err
	}
	fileStore := store.(*fileInputStore)
	fileStore.mu.Lock()
	state, readErr := fileStore.readLocked()
	fileStore.mu.Unlock()
	return state, errors.Join(readErr, fileStore.Close(context.Background()))
}

func readLegacyInteractionState(directory string) (fileInteractionState, error) {
	store, err := NewFileInteractionStore(filepath.Clean(directory))
	if err != nil {
		return fileInteractionState{}, err
	}
	fileStore := store.(*fileInteractionStore)
	fileStore.mu.Lock()
	state, readErr := fileStore.readLocked()
	fileStore.mu.Unlock()
	return state, errors.Join(readErr, fileStore.Close(context.Background()))
}

func (store *gormSessionStore) importLegacy(
	ctx context.Context,
	state fileSessionState,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	ids := make([]string, 0, len(state.Sessions))
	for id := range state.Sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, id := range ids {
			session := state.Sessions[id].Session
			row, err := gatewaySessionRowFromSession(store.namespace, session)
			if err != nil {
				return err
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *gormInputStore) importLegacy(
	ctx context.Context,
	state fileInputState,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for sessionID, stream := range state.Streams {
			for _, input := range stream.Inputs {
				row, err := gatewayInputRowFromInput(store.namespace, input)
				if err != nil {
					return err
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
					return err
				}
			}
			if err := upsertInputCursor(
				tx, store.namespace, sessionID, stream.NextSequence,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func upsertInputCursor(
	tx *gorm.DB,
	namespace string,
	sessionID string,
	next uint64,
) error {
	var current gatewayInputCursorRow
	result := tx.Where(
		"namespace = ? AND session_id = ?", namespace, sessionID,
	).First(&current)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return tx.Create(&gatewayInputCursorRow{
			Namespace: namespace, SessionID: sessionID, NextSequence: next,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}
	if current.NextSequence >= next {
		return nil
	}
	return tx.Model(&gatewayInputCursorRow{}).Where(
		"namespace = ? AND session_id = ?", namespace, sessionID,
	).Update("next_sequence", next).Error
}

func (store *gormInteractionStore) importLegacy(
	ctx context.Context,
	state fileInteractionState,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for sessionID, stream := range state.Streams {
			for _, item := range stream.Items {
				row, err := gatewayInteractionRowFromInteraction(store.namespace, item)
				if err != nil {
					return err
				}
				if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
					return err
				}
			}
			if err := upsertInteractionCursor(
				tx, store.namespace, sessionID, stream.NextSequence,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func upsertInteractionCursor(
	tx *gorm.DB,
	namespace string,
	sessionID string,
	next uint64,
) error {
	var current gatewayInteractionCursorRow
	result := tx.Where(
		"namespace = ? AND session_id = ?", namespace, sessionID,
	).First(&current)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return tx.Create(&gatewayInteractionCursorRow{
			Namespace: namespace, SessionID: sessionID, NextSequence: next,
		}).Error
	}
	if result.Error != nil {
		return result.Error
	}
	if current.NextSequence >= next {
		return nil
	}
	return tx.Model(&gatewayInteractionCursorRow{}).Where(
		"namespace = ? AND session_id = ?", namespace, sessionID,
	).Update("next_sequence", next).Error
}

func (stores *GORMGatewayStores) String() string {
	return fmt.Sprintf(
		"gorm gateway stores (%T, %T, %T, %T)",
		stores.Sessions, stores.Events, stores.Inputs, stores.Interactions,
	)
}
