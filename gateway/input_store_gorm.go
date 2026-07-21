package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type gatewayInputRow struct {
	Namespace string `gorm:"primaryKey;size:255"`
	SessionID string `gorm:"primaryKey;size:255"`
	Sequence  uint64 `gorm:"primaryKey;autoIncrement:false"`
	InputID   string `gorm:"size:255;not null"`
	State     string `gorm:"size:32;not null"`
	Revision  uint64 `gorm:"not null"`
	Payload   []byte `gorm:"not null"`
}

func (gatewayInputRow) TableName() string { return "ag_gateway_inputs" }

type gatewayInputCursorRow struct {
	Namespace    string `gorm:"primaryKey;size:255"`
	SessionID    string `gorm:"primaryKey;size:255"`
	NextSequence uint64 `gorm:"not null"`
}

func (gatewayInputCursorRow) TableName() string { return "ag_gateway_input_cursors" }

type gormInputStore struct{ *gatewayGORMCore }

func NewGORMInputStore(
	ctx context.Context,
	rawURI string,
) (InputStore, error) {
	core, err := openGatewayGORMCore(ctx, rawURI)
	if err != nil {
		return nil, err
	}
	core.writeMu.Lock()
	migrateErr := migrateGatewayInputSchema(core.db)
	core.writeMu.Unlock()
	if migrateErr != nil {
		_ = core.closeDB()
		return nil, migrateErr
	}
	return &gormInputStore{gatewayGORMCore: core}, nil
}

func migrateGatewayInputSchema(db *gorm.DB) error {
	if err := db.AutoMigrate(&gatewayInputRow{}, &gatewayInputCursorRow{}); err != nil {
		return fmt.Errorf("auto-migrate gateway input schema: %w", err)
	}
	if err := db.Exec(
		`CREATE UNIQUE INDEX IF NOT EXISTS ag_gateway_input_id_idx
			ON ag_gateway_inputs (namespace, session_id, input_id)`,
	).Error; err != nil {
		return fmt.Errorf("create gateway input ID index: %w", err)
	}
	return nil
}

func (store *gormInputStore) Enqueue(
	ctx context.Context,
	input AgentInput,
) (AgentInput, error) {
	input, err := normalizeAgentInput(input)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return AgentInput{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	stream, err := store.loadInputStreamLocked(ctx, input.SessionID)
	if err != nil {
		return AgentInput{}, err
	}
	updated, created, changed, err := enqueueAgentInput(stream, input, store.clock())
	if err != nil || !changed {
		return created, err
	}
	row, err := gatewayInputRowFromInput(store.namespace, created)
	if err != nil {
		return AgentInput{}, err
	}
	err = store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		cursor := gatewayInputCursorRow{
			Namespace: store.namespace, SessionID: input.SessionID,
			NextSequence: updated.NextSequence,
		}
		var existing gatewayInputCursorRow
		result := tx.Where(
			"namespace = ? AND session_id = ?", store.namespace, input.SessionID,
		).First(&existing)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return tx.Create(&cursor).Error
		}
		if result.Error != nil {
			return result.Error
		}
		return tx.Model(&gatewayInputCursorRow{}).Where(
			"namespace = ? AND session_id = ?", store.namespace, input.SessionID,
		).Update("next_sequence", updated.NextSequence).Error
	})
	if err != nil {
		return AgentInput{}, fmt.Errorf("enqueue gateway input: %w", err)
	}
	return created, nil
}

func (store *gormInputStore) Get(
	ctx context.Context,
	sessionID string,
	inputID string,
) (AgentInput, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return AgentInput{}, err
	}
	var row gatewayInputRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ? AND input_id = ?",
		store.namespace, sessionID, inputID,
	).First(&row)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return AgentInput{}, fmt.Errorf("%w: %s", ErrInputNotFound, inputID)
	}
	if result.Error != nil {
		return AgentInput{}, result.Error
	}
	return inputFromGatewayRow(row)
}

func (store *gormInputStore) List(
	ctx context.Context,
	sessionID string,
	query InputQuery,
) (InputPage, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return InputPage{}, err
	}
	query, err = normalizeInputQuery(query)
	if err != nil {
		return InputPage{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return InputPage{}, err
	}
	var rows []gatewayInputRow
	if err := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ? AND sequence > ?",
		store.namespace, sessionID, query.After,
	).Order("sequence ASC").Limit(query.Limit).Find(&rows).Error; err != nil {
		return InputPage{}, err
	}
	page := InputPage{Items: make([]AgentInput, 0, len(rows))}
	for _, row := range rows {
		input, err := inputFromGatewayRow(row)
		if err != nil {
			return InputPage{}, err
		}
		page.Items = append(page.Items, input)
	}
	if len(page.Items) > 0 {
		page.Next = page.Items[len(page.Items)-1].Sequence
	}
	return page, nil
}

func (store *gormInputStore) AcquireNext(
	ctx context.Context,
	sessionID string,
) (AcquiredInput, bool, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return AcquiredInput{}, false, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	stream, err := store.loadInputStreamLocked(ctx, sessionID)
	if err != nil {
		return AcquiredInput{}, false, err
	}
	_, acquired, ok, changed, err := acquireAgentInput(stream, store.clock())
	if err != nil || !ok || !changed {
		return acquired, ok, err
	}
	if err := store.saveInputLocked(ctx, acquired.Input, acquired.Input.Revision-1); err != nil {
		return AcquiredInput{}, false, err
	}
	return acquired, true, nil
}

func (store *gormInputStore) BindExecution(
	ctx context.Context,
	sessionID string,
	inputID string,
	executionID string,
) (AgentInput, error) {
	return store.mutateInput(ctx, sessionID, func(stream inputStream) (AgentInput, bool, error) {
		_, input, changed, err := bindAgentInputExecution(
			stream, inputID, executionID, store.clock(),
		)
		return input, changed, err
	})
}

func (store *gormInputStore) Complete(
	ctx context.Context,
	sessionID string,
	inputID string,
	state AgentInputState,
	lastError string,
) (AgentInput, error) {
	return store.mutateInput(ctx, sessionID, func(stream inputStream) (AgentInput, bool, error) {
		_, input, changed, err := completeAgentInput(
			stream, inputID, state, lastError, store.clock(),
		)
		return input, changed, err
	})
}

func (store *gormInputStore) CancelQueued(
	ctx context.Context,
	sessionID string,
	inputID string,
	expectedRevision uint64,
) (AgentInput, error) {
	return store.mutateInput(ctx, sessionID, func(stream inputStream) (AgentInput, bool, error) {
		_, input, changed, err := cancelQueuedAgentInput(
			stream, inputID, expectedRevision, store.clock(),
		)
		return input, changed, err
	})
}

func (store *gormInputStore) mutateInput(
	ctx context.Context,
	sessionID string,
	mutation func(inputStream) (AgentInput, bool, error),
) (AgentInput, error) {
	sessionID, err := normalizeEventSessionID(sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.checkLocked(ctx); err != nil {
		return AgentInput{}, err
	}
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	stream, err := store.loadInputStreamLocked(ctx, sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	input, changed, err := mutation(stream)
	if err != nil || !changed {
		return input, err
	}
	if err := store.saveInputLocked(ctx, input, input.Revision-1); err != nil {
		return AgentInput{}, err
	}
	return input, nil
}

func (store *gormInputStore) loadInputStreamLocked(
	ctx context.Context,
	sessionID string,
) (inputStream, error) {
	var rows []gatewayInputRow
	if err := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ?", store.namespace, sessionID,
	).Order("sequence ASC").Find(&rows).Error; err != nil {
		return inputStream{}, err
	}
	stream := inputStream{NextSequence: 1, Inputs: make([]AgentInput, 0, len(rows))}
	for _, row := range rows {
		input, err := inputFromGatewayRow(row)
		if err != nil {
			return inputStream{}, err
		}
		stream.Inputs = append(stream.Inputs, input)
	}
	var cursor gatewayInputCursorRow
	result := store.db.WithContext(ctx).Where(
		"namespace = ? AND session_id = ?", store.namespace, sessionID,
	).First(&cursor)
	if result.Error == nil {
		stream.NextSequence = cursor.NextSequence
	} else if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return inputStream{}, result.Error
	} else if len(stream.Inputs) > 0 {
		stream.NextSequence = stream.Inputs[len(stream.Inputs)-1].Sequence + 1
	}
	return validateInputStream(sessionID, stream)
}

func (store *gormInputStore) saveInputLocked(
	ctx context.Context,
	input AgentInput,
	expectedRevision uint64,
) error {
	row, err := gatewayInputRowFromInput(store.namespace, input)
	if err != nil {
		return err
	}
	result := store.db.WithContext(ctx).Model(&gatewayInputRow{}).Where(
		"namespace = ? AND session_id = ? AND sequence = ? AND revision = ?",
		store.namespace, input.SessionID, input.Sequence, expectedRevision,
	).Updates(map[string]any{
		"state": row.State, "revision": row.Revision, "payload": row.Payload,
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: input %s revision changed", ErrInputConflict, input.ID)
	}
	return nil
}

func (store *gormInputStore) Close(context.Context) error { return store.close() }

func gatewayInputRowFromInput(namespace string, input AgentInput) (gatewayInputRow, error) {
	payload, err := json.Marshal(input)
	if err != nil {
		return gatewayInputRow{}, fmt.Errorf("encode gateway input: %w", err)
	}
	return gatewayInputRow{
		Namespace: namespace, SessionID: input.SessionID, Sequence: input.Sequence,
		InputID: input.ID, State: string(input.State), Revision: input.Revision,
		Payload: payload,
	}, nil
}

func inputFromGatewayRow(row gatewayInputRow) (AgentInput, error) {
	var input AgentInput
	if err := json.Unmarshal(row.Payload, &input); err != nil {
		return AgentInput{}, fmt.Errorf("decode gateway input %s: %w", row.InputID, err)
	}
	if input.ID != row.InputID || input.SessionID != row.SessionID ||
		input.Sequence != row.Sequence || string(input.State) != row.State ||
		input.Revision != row.Revision {
		return AgentInput{}, fmt.Errorf("gateway input %s has inconsistent indexed metadata", row.InputID)
	}
	return input, nil
}

var _ InputStore = (*gormInputStore)(nil)
