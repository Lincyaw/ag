package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type trajectoryCheckpointPayload struct {
	Messages     []sdk.Message `json:"messages"`
	System       string        `json:"system,omitempty"`
	Provider     string        `json:"provider,omitempty"`
	Output       string        `json:"output,omitempty"`
	Turns        int           `json:"turns"`
	ToolCalls    int           `json:"tool_calls"`
	Generation   uint64        `json:"generation,omitempty"`
	Action       sdk.Action    `json:"action"`
	Dependencies []string      `json:"dependencies,omitempty"`
}

type trajectoryProviderRequestPayload struct {
	Turn         int              `json:"turn"`
	Provider     string           `json:"provider"`
	Model        string           `json:"model,omitempty"`
	OperationKey string           `json:"operation_key"`
	Request      sdk.ModelRequest `json:"request"`
}

type trajectoryToolCallPayload struct {
	Turn         int          `json:"turn"`
	Call         sdk.ToolCall `json:"call"`
	OperationKey string       `json:"operation_key"`
}

type trajectoryDecisionPayload struct {
	Turn   int        `json:"turn"`
	Action sdk.Action `json:"action"`
}

func trajectoryEntryFields(payload any) sdk.TrajectoryEntryFields {
	var fields sdk.TrajectoryEntryFields
	setTurn := func(turn int) { fields.Turn = &turn }
	setError := func(isError bool) { fields.IsError = &isError }
	switch value := payload.(type) {
	case trajectoryProviderRequestPayload:
		setTurn(value.Turn)
		fields.Provider = value.Provider
		fields.Model = value.Model
		fields.OperationKey = value.OperationKey
	case sdk.AfterProviderPayload:
		setTurn(value.Turn)
		fields.Provider = value.Provider
		setError(value.Error != "")
		if value.Response != nil {
			fields.Model = value.Response.Model
			fields.FinishReason = value.Response.FinishReason
			fields.InputTokens = value.Response.Usage.InputTokens
			fields.OutputTokens = value.Response.Usage.OutputTokens
		}
	case sdk.BeforeToolPayload:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
	case trajectoryToolCallPayload:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
		fields.OperationKey = value.OperationKey
	case sdk.AfterToolPayload:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
		setError(value.Result.IsError)
	case trajectoryDecisionPayload:
		setTurn(value.Turn)
		fields.ActionKind = value.Action.Kind
		if value.Action.Cause != nil {
			fields.CauseCode = value.Action.Cause.Code
		}
	case trajectoryCheckpointPayload:
		if value.Turns > 0 {
			setTurn(value.Turns - 1)
		}
		fields.ActionKind = value.Action.Kind
		if value.Action.Cause != nil {
			fields.CauseCode = value.Action.Cause.Code
		}
	case sdk.AgentEndPayload:
		fields.CauseCode = value.Cause.Code
	}
	return fields
}

func latestTrajectoryCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (sdk.TrajectoryEntry, *trajectoryCheckpointPayload, error) {
	if metadata.Checkpoint == "" {
		return sdk.TrajectoryEntry{}, nil, nil
	}
	entry, err := store.LoadEntry(ctx, metadata.ID, metadata.Checkpoint)
	if err != nil {
		return sdk.TrajectoryEntry{}, nil, err
	}
	if entry.Kind != sdk.TrajectoryKindCheckpoint {
		return sdk.TrajectoryEntry{}, nil, fmt.Errorf(
			"trajectory %q cached checkpoint %q is %q",
			metadata.ID,
			entry.ID,
			entry.Kind,
		)
	}
	checkpoint, err := decodeTrajectoryCheckpoint(metadata.ID, entry)
	return entry, checkpoint, err
}

func decodeTrajectoryCheckpoint(
	trajectoryID string,
	entry sdk.TrajectoryEntry,
) (*trajectoryCheckpointPayload, error) {
	var checkpoint trajectoryCheckpointPayload
	if err := json.Unmarshal(entry.Payload, &checkpoint); err != nil {
		return nil, fmt.Errorf(
			"decode trajectory %q checkpoint %q: %w",
			trajectoryID,
			entry.ID,
			err,
		)
	}
	checkpoint.Messages = cloneMessages(checkpoint.Messages)
	return &checkpoint, nil
}

func checkpointMessages(checkpoint *trajectoryCheckpointPayload) []sdk.Message {
	if checkpoint == nil {
		return nil
	}
	return checkpoint.Messages
}

func trajectoryHeadRestoresCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	head string,
	checkpointID string,
) (bool, error) {
	if head == "" {
		return checkpointID == "", nil
	}
	if head == checkpointID {
		return true, nil
	}
	entry, err := store.LoadEntry(ctx, trajectoryID, head)
	if err != nil {
		return false, err
	}
	return (entry.Kind == sdk.TrajectoryKindRestore ||
		entry.Kind == sdk.TrajectoryKindRollback) &&
		entry.ParentID == checkpointID, nil
}

func (runtime *Runtime) moveTrajectoryHead(
	ctx context.Context,
	trajectoryID string,
	head string,
	checkpointID string,
	kind sdk.TrajectoryKind,
) (string, error) {
	payload, err := json.Marshal(map[string]string{
		"from": head,
		"to":   checkpointID,
	})
	if err != nil {
		return "", err
	}
	return runtime.trajectories.Append(
		ctx,
		trajectoryID,
		head,
		sdk.TrajectoryEntry{
			ID:        sdk.NewID(),
			ParentID:  checkpointID,
			Kind:      kind,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		},
	)
}

func (runtime *Runtime) RollbackTrajectory(
	ctx context.Context,
	id string,
	checkpointID string,
) error {
	_, _, err := runtime.rollbackTrajectory(ctx, id, checkpointID)
	return err
}

func (runtime *Runtime) rollbackTrajectory(
	ctx context.Context,
	id string,
	checkpointID string,
) (string, *trajectoryCheckpointPayload, error) {
	if runtime == nil {
		return "", nil, errors.New("runtime is nil")
	}
	metadata, err := runtime.trajectories.LoadMetadata(ctx, id)
	if err != nil {
		return "", nil, err
	}
	target, err := runtime.trajectories.LoadEntry(ctx, id, checkpointID)
	if errors.Is(err, sdk.ErrTrajectoryEntryNotFound) {
		return "", nil, fmt.Errorf(
			"trajectory %q checkpoint %q not found",
			id,
			checkpointID,
		)
	}
	if err != nil {
		return "", nil, err
	}
	if target.Kind != sdk.TrajectoryKindCheckpoint {
		return "", nil, fmt.Errorf(
			"trajectory entry %q is %q, not a checkpoint",
			checkpointID,
			target.Kind,
		)
	}
	checkpoint, err := decodeTrajectoryCheckpoint(id, target)
	if err != nil {
		return "", nil, err
	}
	head, err := runtime.moveTrajectoryHead(
		ctx,
		id,
		metadata.Head,
		checkpointID,
		sdk.TrajectoryKindRollback,
	)
	if err != nil {
		return "", nil, err
	}
	runtime.emitTrajectoryEvent(ctx, sdk.EventTrajectoryRollback, sdk.TrajectoryEventPayload{
		TrajectoryID: id,
		EntryID:      head,
		EntryKind:    sdk.TrajectoryKindRollback,
		From:         metadata.Head,
		To:           checkpointID,
	})
	runtime.logger.InfoContext(
		ctx,
		"trajectory rolled back",
		"trajectory_id",
		id,
		"from",
		metadata.Head,
		"to",
		checkpointID,
	)
	return head, checkpoint, nil
}

func (session *Session) Rollback(
	ctx context.Context,
	checkpointID string,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	head, checkpoint, err := session.runtime.rollbackTrajectory(
		ctx,
		session.config.ID,
		checkpointID,
	)
	if err != nil {
		return err
	}
	session.messages = cloneMessages(checkpoint.Messages)
	session.config.System = checkpoint.System
	if checkpoint.Provider != "" {
		session.config.Provider = checkpoint.Provider
	}
	session.head = head
	return nil
}

func (session *Session) appendTrajectory(
	ctx context.Context,
	kind sdk.TrajectoryKind,
	generation uint64,
	payload any,
) error {
	return session.appendTrajectoryState(
		ctx,
		kind,
		generation,
		payload,
		"",
		"",
	)
}

func (session *Session) appendTrajectoryState(
	ctx context.Context,
	kind sdk.TrajectoryKind,
	generation uint64,
	payload any,
	state sdk.TrajectoryExecutionState,
	executionError string,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s trajectory entry: %w", kind, err)
	}
	entryID := sdk.NewID()
	fields := trajectoryEntryFields(payload)
	if fields.OperationKey == "" {
		switch kind {
		case sdk.TrajectoryKindProviderRequest,
			sdk.TrajectoryKindToolCall:
			// Legacy async resources use the just-appended trajectory head as
			// their idempotency key. Newer callers can provide an explicit key.
			fields.OperationKey = entryID
		}
	}
	entry := sdk.TrajectoryEntry{
		ID:         entryID,
		ParentID:   session.head,
		Kind:       kind,
		Timestamp:  time.Now().UTC(),
		Generation: generation,
		Fields:     fields,
		Payload:    raw,
	}
	executionID, token := session.activeExecution()
	if executionID != "" && token != "" {
		if err := session.commitExecution(
			ctx,
			entry,
			state,
			executionError,
		); err != nil {
			return fmt.Errorf("commit %s trajectory entry: %w", kind, err)
		}
	} else {
		if state != "" {
			return fmt.Errorf(
				"complete %s trajectory entry without an active execution",
				kind,
			)
		}
		head, appendErr := session.runtime.trajectories.Append(
			ctx,
			session.config.ID,
			session.head,
			entry,
		)
		if appendErr != nil {
			return fmt.Errorf("append %s trajectory entry: %w", kind, appendErr)
		}
		session.head = head
	}
	session.runtime.emitTrajectoryEvent(ctx, sdk.EventTrajectoryAppend, sdk.TrajectoryEventPayload{
		TrajectoryID: session.config.ID,
		EntryID:      entry.ID,
		EntryKind:    kind,
		Generation:   generation,
	})
	return nil
}

func (runtime *Runtime) emitTrajectoryEvent(
	ctx context.Context,
	eventName string,
	payload sdk.TrajectoryEventPayload,
) {
	if _, err := runtime.Emit(ctx, eventName, payload.TrajectoryID, payload); err != nil {
		runtime.logger.WarnContext(
			ctx,
			"trajectory event failed",
			"event",
			eventName,
			"trajectory_id",
			payload.TrajectoryID,
			"error",
			err,
		)
	}
}

func (session *Session) checkpointTrajectory(
	ctx context.Context,
	generation uint64,
	messages []sdk.Message,
	result Result,
	action sdk.Action,
	system string,
	dependencies ...string,
) error {
	err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindCheckpoint,
		generation,
		trajectoryCheckpointPayload{
			Messages:   cloneMessages(messages),
			System:     system,
			Provider:   session.config.Provider,
			Output:     result.Output,
			Turns:      result.Turns,
			ToolCalls:  result.ToolCalls,
			Generation: result.Generation,
			Action:     action,
			Dependencies: append(
				[]string(nil),
				dependencies...,
			),
		},
	)
	if err == nil {
		session.config.System = system
	}
	return err
}
