package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// moveTrajectoryHead publishes an explicit restore or rollback branch edge.
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
) (string, *durability.Checkpoint, error) {
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
	checkpoint, err := durability.DecodeCheckpoint(id, target)
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
	fields := durability.EntryFields(payload)
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
		durability.Checkpoint{
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
