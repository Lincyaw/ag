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
	Messages  []sdk.Message `json:"messages"`
	System    string        `json:"system,omitempty"`
	Provider  string        `json:"provider,omitempty"`
	Turns     int           `json:"turns"`
	ToolCalls int           `json:"tool_calls"`
	Action    sdk.Action    `json:"action"`
}

type trajectoryProviderRequestPayload struct {
	Turn     int              `json:"turn"`
	Provider string           `json:"provider"`
	Request  sdk.ModelRequest `json:"request"`
}

type trajectoryDecisionPayload struct {
	Turn   int        `json:"turn"`
	Action sdk.Action `json:"action"`
}

func latestTrajectoryCheckpoint(
	trajectory sdk.Trajectory,
) (string, *trajectoryCheckpointPayload, error) {
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		return "", nil, err
	}
	for index := len(branch) - 1; index >= 0; index-- {
		if branch[index].Kind != sdk.TrajectoryKindCheckpoint {
			continue
		}
		checkpoint, err := decodeTrajectoryCheckpoint(trajectory.ID, branch[index])
		return branch[index].ID, checkpoint, err
	}
	return "", nil, nil
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
	trajectory sdk.Trajectory,
	checkpointID string,
) bool {
	if trajectory.Head == "" {
		return checkpointID == ""
	}
	for _, entry := range trajectory.Entries {
		if entry.ID != trajectory.Head {
			continue
		}
		return (entry.Kind == sdk.TrajectoryKindRestore ||
			entry.Kind == sdk.TrajectoryKindRollback) &&
			entry.ParentID == checkpointID
	}
	return false
}

func (runtime *Runtime) moveTrajectoryHead(
	ctx context.Context,
	trajectory sdk.Trajectory,
	checkpointID string,
	kind string,
) (string, error) {
	payload, err := json.Marshal(map[string]string{
		"from": trajectory.Head,
		"to":   checkpointID,
	})
	if err != nil {
		return "", err
	}
	return runtime.trajectories.Append(
		ctx,
		trajectory.ID,
		trajectory.Head,
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
	trajectory, err := runtime.trajectories.Load(ctx, id)
	if err != nil {
		return "", nil, err
	}
	var target *sdk.TrajectoryEntry
	for index := range trajectory.Entries {
		entry := &trajectory.Entries[index]
		if entry.ID == checkpointID {
			target = entry
			break
		}
	}
	if target == nil {
		return "", nil, fmt.Errorf(
			"trajectory %q checkpoint %q not found",
			id,
			checkpointID,
		)
	}
	if target.Kind != sdk.TrajectoryKindCheckpoint {
		return "", nil, fmt.Errorf(
			"trajectory entry %q is %q, not a checkpoint",
			checkpointID,
			target.Kind,
		)
	}
	checkpoint, err := decodeTrajectoryCheckpoint(trajectory.ID, *target)
	if err != nil {
		return "", nil, err
	}
	head, err := runtime.moveTrajectoryHead(
		ctx,
		trajectory,
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
		From:         trajectory.Head,
		To:           checkpointID,
	})
	runtime.logger.InfoContext(
		ctx,
		"trajectory rolled back",
		"trajectory_id",
		id,
		"from",
		trajectory.Head,
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
	kind string,
	generation uint64,
	payload any,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s trajectory entry: %w", kind, err)
	}
	entry := sdk.TrajectoryEntry{
		ID:         sdk.NewID(),
		ParentID:   session.head,
		Kind:       kind,
		Timestamp:  time.Now().UTC(),
		Generation: generation,
		Payload:    raw,
	}
	head, err := session.runtime.trajectories.Append(
		ctx,
		session.config.ID,
		session.head,
		entry,
	)
	if err != nil {
		return fmt.Errorf("append %s trajectory entry: %w", kind, err)
	}
	session.head = head
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
) error {
	err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindCheckpoint,
		generation,
		trajectoryCheckpointPayload{
			Messages:  cloneMessages(messages),
			System:    system,
			Provider:  session.config.Provider,
			Turns:     result.Turns,
			ToolCalls: result.ToolCalls,
			Action:    action,
		},
	)
	if err == nil {
		session.config.System = system
	}
	return err
}

func (session *Session) restoreLatestCheckpoint(ctx context.Context) error {
	trajectory, err := session.runtime.trajectories.Load(ctx, session.config.ID)
	if err != nil {
		return fmt.Errorf("load trajectory for failure restore: %w", err)
	}
	checkpointID, checkpoint, err := latestTrajectoryCheckpoint(trajectory)
	if err != nil {
		return err
	}
	head := trajectory.Head
	if !trajectoryHeadRestoresCheckpoint(trajectory, checkpointID) {
		head, err = session.runtime.moveTrajectoryHead(
			ctx,
			trajectory,
			checkpointID,
			sdk.TrajectoryKindRestore,
		)
		if err != nil {
			return fmt.Errorf("restore failed prompt trajectory: %w", err)
		}
		session.runtime.emitTrajectoryEvent(
			ctx,
			sdk.EventTrajectoryRestore,
			sdk.TrajectoryEventPayload{
				TrajectoryID: session.config.ID,
				EntryID:      head,
				EntryKind:    sdk.TrajectoryKindRestore,
				From:         trajectory.Head,
				To:           checkpointID,
			},
		)
	}
	session.head = head
	session.messages = cloneMessages(checkpointMessages(checkpoint))
	if checkpoint != nil {
		session.config.System = checkpoint.System
		if checkpoint.Provider != "" {
			session.config.Provider = checkpoint.Provider
		}
	}
	return nil
}
