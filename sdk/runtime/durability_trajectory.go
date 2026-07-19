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
	snapshot *registrySnapshot,
	trajectoryID string,
	head string,
	anchorID string,
	kind sdk.TrajectoryKind,
) (string, error) {
	updated, events, err := runtime.commitTrajectoryHeadMove(
		ctx,
		snapshot,
		trajectoryID,
		head,
		anchorID,
		kind,
	)
	if err != nil {
		return "", err
	}
	events.dispatch(ctx, runtime)
	return updated, nil
}

func (runtime *Runtime) commitTrajectoryHeadMove(
	ctx context.Context,
	snapshot *registrySnapshot,
	trajectoryID string,
	head string,
	anchorID string,
	kind sdk.TrajectoryKind,
) (string, postCommitEventBundle, error) {
	move, events, err := runtime.prepareTrajectoryHeadMove(
		snapshot,
		trajectoryID,
		head,
		anchorID,
		kind,
	)
	if err != nil {
		return "", nil, err
	}
	updated, err := runtime.appendTrajectoryEntries(
		ctx,
		trajectoryID,
		head,
		[]sdk.TrajectoryEntry{move.Entry},
		events,
	)
	if err != nil {
		return "", nil, err
	}
	return updated, events, nil
}

func (runtime *Runtime) trajectoryHeadNeedsMove(
	ctx context.Context,
	trajectoryID string,
	head string,
	anchorID string,
) (bool, error) {
	if head == anchorID {
		return false, nil
	}
	restored, err := durability.HeadRestoresAnchor(
		ctx,
		runtime.trajectories,
		trajectoryID,
		head,
		anchorID,
	)
	if err != nil {
		return false, err
	}
	return !restored, nil
}

func (runtime *Runtime) prepareTrajectoryHeadMove(
	snapshot *registrySnapshot,
	trajectoryID string,
	head string,
	anchorID string,
	kind sdk.TrajectoryKind,
) (trajectoryHeadMove, postCommitEventBundle, error) {
	move, err := newTrajectoryHeadMove(
		head,
		anchorID,
		kind,
		snapshotGeneration(snapshot),
		time.Time{},
	)
	if err != nil {
		return trajectoryHeadMove{}, nil, err
	}
	event, err := move.eventPlan(runtime, snapshot, trajectoryID)
	if err != nil {
		return trajectoryHeadMove{}, nil, err
	}
	return move, postCommitEventBundle{event}, nil
}

type trajectoryHeadMove struct {
	Entry sdk.TrajectoryEntry
	From  string
	To    string
}

func newPayloadTrajectoryEntry(
	parentID string,
	kind sdk.TrajectoryKind,
	generation uint64,
	at time.Time,
	payload any,
) (sdk.TrajectoryEntry, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return sdk.TrajectoryEntry{}, fmt.Errorf(
			"encode %s trajectory entry: %w",
			kind,
			err,
		)
	}
	return sdk.TrajectoryEntry{
		ID:         sdk.NewID(),
		ParentID:   parentID,
		Kind:       kind,
		Timestamp:  at,
		Generation: generation,
		Payload:    raw,
	}, nil
}

func newTrajectoryHeadMove(
	from string,
	to string,
	kind sdk.TrajectoryKind,
	generation uint64,
	at time.Time,
) (trajectoryHeadMove, error) {
	entry, err := newPayloadTrajectoryEntry(
		to,
		kind,
		generation,
		at,
		map[string]string{
			"from": from,
			"to":   to,
		},
	)
	if err != nil {
		return trajectoryHeadMove{}, err
	}
	return trajectoryHeadMove{
		Entry: entry,
		From:  from,
		To:    to,
	}, nil
}

func (move trajectoryHeadMove) eventName() (string, error) {
	switch move.Entry.Kind {
	case sdk.TrajectoryKindRestore:
		return sdk.EventTrajectoryRestore, nil
	case sdk.TrajectoryKindRollback:
		return sdk.EventTrajectoryRollback, nil
	default:
		return "", fmt.Errorf(
			"trajectory head move entry %q has unsupported kind %q",
			move.Entry.ID,
			move.Entry.Kind,
		)
	}
}

func trajectoryAppendEventPayload(
	trajectoryID string,
	entry sdk.TrajectoryEntry,
) sdk.TrajectoryEventPayload {
	return sdk.TrajectoryEventPayload{
		TrajectoryID: trajectoryID,
		EntryID:      entry.ID,
		EntryKind:    entry.Kind,
		Generation:   entry.Generation,
	}
}

func (move trajectoryHeadMove) eventPayload(
	trajectoryID string,
) sdk.TrajectoryEventPayload {
	payload := trajectoryAppendEventPayload(trajectoryID, move.Entry)
	payload.From = move.From
	payload.To = move.To
	return payload
}

func (move trajectoryHeadMove) eventPlan(
	runtime *Runtime,
	snapshot *registrySnapshot,
	trajectoryID string,
) (postCommitEventPlan, error) {
	eventName, err := move.eventName()
	if err != nil {
		return postCommitEventPlan{}, err
	}
	return runtime.prepareTrajectoryEventPlan(
		snapshot,
		eventName,
		move.eventPayload(trajectoryID),
	)
}

func (runtime *Runtime) RollbackTrajectory(
	ctx context.Context,
	id string,
	checkpointID string,
) error {
	_, _, events, err := runtime.rollbackTrajectory(ctx, id, checkpointID)
	if err != nil {
		return err
	}
	defer events.release()
	events.dispatch(ctx, runtime)
	return nil
}

func (runtime *Runtime) rollbackTrajectory(
	ctx context.Context,
	id string,
	checkpointID string,
) (string, *durability.Checkpoint, retainedPostCommitEventBundle, error) {
	if runtime == nil {
		return "", nil, retainedPostCommitEventBundle{}, errors.New("runtime is nil")
	}
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
	defer releaseWork()
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
	eventLease := lease
	defer func() {
		if eventLease != nil {
			eventLease.release()
		}
	}()
	metadata, err := runtime.trajectories.LoadMetadata(ctx, id)
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
	target, err := runtime.trajectories.LoadEntry(ctx, id, checkpointID)
	if errors.Is(err, sdk.ErrTrajectoryEntryNotFound) {
		return "", nil, retainedPostCommitEventBundle{}, fmt.Errorf(
			"trajectory %q checkpoint %q not found",
			id,
			checkpointID,
		)
	}
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
	if target.Kind != sdk.TrajectoryKindCheckpoint {
		return "", nil, retainedPostCommitEventBundle{}, fmt.Errorf(
			"trajectory entry %q is %q, not a checkpoint",
			checkpointID,
			target.Kind,
		)
	}
	checkpoint, err := durability.DecodeCheckpoint(id, target)
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
	head, events, err := runtime.commitTrajectoryHeadMove(
		ctx,
		eventLease.snapshot,
		id,
		metadata.Head,
		checkpointID,
		sdk.TrajectoryKindRollback,
	)
	if err != nil {
		return "", nil, retainedPostCommitEventBundle{}, err
	}
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
	retained := retainedPostCommitEventBundle{
		events: events,
		lease:  eventLease,
	}
	eventLease = nil
	return head, checkpoint, retained, nil
}

func (session *Session) Rollback(
	ctx context.Context,
	checkpointID string,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	head, checkpoint, events, err := session.runtime.rollbackTrajectory(
		ctx,
		session.config.ID,
		checkpointID,
	)
	if err != nil {
		return err
	}
	defer events.release()
	session.applyCheckpointProjection(checkpoint)
	session.head = head
	events.dispatch(ctx, session.runtime)
	return nil
}

func (session *Session) appendTrajectory(
	ctx context.Context,
	snapshot *registrySnapshot,
	kind sdk.TrajectoryKind,
	payload any,
) error {
	return session.appendTrajectoryWithAudit(
		ctx,
		snapshot,
		kind,
		payload,
		nil,
	)
}

func (session *Session) appendTrajectoryWithAudit(
	ctx context.Context,
	snapshot *registrySnapshot,
	kind sdk.TrajectoryKind,
	payload any,
	audit []sdk.EventAudit,
	extraEvents ...postCommitEventPlan,
) error {
	return session.appendTrajectoryState(
		ctx,
		snapshot,
		kind,
		payload,
		"",
		"",
		audit,
		extraEvents...,
	)
}

func (session *Session) appendTrajectoryWithExecutionEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	kind sdk.TrajectoryKind,
	payload any,
	audit []sdk.EventAudit,
	eventName string,
	eventPayload any,
) error {
	return session.appendTrajectoryStateWithExecutionEvent(
		ctx,
		snapshot,
		kind,
		payload,
		"",
		"",
		audit,
		eventName,
		eventPayload,
	)
}

func (session *Session) appendTrajectoryStateWithExecutionEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	kind sdk.TrajectoryKind,
	payload any,
	state sdk.TrajectoryExecutionState,
	executionError string,
	audit []sdk.EventAudit,
	eventName string,
	eventPayload any,
) error {
	event, err := session.prepareExecutionEventPlan(
		snapshot,
		eventName,
		eventPayload,
	)
	if err != nil {
		return err
	}
	return session.appendTrajectoryState(
		ctx,
		snapshot,
		kind,
		payload,
		state,
		executionError,
		audit,
		event,
	)
}

func (session *Session) appendTrajectoryState(
	ctx context.Context,
	snapshot *registrySnapshot,
	kind sdk.TrajectoryKind,
	payload any,
	state sdk.TrajectoryExecutionState,
	executionError string,
	audit []sdk.EventAudit,
	extraEvents ...postCommitEventPlan,
) error {
	if snapshot == nil {
		return errors.New("trajectory append snapshot is nil")
	}
	entry, err := newPayloadTrajectoryEntry(
		session.head,
		kind,
		snapshot.generation,
		time.Time{},
		payload,
	)
	if err != nil {
		return err
	}
	entry.Fields = durability.EntryFields(payload)
	entry.Audit = sdk.CloneEventAudits(audit)
	eventPayload := trajectoryAppendEventPayload(session.config.ID, entry)
	executionID, token := session.activeExecution()
	appendEvent, err := session.runtime.prepareTrajectoryEventPlan(
		snapshot,
		sdk.EventTrajectoryAppend,
		eventPayload,
	)
	if err != nil {
		return err
	}
	events := append(postCommitEventBundle{appendEvent}, extraEvents...)
	if executionID != "" && token != "" {
		if err := session.commitExecution(
			ctx,
			entry,
			state,
			executionError,
			events,
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
		head, appendErr := session.runtime.appendTrajectoryEntries(
			ctx,
			session.config.ID,
			session.head,
			[]sdk.TrajectoryEntry{entry},
			events,
		)
		if appendErr != nil {
			return fmt.Errorf("append %s trajectory entry: %w", kind, appendErr)
		}
		session.head = head
		events.dispatch(ctx, session.runtime)
	}
	return nil
}

func trajectoryAudits(audits ...sdk.EventAudit) []sdk.EventAudit {
	result := make([]sdk.EventAudit, 0, len(audits))
	for _, audit := range audits {
		if !hasTrajectoryAudit(audit) {
			continue
		}
		result = append(result, sdk.CloneEventAudit(audit))
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func hasTrajectoryAudit(audit sdk.EventAudit) bool {
	if audit.EventID == "" && audit.EventName == "" {
		return false
	}
	if len(audit.Steps) > 0 {
		return true
	}
	return audit.Resolution.Outcome != "" &&
		audit.Resolution.Outcome != sdk.EffectResolutionNoEffect
}

func (runtime *Runtime) appendTrajectoryEntries(
	ctx context.Context,
	trajectoryID string,
	expectedHead string,
	entries []sdk.TrajectoryEntry,
	events stateMutationDeliverySource,
) (string, error) {
	mutationOutbox, err := runtime.stateMutationHostOutbox(events)
	if err != nil {
		return "", err
	}
	if runtime.atomicState != nil {
		result, err := runtime.atomicState.AppendTrajectory(
			ctx,
			sdk.TrajectoryAppendCommit{
				TrajectoryID: trajectoryID,
				ExpectedHead: expectedHead,
				Entries:      entries,
				Outbox:       mutationOutbox,
			},
		)
		return result.Trajectory.Head, err
	}
	return runtime.trajectories.Append(
		ctx,
		trajectoryID,
		expectedHead,
		entries...,
	)
}

func (session *Session) checkpointTrajectory(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	result Result,
	action sdk.Action,
	system string,
	dependencies ...string,
) error {
	return session.checkpointTrajectoryWithAudit(
		ctx,
		snapshot,
		messages,
		result,
		action,
		system,
		nil,
		dependencies...,
	)
}

func (session *Session) checkpointTrajectoryWithAudit(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	result Result,
	action sdk.Action,
	system string,
	audit []sdk.EventAudit,
	dependencies ...string,
) error {
	err := session.appendTrajectoryWithAudit(
		ctx,
		snapshot,
		sdk.TrajectoryKindCheckpoint,
		durability.Checkpoint{
			Messages:   sdk.CloneMessages(messages),
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
		audit,
	)
	if err == nil {
		session.config.System = system
	}
	return err
}
