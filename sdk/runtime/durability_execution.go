package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

type executionAcceptance struct {
	Input     durability.ExecutionInput
	Execution sdk.TrajectoryExecution
}

func (session *Session) beginExecution(
	ctx context.Context,
	userMessage sdk.Message,
) (executionAcceptance, error) {
	lease, err := session.acquireSnapshot()
	if err != nil {
		return executionAcceptance{}, err
	}
	defer lease.release()
	executionSnapshot, environment, err := newExecutionEnvironment(
		session.runtime,
		lease.snapshot,
		session.config,
	)
	if err != nil {
		return executionAcceptance{}, err
	}
	generation := executionSnapshot.generation

	executionID := sdk.NewID()
	input := durability.NewExecutionInput(
		userMessage,
		environment,
		session.messages,
	)
	entry, err := newPayloadTrajectoryEntry(
		session.head,
		sdk.TrajectoryKindUserMessage,
		generation,
		time.Time{},
		input,
	)
	if err != nil {
		return executionAcceptance{}, err
	}
	entry.Fields.ExecutionID = executionID
	appendEvent, err := session.runtime.prepareTrajectoryEventPlan(
		executionSnapshot,
		sdk.EventTrajectoryAppend,
		trajectoryAppendEventPayload(session.config.ID, entry),
	)
	if err != nil {
		return executionAcceptance{}, err
	}
	events := postCommitEventBundle{appendEvent}
	metadata, err := session.runtime.beginTrajectoryExecution(
		ctx,
		sdk.ExecutionStartCommit{
			TrajectoryID: session.config.ID,
			ExpectedHead: session.head,
			Start: sdk.TrajectoryExecutionStart{
				ID:       executionID,
				Provider: session.config.Provider,
				System:   session.config.System,
				MaxTurns: effectiveMaxTurns(session.config.MaxTurns),
			},
			Input: entry,
		},
		events,
	)
	if err != nil {
		return executionAcceptance{}, fmt.Errorf(
			"begin trajectory execution: %w",
			err,
		)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != executionID ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		return executionAcceptance{}, fmt.Errorf(
			"%w: trajectory %s did not accept pending execution %s",
			sdk.ErrTrajectoryExecution,
			session.config.ID,
			executionID,
		)
	}
	session.head = metadata.Head
	events.dispatchAfterCommit(ctx, session.runtime)
	return executionAcceptance{
		Input:     input,
		Execution: *metadata.Execution,
	}, nil
}

func (session *Session) claimExecution(ctx context.Context) error {
	execution, err := session.runtime.trajectoryExecution.claim(
		ctx,
		session.runtime.trajectories,
		session.config.ID,
	)
	if err != nil {
		return fmt.Errorf("claim trajectory execution: %w", err)
	}
	session.executionMu.Lock()
	session.executionID = execution.ID
	session.executionToken = execution.LeaseToken
	session.executionMu.Unlock()
	return nil
}

func (session *Session) activeExecution() (string, string) {
	session.executionMu.Lock()
	defer session.executionMu.Unlock()
	return session.executionID, session.executionToken
}

func (session *Session) executionOperationKey(
	kind string,
	coordinate string,
) string {
	executionID, _ := session.activeExecution()
	if executionID == "" {
		return session.head
	}
	sum := sha256.Sum256([]byte(
		executionID + "\x00" + kind + "\x00" + coordinate,
	))
	return hex.EncodeToString(sum[:])
}

func (session *Session) clearExecution(executionID string, token string) {
	session.executionMu.Lock()
	cleared := false
	if session.executionID == executionID &&
		session.executionToken == token {
		session.executionID = ""
		session.executionToken = ""
		cleared = true
	}
	session.executionMu.Unlock()
	if cleared {
		session.clearContextInjectionInterruptExecution(executionID)
	}
}

func (session *Session) executionHeartbeat(
	parent context.Context,
) (context.Context, func() error) {
	ctx, cancel := context.WithCancelCause(parent)
	stopRuntimeCancel := session.runtime.trajectoryExecution.cancelAfter(cancel)
	done := make(chan struct{})
	lost := make(chan error, 1)
	executionID, token := session.activeExecution()
	unregister := session.runtime.trajectoryExecution.registerHosted(
		session.config.ID,
		executionID,
		cancel,
		func(ctx context.Context, injection sdk.ContextInjection) error {
			return session.notifyHostedContextInjection(
				ctx,
				executionID,
				injection,
			)
		},
	)
	go func() {
		defer close(done)
		defer session.recoverExecutionHeartbeatPanic(
			ctx,
			executionID,
			cancel,
			lost,
		)
		interval := session.runtime.trajectoryExecution.heartbeatInterval()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_, err := session.runtime.trajectoryExecution.renew(
					ctx,
					session.runtime.trajectories,
					session.config.ID,
					executionID,
					token,
					now.UTC(),
				)
				if err == nil {
					continue
				}
				activeID, activeToken := session.activeExecution()
				if activeID != executionID || activeToken != token {
					return
				}
				select {
				case lost <- err:
				default:
				}
				cancel(err)
				return
			}
		}
	}()
	return ctx, func() error {
		stopRuntimeCancel()
		cancel(context.Canceled)
		<-done
		unregister()
		select {
		case err := <-lost:
			return fmt.Errorf("trajectory execution lease lost: %w", err)
		default:
			return nil
		}
	}
}

func (session *Session) recoverExecutionHeartbeatPanic(
	ctx context.Context,
	executionID string,
	cancel context.CancelCauseFunc,
	lost chan<- error,
) {
	if recovered := recover(); recovered != nil {
		err := fmt.Errorf(
			"trajectory execution heartbeat panic: %v\n%s",
			recovered,
			debug.Stack(),
		)
		session.runtime.logger.ErrorContext(
			ctx,
			"trajectory execution heartbeat panic",
			"session_id",
			session.config.ID,
			"execution_id",
			executionID,
			"panic",
			recovered,
		)
		select {
		case lost <- err:
		default:
		}
		cancel(err)
	}
}

type claimedExecutionRunner func(context.Context) (Result, error)

func (session *Session) runClaimedExecution(
	ctx context.Context,
	run claimedExecutionRunner,
) (result Result, returnErr error) {
	executionCtx, stopHeartbeat := session.executionHeartbeat(ctx)
	defer func() {
		returnErr = errors.Join(returnErr, stopHeartbeat())
	}()
	defer func() {
		if returnErr == nil {
			return
		}
		failure := executionFailureForUnwind(returnErr, executionCtx)
		restoreCtx, cancel := lifecycle.WithDetachedFinalization(executionCtx)
		defer cancel()
		returnErr = errors.Join(
			returnErr,
			session.failExecution(restoreCtx, failure, result),
		)
	}()
	return run(executionCtx)
}

func executionFailureForUnwind(
	err error,
	executionCtx context.Context,
) error {
	if err == nil {
		return nil
	}
	cause := context.Cause(executionCtx)
	if cause == nil || errors.Is(err, cause) {
		return err
	}
	if errors.Is(err, context.Canceled) &&
		!errors.Is(cause, context.Canceled) &&
		!errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return errors.Join(err, cause)
}

func (session *Session) commitExecution(
	ctx context.Context,
	entry sdk.TrajectoryEntry,
	state sdk.TrajectoryExecutionState,
	executionError string,
	events postCommitEventBundle,
) error {
	executionID, token := session.activeExecution()
	if executionID == "" || token == "" {
		return errors.New("session has no claimed trajectory execution")
	}
	entry.Fields.ExecutionID = executionID
	commit := sdk.TrajectoryExecutionCommit{
		TrajectoryID: session.config.ID,
		ExecutionID:  executionID,
		LeaseToken:   token,
		ExpectedHead: session.head,
		State:        state,
		Error:        executionError,
	}
	if entry.ID != "" {
		commit.Entries = []sdk.TrajectoryEntry{entry}
	}
	metadata, err := session.runtime.commitTrajectoryExecution(
		ctx,
		commit,
		events,
	)
	if err != nil {
		return err
	}
	session.head = metadata.Head
	if state != "" && state != sdk.TrajectoryExecutionRunning {
		session.clearExecution(executionID, token)
	}
	events.dispatchAfterCommit(ctx, session.runtime)
	return nil
}

func (runtime *Runtime) beginTrajectoryExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
	events hostOutboxDeliverySource,
) (sdk.TrajectoryMetadata, error) {
	mutationOutbox, err := runtime.stateMutationHostOutbox(events)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if runtime.atomicState != nil {
		mutation := commit
		mutation.Outbox = mutationOutbox
		result, err := runtime.atomicState.StartExecution(ctx, mutation)
		return result.Trajectory, err
	}
	return runtime.trajectories.BeginExecution(
		ctx,
		commit.TrajectoryID,
		commit.ExpectedHead,
		commit.Start,
		commit.Input,
	)
}

func (runtime *Runtime) commitTrajectoryExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
	events hostOutboxDeliverySource,
) (sdk.TrajectoryMetadata, error) {
	mutationOutbox, err := runtime.stateMutationHostOutbox(events)
	if err != nil {
		return sdk.TrajectoryMetadata{}, err
	}
	if runtime.atomicState != nil {
		mutation := sdk.ExecutionMutationCommit{Trajectory: commit}
		mutation.Outbox = mutationOutbox
		result, err := runtime.atomicState.CommitExecution(
			ctx,
			mutation,
		)
		return result.Trajectory, err
	}
	return runtime.trajectories.CommitExecution(ctx, commit)
}

func (runtime *Runtime) cancelTrajectoryExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
	now time.Time,
) (ExecutionView, error) {
	var conflict error
	for attempt := 0; attempt < 2; attempt++ {
		view, err := runtime.cancelTrajectoryExecutionOnce(
			ctx,
			trajectoryID,
			executionID,
			reason,
			now,
		)
		if err == nil {
			return view, nil
		}
		if !errors.Is(err, sdk.ErrTrajectoryConflict) {
			return ExecutionView{}, err
		}
		conflict = err
	}
	return ExecutionView{}, conflict
}

func (runtime *Runtime) cancelTrajectoryExecutionOnce(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
	now time.Time,
) (ExecutionView, error) {
	metadata, err := runtime.trajectories.LoadMetadata(ctx, trajectoryID)
	if err != nil {
		return ExecutionView{}, err
	}
	if metadata.Execution == nil {
		return ExecutionView{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			trajectoryID,
		)
	}
	base, err := durability.LoadExecutionCompletionBase(
		ctx,
		runtime.trajectories,
		metadata,
	)
	if err != nil {
		return ExecutionView{}, err
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return ExecutionView{}, err
	}
	defer lease.release()
	cancellationResult, err := loadCancellationResult(
		ctx,
		runtime.trajectories,
		metadata,
	)
	if err != nil {
		return ExecutionView{}, err
	}
	end := agentEndPayloadForCancellation(
		reason,
		cancellationResult,
		base.Messages,
	)
	completion, err := newExecutionCompletionEntries(
		executionCompletionEntrySpec{
			From:        metadata.Head,
			BaseHead:    base.Head,
			ExecutionID: executionID,
			Generation:  snapshotGeneration(lease.snapshot),
			At:          now,
			End:         &end,
			RestoreHead: true,
		},
	)
	if err != nil {
		return ExecutionView{}, err
	}
	events, err := completion.eventBundle(
		executionCompletionEventSpec{
			runtime:             runtime,
			snapshot:            lease.snapshot,
			trajectoryID:        trajectoryID,
			end:                 &end,
			endDeliveryBoundary: runtime.mutationPostCommitDeliveryBoundary(),
		},
	)
	if err != nil {
		return ExecutionView{}, err
	}
	trajectoryCommit := sdk.TrajectoryExecutionCancelCommit{
		TrajectoryID: trajectoryID,
		ExecutionID:  executionID,
		ExpectedHead: metadata.Head,
		Reason:       reason,
		At:           now,
		Entries:      completion.Entries,
	}
	result, err := runtime.cancelTrajectoryExecutionMutation(
		ctx,
		trajectoryCommit,
		events,
	)
	if err != nil {
		return ExecutionView{}, err
	}
	if result.Changed {
		events.dispatchAfterCommit(ctx, runtime)
	}
	return LoadExecutionViewFromMetadata(ctx, runtime.trajectories, result.Trajectory)
}

func (runtime *Runtime) cancelTrajectoryExecutionMutation(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCancelCommit,
	events hostOutboxDeliverySource,
) (executionCancelMutationResult, error) {
	if runtime.atomicState == nil {
		result, err := runtime.trajectories.CancelExecution(ctx, commit)
		return executionCancelMutationResult{
			Trajectory: result.Trajectory,
			Changed:    result.Changed,
		}, err
	}
	mutationOutbox, err := runtime.stateMutationHostOutbox(events)
	if err != nil {
		return executionCancelMutationResult{}, err
	}
	result, err := runtime.atomicState.CancelExecution(
		ctx,
		sdk.ExecutionCancelCommit{
			TrajectoryID: commit.TrajectoryID,
			ExecutionID:  commit.ExecutionID,
			ExpectedHead: commit.ExpectedHead,
			Reason:       commit.Reason,
			At:           commit.At,
			Entries:      commit.Entries,
			Outbox:       mutationOutbox,
		},
	)
	return executionCancelMutationResult{
		Trajectory: result.Trajectory,
		Changed:    result.Changed,
	}, err
}

type executionCancelMutationResult struct {
	Trajectory sdk.TrajectoryMetadata
	Changed    bool
}

func (session *Session) failExecution(
	ctx context.Context,
	cause error,
	result Result,
) error {
	executionID, token := session.activeExecution()
	if executionID == "" || token == "" {
		return nil
	}
	metadata, err := session.runtime.trajectories.LoadMetadata(
		ctx,
		session.config.ID,
	)
	if err != nil {
		return fmt.Errorf("load trajectory for failure restore: %w", err)
	}
	_, checkpoint, err := durability.LatestCheckpoint(
		ctx,
		session.runtime.trajectories,
		metadata,
	)
	if err != nil {
		return err
	}
	base, err := durability.LoadExecutionCompletionBase(
		ctx,
		session.runtime.trajectories,
		metadata,
	)
	if err != nil {
		return err
	}
	headRestoresBase, err := durability.HeadRestoresAnchor(
		ctx,
		session.runtime.trajectories,
		metadata.ID,
		metadata.Head,
		base.Head,
	)
	if err != nil {
		return err
	}
	outcome := session.runtime.executionFailureOutcome(cause, headRestoresBase)
	plan, err := session.prepareExecutionFailurePlan(
		ctx,
		metadata,
		base,
		outcome,
		cause,
		result,
	)
	if err != nil {
		return err
	}
	defer plan.release()
	updated, err := session.runtime.commitTrajectoryExecution(
		ctx,
		plan.commit,
		plan.events,
	)
	if err != nil {
		return fmt.Errorf("fail trajectory execution: %w", err)
	}
	session.head = updated.Head
	session.applyExecutionBaseProjection(base, checkpoint)
	session.clearExecution(executionID, token)
	plan.events.dispatchAfterCommit(ctx, session.runtime)
	return nil
}

type executionFailurePlan struct {
	commit sdk.TrajectoryExecutionCommit
	events leasedPostCommitEventBundle
}

type executionFailureOutcome struct {
	state          sdk.TrajectoryExecutionState
	recordTerminal bool
	restoreHead    bool
}

func (plan *executionFailurePlan) release() {
	if plan == nil {
		return
	}
	plan.events.release()
}

func (session *Session) prepareExecutionFailurePlan(
	ctx context.Context,
	metadata sdk.TrajectoryMetadata,
	base durability.ExecutionCompletionBase,
	outcome executionFailureOutcome,
	cause error,
	result Result,
) (executionFailurePlan, error) {
	executionID, token := session.activeExecution()
	plan := executionFailurePlan{
		commit: sdk.TrajectoryExecutionCommit{
			TrajectoryID: session.config.ID,
			ExecutionID:  executionID,
			LeaseToken:   token,
			ExpectedHead: metadata.Head,
			State:        outcome.state,
		},
	}
	if cause != nil {
		plan.commit.Error = cause.Error()
	}

	var eventLease *snapshotLease
	var err error
	defer func() {
		if eventLease != nil {
			eventLease.release()
		}
	}()
	now := time.Now().UTC()
	if outcome.recordTerminal {
		lease, err := session.acquireSnapshot()
		if err != nil {
			return executionFailurePlan{}, err
		}
		eventLease = lease

		end := agentEndPayloadForFailure(
			result,
			base.Messages,
			cause,
			outcome.state,
		)
		completion, err := newExecutionCompletionEntries(
			executionCompletionEntrySpec{
				From:        metadata.Head,
				BaseHead:    base.Head,
				ExecutionID: executionID,
				Generation:  snapshotGeneration(eventLease.snapshot),
				At:          now,
				End:         &end,
				RestoreHead: outcome.restoreHead,
			},
		)
		if err != nil {
			return executionFailurePlan{}, err
		}
		plan.commit.Entries = append(
			plan.commit.Entries,
			completion.Entries...,
		)
		events, err := completion.eventBundle(
			executionCompletionEventSpec{
				runtime:             session.runtime,
				snapshot:            eventLease.snapshot,
				trajectoryID:        session.config.ID,
				end:                 &end,
				endDeliveryBoundary: session.executionPostCommitDeliveryBoundary(),
			},
		)
		if err != nil {
			return executionFailurePlan{}, err
		}
		plan.events.append(events...)
		plan.events.lease = eventLease
		eventLease = nil
		return plan, nil
	}
	if !outcome.restoreHead {
		return plan, nil
	}
	completion, err := newExecutionCompletionEntries(
		executionCompletionEntrySpec{
			From:        metadata.Head,
			BaseHead:    base.Head,
			ExecutionID: executionID,
			Generation:  0,
			At:          now,
			RestoreHead: true,
		},
	)
	if err != nil {
		return executionFailurePlan{}, err
	}
	plan.commit.Entries = append(plan.commit.Entries, completion.Entries...)
	plan.events.lease = eventLease
	eventLease = nil
	return plan, nil
}

func (runtime *Runtime) executionFailureOutcome(
	cause error,
	headRestoresBase bool,
) executionFailureOutcome {
	if runtime.executionRecoveryHandoffActive() {
		return executionFailureOutcome{
			state:       sdk.TrajectoryExecutionPending,
			restoreHead: !headRestoresBase,
		}
	}
	if errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		return executionFailureOutcome{
			state:          sdk.TrajectoryExecutionCancelled,
			recordTerminal: true,
			restoreHead:    true,
		}
	}
	return executionFailureOutcome{
		state:          sdk.TrajectoryExecutionFailed,
		recordTerminal: true,
		restoreHead:    true,
	}
}

func agentEndTrajectoryEntry(
	parentID string,
	generation uint64,
	now time.Time,
	end sdk.AgentEndPayload,
) (sdk.TrajectoryEntry, error) {
	entry, err := newPayloadTrajectoryEntry(
		parentID,
		sdk.TrajectoryKindTerminal,
		generation,
		now,
		end,
	)
	if err != nil {
		return sdk.TrajectoryEntry{}, err
	}
	entry.Fields = durability.EntryFields(end)
	return entry, nil
}

func executionRestoreHeadMove(
	from string,
	anchorID string,
	executionID string,
	generation uint64,
	now time.Time,
) (trajectoryHeadMove, error) {
	restore, err := newTrajectoryHeadMove(
		from,
		anchorID,
		sdk.TrajectoryKindRestore,
		generation,
		now,
	)
	if err != nil {
		return trajectoryHeadMove{}, err
	}
	restore.Entry.Fields.ExecutionID = executionID
	return restore, nil
}

func snapshotGeneration(snapshot *registrySnapshot) uint64 {
	if snapshot == nil {
		return 0
	}
	return snapshot.generation
}

func agentEndPayloadForFailure(
	result Result,
	fallbackMessages []sdk.Message,
	cause error,
	state sdk.TrajectoryExecutionState,
) sdk.AgentEndPayload {
	messages := sdk.CloneMessages(result.Messages)
	if len(messages) == 0 {
		messages = sdk.CloneMessages(fallbackMessages)
	}
	endCause := result.Cause
	if endCause.Code == "" {
		endCause.Code = sdk.CauseExecutionError
		if state == sdk.TrajectoryExecutionCancelled {
			endCause.Code = sdk.CauseCancelled
		}
	}
	if endCause.Detail == "" && cause != nil {
		endCause.Detail = cause.Error()
	}
	return agentEndPayloadFromResult(result, messages, endCause)
}

func agentEndPayloadForCancellation(
	reason string,
	result Result,
	messages []sdk.Message,
) sdk.AgentEndPayload {
	if len(result.Messages) > 0 {
		messages = result.Messages
	}
	return agentEndPayloadFromResult(
		result,
		messages,
		sdk.Cause{
			Code:   sdk.CauseCancelled,
			Detail: reason,
			Final:  true,
		},
	)
}

func loadCancellationResult(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (Result, error) {
	entry, checkpoint, found, err := durability.LatestExecutionCheckpoint(
		ctx,
		store,
		metadata,
	)
	if err != nil || !found {
		return Result{}, err
	}
	result := resultFromCheckpoint(entry, checkpoint)
	if result == nil {
		return Result{}, nil
	}
	return *result, nil
}

func agentEndPayloadFromResult(
	result Result,
	messages []sdk.Message,
	cause sdk.Cause,
) sdk.AgentEndPayload {
	messages = sdk.CloneMessages(messages)
	output := result.Output
	if output == "" {
		output = latestAssistantOutput(messages)
	}
	return sdk.AgentEndPayload{
		Messages: messages,
		ContextInjections: sdk.CloneContextInjections(
			result.ContextInjections,
		),
		Output:       output,
		Turns:        result.Turns,
		ToolCalls:    result.ToolCalls,
		InputTokens:  result.InputTokens,
		OutputTokens: result.OutputTokens,
		Cause:        cause,
	}
}

type executionCompletionEntrySpec struct {
	From        string
	BaseHead    string
	ExecutionID string
	Generation  uint64
	At          time.Time
	End         *sdk.AgentEndPayload
	RestoreHead bool
}

type executionCompletionEntries struct {
	Entries  []sdk.TrajectoryEntry
	Terminal sdk.TrajectoryEntry
	Restore  trajectoryHeadMove
}

type executionCompletionEventSpec struct {
	runtime             *Runtime
	snapshot            *registrySnapshot
	trajectoryID        string
	end                 *sdk.AgentEndPayload
	endDeliveryBoundary postCommitDeliveryBoundary
}

func newExecutionCompletionEntries(
	spec executionCompletionEntrySpec,
) (executionCompletionEntries, error) {
	if spec.At.IsZero() {
		spec.At = time.Now().UTC()
	} else {
		spec.At = spec.At.UTC()
	}
	completion := executionCompletionEntries{}
	restoreFrom := spec.From
	if spec.End != nil {
		terminal, err := agentEndTrajectoryEntry(
			spec.From,
			spec.Generation,
			spec.At,
			*spec.End,
		)
		if err != nil {
			return executionCompletionEntries{}, err
		}
		completion.Terminal = terminal
		completion.Entries = append(completion.Entries, terminal)
		restoreFrom = terminal.ID
	}
	if spec.RestoreHead {
		restore, err := executionRestoreHeadMove(
			restoreFrom,
			spec.BaseHead,
			spec.ExecutionID,
			spec.Generation,
			spec.At,
		)
		if err != nil {
			return executionCompletionEntries{}, err
		}
		completion.Restore = restore
		completion.Entries = append(completion.Entries, restore.Entry)
	}
	return completion, nil
}

func (completion executionCompletionEntries) eventBundle(
	spec executionCompletionEventSpec,
) (postCommitEventBundle, error) {
	if spec.runtime == nil {
		return nil, errors.New("execution completion event runtime is nil")
	}
	var events postCommitEventBundle
	if completion.Terminal.ID != "" {
		appendEvent, err := spec.runtime.prepareTrajectoryEventPlan(
			spec.snapshot,
			sdk.EventTrajectoryAppend,
			trajectoryAppendEventPayload(
				spec.trajectoryID,
				completion.Terminal,
			),
		)
		if err != nil {
			return nil, err
		}
		events = append(events, appendEvent)
	}
	if completion.Restore.Entry.ID != "" {
		if spec.snapshot == nil {
			return nil, errors.New("execution restore event snapshot is nil")
		}
		restoreEvent, err := completion.Restore.eventPlan(
			spec.runtime,
			spec.snapshot,
			spec.trajectoryID,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, restoreEvent)
	}
	if spec.end != nil {
		endEvent, err := spec.runtime.prepareSessionEventPlan(
			spec.snapshot,
			sdk.EventAgentEnd,
			spec.trajectoryID,
			*spec.end,
			spec.endDeliveryBoundary,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, endEvent)
	}
	return events, nil
}

func executionStateForCause(cause sdk.Cause) sdk.TrajectoryExecutionState {
	switch cause.Code {
	case sdk.CauseProviderError, sdk.CauseHookError:
		return sdk.TrajectoryExecutionFailed
	case sdk.CauseCancelled:
		return sdk.TrajectoryExecutionCancelled
	default:
		return sdk.TrajectoryExecutionSucceeded
	}
}
