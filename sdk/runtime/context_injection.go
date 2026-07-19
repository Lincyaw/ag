package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

var errContextInjectionInterrupt = errors.New("interrupted by context injection")

type contextInjectionDrainBoundary uint8

const (
	contextInjectionBeforeProvider contextInjectionDrainBoundary = iota
	contextInjectionAfterTurn
)

type contextInjectionInterruptSlot struct {
	mu          sync.Mutex
	token       uint64
	executionID string
	cancel      context.CancelCauseFunc
}

// EnqueueContextInjection schedules model-visible context for the next execution
// boundary owned by this session.
func (session *Session) EnqueueContextInjection(
	ctx context.Context,
	injection sdk.ContextInjection,
) error {
	if session == nil {
		return errors.New("session is nil")
	}
	if session.runtime == nil {
		return errors.New("session runtime is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	releaseWork, err := session.runtime.beginTrajectoryWork()
	if err != nil {
		return err
	}
	defer releaseWork()
	executionID, _ := session.activeExecution()
	queued, err := session.enqueueContextInjectionForExecution(ctx, executionID, injection)
	if err != nil {
		return err
	}
	if queued.Priority == sdk.ContextInjectionNow {
		session.signalContextInjectionInterrupt(executionID)
	}
	return nil
}

// EnqueueContextInjection schedules model-visible context for an execution.
// The payload is persisted before any live-host interrupt is attempted, so a
// missing local host no longer makes the injection disappear.
func (runtime *Runtime) EnqueueContextInjection(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) error {
	if runtime == nil {
		return errors.New("runtime is nil")
	}
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return err
	}
	defer releaseWork()
	if err := runtime.validateContextInjectionExecutionTarget(
		ctx,
		trajectoryID,
		executionID,
	); err != nil {
		return err
	}
	normalized, err := normalizeContextInjectionForExecution(
		trajectoryID,
		executionID,
		injection,
	)
	if err != nil {
		return err
	}
	if err := runtime.contextInjections.Enqueue(ctx, normalized); err != nil {
		return err
	}
	err = runtime.trajectoryExecution.enqueueHostedContext(
		ctx,
		trajectoryID,
		executionID,
		normalized,
	)
	if errors.Is(err, ErrExecutionNotFound) {
		return nil
	}
	return err
}

func (control ExecutionControl) EnqueueContextInjection(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) error {
	if control.runtime != nil {
		return control.runtime.EnqueueContextInjection(
			ctx,
			trajectoryID,
			executionID,
			injection,
		)
	}
	return control.lifecycle.EnqueueContextInjection(
		ctx,
		trajectoryID,
		executionID,
		injection,
	)
}

// EnqueueContextInjectionView schedules model-visible context and returns the
// current execution view after the durable enqueue boundary.
func (control ExecutionControl) EnqueueContextInjectionView(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) (ExecutionView, error) {
	if err := control.EnqueueContextInjection(
		ctx,
		trajectoryID,
		executionID,
		injection,
	); err != nil {
		return ExecutionView{}, err
	}
	return control.LoadView(ctx, trajectoryID)
}

func (lifecycle ExecutionLifecycle) EnqueueContextInjection(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) error {
	if lifecycle.store == nil {
		return errors.New("execution lifecycle store is nil")
	}
	if lifecycle.contexts == nil {
		return errors.New("execution lifecycle context injection store is nil")
	}
	if err := validateContextInjectionExecutionStoreTarget(
		ctx,
		lifecycle.store,
		trajectoryID,
		executionID,
	); err != nil {
		return err
	}
	normalized, err := normalizeContextInjectionForExecution(
		trajectoryID,
		executionID,
		injection,
	)
	if err != nil {
		return err
	}
	return lifecycle.contexts.Enqueue(ctx, normalized)
}

func (session *Session) enqueueHostedContextInjection(
	ctx context.Context,
	executionID string,
	injection sdk.ContextInjection,
) error {
	if session == nil {
		return errors.New("session is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	activeID, _ := session.activeExecution()
	if activeID != executionID {
		return fmt.Errorf(
			"%w: trajectory %s execution %s is not active",
			ErrExecutionNotFound,
			session.config.ID,
			executionID,
		)
	}
	queued, err := session.enqueueContextInjectionForExecution(ctx, executionID, injection)
	if err != nil {
		return err
	}
	if queued.Priority == sdk.ContextInjectionNow {
		session.signalContextInjectionInterrupt(executionID)
	}
	return nil
}

func (session *Session) enqueueContextInjectionForExecution(
	ctx context.Context,
	executionID string,
	injection sdk.ContextInjection,
) (sdk.ContextInjection, error) {
	if session.runtime.contextInjections == nil {
		return sdk.ContextInjection{}, errors.New(
			"context injection store is nil",
		)
	}
	normalized, err := normalizeContextInjectionForExecution(
		session.config.ID,
		executionID,
		injection,
	)
	if err != nil {
		return sdk.ContextInjection{}, err
	}
	if err := session.runtime.contextInjections.Enqueue(
		ctx,
		normalized,
	); err != nil {
		return sdk.ContextInjection{}, err
	}
	return sdk.CloneContextInjection(normalized), nil
}

func normalizeContextInjectionForExecution(
	sessionID string,
	executionID string,
	injection sdk.ContextInjection,
) (sdk.ContextInjection, error) {
	normalized, err := sdk.NormalizeContextInjection(injection, time.Now().UTC())
	if err != nil {
		return sdk.ContextInjection{}, err
	}
	if err := validateContextInjectionTarget(sessionID, normalized); err != nil {
		return sdk.ContextInjection{}, err
	}
	if normalized.TargetSessionID == "" {
		normalized.TargetSessionID = sessionID
	}
	if executionID != "" {
		if normalized.TargetExecutionID == "" {
			normalized.TargetExecutionID = executionID
		} else if normalized.TargetExecutionID != executionID {
			return sdk.ContextInjection{}, fmt.Errorf(
				"context injection targets execution %q, not %q",
				normalized.TargetExecutionID,
				executionID,
			)
		}
	}
	return sdk.CloneContextInjection(normalized), nil
}

func (runtime *Runtime) validateContextInjectionExecutionTarget(
	ctx context.Context,
	trajectoryID string,
	executionID string,
) error {
	return validateContextInjectionExecutionStoreTarget(
		ctx,
		runtime.trajectories,
		trajectoryID,
		executionID,
	)
}

func validateContextInjectionExecutionStoreTarget(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	executionID string,
) error {
	if executionID == "" {
		return errors.New("context injection execution ID is empty")
	}
	if store == nil {
		return errors.New("context injection trajectory store is nil")
	}
	metadata, err := store.LoadMetadata(ctx, trajectoryID)
	if err != nil {
		return err
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != executionID ||
		metadata.Execution.Terminal() {
		return fmt.Errorf(
			"%w: trajectory %s execution %s is not active",
			ErrExecutionNotFound,
			trajectoryID,
			executionID,
		)
	}
	return nil
}

func (session *Session) pendingContextInjections(
	ctx context.Context,
	boundary contextInjectionDrainBoundary,
) ([]sdk.ContextInjection, error) {
	if session.runtime.contextInjections == nil {
		return nil, errors.New("context injection store is nil")
	}
	executionID, _ := session.activeExecution()
	listed, err := session.runtime.contextInjections.List(
		ctx,
		sdk.ContextInjectionQuery{
			TargetSessionID:   session.config.ID,
			TargetExecutionID: executionID,
		},
	)
	if err != nil {
		return nil, err
	}
	if len(listed) == 0 {
		return nil, nil
	}
	consumed := session.consumedContextInjectionSet()
	drained := make([]sdk.ContextInjection, 0, len(listed))
	for rank := 0; rank <= 2; rank++ {
		for _, injection := range listed {
			if contextInjectionPriorityRank(
				injection.Priority,
			) != rank ||
				!contextInjectionMatchesSession(injection, session.config.ID) ||
				!contextInjectionMatchesExecution(injection, executionID) ||
				!contextInjectionEligible(injection, boundary) {
				continue
			}
			if _, ok := consumed[injection.ID]; ok {
				continue
			}
			drained = append(drained, sdk.CloneContextInjection(injection))
		}
	}
	if len(drained) == 0 {
		return nil, nil
	}
	return drained, nil
}

func contextInjectionMatchesExecution(
	injection sdk.ContextInjection,
	executionID string,
) bool {
	target := injection.TargetExecutionID
	return target == "" || target == executionID
}

func contextInjectionMatchesSession(
	injection sdk.ContextInjection,
	sessionID string,
) bool {
	return injection.TargetSessionID == "" ||
		injection.TargetSessionID == sessionID
}

func validateContextInjectionTarget(
	sessionID string,
	injection sdk.ContextInjection,
) error {
	if contextInjectionMatchesSession(injection, sessionID) {
		return nil
	}
	return fmt.Errorf(
		"context injection targets session %q, not %q",
		injection.TargetSessionID,
		sessionID,
	)
}

func contextInjectionEligible(
	injection sdk.ContextInjection,
	boundary contextInjectionDrainBoundary,
) bool {
	switch boundary {
	case contextInjectionBeforeProvider:
		return injection.Priority == sdk.ContextInjectionNow ||
			injection.Priority == sdk.ContextInjectionNext
	case contextInjectionAfterTurn:
		return true
	default:
		return false
	}
}

func contextInjectionPriorityRank(
	priority sdk.ContextInjectionPriority,
) int {
	switch priority {
	case sdk.ContextInjectionNow:
		return 0
	case sdk.ContextInjectionNext:
		return 1
	case sdk.ContextInjectionLater:
		return 2
	default:
		return 1
	}
}

func (execution *promptExecution) checkpointQueuedContext(
	ctx context.Context,
	snapshot *registrySnapshot,
	boundary contextInjectionDrainBoundary,
) (bool, error) {
	injections, err := execution.session.pendingContextInjections(ctx, boundary)
	if err != nil {
		return false, err
	}
	if len(injections) == 0 {
		return false, nil
	}
	injectedMessages := messagesFromContextInjections(injections)
	baseLength := len(execution.messages)
	execution.messages = append(execution.messages, injectedMessages...)
	action := sdk.Action{
		Kind:     sdk.ActionInject,
		Messages: injectedMessages,
	}
	err = execution.session.checkpointTrajectory(
		ctx,
		snapshot,
		trajectoryCheckpointCommit{
			Messages:          execution.messages,
			Result:            execution.result,
			Action:            action,
			System:            execution.system,
			ContextInjections: sdk.CloneContextInjections(injections),
			Dependencies:      execution.dependencies,
		},
	)
	if err != nil {
		execution.messages = execution.messages[:baseLength]
		return false, err
	}
	execution.session.markContextInjectionsConsumed(injections)
	execution.result.ContextInjections = execution.session.contextInjectionProjection(nil)
	execution.session.applyMessageProjection(execution.messages)
	return true, nil
}

func messagesFromContextInjections(
	injections []sdk.ContextInjection,
) []sdk.Message {
	count := 0
	for _, injection := range injections {
		count += len(injection.Messages)
	}
	messages := make([]sdk.Message, 0, count)
	for _, injection := range injections {
		messages = append(
			messages,
			sdk.CloneMessages(injection.Messages)...,
		)
	}
	return messages
}

func actionFinal(action sdk.Action) bool {
	return action.Cause != nil && action.Cause.Final
}

func actionDrainsAfterTurn(action sdk.Action) bool {
	return action.Kind == sdk.ActionStep ||
		(action.Kind == sdk.ActionStop && !actionFinal(action))
}

func (session *Session) contextInjectionInterruptContext(
	ctx context.Context,
	enabled bool,
) (context.Context, func()) {
	if !enabled {
		return ctx, func() {}
	}
	executionID, _ := session.activeExecution()
	if executionID == "" {
		return ctx, func() {}
	}
	interruptCtx, cancel := context.WithCancelCause(ctx)
	token := session.contextInterrupt.install(executionID, cancel)
	return interruptCtx, func() {
		session.contextInterrupt.clear(executionID, token)
		cancel(nil)
	}
}

func (slot *contextInjectionInterruptSlot) install(
	executionID string,
	cancel context.CancelCauseFunc,
) uint64 {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	slot.token++
	slot.executionID = executionID
	slot.cancel = cancel
	return slot.token
}

func (slot *contextInjectionInterruptSlot) clear(
	executionID string,
	token uint64,
) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.executionID != executionID ||
		slot.token != token {
		return
	}
	slot.executionID = ""
	slot.cancel = nil
}

func (session *Session) clearContextInjectionInterruptExecution(
	executionID string,
) {
	session.contextInterrupt.clearExecution(executionID)
}

func (slot *contextInjectionInterruptSlot) clearExecution(executionID string) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.executionID != executionID {
		return
	}
	slot.executionID = ""
	slot.cancel = nil
}

func (session *Session) signalContextInjectionInterrupt(
	executionID string,
) bool {
	return session.contextInterrupt.signal(executionID)
}

func (slot *contextInjectionInterruptSlot) signal(executionID string) bool {
	if executionID == "" {
		return false
	}
	slot.mu.Lock()
	cancel := slot.cancel
	if slot.executionID != executionID {
		cancel = nil
	}
	slot.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel(errContextInjectionInterrupt)
	return true
}
