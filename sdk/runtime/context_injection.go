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

type queuedContextInjection struct {
	injection   sdk.ContextInjection
	executionID string
	sequence    uint64
}

type contextInjectionQueue struct {
	mu       sync.Mutex
	sequence uint64
	items    []queuedContextInjection
}

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
	queued, err := session.contextQueue.enqueue(
		session.config.ID,
		injection,
	)
	if err != nil {
		return err
	}
	if queued.Priority == sdk.ContextInjectionNow {
		executionID, _ := session.activeExecution()
		session.signalContextInjectionInterrupt(executionID)
	}
	return nil
}

// EnqueueContextInjection schedules model-visible context for a live hosted
// execution. State-only controls cannot provide this boundary because the
// injection queue is intentionally owned by the running session.
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
	return runtime.trajectoryExecution.enqueueHostedContext(
		ctx,
		trajectoryID,
		executionID,
		injection,
	)
}

func (control ExecutionControl) EnqueueContextInjection(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) error {
	if control.runtime == nil {
		return errors.New("execution control runtime is nil")
	}
	return control.runtime.EnqueueContextInjection(
		ctx,
		trajectoryID,
		executionID,
		injection,
	)
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
	queued, err := session.contextQueue.enqueueForExecution(
		session.config.ID,
		executionID,
		injection,
	)
	if err != nil {
		return err
	}
	if queued.Priority == sdk.ContextInjectionNow {
		session.signalContextInjectionInterrupt(executionID)
	}
	return nil
}

func (queue *contextInjectionQueue) enqueue(
	sessionID string,
	injection sdk.ContextInjection,
) (sdk.ContextInjection, error) {
	return queue.enqueueForExecution(sessionID, "", injection)
}

func (queue *contextInjectionQueue) enqueueForExecution(
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
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.sequence++
	queue.items = append(queue.items, queuedContextInjection{
		injection:   normalized,
		executionID: executionID,
		sequence:    queue.sequence,
	})
	return sdk.CloneContextInjection(normalized), nil
}

func (queue *contextInjectionQueue) drain(
	boundary contextInjectionDrainBoundary,
	sessionID string,
	executionID string,
) []queuedContextInjection {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items) == 0 {
		return nil
	}
	drained := make([]queuedContextInjection, 0, len(queue.items))
	drainedSequences := make(map[uint64]struct{})
	for rank := 0; rank <= 2; rank++ {
		for _, item := range queue.items {
			if contextInjectionPriorityRank(
				item.injection.Priority,
			) != rank ||
				!contextInjectionMatchesSession(item.injection, sessionID) ||
				!contextInjectionMatchesExecution(item, executionID) ||
				!contextInjectionEligible(item.injection, boundary) {
				continue
			}
			drained = append(drained, item)
			drainedSequences[item.sequence] = struct{}{}
		}
	}
	if len(drained) == 0 {
		return nil
	}
	kept := queue.items[:0]
	for _, item := range queue.items {
		if _, ok := drainedSequences[item.sequence]; ok {
			continue
		}
		kept = append(kept, item)
	}
	queue.items = kept
	return cloneQueuedContextInjections(drained)
}

func (queue *contextInjectionQueue) restoreFront(
	injections []queuedContextInjection,
) {
	if len(injections) == 0 {
		return
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	restored := make([]queuedContextInjection, len(injections))
	for index, injection := range injections {
		queue.sequence++
		restored[index] = queuedContextInjection{
			injection:   sdk.CloneContextInjection(injection.injection),
			executionID: injection.executionID,
			sequence:    queue.sequence,
		}
	}
	queue.items = append(restored, queue.items...)
}

func (queue *contextInjectionQueue) discardExecution(executionID string) {
	if executionID == "" {
		return
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items) == 0 {
		return
	}
	kept := make([]queuedContextInjection, 0, len(queue.items))
	for _, item := range queue.items {
		if item.executionID == executionID {
			continue
		}
		kept = append(kept, item)
	}
	queue.items = kept
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

func cloneQueuedContextInjections(
	injections []queuedContextInjection,
) []queuedContextInjection {
	if len(injections) == 0 {
		return nil
	}
	result := make([]queuedContextInjection, len(injections))
	for index, injection := range injections {
		result[index] = queuedContextInjection{
			injection:   sdk.CloneContextInjection(injection.injection),
			executionID: injection.executionID,
			sequence:    injection.sequence,
		}
	}
	return result
}

func contextInjectionMatchesExecution(
	injection queuedContextInjection,
	executionID string,
) bool {
	return injection.executionID == "" || injection.executionID == executionID
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
	executionID, _ := execution.session.activeExecution()
	injections := execution.session.contextQueue.drain(
		boundary,
		execution.session.config.ID,
		executionID,
	)
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
	err := execution.session.checkpointTrajectory(
		ctx,
		snapshot,
		trajectoryCheckpointCommit{
			Messages:          execution.messages,
			Result:            execution.result,
			Action:            action,
			System:            execution.system,
			ContextInjections: contextInjectionsFromQueued(injections),
			Dependencies:      execution.dependencies,
		},
	)
	if err != nil {
		execution.messages = execution.messages[:baseLength]
		execution.session.contextQueue.restoreFront(injections)
		return false, err
	}
	execution.session.applyMessageProjection(execution.messages)
	return true, nil
}

func contextInjectionsFromQueued(
	injections []queuedContextInjection,
) []sdk.ContextInjection {
	if len(injections) == 0 {
		return nil
	}
	result := make([]sdk.ContextInjection, len(injections))
	for index, injection := range injections {
		result[index] = sdk.CloneContextInjection(injection.injection)
	}
	return result
}

func messagesFromContextInjections(
	injections []queuedContextInjection,
) []sdk.Message {
	count := 0
	for _, injection := range injections {
		count += len(injection.injection.Messages)
	}
	messages := make([]sdk.Message, 0, count)
	for _, injection := range injections {
		messages = append(
			messages,
			sdk.CloneMessages(injection.injection.Messages)...,
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
