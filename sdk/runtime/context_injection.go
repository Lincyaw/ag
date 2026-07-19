package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type contextInjectionDrainBoundary uint8

const (
	contextInjectionBeforeProvider contextInjectionDrainBoundary = iota
	contextInjectionBeforeStop
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
	return session.contextQueue.enqueue(injection)
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
	return session.contextQueue.enqueueForExecution(executionID, injection)
}

func (queue *contextInjectionQueue) enqueue(
	injection sdk.ContextInjection,
) error {
	return queue.enqueueForExecution("", injection)
}

func (queue *contextInjectionQueue) enqueueForExecution(
	executionID string,
	injection sdk.ContextInjection,
) error {
	normalized, err := normalizeContextInjection(injection, time.Now().UTC())
	if err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.sequence++
	queue.items = append(queue.items, queuedContextInjection{
		injection:   normalized,
		executionID: executionID,
		sequence:    queue.sequence,
	})
	return nil
}

func normalizeContextInjection(
	injection sdk.ContextInjection,
	now time.Time,
) (sdk.ContextInjection, error) {
	if injection.ID == "" {
		injection.ID = sdk.NewID()
	} else if err := sdk.ValidateResourceName("context injection", injection.ID); err != nil {
		return sdk.ContextInjection{}, err
	}
	if injection.Priority == "" {
		injection.Priority = sdk.ContextInjectionNext
	}
	switch injection.Priority {
	case sdk.ContextInjectionNow,
		sdk.ContextInjectionNext,
		sdk.ContextInjectionLater:
	default:
		return sdk.ContextInjection{}, fmt.Errorf(
			"unknown context injection priority %q",
			injection.Priority,
		)
	}
	if injection.Mode == "" {
		injection.Mode = sdk.ContextInjectionPrompt
	}
	switch injection.Mode {
	case sdk.ContextInjectionPrompt,
		sdk.ContextInjectionHook,
		sdk.ContextInjectionPermission,
		sdk.ContextInjectionTaskNotification,
		sdk.ContextInjectionInterAgent,
		sdk.ContextInjectionLocalCommand,
		sdk.ContextInjectionSystem:
	default:
		return sdk.ContextInjection{}, fmt.Errorf(
			"unknown context injection mode %q",
			injection.Mode,
		)
	}
	if len(injection.Messages) == 0 {
		return sdk.ContextInjection{}, errors.New(
			"context injection contains no messages",
		)
	}
	if injection.CreatedAt.IsZero() {
		injection.CreatedAt = now
	} else {
		injection.CreatedAt = injection.CreatedAt.UTC()
	}
	return sdk.CloneContextInjection(injection), nil
}

func (queue *contextInjectionQueue) drain(
	boundary contextInjectionDrainBoundary,
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

func contextInjectionEligible(
	injection sdk.ContextInjection,
	boundary contextInjectionDrainBoundary,
) bool {
	switch boundary {
	case contextInjectionBeforeProvider:
		return injection.Priority == sdk.ContextInjectionNow ||
			injection.Priority == sdk.ContextInjectionNext
	case contextInjectionBeforeStop:
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
	injections := execution.session.contextQueue.drain(boundary, executionID)
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
		execution.messages,
		execution.result,
		action,
		execution.system,
		execution.dependencies...,
	)
	if err != nil {
		execution.messages = execution.messages[:baseLength]
		execution.session.contextQueue.restoreFront(injections)
		return false, err
	}
	execution.session.applyMessageProjection(execution.messages)
	return true, nil
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
