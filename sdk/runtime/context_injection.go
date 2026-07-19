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
	injection sdk.ContextInjection
	sequence  uint64
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

func (queue *contextInjectionQueue) enqueue(
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
		injection: normalized,
		sequence:  queue.sequence,
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
) []sdk.ContextInjection {
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
	result := make([]sdk.ContextInjection, len(drained))
	for index, item := range drained {
		result[index] = sdk.CloneContextInjection(item.injection)
	}
	return result
}

func (queue *contextInjectionQueue) restoreFront(
	injections []sdk.ContextInjection,
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
			injection: sdk.CloneContextInjection(injection),
			sequence:  queue.sequence,
		}
	}
	queue.items = append(restored, queue.items...)
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
	injections := execution.session.contextQueue.drain(boundary)
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
	injections []sdk.ContextInjection,
) []sdk.Message {
	count := 0
	for _, injection := range injections {
		count += len(injection.Messages)
	}
	messages := make([]sdk.Message, 0, count)
	for _, injection := range injections {
		messages = append(messages, sdk.CloneMessages(injection.Messages)...)
	}
	return messages
}

func actionFinal(action sdk.Action) bool {
	return action.Cause != nil && action.Cause.Final
}
