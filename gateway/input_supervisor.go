package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	GatewayEventInputQueued      = "gateway_input_queued"
	GatewayEventInputDispatching = "gateway_input_dispatching"
	GatewayEventInputStarted     = "gateway_input_started"
	GatewayEventInputCompleted   = "gateway_input_completed"
	GatewayEventSessionPaused    = "gateway_session_paused"
	GatewayEventSessionResumed   = "gateway_session_resumed"
	GatewayEventSessionUpdated   = "gateway_session_updated"
	defaultInputPoll             = 100 * time.Millisecond
)

var errInputDispatchPaused = errors.New("gateway input dispatch paused")

type inputSupervisor struct {
	inputs     InputStore
	sessions   SessionStore
	executions ExecutionBackend
	events     EventStore
	logger     *slog.Logger
	poll       time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	runners    map[string]struct{}
	wg         sync.WaitGroup
}

func newInputSupervisor(
	inputs InputStore,
	sessions SessionStore,
	executions ExecutionBackend,
	events EventStore,
) *inputSupervisor {
	ctx, cancel := context.WithCancel(context.Background())
	return &inputSupervisor{
		inputs: inputs, sessions: sessions, executions: executions,
		events: events, logger: slog.Default(), poll: defaultInputPoll,
		ctx: ctx, cancel: cancel, runners: make(map[string]struct{}),
	}
}

func (supervisor *inputSupervisor) schedule(sessionID string) {
	if supervisor == nil {
		return
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.ctx.Err() != nil {
		return
	}
	if _, exists := supervisor.runners[sessionID]; exists {
		return
	}
	supervisor.runners[sessionID] = struct{}{}
	supervisor.wg.Add(1)
	go supervisor.run(sessionID)
}

func (supervisor *inputSupervisor) recover(ctx context.Context) error {
	request := sdk.PageRequest{Limit: sdk.MaxPageSize}
	for {
		page, err := supervisor.sessions.List(ctx, request)
		if err != nil {
			return err
		}
		for _, session := range page.Items {
			if !session.Paused {
				supervisor.schedule(session.ID)
			}
		}
		if page.Next == "" {
			return nil
		}
		request.After = page.Next
	}
}

func (supervisor *inputSupervisor) run(sessionID string) {
	defer supervisor.wg.Done()
	for {
		session, err := supervisor.sessions.Get(supervisor.ctx, sessionID)
		if err != nil || session.Paused {
			supervisor.removeRunner(sessionID)
			if err != nil && !errors.Is(err, context.Canceled) &&
				!errors.Is(err, ErrStoreClosed) {
				supervisor.logger.Error(
					"load session for gateway input queue",
					"session_id", sessionID,
					"error", err,
				)
			}
			return
		}

		// Holding mu across the empty check closes the enqueue/schedule race:
		// either this runner sees the new input, or schedule observes removal.
		supervisor.mu.Lock()
		acquired, ok, err := supervisor.inputs.AcquireNext(
			supervisor.ctx,
			sessionID,
		)
		if err != nil || !ok {
			delete(supervisor.runners, sessionID)
			supervisor.mu.Unlock()
			if err != nil && !errors.Is(err, context.Canceled) &&
				!errors.Is(err, ErrStoreClosed) {
				supervisor.logger.Error(
					"acquire gateway input",
					"session_id", sessionID,
					"error", err,
				)
			}
			return
		}
		supervisor.mu.Unlock()

		supervisor.emit(sessionID, GatewayEventInputDispatching, acquired.Input)
		if err := supervisor.process(session, acquired); errors.Is(
			err,
			errInputDispatchPaused,
		) {
			supervisor.removeRunner(sessionID)
			return
		} else if err != nil && !errors.Is(err, context.Canceled) {
			supervisor.logger.Error(
				"dispatch gateway input",
				"session_id", sessionID,
				"input_id", acquired.Input.ID,
				"error", err,
			)
		}
		if supervisor.ctx.Err() != nil {
			supervisor.removeRunner(sessionID)
			return
		}
	}
}

func (supervisor *inputSupervisor) process(
	session Session,
	acquired AcquiredInput,
) error {
	input := acquired.Input
	if acquired.Resumed && input.ExecutionID != "" {
		return supervisor.awaitAndComplete(session, input, input.ExecutionID)
	}
	current, recovered, err := supervisor.waitForDispatchBoundary(
		session,
		input,
		acquired.Resumed && input.ExecutionID == "",
	)
	if err != nil {
		return supervisor.failInput(input, err)
	}
	if recovered {
		bound, err := supervisor.inputs.BindExecution(
			supervisor.ctx,
			input.SessionID,
			input.ID,
			current.Execution.ID,
		)
		if err != nil {
			return err
		}
		supervisor.emit(input.SessionID, GatewayEventInputStarted, bound)
		return supervisor.awaitAndComplete(
			session,
			bound,
			current.Execution.ID,
		)
	}
	latest, err := supervisor.sessions.Get(supervisor.ctx, session.ID)
	if err != nil {
		return supervisor.failInput(input, err)
	}
	if latest.Paused {
		return errInputDispatchPaused
	}
	session = latest

	submitted, err := supervisor.executions.Submit(
		supervisor.ctx,
		session,
		input.Content,
	)
	if err != nil {
		return supervisor.failInput(input, err)
	}
	bound, err := supervisor.inputs.BindExecution(
		supervisor.ctx,
		input.SessionID,
		input.ID,
		submitted.Execution.ID,
	)
	if err != nil {
		return err
	}
	supervisor.emit(input.SessionID, GatewayEventInputStarted, bound)
	return supervisor.awaitAndComplete(
		session,
		bound,
		submitted.Execution.ID,
	)
}

func (supervisor *inputSupervisor) waitForDispatchBoundary(
	session Session,
	input AgentInput,
	recoverUnbound bool,
) (Execution, bool, error) {
	for {
		current, err := supervisor.executions.Current(supervisor.ctx, session)
		if errors.Is(err, ErrExecutionNotFound) {
			return Execution{}, false, nil
		}
		if err != nil {
			if waitErr := supervisor.wait(); waitErr != nil {
				return Execution{}, false, waitErr
			}
			continue
		}
		// A recovered dispatching input can be missing its execution binding
		// when the process stopped after Submit durably accepted the execution.
		// Creation time distinguishes that execution from an older direct run
		// which happened to be current when the queue supervisor restarted.
		if recoverUnbound && !current.Execution.CreatedAt.Before(input.UpdatedAt) {
			return current, true, nil
		}
		if current.Execution.Terminal() {
			return current, false, nil
		}
		if err := supervisor.wait(); err != nil {
			return Execution{}, false, err
		}
	}
}

func (supervisor *inputSupervisor) awaitAndComplete(
	session Session,
	input AgentInput,
	executionID string,
) error {
	for {
		current, err := supervisor.executions.Get(
			supervisor.ctx,
			session,
			executionID,
		)
		if err == nil && current.Execution.Terminal() {
			state := AgentInputSucceeded
			switch current.Execution.State {
			case sdk.TrajectoryExecutionFailed:
				state = AgentInputFailed
			case sdk.TrajectoryExecutionCancelled:
				state = AgentInputCancelled
			}
			completed, completeErr := supervisor.inputs.Complete(
				supervisor.ctx,
				input.SessionID,
				input.ID,
				state,
				current.Execution.LastError,
			)
			if completeErr == nil {
				supervisor.emit(
					input.SessionID,
					GatewayEventInputCompleted,
					completed,
				)
			}
			return completeErr
		}
		if err != nil && !errors.Is(err, ErrExecutionNotFound) {
			supervisor.logger.Warn(
				"poll gateway input execution",
				"session_id", input.SessionID,
				"input_id", input.ID,
				"execution_id", executionID,
				"error", err,
			)
		}
		if err := supervisor.wait(); err != nil {
			return err
		}
	}
}

func (supervisor *inputSupervisor) failInput(
	input AgentInput,
	cause error,
) error {
	if supervisor.ctx.Err() != nil {
		return supervisor.ctx.Err()
	}
	completed, err := supervisor.inputs.Complete(
		supervisor.ctx,
		input.SessionID,
		input.ID,
		AgentInputFailed,
		cause.Error(),
	)
	if err == nil {
		supervisor.emit(input.SessionID, GatewayEventInputCompleted, completed)
	}
	return errors.Join(cause, err)
}

func (supervisor *inputSupervisor) wait() error {
	timer := time.NewTimer(supervisor.poll)
	defer timer.Stop()
	select {
	case <-supervisor.ctx.Done():
		return supervisor.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (supervisor *inputSupervisor) emit(
	sessionID string,
	name string,
	payload any,
) {
	if supervisor.events == nil {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if _, err := supervisor.events.Append(
		supervisor.ctx,
		sessionID,
		sdk.Event{
			ID: managerEventID(name, raw), Name: name,
			SessionID: sessionID, Payload: raw,
		},
	); err != nil && supervisor.ctx.Err() == nil {
		supervisor.logger.Warn(
			"append gateway manager event",
			"session_id", sessionID,
			"event", name,
			"error", err,
		)
	}
}

func managerEventID(name string, payload json.RawMessage) string {
	digest := sha256.Sum256(append([]byte(name+"\x00"), payload...))
	return fmt.Sprintf("manager-%x", digest[:12])
}

func (supervisor *inputSupervisor) removeRunner(sessionID string) {
	supervisor.mu.Lock()
	delete(supervisor.runners, sessionID)
	supervisor.mu.Unlock()
}

func (supervisor *inputSupervisor) close(ctx context.Context) error {
	if supervisor == nil {
		return nil
	}
	supervisor.cancel()
	done := make(chan struct{})
	go func() {
		supervisor.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (supervisor *inputSupervisor) managerEvent(
	sessionID string,
	name string,
	payload any,
) {
	supervisor.emit(sessionID, name, payload)
}
