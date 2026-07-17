package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type RuntimeBuilder func(
	context.Context,
	Session,
	sdk.StateBackend,
) (*agentruntime.Runtime, error)

type RuntimeExecutionConfig struct {
	States StateBackendFactory
	Build  RuntimeBuilder
}

type runtimeExecutionBackend struct {
	states StateBackendFactory
	build  RuntimeBuilder
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	active map[string]*activeRuntimeExecution
	closed bool
	wait   sync.WaitGroup
}

type activeRuntimeExecution struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRuntimeExecutionBackend(
	config RuntimeExecutionConfig,
) (ExecutionBackend, error) {
	if config.States == nil {
		return nil, errors.New("gateway state backend factory is nil")
	}
	if config.Build == nil {
		return nil, errors.New("gateway runtime builder is nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &runtimeExecutionBackend{
		states: config.States,
		build:  config.Build,
		ctx:    ctx,
		cancel: cancel,
		active: make(map[string]*activeRuntimeExecution),
	}, nil
}

func (backend *runtimeExecutionBackend) CreateSession(
	ctx context.Context,
	session Session,
) error {
	runtime, state, err := backend.openRuntime(ctx, session)
	if err != nil {
		return err
	}
	if _, err := runtime.NewSession(ctx, agentruntime.SessionConfig{
		ID: session.ID, Provider: session.Provider,
		System: session.System, MaxTurns: session.MaxTurns,
	}); err != nil {
		return errors.Join(err, closeExecutionHost(runtime, state))
	}
	return closeExecutionHost(runtime, state)
}

func (backend *runtimeExecutionBackend) Submit(
	ctx context.Context,
	session Session,
	content string,
) (Execution, error) {
	backend.mu.Lock()
	if backend.closed {
		backend.mu.Unlock()
		return Execution{}, errors.New("gateway execution backend is closed")
	}
	if active := backend.active[session.ID]; active != nil {
		backend.mu.Unlock()
		return Execution{}, fmt.Errorf(
			"%w: %s",
			ErrExecutionActive,
			active.id,
		)
	}
	backend.mu.Unlock()

	runtime, state, err := backend.openRuntime(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	resumed, err := runtime.ResumeSession(
		ctx,
		session.ID,
		agentruntime.SessionConfig{
			Provider: session.Provider, System: session.System,
			MaxTurns:     session.MaxTurns,
			ResumePolicy: agentruntime.ResumeCurrent,
		},
	)
	if err != nil {
		return Execution{}, errors.Join(
			err,
			closeExecutionHost(runtime, state),
		)
	}
	submission, err := resumed.SubmitPrompt(ctx, content)
	if err != nil {
		return Execution{}, errors.Join(
			err,
			closeExecutionHost(runtime, state),
		)
	}
	execution := submission.Execution()
	runCtx, cancel := context.WithCancel(backend.ctx)
	active := &activeRuntimeExecution{
		id: execution.ID, cancel: cancel, done: make(chan struct{}),
	}
	backend.mu.Lock()
	if backend.closed {
		backend.mu.Unlock()
		cancel()
		return Execution{}, errors.Join(
			errors.New("gateway execution backend closed during submit"),
			closeExecutionHost(runtime, state),
		)
	}
	if existing := backend.active[session.ID]; existing != nil {
		backend.mu.Unlock()
		cancel()
		return Execution{}, errors.Join(
			fmt.Errorf("%w: %s", ErrExecutionActive, existing.id),
			closeExecutionHost(runtime, state),
		)
	}
	backend.active[session.ID] = active
	backend.wait.Add(1)
	backend.mu.Unlock()

	go backend.runSubmission(
		runCtx,
		session.ID,
		active,
		submission,
		runtime,
		state,
	)
	return Execution{
		SessionID: session.ID,
		Execution: execution,
	}, nil
}

func (backend *runtimeExecutionBackend) Recover(
	ctx context.Context,
	session Session,
) (Execution, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	metadata, loadErr := state.Trajectories().LoadMetadata(
		ctx,
		session.ID,
	)
	closeErr := state.Close(context.Background())
	if loadErr != nil || closeErr != nil {
		return Execution{}, errors.Join(loadErr, closeErr)
	}
	if metadata.Execution == nil || metadata.Execution.Terminal() {
		return Execution{}, ErrExecutionNotFound
	}
	execution := *metadata.Execution
	delay := time.Duration(0)
	if execution.State == sdk.TrajectoryExecutionRunning &&
		execution.LeaseExpiresAt.After(time.Now()) {
		delay = time.Until(execution.LeaseExpiresAt)
	}
	runCtx, cancel := context.WithCancel(backend.ctx)
	active := &activeRuntimeExecution{
		id: execution.ID, cancel: cancel, done: make(chan struct{}),
	}
	backend.mu.Lock()
	if backend.closed {
		backend.mu.Unlock()
		cancel()
		return Execution{}, errors.New("gateway execution backend is closed")
	}
	if existing := backend.active[session.ID]; existing != nil {
		backend.mu.Unlock()
		cancel()
		if existing.id == execution.ID {
			return Execution{
				SessionID: session.ID,
				Execution: execution,
			}, nil
		}
		return Execution{}, fmt.Errorf(
			"%w: %s",
			ErrExecutionActive,
			existing.id,
		)
	}
	backend.active[session.ID] = active
	backend.wait.Add(1)
	backend.mu.Unlock()

	go backend.runRecovery(runCtx, session, active, delay)
	return Execution{
		SessionID: session.ID,
		Execution: execution,
	}, nil
}

func (backend *runtimeExecutionBackend) Current(
	ctx context.Context,
	session Session,
) (Execution, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	defer state.Close(context.Background())
	metadata, err := state.Trajectories().LoadMetadata(ctx, session.ID)
	if err != nil {
		return Execution{}, err
	}
	if metadata.Execution == nil {
		return Execution{}, ErrExecutionNotFound
	}
	value := Execution{
		SessionID: session.ID,
		Execution: *metadata.Execution,
	}
	if metadata.Execution.Terminal() {
		value.Result, err = agentruntime.LoadExecutionResult(
			ctx,
			state.Trajectories(),
			metadata,
		)
		if err != nil {
			return Execution{}, err
		}
	}
	return value, nil
}

func (backend *runtimeExecutionBackend) Get(
	ctx context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	execution, err := backend.Current(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	if execution.Execution.ID != executionID {
		return Execution{}, fmt.Errorf(
			"%w: %s",
			ErrExecutionNotFound,
			executionID,
		)
	}
	return execution, nil
}

func (backend *runtimeExecutionBackend) Cancel(
	ctx context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	defer state.Close(context.Background())
	metadata, err := state.Trajectories().CancelExecution(
		ctx,
		session.ID,
		executionID,
		"user requested cancellation",
		time.Now().UTC(),
	)
	if err != nil {
		return Execution{}, err
	}
	backend.mu.Lock()
	active := backend.active[session.ID]
	var done <-chan struct{}
	if active != nil && active.id == executionID {
		active.cancel()
		done = active.done
	}
	backend.mu.Unlock()
	if metadata.Execution == nil {
		return Execution{}, ErrExecutionNotFound
	}
	if done != nil {
		select {
		case <-ctx.Done():
			return Execution{}, ctx.Err()
		case <-done:
		}
	}
	return Execution{
		SessionID: session.ID,
		Execution: *metadata.Execution,
	}, nil
}

func (backend *runtimeExecutionBackend) Close(ctx context.Context) error {
	backend.mu.Lock()
	if !backend.closed {
		backend.closed = true
		backend.cancel()
	}
	backend.mu.Unlock()
	done := make(chan struct{})
	go func() {
		backend.wait.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (backend *runtimeExecutionBackend) openRuntime(
	ctx context.Context,
	session Session,
) (*agentruntime.Runtime, sdk.StateBackend, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"open gateway session %s state: %w",
			session.ID,
			err,
		)
	}
	runtime, err := backend.build(ctx, session, state)
	if err != nil {
		closeErr := state.Close(context.Background())
		return nil, nil, errors.Join(err, closeErr)
	}
	if runtime == nil {
		closeErr := state.Close(context.Background())
		return nil, nil, errors.Join(
			errors.New("gateway runtime builder returned nil"),
			closeErr,
		)
	}
	return runtime, state, nil
}

func (backend *runtimeExecutionBackend) runSubmission(
	ctx context.Context,
	sessionID string,
	active *activeRuntimeExecution,
	submission *agentruntime.PromptSubmission,
	runtime *agentruntime.Runtime,
	state sdk.StateBackend,
) {
	defer backend.wait.Done()
	defer backend.releaseActive(sessionID, active)
	_, _ = submission.Run(ctx)
	_ = closeExecutionHost(runtime, state)
}

func (backend *runtimeExecutionBackend) runRecovery(
	ctx context.Context,
	session Session,
	active *activeRuntimeExecution,
	delay time.Duration,
) {
	defer backend.wait.Done()
	defer backend.releaseActive(session.ID, active)
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
	runtime, state, err := backend.openRuntime(ctx, session)
	if err != nil {
		return
	}
	_, _ = runtime.RecoverExecution(ctx, session.ID)
	_ = closeExecutionHost(runtime, state)
}

func (backend *runtimeExecutionBackend) releaseActive(
	sessionID string,
	active *activeRuntimeExecution,
) {
	backend.mu.Lock()
	if current := backend.active[sessionID]; current == active {
		delete(backend.active, sessionID)
	}
	close(active.done)
	backend.mu.Unlock()
}

func closeExecutionHost(
	runtime *agentruntime.Runtime,
	state sdk.StateBackend,
) error {
	var result error
	if runtime != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result = errors.Join(result, runtime.DrainDeliveries(ctx))
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		result = errors.Join(result, runtime.Close(ctx))
		cancel()
	}
	if state != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result = errors.Join(result, state.Close(ctx))
		cancel()
	}
	return result
}

var (
	_ ExecutionBackend         = (*runtimeExecutionBackend)(nil)
	_ ExecutionRecoveryBackend = (*runtimeExecutionBackend)(nil)
)
