package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type RuntimeBuilder func(
	context.Context,
	RuntimeBuildSpec,
	sdk.StateBackend,
) (*agentruntime.Runtime, error)

type RuntimeBuildSpec struct {
	Plugins       []PluginBinding
	WorkspaceRoot string
	RuntimeConfig []byte
	Model         string
	AutoCompact   *bool
	ThinkingLevel string
	Permissions   PermissionRules
	EventObserver func(context.Context, sdk.Event)
	Interactions  *InteractionManager
}

type RuntimeExecutionConfig struct {
	States          StateBackendFactory
	Build           RuntimeBuilder
	Events          EventStore
	Interactions    *InteractionManager
	ValidateSession SessionValidator
	Logger          *slog.Logger
}

const gatewayCancellationReason = "user requested cancellation"

type runtimeExecutionBackend struct {
	states          StateBackendFactory
	build           RuntimeBuilder
	events          EventStore
	interactions    *InteractionManager
	validateSession SessionValidator
	logger          *slog.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	hosts           *activeHostRegistry
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
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &runtimeExecutionBackend{
		states:          config.States,
		build:           config.Build,
		events:          config.Events,
		interactions:    config.Interactions,
		validateSession: config.ValidateSession,
		logger:          config.Logger,
		ctx:             ctx,
		cancel:          cancel,
		hosts:           newActiveHostRegistry(),
	}, nil
}

func (backend *runtimeExecutionBackend) CreateSession(
	ctx context.Context,
	session Session,
) error {
	if err := backend.validateSessionBinding(ctx, session); err != nil {
		return err
	}
	host, err := backend.openRuntime(ctx, session)
	if err != nil {
		return err
	}
	if _, err := host.Runtime.NewSession(ctx, agentruntime.SessionConfig{
		ID: session.ID, Provider: session.Provider,
		System: session.System, MaxTurns: session.MaxTurns,
		ReasoningEffort: session.ThinkingLevel,
	}); err != nil {
		return errors.Join(err, host.CloseDetached(ctx))
	}
	return host.CloseDetached(ctx)
}

func (backend *runtimeExecutionBackend) LoadTrajectory(
	ctx context.Context,
	session Session,
	head string,
) (sdk.Trajectory, error) {
	host, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	var trajectory sdk.Trajectory
	if head == "" {
		trajectory, err = host.State.Trajectories().Load(ctx, session.ID)
	} else {
		trajectory, err = host.State.Trajectories().LoadBranchView(
			ctx, session.ID, head,
		)
	}
	return trajectory, errors.Join(err, host.CloseDetached(ctx))
}

func (backend *runtimeExecutionBackend) ListTrajectoryEntries(
	ctx context.Context,
	session Session,
	head string,
	query TrajectoryEntryQuery,
) (TrajectoryEntryPage, error) {
	host, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return TrajectoryEntryPage{}, err
	}
	store := host.State.Trajectories()
	if inspector, ok := store.(sdk.TrajectoryEntryInspector); ok {
		metadata, entries, inspectErr := inspector.InspectTrajectoryEntries(
			ctx,
			session.ID,
			head,
		)
		if inspectErr != nil {
			return TrajectoryEntryPage{}, errors.Join(
				inspectErr,
				host.CloseDetached(ctx),
			)
		}
		page, projectErr := projectInspectedTrajectoryEntryPage(
			metadata,
			entries,
			query,
		)
		return page, errors.Join(projectErr, host.CloseDetached(ctx))
	}

	var trajectory sdk.Trajectory
	if head == "" {
		trajectory, err = store.Load(ctx, session.ID)
	} else {
		trajectory, err = store.LoadBranchView(ctx, session.ID, head)
	}
	if err != nil {
		return TrajectoryEntryPage{}, errors.Join(err, host.CloseDetached(ctx))
	}
	page, projectErr := projectTrajectoryEntryPage(trajectory, query)
	return page, errors.Join(projectErr, host.CloseDetached(ctx))
}

func (backend *runtimeExecutionBackend) ListConversation(
	ctx context.Context,
	session Session,
	head string,
	query ConversationQuery,
) (ConversationPage, error) {
	host, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return ConversationPage{}, err
	}
	store := host.State.Trajectories()
	if inspector, ok := store.(sdk.TrajectoryEntryInspector); ok {
		metadata, entries, inspectErr := inspector.InspectTrajectoryEntries(
			ctx,
			session.ID,
			head,
		)
		if inspectErr != nil {
			return ConversationPage{}, errors.Join(
				inspectErr,
				host.CloseDetached(ctx),
			)
		}
		messages, projectErr := agentruntime.ProjectStoredTrajectoryMessages(
			ctx,
			store,
			session.ID,
			entries,
		)
		if projectErr != nil {
			return ConversationPage{}, errors.Join(
				projectErr,
				host.CloseDetached(ctx),
			)
		}
		page, pageErr := projectConversationMessagesPage(
			metadata.Head,
			metadata.Execution,
			messages,
			query,
		)
		return page, errors.Join(pageErr, host.CloseDetached(ctx))
	}

	var trajectory sdk.Trajectory
	if head == "" {
		trajectory, err = store.Load(ctx, session.ID)
	} else {
		trajectory, err = store.LoadBranchView(ctx, session.ID, head)
	}
	if err != nil {
		return ConversationPage{}, errors.Join(err, host.CloseDetached(ctx))
	}
	page, projectErr := projectConversationPage(trajectory, query)
	return page, errors.Join(projectErr, host.CloseDetached(ctx))
}

func (backend *runtimeExecutionBackend) RollbackTrajectory(
	ctx context.Context,
	session Session,
	checkpointID string,
) (sdk.Trajectory, error) {
	host, err := backend.openRuntime(ctx, session)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	if err := host.Runtime.RollbackTrajectory(
		ctx, session.ID, checkpointID,
	); err != nil {
		return sdk.Trajectory{}, errors.Join(err, host.CloseDetached(ctx))
	}
	trajectory, err := host.State.Trajectories().Load(ctx, session.ID)
	return trajectory, errors.Join(err, host.CloseDetached(ctx))
}

func (backend *runtimeExecutionBackend) Submit(
	ctx context.Context,
	session Session,
	content string,
) (Execution, error) {
	slot, runCtx := newActiveHostSlot(backend.ctx, "")
	if _, err := backend.hosts.reserve(session.ID, slot); err != nil {
		slot.cancel()
		return Execution{}, err
	}

	cleanup := func(
		cause error,
		host agentruntime.ExecutionHost,
	) (Execution, error) {
		slot.cancel()
		backend.hosts.complete(session.ID, slot)
		return Execution{}, errors.Join(cause, host.CloseDetached(ctx))
	}
	setupCtx, stopSetup := context.WithCancel(ctx)
	stopBackendCancel := context.AfterFunc(backend.ctx, stopSetup)
	defer func() {
		stopBackendCancel()
		stopSetup()
	}()
	if err := backend.validateSessionBinding(setupCtx, session); err != nil {
		return cleanup(err, agentruntime.ExecutionHost{})
	}
	host, err := backend.openRuntime(setupCtx, session)
	if err != nil {
		return cleanup(err, agentruntime.ExecutionHost{})
	}
	submission, err := host.Runtime.SubmitPrompt(
		setupCtx,
		session.ID,
		agentruntime.SessionConfig{
			Provider: session.Provider, System: session.System,
			MaxTurns:        session.MaxTurns,
			ReasoningEffort: session.ThinkingLevel,
			ResumePolicy:    agentruntime.ResumeCurrent,
		},
		content,
	)
	if err != nil {
		return cleanup(err, host)
	}
	view, err := submission.LoadExecutionView(setupCtx)
	if err != nil {
		return cleanup(err, host)
	}
	if err := backend.hosts.bind(
		session.ID,
		slot,
		view.Execution.ID,
		host.Runtime,
		host.Control(),
	); err != nil {
		return cleanup(err, host)
	}

	go backend.runSubmission(
		runCtx,
		session.ID,
		slot,
		submission,
		host,
	)
	return gatewayExecutionFromView(view), nil
}

func (backend *runtimeExecutionBackend) Recover(
	ctx context.Context,
	session Session,
) (Execution, error) {
	candidate, err := backend.loadRecoveryCandidate(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	slot, runCtx := newActiveHostSlot(
		backend.ctx,
		candidate.Execution.ID,
	)
	if existing, err := backend.hosts.reserve(
		session.ID,
		slot,
	); err != nil {
		slot.cancel()
		if existing.matchesExecution(candidate.Execution.ID) {
			return backend.Current(ctx, session)
		}
		return Execution{}, err
	}
	if err := backend.validateSessionBinding(ctx, session); err != nil {
		slot.cancel()
		backend.hosts.complete(session.ID, slot)
		return Execution{}, err
	}

	go backend.runRecovery(runCtx, session, slot, candidate)
	return gatewayExecutionFromView(candidate.ExecutionView()), nil
}

func (backend *runtimeExecutionBackend) EnqueueContextInjection(
	ctx context.Context,
	session Session,
	executionID string,
	injection sdk.ContextInjection,
) (Execution, error) {
	plan, err := backend.hosts.contextPlan(session.ID, executionID)
	if err == nil {
		view, err := plan.control.EnqueueContextInjectionView(
			ctx,
			session.ID,
			executionID,
			injection,
		)
		if err != nil {
			return Execution{}, gatewayExecutionViewError(err)
		}
		return gatewayExecutionFromView(view), nil
	}
	if !errors.Is(err, ErrExecutionNotFound) {
		return Execution{}, err
	}
	stateHost, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	view, err := stateHost.EnqueueContextInjection(
		ctx,
		session.ID,
		executionID,
		injection,
	)
	if err := gatewayExecutionViewError(err); err != nil {
		return Execution{}, err
	}
	return gatewayExecutionFromView(view), nil
}

func (backend *runtimeExecutionBackend) loadRecoveryCandidate(
	ctx context.Context,
	session Session,
) (agentruntime.ExecutionRecoveryCandidate, error) {
	candidate, err := backend.loadStateRecoveryCandidate(ctx, session)
	return candidate, gatewayRecoveryCandidateError(err)
}

func (backend *runtimeExecutionBackend) validateSessionBinding(
	ctx context.Context,
	session Session,
) error {
	if backend.validateSession == nil {
		return nil
	}
	return backend.validateSession(ctx, session)
}

func (backend *runtimeExecutionBackend) Current(
	ctx context.Context,
	session Session,
) (Execution, error) {
	readPlan, err := backend.hosts.readPlan(session.ID)
	if err != nil {
		return Execution{}, err
	}
	view, err := backend.loadExecutionView(ctx, session, readPlan)
	if err := gatewayExecutionViewError(err); err != nil {
		return Execution{}, err
	}
	value := gatewayExecutionFromView(view)
	if view.Execution.Terminal() && readPlan.active() {
		if err := readPlan.wait(ctx); err != nil {
			return Execution{}, err
		}
	}
	return value, nil
}

func (backend *runtimeExecutionBackend) loadExecutionView(
	ctx context.Context,
	session Session,
	readPlan activeHostReadPlan,
) (agentruntime.ExecutionView, error) {
	if readPlan.active() {
		return readPlan.loadView(ctx, session.ID)
	}
	return backend.loadStateExecutionView(ctx, session)
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
	cancelPlan, activeErr := backend.hosts.cancelPlan(
		session.ID,
		executionID,
	)
	if activeErr != nil {
		return Execution{}, activeErr
	}

	view, err := cancelPlan.cancelExecution(
		ctx,
		func(control agentruntime.ExecutionControl) (
			agentruntime.ExecutionView,
			error,
		) {
			return control.CancelWithAvailableBoundary(
				ctx,
				session.ID,
				executionID,
				gatewayCancellationReason,
			)
		},
		func() (agentruntime.ExecutionView, error) {
			return backend.cancelUnhosted(
				ctx,
				session,
				executionID,
			)
		},
	)
	if err != nil {
		return Execution{}, err
	}
	return gatewayExecutionFromView(view), nil
}

func (backend *runtimeExecutionBackend) cancelUnhosted(
	ctx context.Context,
	session Session,
	executionID string,
) (agentruntime.ExecutionView, error) {
	host, err := backend.openRuntime(ctx, session)
	if err == nil {
		return host.CancelWithAvailableBoundary(
			ctx,
			session.ID,
			executionID,
			gatewayCancellationReason,
		)
	}

	stateHost, stateErr := backend.openStateExecutionHost(ctx, session)
	if stateErr != nil {
		return agentruntime.ExecutionView{}, errors.Join(err, stateErr)
	}
	view, cancelErr := stateHost.CancelWithAvailableBoundary(
		ctx,
		session.ID,
		executionID,
		gatewayCancellationReason,
	)
	if cancelErr != nil {
		return agentruntime.ExecutionView{}, errors.Join(err, cancelErr)
	}
	return view, nil
}

func (backend *runtimeExecutionBackend) loadStateExecutionView(
	ctx context.Context,
	session Session,
) (agentruntime.ExecutionView, error) {
	host, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return agentruntime.ExecutionView{}, err
	}
	return host.LoadExecutionView(ctx, session.ID)
}

func (backend *runtimeExecutionBackend) loadStateRecoveryCandidate(
	ctx context.Context,
	session Session,
) (agentruntime.ExecutionRecoveryCandidate, error) {
	host, err := backend.openStateExecutionHost(ctx, session)
	if err != nil {
		return agentruntime.ExecutionRecoveryCandidate{}, err
	}
	return host.LoadRecoveryCandidate(ctx, session.ID)
}

func (backend *runtimeExecutionBackend) openStateExecutionHost(
	ctx context.Context,
	session Session,
) (agentruntime.ExecutionHost, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return agentruntime.ExecutionHost{}, err
	}
	return agentruntime.ExecutionHost{State: state}, nil
}

func closeGatewayState(ctx context.Context, state sdk.StateBackend) error {
	if state == nil {
		return nil
	}
	return closeGatewayHost(ctx, agentruntime.ExecutionHost{State: state})
}

func closeGatewayHost(
	ctx context.Context,
	host agentruntime.ExecutionHost,
) error {
	return host.CloseDetached(ctx)
}

func gatewayExecutionViewError(err error) error {
	if errors.Is(err, agentruntime.ErrExecutionNotFound) {
		return ErrExecutionNotFound
	}
	return err
}

func gatewayRecoveryCandidateError(err error) error {
	if errors.Is(err, agentruntime.ErrExecutionNotFound) ||
		errors.Is(err, agentruntime.ErrExecutionNotRecoverable) {
		return ErrExecutionNotFound
	}
	return err
}

func gatewayExecutionFromView(view agentruntime.ExecutionView) Execution {
	return Execution{
		SessionID: view.TrajectoryID,
		Execution: view.Execution,
		Result:    view.Result,
	}
}

func (backend *runtimeExecutionBackend) Close(ctx context.Context) error {
	runtimes, _ := backend.hosts.beginClose()
	for _, runtime := range runtimes {
		runtime.RequestClose(ctx)
	}
	backend.cancel()
	return backend.hosts.waitClosed(ctx)
}

func (backend *runtimeExecutionBackend) Drain(ctx context.Context) error {
	runtimes, _ := backend.hosts.beginClose()
	for _, runtime := range runtimes {
		runtime.RequestDrain()
	}
	return backend.hosts.waitClosed(ctx)
}

func (backend *runtimeExecutionBackend) openRuntime(
	ctx context.Context,
	session Session,
) (agentruntime.ExecutionHost, error) {
	state, err := backend.states.Open(ctx, session)
	if err != nil {
		return agentruntime.ExecutionHost{}, fmt.Errorf(
			"open gateway session %s state: %w",
			session.ID,
			err,
		)
	}
	spec := runtimeBuildSpec(session)
	spec.EventObserver = backend.eventObserver(session.ID)
	spec.Interactions = backend.interactions
	runtime, err := backend.build(ctx, spec, state)
	if err != nil {
		closeErr := closeGatewayState(ctx, state)
		return agentruntime.ExecutionHost{}, errors.Join(err, closeErr)
	}
	if runtime == nil {
		closeErr := closeGatewayState(ctx, state)
		return agentruntime.ExecutionHost{}, errors.Join(
			errors.New("gateway runtime builder returned nil"),
			closeErr,
		)
	}
	return agentruntime.ExecutionHost{
		Runtime: runtime,
		State:   state,
	}, nil
}

func runtimeBuildSpec(session Session) RuntimeBuildSpec {
	return RuntimeBuildSpec{
		Plugins:       clonePluginBindings(session.Plugins),
		WorkspaceRoot: session.WorkspaceRoot,
		RuntimeConfig: append([]byte(nil), session.RuntimeConfig...),
		Model:         session.Model,
		AutoCompact:   cloneBool(session.AutoCompact),
		ThinkingLevel: session.ThinkingLevel,
		Permissions:   clonePermissionRules(session.Permissions),
	}
}

func (backend *runtimeExecutionBackend) eventObserver(
	sessionID string,
) func(context.Context, sdk.Event) {
	if backend.events == nil {
		return nil
	}
	return func(ctx context.Context, event sdk.Event) {
		if _, err := backend.events.Append(
			lifecycle.Detached(ctx),
			sessionID,
			event,
		); err != nil {
			backend.logger.WarnContext(
				lifecycle.Detached(ctx),
				"project gateway runtime event",
				"session_id",
				sessionID,
				"event",
				event.Name,
				"error",
				err,
			)
		}
	}
}

func (backend *runtimeExecutionBackend) runSubmission(
	ctx context.Context,
	sessionID string,
	slot *activeHostSlot,
	submission *agentruntime.PromptSubmission,
	host agentruntime.ExecutionHost,
) {
	defer backend.hosts.complete(sessionID, slot)
	_, runErr := host.RunPromptSubmission(ctx, submission)
	backend.observeHostCompletion(
		ctx,
		"submit",
		sessionID,
		slot.executionID,
		runErr,
	)
}

func (backend *runtimeExecutionBackend) runRecovery(
	ctx context.Context,
	session Session,
	slot *activeHostSlot,
	candidate agentruntime.ExecutionRecoveryCandidate,
) {
	defer backend.hosts.complete(session.ID, slot)
	if err := candidate.Wait(ctx); err != nil {
		return
	}
	host, err := backend.openRuntime(ctx, session)
	if err != nil {
		backend.observeHostCompletion(
			ctx,
			"recover",
			session.ID,
			slot.executionID,
			err,
		)
		return
	}
	if err := backend.hosts.bind(
		session.ID,
		slot,
		slot.executionID,
		host.Runtime,
		host.Control(),
	); err != nil {
		closeErr := host.CloseDetached(ctx)
		backend.observeHostCompletion(
			ctx,
			"recover",
			session.ID,
			slot.executionID,
			errors.Join(err, closeErr),
		)
		return
	}
	_, recoverErr := host.RecoverExecution(ctx, session.ID)
	backend.observeHostCompletion(
		ctx,
		"recover",
		session.ID,
		slot.executionID,
		recoverErr,
	)
}

func (backend *runtimeExecutionBackend) observeHostCompletion(
	ctx context.Context,
	operation string,
	sessionID string,
	executionID string,
	err error,
) {
	if err == nil || lifecycle.ExpectedCancellation(ctx, err) {
		return
	}
	if errors.Is(err, agentruntime.ErrRuntimeDraining) {
		return
	}
	backend.logger.WarnContext(
		lifecycle.Detached(ctx),
		"gateway execution host completed with error",
		"operation",
		operation,
		"session_id",
		sessionID,
		"execution_id",
		executionID,
		"error",
		err,
	)
}

var _ ExecutionBackend = (*runtimeExecutionBackend)(nil)
