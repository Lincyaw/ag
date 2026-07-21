package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

var (
	ErrExecutionNotFound = errors.New("gateway execution not found")
	ErrExecutionActive   = errors.New("gateway session execution is active")
	ErrGatewayDraining   = errors.New("gateway is draining")
)

type Execution struct {
	SessionID string                  `json:"session_id"`
	Execution sdk.TrajectoryExecution `json:"execution"`
	Result    *agentruntime.Result    `json:"result,omitempty"`
}

type ExecutionBackend interface {
	CreateSession(context.Context, Session) error
	// Submit owns the active execution gate for the session. Service-level
	// check-then-submit would leave a composition mutation race before reservation.
	Submit(context.Context, Session, string) (Execution, error)
	// EnqueueContextInjection schedules model-visible context for a live hosted
	// execution without starting a new user turn.
	EnqueueContextInjection(
		context.Context,
		Session,
		string,
		sdk.ContextInjection,
	) (Execution, error)
	// Recover owns the recoverability check, active execution gate, and session
	// binding validation before it builds or hosts runtime recovery.
	Recover(context.Context, Session) (Execution, error)
	Current(context.Context, Session) (Execution, error)
	Get(context.Context, Session, string) (Execution, error)
	Cancel(context.Context, Session, string) (Execution, error)
	Close(context.Context) error
}

// ExecutionDrainer is the optional graceful handoff phase implemented by
// hosted backends. It stops admission and waits for active executions to hand
// ownership back at a durable model-turn boundary without cancelling them.
type ExecutionDrainer interface {
	Drain(context.Context) error
}

// TrajectoryBackend is the optional durable trajectory control surface of an
// execution backend. Gateway RPC uses it so show/rollback operate on the same
// state hosted by background execution rather than opening a second store.
type TrajectoryBackend interface {
	LoadTrajectory(context.Context, Session, string) (sdk.Trajectory, error)
	RollbackTrajectory(context.Context, Session, string) (sdk.Trajectory, error)
}

// TrajectoryEntryPageBackend is the optional payload-free inspection path.
// Runtime-backed gateways implement it when their StateBackend can inspect
// branch metadata without materializing trajectory payloads.
type TrajectoryEntryPageBackend interface {
	ListTrajectoryEntries(
		context.Context,
		Session,
		string,
		TrajectoryEntryQuery,
	) (TrajectoryEntryPage, error)
}

type TrajectoryConversationBackend interface {
	ListConversation(
		context.Context,
		Session,
		string,
		ConversationQuery,
	) (ConversationPage, error)
}

type ServiceConfig struct {
	Store            SessionStore
	Events           EventStore
	Inputs           InputStore
	Interactions     *InteractionManager
	Directory        PluginDirectory
	Executions       ExecutionBackend
	DefaultNamespace string
	DefaultProvider  string
	DefaultSystem    string
	DefaultMaxTurns  int
}

type Service struct {
	store        SessionStore
	events       EventStore
	inputs       InputStore
	interactions *InteractionManager
	directory    PluginDirectory
	executions   ExecutionBackend
	trajectories TrajectoryBackend
	manager      *Manager
	supervisor   *inputSupervisor
	gates        *sessionGate
	draining     atomic.Bool
	defaults     Session
}

func NewService(config ServiceConfig) (*Service, error) {
	if config.Executions == nil {
		return nil, errors.New("gateway execution backend is nil")
	}
	if config.Events == nil {
		config.Events = NewMemoryEventStore()
	}
	if config.Inputs == nil {
		config.Inputs = NewMemoryInputStore()
	}
	if config.Interactions == nil {
		var err error
		config.Interactions, err = NewInteractionManager(
			NewMemoryInteractionStore(),
			config.Events,
		)
		if err != nil {
			return nil, err
		}
	}
	config.DefaultProvider = strings.TrimSpace(config.DefaultProvider)
	if config.DefaultProvider != "" {
		if err := sdk.ValidateResourceName(
			"gateway default provider",
			config.DefaultProvider,
		); err != nil {
			return nil, err
		}
	}
	if config.DefaultMaxTurns < 0 {
		return nil, errors.New(
			"gateway default max turns cannot be negative",
		)
	}
	service := &Service{
		store:        config.Store,
		events:       config.Events,
		inputs:       config.Inputs,
		interactions: config.Interactions,
		directory:    config.Directory,
		executions:   config.Executions,
		gates:        newSessionGate(),
		defaults: Session{
			Provider: config.DefaultProvider,
			System:   config.DefaultSystem,
			MaxTurns: config.DefaultMaxTurns,
		},
	}
	service.trajectories, _ = config.Executions.(TrajectoryBackend)
	manager, err := NewManager(ManagerConfig{
		Store:            config.Store,
		Directory:        config.Directory,
		DefaultNamespace: config.DefaultNamespace,
		RequireIdle:      service.requireIdle,
	})
	if err != nil {
		return nil, err
	}
	service.manager = manager
	service.supervisor = newInputSupervisor(
		service.inputs,
		service.store,
		service.executions,
		service.events,
	)
	return service, nil
}

func (service *Service) LoadTrajectory(
	ctx context.Context,
	userID string,
	trajectoryID string,
	head string,
) (sdk.Trajectory, error) {
	if service.trajectories == nil {
		return sdk.Trajectory{}, errors.New("gateway trajectory inspection is unavailable")
	}
	session, err := service.manager.ownedSession(ctx, userID, trajectoryID)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	return service.trajectories.LoadTrajectory(ctx, session, strings.TrimSpace(head))
}

func (service *Service) ListConversation(
	ctx context.Context,
	userID string,
	trajectoryID string,
	head string,
	query ConversationQuery,
) (ConversationPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, trajectoryID)
	if err != nil {
		return ConversationPage{}, err
	}
	if projector, ok := service.executions.(TrajectoryConversationBackend); ok {
		return projector.ListConversation(
			ctx,
			session,
			strings.TrimSpace(head),
			query,
		)
	}
	trajectory, err := service.LoadTrajectory(
		ctx,
		userID,
		trajectoryID,
		strings.TrimSpace(head),
	)
	if err != nil {
		return ConversationPage{}, err
	}
	return projectConversationPage(trajectory, query)
}

func (service *Service) ListTrajectoryEntries(
	ctx context.Context,
	userID string,
	trajectoryID string,
	head string,
	query TrajectoryEntryQuery,
) (TrajectoryEntryPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, trajectoryID)
	if err != nil {
		return TrajectoryEntryPage{}, err
	}
	if inspector, ok := service.executions.(TrajectoryEntryPageBackend); ok {
		return inspector.ListTrajectoryEntries(
			ctx,
			session,
			strings.TrimSpace(head),
			query,
		)
	}
	trajectory, err := service.LoadTrajectory(
		ctx,
		userID,
		trajectoryID,
		strings.TrimSpace(head),
	)
	if err != nil {
		return TrajectoryEntryPage{}, err
	}
	return projectTrajectoryEntryPage(trajectory, query)
}

func (service *Service) RollbackTrajectory(
	ctx context.Context,
	userID string,
	trajectoryID string,
	checkpointID string,
) (sdk.Trajectory, error) {
	if service.trajectories == nil {
		return sdk.Trajectory{}, errors.New("gateway trajectory rollback is unavailable")
	}
	unlock, err := service.lockSession(ctx, trajectoryID)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	defer unlock()
	session, err := service.manager.ownedSession(ctx, userID, trajectoryID)
	if err != nil {
		return sdk.Trajectory{}, err
	}
	if err := service.requireIdle(ctx, session); err != nil {
		return sdk.Trajectory{}, err
	}
	return service.trajectories.RollbackTrajectory(
		ctx, session, strings.TrimSpace(checkpointID),
	)
}

func (service *Service) CreateSession(
	ctx context.Context,
	session Session,
) (Session, error) {
	if service.draining.Load() {
		return Session{}, ErrGatewayDraining
	}
	if session.Provider == "" {
		session.Provider = service.defaults.Provider
	}
	if session.System == "" {
		session.System = service.defaults.System
	}
	if session.MaxTurns == 0 {
		session.MaxTurns = service.defaults.MaxTurns
	}
	session, err := normalizeSession(session)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	created, err := service.store.Create(ctx, session)
	if err != nil {
		return Session{}, err
	}
	if err := service.executions.CreateSession(ctx, created); err != nil {
		deleteErr := service.store.Delete(
			lifecycle.Detached(ctx),
			created.ID,
			created.Revision,
		)
		return Session{}, errors.Join(
			fmt.Errorf("create gateway execution session: %w", err),
			deleteErr,
		)
	}
	return created, nil
}

func (service *Service) GetSession(
	ctx context.Context,
	userID string,
	id string,
) (Session, error) {
	return service.manager.ownedSession(ctx, userID, id)
}

func (service *Service) UpdateSession(
	ctx context.Context,
	userID string,
	id string,
	expectedRevision uint64,
	patch SessionPatch,
) (Session, error) {
	unlock, err := service.lockSession(ctx, id)
	if err != nil {
		return Session{}, err
	}
	defer unlock()
	session, err := service.manager.ownedSession(ctx, userID, id)
	if err != nil {
		return Session{}, err
	}
	if session.Revision != expectedRevision {
		return Session{}, fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict,
			session.ID,
			session.Revision,
			expectedRevision,
		)
	}
	updated, changed, err := applySessionPatch(session, patch)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if !changed {
		return session, nil
	}
	updated, err = service.store.Save(ctx, updated, expectedRevision)
	if err != nil {
		return Session{}, err
	}
	service.supervisor.managerEvent(
		session.ID,
		GatewayEventSessionUpdated,
		updated,
	)
	return updated, nil
}

func applySessionPatch(session Session, patch SessionPatch) (Session, bool, error) {
	changed := false
	if patch.Title != nil {
		value := strings.TrimSpace(*patch.Title)
		if session.Title != value {
			session.Title = value
			changed = true
		}
	}
	if patch.Model != nil {
		value := strings.TrimSpace(*patch.Model)
		if value == "" {
			return Session{}, false, errors.New("gateway session model is empty")
		}
		if session.Model != value {
			session.Model = value
			changed = true
		}
	}
	if patch.AutoCompact != nil &&
		(session.AutoCompact == nil || *session.AutoCompact != *patch.AutoCompact) {
		enabled := *patch.AutoCompact
		session.AutoCompact = &enabled
		changed = true
	}
	if patch.ThinkingLevel != nil {
		value := strings.ToLower(strings.TrimSpace(*patch.ThinkingLevel))
		if session.ThinkingLevel != value {
			session.ThinkingLevel = value
			changed = true
		}
	}
	if patch.PermissionRule != nil {
		kind := strings.ToLower(strings.TrimSpace(patch.PermissionRule.Kind))
		pattern := strings.TrimSpace(patch.PermissionRule.Pattern)
		if pattern == "" {
			return Session{}, false, errors.New("gateway permission pattern is empty")
		}
		var target *[]string
		switch kind {
		case "allow":
			target = &session.Permissions.Allow
		case "ask":
			target = &session.Permissions.Ask
		case "deny":
			target = &session.Permissions.Deny
		default:
			return Session{}, false, fmt.Errorf(
				"gateway permission rule kind %q is invalid",
				kind,
			)
		}
		if !slices.Contains(*target, pattern) {
			*target = append(*target, pattern)
			changed = true
		}
	}
	normalized, err := normalizeSession(session)
	if err != nil {
		return Session{}, false, err
	}
	return normalized, changed, nil
}

func (service *Service) ListSessions(
	ctx context.Context,
	userID string,
	request sdk.PageRequest,
) (SessionPage, error) {
	userID, err := normalizeUserID(userID)
	if err != nil {
		return SessionPage{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	request, err = validatePage(request)
	if err != nil {
		return SessionPage{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	page, err := service.store.ListByUser(ctx, userID, request)
	if err != nil {
		return SessionPage{}, err
	}
	return page, nil
}

func (service *Service) DiscoverPlugins(
	ctx context.Context,
	query registry.DiscoveryQuery,
	request registry.PageRequest,
) (registry.DiscoveryPage, error) {
	return service.manager.Discover(ctx, query, request)
}

func (service *Service) AttachPlugin(
	ctx context.Context,
	userID string,
	sessionID string,
	selector string,
	expectedRevision uint64,
) (Session, error) {
	unlock, err := service.lockSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	defer unlock()
	return service.manager.AttachPlugin(
		ctx,
		userID,
		sessionID,
		selector,
		expectedRevision,
	)
}

func (service *Service) DetachPlugin(
	ctx context.Context,
	userID string,
	sessionID string,
	name string,
	expectedRevision uint64,
) (Session, error) {
	unlock, err := service.lockSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	defer unlock()
	return service.manager.DetachPlugin(
		ctx,
		userID,
		sessionID,
		name,
		expectedRevision,
	)
}

func (service *Service) SubmitMessage(
	ctx context.Context,
	userID string,
	sessionID string,
	content string,
) (Execution, error) {
	if service.draining.Load() {
		return Execution{}, ErrGatewayDraining
	}
	if strings.TrimSpace(content) == "" {
		return Execution{}, fmt.Errorf(
			"%w: gateway message content is empty",
			ErrInvalidRequest,
		)
	}
	unlock, err := service.lockSession(ctx, sessionID)
	if err != nil {
		return Execution{}, err
	}
	defer unlock()
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Execution{}, err
	}
	return service.executions.Submit(ctx, session, content)
}

func (service *Service) EnqueueContextInjection(
	ctx context.Context,
	userID string,
	sessionID string,
	executionID string,
	injection sdk.ContextInjection,
) (Execution, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Execution{}, err
	}
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return Execution{}, fmt.Errorf(
			"%w: gateway execution ID is empty",
			ErrInvalidRequest,
		)
	}
	injection, err = normalizeGatewayContextInjection(injection)
	if err != nil {
		return Execution{}, err
	}
	return service.executions.EnqueueContextInjection(
		ctx,
		session,
		executionID,
		injection,
	)
}

func (service *Service) ListEvents(
	ctx context.Context,
	userID string,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return EventPage{}, err
	}
	return service.events.List(ctx, session.ID, query)
}

func (service *Service) GetEventCursor(
	ctx context.Context,
	userID string,
	trajectoryID string,
) (EventCursor, error) {
	if _, err := service.manager.ownedSession(ctx, userID, trajectoryID); err != nil {
		return EventCursor{}, err
	}
	sequence, err := service.events.Latest(ctx, trajectoryID)
	return EventCursor{Sequence: sequence}, err
}

func (service *Service) EnqueueInput(
	ctx context.Context,
	userID string,
	sessionID string,
	input AgentInput,
) (AgentInput, error) {
	if service.draining.Load() {
		return AgentInput{}, ErrGatewayDraining
	}
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	input.SessionID = session.ID
	input, err = normalizeAgentInput(input)
	if err != nil {
		return AgentInput{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	input, err = service.inputs.Enqueue(ctx, input)
	if err != nil {
		return AgentInput{}, err
	}
	service.supervisor.managerEvent(
		session.ID,
		GatewayEventInputQueued,
		queuedInputEvent(input),
	)
	if !session.Paused {
		service.supervisor.schedule(session.ID)
	}
	return input, nil
}

func queuedInputEvent(input AgentInput) AgentInput {
	input.State = AgentInputQueued
	input.ExecutionID = ""
	input.LastError = ""
	input.Revision = 1
	input.UpdatedAt = input.CreatedAt
	return input
}

func (service *Service) GetInput(
	ctx context.Context,
	userID string,
	sessionID string,
	inputID string,
) (AgentInput, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	return service.inputs.Get(ctx, session.ID, inputID)
}

func (service *Service) ListInputs(
	ctx context.Context,
	userID string,
	sessionID string,
	query InputQuery,
) (InputPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return InputPage{}, err
	}
	return service.inputs.List(ctx, session.ID, query)
}

func (service *Service) CancelInput(
	ctx context.Context,
	userID string,
	sessionID string,
	inputID string,
	expectedRevision uint64,
) (AgentInput, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return AgentInput{}, err
	}
	input, err := service.inputs.Get(ctx, session.ID, inputID)
	if err != nil {
		return AgentInput{}, err
	}
	if input.Revision != expectedRevision {
		return AgentInput{}, fmt.Errorf(
			"%w: input %s has revision %d, expected %d",
			ErrInputConflict,
			input.ID,
			input.Revision,
			expectedRevision,
		)
	}
	if input.State.Terminal() {
		return input, nil
	}
	if input.State == AgentInputQueued {
		cancelled, err := service.inputs.CancelQueued(
			ctx,
			session.ID,
			input.ID,
			expectedRevision,
		)
		if err == nil {
			service.supervisor.managerEvent(
				session.ID,
				GatewayEventInputCompleted,
				cancelled,
			)
		}
		return cancelled, err
	}
	if input.ExecutionID == "" {
		return AgentInput{}, fmt.Errorf(
			"%w: input %s is being bound to an execution; retry cancellation",
			ErrInputConflict,
			input.ID,
		)
	}
	if _, err := service.executions.Cancel(
		ctx,
		session,
		input.ExecutionID,
	); err != nil {
		return AgentInput{}, err
	}
	return input, nil
}

func (service *Service) SetSessionPaused(
	ctx context.Context,
	userID string,
	sessionID string,
	paused bool,
	expectedRevision uint64,
) (Session, error) {
	unlock, err := service.lockSession(ctx, sessionID)
	if err != nil {
		return Session{}, err
	}
	defer unlock()
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Session{}, err
	}
	if session.Revision != expectedRevision {
		return Session{}, fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict,
			session.ID,
			session.Revision,
			expectedRevision,
		)
	}
	if session.Paused == paused {
		return session, nil
	}
	session.Paused = paused
	updated, err := service.store.Save(ctx, session, expectedRevision)
	if err != nil {
		return Session{}, err
	}
	eventName := GatewayEventSessionPaused
	if !paused {
		eventName = GatewayEventSessionResumed
		service.supervisor.schedule(session.ID)
	}
	service.supervisor.managerEvent(session.ID, eventName, updated)
	return updated, nil
}

func (service *Service) GetInteraction(
	ctx context.Context,
	userID string,
	sessionID string,
	interactionID string,
) (Interaction, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Interaction{}, err
	}
	return service.interactions.Get(ctx, session.ID, interactionID)
}

func (service *Service) ListInteractions(
	ctx context.Context,
	userID string,
	sessionID string,
	query InteractionQuery,
) (InteractionPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return InteractionPage{}, err
	}
	return service.interactions.List(ctx, session.ID, query)
}

func (service *Service) ResolveInteraction(
	ctx context.Context,
	userID string,
	sessionID string,
	interactionID string,
	expectedRevision uint64,
	answer InteractionAnswer,
) (Interaction, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Interaction{}, err
	}
	resolved, err := service.interactions.Resolve(
		ctx,
		session.ID,
		interactionID,
		expectedRevision,
		answer,
	)
	if err != nil {
		if errors.Is(err, ErrInteractionConflict) {
			return Interaction{}, err
		}
		return Interaction{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	return resolved, nil
}

func (service *Service) WaitEvents(
	ctx context.Context,
	userID string,
	sessionID string,
	query EventQuery,
) (EventPage, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return EventPage{}, err
	}
	return service.events.Wait(ctx, session.ID, query)
}

func (service *Service) lockSession(
	ctx context.Context,
	sessionID string,
) (func(), error) {
	if service.gates == nil {
		return func() {}, nil
	}
	return service.gates.lock(ctx, sessionID)
}

func (service *Service) GetExecution(
	ctx context.Context,
	userID string,
	sessionID string,
	executionID string,
) (Execution, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Execution{}, err
	}
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return Execution{}, fmt.Errorf(
			"%w: gateway execution ID is empty",
			ErrInvalidRequest,
		)
	}
	return service.executions.Get(ctx, session, executionID)
}

func (service *Service) CancelExecution(
	ctx context.Context,
	userID string,
	sessionID string,
	executionID string,
) (Execution, error) {
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Execution{}, err
	}
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return Execution{}, fmt.Errorf(
			"%w: gateway execution ID is empty",
			ErrInvalidRequest,
		)
	}
	return service.executions.Cancel(
		ctx,
		session,
		executionID,
	)
}

func normalizeGatewayContextInjection(
	injection sdk.ContextInjection,
) (sdk.ContextInjection, error) {
	normalized, err := sdk.NormalizeContextInjection(
		injection,
		time.Now().UTC(),
	)
	if err != nil {
		return sdk.ContextInjection{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return normalized, nil
}

func (service *Service) RecoverSessions(
	ctx context.Context,
) ([]Execution, error) {
	request := sdk.PageRequest{Limit: sdk.MaxPageSize}
	var (
		scheduled []Execution
		failures  []error
	)
	for {
		page, err := service.store.List(ctx, request)
		if err != nil {
			failures = append(failures, err)
			queueErr := service.supervisor.recover(ctx)
			return scheduled, errors.Join(
				errors.Join(failures...),
				queueErr,
			)
		}
		for _, session := range page.Items {
			execution, err := service.executions.Recover(ctx, session)
			if errors.Is(err, ErrExecutionNotFound) ||
				errors.Is(err, ErrExecutionActive) {
				continue
			}
			if err != nil {
				failures = append(failures, fmt.Errorf(
					"recover gateway session %s: %w",
					session.ID,
					err,
				))
				continue
			}
			scheduled = append(scheduled, execution)
		}
		if page.Next == "" {
			queueErr := service.supervisor.recover(ctx)
			return scheduled, errors.Join(
				errors.Join(failures...),
				queueErr,
			)
		}
		request.After = page.Next
	}
}

func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	return errors.Join(
		service.supervisor.close(ctx),
		service.executions.Close(ctx),
		service.inputs.Close(ctx),
		service.interactions.Close(ctx),
		service.events.Close(ctx),
		service.directory.Close(ctx),
		service.store.Close(ctx),
	)
}

// Drain stops new work and waits for active execution hosts to yield at their
// next durable model-turn boundary. Close remains the forced cancellation and
// resource cleanup phase when this wait exceeds the host shutdown deadline.
func (service *Service) Drain(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.draining.Store(true)
	queueErr := service.supervisor.close(ctx)
	drainer, ok := service.executions.(ExecutionDrainer)
	if !ok {
		return queueErr
	}
	return errors.Join(queueErr, drainer.Drain(ctx))
}

func (service *Service) requireIdle(
	ctx context.Context,
	session Session,
) error {
	execution, err := service.executions.Current(ctx, session)
	if errors.Is(err, ErrExecutionNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !execution.Execution.Terminal() {
		return fmt.Errorf(
			"%w: %s",
			ErrExecutionActive,
			execution.Execution.ID,
		)
	}
	return nil
}
