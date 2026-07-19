package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

var (
	ErrExecutionNotFound = errors.New("gateway execution not found")
	ErrExecutionActive   = errors.New("gateway session execution is active")
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
	RecoveryCandidate(context.Context, Session) (Execution, error)
	Recover(context.Context, Session) (Execution, error)
	Current(context.Context, Session) (Execution, error)
	Get(context.Context, Session, string) (Execution, error)
	Cancel(context.Context, Session, string) (Execution, error)
	Close(context.Context) error
}

type ServiceConfig struct {
	Store            SessionStore
	Directory        PluginDirectory
	Executions       ExecutionBackend
	DefaultNamespace string
	DefaultProvider  string
	DefaultSystem    string
	DefaultMaxTurns  int
}

type Service struct {
	store      SessionStore
	directory  PluginDirectory
	executions ExecutionBackend
	manager    *Manager
	gates      *sessionGate
	defaults   Session
}

func NewService(config ServiceConfig) (*Service, error) {
	if config.Executions == nil {
		return nil, errors.New("gateway execution backend is nil")
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
		store:      config.Store,
		directory:  config.Directory,
		executions: config.Executions,
		gates:      newSessionGate(),
		defaults: Session{
			Provider: config.DefaultProvider,
			System:   config.DefaultSystem,
			MaxTurns: config.DefaultMaxTurns,
		},
	}
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
	return service, nil
}

func (service *Service) CreateSession(
	ctx context.Context,
	session Session,
) (Session, error) {
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
			return scheduled, errors.Join(failures...)
		}
		for _, session := range page.Items {
			_, err := service.executions.RecoveryCandidate(ctx, session)
			if errors.Is(err, ErrExecutionNotFound) ||
				errors.Is(err, ErrExecutionActive) {
				continue
			}
			if err != nil {
				failures = append(failures, fmt.Errorf(
					"inspect gateway session %s recovery: %w",
					session.ID,
					err,
				))
				continue
			}
			if err := service.manager.validatePluginBindings(
				ctx,
				session,
			); err != nil {
				failures = append(failures, fmt.Errorf(
					"recover gateway session %s plugins: %w",
					session.ID,
					err,
				))
				continue
			}
			execution, err := service.executions.Recover(ctx, session)
			if errors.Is(err, ErrExecutionNotFound) {
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
			return scheduled, errors.Join(failures...)
		}
		request.After = page.Next
	}
}

func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	return errors.Join(
		service.executions.Close(ctx),
		service.directory.Close(ctx),
		service.store.Close(ctx),
	)
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
