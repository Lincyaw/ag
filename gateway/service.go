package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	Submit(context.Context, Session, string) (Execution, error)
	Current(context.Context, Session) (Execution, error)
	Get(context.Context, Session, string) (Execution, error)
	Cancel(context.Context, Session, string) (Execution, error)
	Close(context.Context) error
}

type ServiceConfig struct {
	Store            SessionStore
	Directory        registry.Directory
	Executions       ExecutionBackend
	DefaultNamespace string
}

type Service struct {
	store      SessionStore
	directory  registry.Directory
	executions ExecutionBackend
	manager    *Manager
}

func NewService(config ServiceConfig) (*Service, error) {
	if config.Executions == nil {
		return nil, errors.New("gateway execution backend is nil")
	}
	service := &Service{
		store:      config.Store,
		directory:  config.Directory,
		executions: config.Executions,
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
			context.WithoutCancel(ctx),
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
	userID = strings.TrimSpace(userID)
	if err := validateUserID(userID); err != nil {
		return SessionPage{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	request, err := validatePage(request)
	if err != nil {
		return SessionPage{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	result := SessionPage{Items: make([]Session, 0, request.Limit)}
	cursor := request.After
	for len(result.Items) < request.Limit {
		page, err := service.store.List(ctx, sdk.PageRequest{
			After: cursor,
			Limit: sdk.MaxPageSize,
		})
		if err != nil {
			return SessionPage{}, err
		}
		for index, session := range page.Items {
			cursor = session.ID
			if session.UserID == userID {
				result.Items = append(result.Items, session)
				if len(result.Items) == request.Limit {
					if index+1 < len(page.Items) || page.Next != "" {
						result.Next = cursor
					}
					return result, nil
				}
			}
		}
		if page.Next == "" {
			return result, nil
		}
		cursor = page.Next
	}
	return result, nil
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
	session, err := service.manager.ownedSession(ctx, userID, sessionID)
	if err != nil {
		return Execution{}, err
	}
	if err := service.requireIdle(ctx, session); err != nil {
		return Execution{}, err
	}
	if _, err := service.manager.ResolvePlugins(ctx, session); err != nil {
		return Execution{}, err
	}
	return service.executions.Submit(ctx, session, content)
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
