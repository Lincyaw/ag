package gatewayrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/lincyaw/ag/gateway"
	gatewayv1 "github.com/lincyaw/ag/gatewayrpc/v1"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	DefaultMaxMessageBytes = 16 << 20
	streamEventPageSize    = 1000
)

// Server adapts the durable gateway service to the private, trajectory-named
// gRPC control plane used by ag frontends.
type Server struct {
	gatewayv1.UnimplementedGatewayServiceServer
	service *gateway.Service
}

func NewServer(service *gateway.Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("gateway RPC service is nil")
	}
	return &Server{service: service}, nil
}

func NewGRPCServer(
	service *gateway.Service,
	maxMessageBytes int,
	options ...grpc.ServerOption,
) (*grpc.Server, error) {
	adapter, err := NewServer(service)
	if err != nil {
		return nil, err
	}
	if maxMessageBytes == 0 {
		maxMessageBytes = DefaultMaxMessageBytes
	}
	if maxMessageBytes < 1 {
		return nil, errors.New("gateway RPC max message bytes must be positive")
	}
	base := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 20 * time.Second, PermitWithoutStream: true,
		}),
	}
	server := grpc.NewServer(append(base, options...)...)
	gatewayv1.RegisterGatewayServiceServer(server, adapter)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(
		gatewayv1.GatewayService_ServiceDesc.ServiceName,
		healthv1.HealthCheckResponse_SERVING,
	)
	healthv1.RegisterHealthServer(server, healthServer)
	return server, nil
}

func (server *Server) Health(
	context.Context,
	*gatewayv1.HealthRequest,
) (*gatewayv1.HealthResponse, error) {
	return &gatewayv1.HealthResponse{
		Status: "ok", ProtocolVersion: gatewayv1.ProtocolVersion,
	}, nil
}

func (server *Server) CreateTrajectory(
	ctx context.Context,
	request *gatewayv1.CreateTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	created, err := server.service.CreateSession(ctx, gateway.Session{
		ID: request.GetId(), UserID: request.GetUserId(),
		Provider: request.GetProvider(), System: request.GetSystem(),
		MaxTurns:      int(request.GetMaxTurns()),
		WorkspaceRoot: request.GetWorkspaceRoot(),
		RuntimeConfig: request.GetRuntimeConfigJson(),
	})
	return rpcValue(created, err)
}

func (server *Server) GetTrajectory(
	ctx context.Context,
	request *gatewayv1.GetTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.GetSession(
		ctx, request.GetUserId(), request.GetTrajectoryId(),
	)
	return rpcValue(value, err)
}

func (server *Server) ListTrajectories(
	ctx context.Context,
	request *gatewayv1.ListTrajectoriesRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListSessions(
		ctx,
		request.GetUserId(),
		sdk.PageRequest{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) LoadTrajectory(
	ctx context.Context,
	request *gatewayv1.LoadTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.LoadTrajectory(
		ctx, request.GetUserId(), request.GetTrajectoryId(), request.GetHead(),
	)
	return rpcValue(value, err)
}

func (server *Server) ListConversation(
	ctx context.Context,
	request *gatewayv1.ListConversationRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListConversation(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetHead(),
		gateway.ConversationQuery{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) ListTrajectoryEntries(
	ctx context.Context,
	request *gatewayv1.ListTrajectoryEntriesRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListTrajectoryEntries(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetHead(),
		gateway.TrajectoryEntryQuery{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) RollbackTrajectory(
	ctx context.Context,
	request *gatewayv1.RollbackTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.RollbackTrajectory(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetCheckpointId(),
	)
	if err == nil {
		value.Entries = nil
	}
	return rpcValue(value, err)
}

func (server *Server) AttachPlugin(
	ctx context.Context,
	request *gatewayv1.AttachPluginRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.AttachPlugin(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetSelector(),
		request.GetExpectedRevision(),
	)
	return rpcValue(value, err)
}

func (server *Server) DetachPlugin(
	ctx context.Context,
	request *gatewayv1.DetachPluginRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.DetachPlugin(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetName(),
		request.GetExpectedRevision(),
	)
	return rpcValue(value, err)
}

func (server *Server) SubmitMessage(
	ctx context.Context,
	request *gatewayv1.SubmitMessageRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.SubmitMessage(
		ctx, request.GetUserId(), request.GetTrajectoryId(), request.GetContent(),
	)
	return rpcValue(value, err)
}

func (server *Server) EnqueueContextInjection(
	ctx context.Context,
	request *gatewayv1.EnqueueContextInjectionRequest,
) (*gatewayv1.JsonValue, error) {
	var injection sdk.ContextInjection
	if err := json.Unmarshal(request.GetInjectionJson(), &injection); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid context injection JSON")
	}
	value, err := server.service.EnqueueContextInjection(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetExecutionId(),
		injection,
	)
	return rpcValue(value, err)
}

func (server *Server) EnqueueInput(
	ctx context.Context,
	request *gatewayv1.EnqueueInputRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.EnqueueInput(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		gateway.AgentInput{
			ID: request.GetInputId(), Kind: gateway.AgentInputPrompt,
			Content: request.GetContent(),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) GetInput(
	ctx context.Context,
	request *gatewayv1.GetInputRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.GetInput(
		ctx, request.GetUserId(), request.GetTrajectoryId(), request.GetInputId(),
	)
	return rpcValue(value, err)
}

func (server *Server) ListInputs(
	ctx context.Context,
	request *gatewayv1.ListInputsRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListInputs(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		gateway.InputQuery{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) CancelInput(
	ctx context.Context,
	request *gatewayv1.CancelInputRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.CancelInput(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetInputId(),
		request.GetExpectedRevision(),
	)
	return rpcValue(value, err)
}

func (server *Server) SetPaused(
	ctx context.Context,
	request *gatewayv1.SetPausedRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.SetSessionPaused(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetPaused(),
		request.GetExpectedRevision(),
	)
	return rpcValue(value, err)
}

func (server *Server) ListEvents(
	ctx context.Context,
	request *gatewayv1.ListEventsRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListEvents(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		gateway.EventQuery{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) GetEventCursor(
	ctx context.Context,
	request *gatewayv1.GetEventCursorRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.GetEventCursor(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
	)
	return rpcValue(value, err)
}

func (server *Server) GetInteraction(
	ctx context.Context,
	request *gatewayv1.GetInteractionRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.GetInteraction(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetInteractionId(),
	)
	return rpcValue(value, err)
}

func (server *Server) ListInteractions(
	ctx context.Context,
	request *gatewayv1.ListInteractionsRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ListInteractions(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		gateway.InteractionQuery{
			After: request.GetAfter(), Limit: int(request.GetLimit()),
			State: gateway.InteractionState(request.GetState()),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) ResolveInteraction(
	ctx context.Context,
	request *gatewayv1.ResolveInteractionRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.ResolveInteraction(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetInteractionId(),
		request.GetExpectedRevision(),
		gateway.InteractionAnswer{
			OptionID: request.GetOptionId(), Text: request.GetText(),
		},
	)
	return rpcValue(value, err)
}

func (server *Server) GetExecution(
	ctx context.Context,
	request *gatewayv1.GetExecutionRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.GetExecution(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetExecutionId(),
	)
	return rpcValue(value, err)
}

func (server *Server) CancelExecution(
	ctx context.Context,
	request *gatewayv1.CancelExecutionRequest,
) (*gatewayv1.JsonValue, error) {
	value, err := server.service.CancelExecution(
		ctx,
		request.GetUserId(),
		request.GetTrajectoryId(),
		request.GetExecutionId(),
	)
	return rpcValue(value, err)
}

func (server *Server) Connect(
	stream grpc.BidiStreamingServer[gatewayv1.ViewRequest, gatewayv1.ViewResponse],
) error {
	first, err := stream.Recv()
	if err != nil {
		return rpcError(err)
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "first view frame must be open")
	}
	trajectory, err := server.service.GetSession(
		stream.Context(), open.GetUserId(), open.GetTrajectoryId(),
	)
	if err != nil {
		return rpcError(err)
	}
	trajectoryValue, err := marshalValue(trajectory)
	if err != nil {
		return rpcError(err)
	}
	if err := stream.Send(&gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Ready{Ready: &gatewayv1.ViewReady{
			Trajectory: trajectoryValue, Cursor: open.GetAfterEvent(),
		}},
	}); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	responses := make(chan *gatewayv1.ViewResponse, 64)
	failed := make(chan error, 2)
	go server.receiveViewCommands(
		ctx, stream, open.GetUserId(), trajectory.ID, responses, failed,
	)
	go server.forwardViewEvents(
		ctx, open.GetUserId(), trajectory.ID, open.GetAfterEvent(), responses, failed,
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-failed:
			if err == nil || errors.Is(err, io.EOF) ||
				errors.Is(err, context.Canceled) {
				return nil
			}
			return rpcError(err)
		case response := <-responses:
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func (server *Server) receiveViewCommands(
	ctx context.Context,
	stream grpc.BidiStreamingServer[gatewayv1.ViewRequest, gatewayv1.ViewResponse],
	userID string,
	trajectoryID string,
	responses chan<- *gatewayv1.ViewResponse,
	failed chan<- error,
) {
	for {
		request, err := stream.Recv()
		if err != nil {
			sendFailure(ctx, failed, err)
			return
		}
		command := request.GetCommand()
		if command == nil {
			sendFailure(ctx, failed, status.Error(
				codes.InvalidArgument, "view is already open",
			))
			return
		}
		response := server.applyViewCommand(
			ctx, userID, trajectoryID, command,
		)
		select {
		case <-ctx.Done():
			return
		case responses <- response:
		}
	}
}

func (server *Server) forwardViewEvents(
	ctx context.Context,
	userID string,
	trajectoryID string,
	after uint64,
	responses chan<- *gatewayv1.ViewResponse,
	failed chan<- error,
) {
	query := gateway.EventQuery{After: after, Limit: streamEventPageSize}
	for {
		page, err := server.service.WaitEvents(ctx, userID, trajectoryID, query)
		if err != nil {
			sendFailure(ctx, failed, err)
			return
		}
		for _, event := range page.Items {
			value, err := marshalValue(event)
			if err != nil {
				sendFailure(ctx, failed, err)
				return
			}
			response := &gatewayv1.ViewResponse{
				Frame: &gatewayv1.ViewResponse_Event{Event: &gatewayv1.ViewEvent{
					Event: value,
				}},
			}
			select {
			case <-ctx.Done():
				return
			case responses <- response:
			}
			query.After = event.Sequence
		}
	}
}

func (server *Server) applyViewCommand(
	ctx context.Context,
	userID string,
	trajectoryID string,
	command *gatewayv1.ViewCommand,
) *gatewayv1.ViewResponse {
	requestID := strings.TrimSpace(command.GetRequestId())
	if requestID == "" {
		return viewFailure("", status.Error(
			codes.InvalidArgument, "view command request ID is empty",
		))
	}
	var (
		value any
		err   error
	)
	switch current := command.GetCommand().(type) {
	case *gatewayv1.ViewCommand_EnqueueInput:
		value, err = server.service.EnqueueInput(
			ctx, userID, trajectoryID, gateway.AgentInput{
				ID:      current.EnqueueInput.GetInputId(),
				Kind:    gateway.AgentInputPrompt,
				Content: current.EnqueueInput.GetContent(),
			},
		)
	case *gatewayv1.ViewCommand_CancelInput:
		value, err = server.service.CancelInput(
			ctx, userID, trajectoryID,
			current.CancelInput.GetInputId(),
			current.CancelInput.GetExpectedRevision(),
		)
	case *gatewayv1.ViewCommand_ResolveInteraction:
		value, err = server.service.ResolveInteraction(
			ctx, userID, trajectoryID,
			current.ResolveInteraction.GetInteractionId(),
			current.ResolveInteraction.GetExpectedRevision(),
			gateway.InteractionAnswer{
				OptionID: current.ResolveInteraction.GetOptionId(),
				Text:     current.ResolveInteraction.GetText(),
			},
		)
	case *gatewayv1.ViewCommand_SetPaused:
		value, err = server.service.SetSessionPaused(
			ctx, userID, trajectoryID,
			current.SetPaused.GetPaused(),
			current.SetPaused.GetExpectedRevision(),
		)
	case *gatewayv1.ViewCommand_CancelExecution:
		value, err = server.service.CancelExecution(
			ctx, userID, trajectoryID,
			current.CancelExecution.GetExecutionId(),
		)
	default:
		err = status.Error(codes.InvalidArgument, "view command is empty")
	}
	if err != nil {
		return viewFailure(requestID, err)
	}
	encoded, err := marshalValue(value)
	if err != nil {
		return viewFailure(requestID, err)
	}
	return &gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Result{Result: &gatewayv1.ViewResult{
			RequestId: requestID, Value: encoded,
		}},
	}
}

func viewFailure(requestID string, err error) *gatewayv1.ViewResponse {
	converted := rpcError(err)
	return &gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Failure{Failure: &gatewayv1.ViewFailure{
			RequestId: requestID,
			Code:      status.Code(converted).String(),
			Message:   status.Convert(converted).Message(),
		}},
	}
}

func sendFailure(ctx context.Context, target chan<- error, err error) {
	select {
	case <-ctx.Done():
	case target <- err:
	}
}

func rpcValue[T any](value T, err error) (*gatewayv1.JsonValue, error) {
	if err != nil {
		return nil, rpcError(err)
	}
	encoded, err := marshalValue(value)
	if err != nil {
		return nil, rpcError(err)
	}
	return encoded, nil
}

func marshalValue(value any) (*gatewayv1.JsonValue, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode gateway RPC value: %w", err)
	}
	return &gatewayv1.JsonValue{Json: raw}, nil
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, gateway.ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, gateway.ErrGatewayDraining):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, gateway.ErrSessionNotFound),
		errors.Is(err, gateway.ErrExecutionNotFound),
		errors.Is(err, gateway.ErrInputNotFound),
		errors.Is(err, gateway.ErrInteractionNotFound),
		errors.Is(err, sdk.ErrTrajectoryNotFound),
		errors.Is(err, sdk.ErrTrajectoryEntryNotFound),
		errors.Is(err, registry.ErrInstanceNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, gateway.ErrSessionExists),
		errors.Is(err, gateway.ErrSessionConflict),
		errors.Is(err, gateway.ErrExecutionActive),
		errors.Is(err, gateway.ErrInputConflict),
		errors.Is(err, gateway.ErrInteractionConflict),
		errors.Is(err, gateway.ErrPluginAmbiguous),
		errors.Is(err, gateway.ErrPluginNotBound),
		errors.Is(err, gateway.ErrBindingStale),
		errors.Is(err, sdk.ErrTrajectoryExecution),
		errors.Is(err, sdk.ErrTrajectoryClaimed),
		errors.Is(err, sdk.ErrTrajectoryFence):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, gateway.ErrInvalidRequest),
		errors.Is(err, registry.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal gateway error")
	}
}
