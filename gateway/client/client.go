// Package client provides the typed gRPC boundary used by gateway frontends.
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/lincyaw/ag/gateway"
	gatewayv1 "github.com/lincyaw/ag/gatewayrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const defaultMaxMessageBytes = 16 << 20

type Config struct {
	Target          string
	UserID          string
	TLSConfig       *tls.Config
	MaxMessageBytes int
	DialOptions     []grpc.DialOption
}

type Client struct {
	target     string
	userID     string
	connection *grpc.ClientConn
	remote     gatewayv1.GatewayServiceClient
	closeOnce  sync.Once
	closeErr   error
}

type CreateSessionRequest struct {
	ID            string
	Provider      string
	System        string
	MaxTurns      int
	WorkspaceRoot string
	RuntimeConfig []byte
	Settings      gateway.SessionSettings
}

func New(config Config) (*Client, error) {
	target, transport, err := clientTransport(config)
	if err != nil {
		return nil, err
	}
	userID := strings.TrimSpace(config.UserID)
	if userID == "" {
		return nil, errors.New("gateway user ID is empty")
	}
	for _, character := range userID {
		if unicode.IsControl(character) {
			return nil, errors.New("gateway user ID contains control characters")
		}
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = defaultMaxMessageBytes
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("gateway RPC max message bytes must be positive")
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(config.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(config.MaxMessageBytes),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 30 * time.Second, Timeout: 10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	options = append(options, config.DialOptions...)
	connection, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, fmt.Errorf("create gateway RPC client: %w", err)
	}
	return &Client{
		target: target, userID: userID, connection: connection,
		remote: gatewayv1.NewGatewayServiceClient(connection),
	}, nil
}

func clientTransport(config Config) (string, credentials.TransportCredentials, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.Target))
	if err != nil {
		return "", nil, fmt.Errorf("parse gateway RPC target: %w", err)
	}
	if parsed.Scheme != "grpc" && parsed.Scheme != "grpcs" {
		return "", nil, errors.New(
			"gateway RPC target must use grpc:// or grpcs://",
		)
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", nil, errors.New("gateway RPC target must be an absolute host:port URI")
	}
	if parsed.Scheme == "grpc" {
		if config.TLSConfig != nil {
			return "", nil, errors.New("TLS config requires a grpcs:// gateway target")
		}
		return parsed.Host, insecure.NewCredentials(), nil
	}
	tlsConfig := config.TLSConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tlsConfig = tlsConfig.Clone()
	}
	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = parsed.Hostname()
	}
	return parsed.Host, credentials.NewTLS(tlsConfig), nil
}

func (client *Client) Target() string {
	if client == nil {
		return ""
	}
	return client.target
}

func (client *Client) Close() error {
	if client == nil {
		return nil
	}
	client.closeOnce.Do(func() {
		client.closeErr = client.connection.Close()
	})
	return client.closeErr
}

func (client *Client) Health(ctx context.Context) error {
	response, err := client.remote.Health(ctx, &gatewayv1.HealthRequest{})
	if err != nil {
		return fmt.Errorf("check gateway RPC health: %w", err)
	}
	if response.GetStatus() != "ok" {
		return fmt.Errorf("gateway RPC health is %q", response.GetStatus())
	}
	if response.GetProtocolVersion() != gatewayv1.ProtocolVersion {
		return fmt.Errorf(
			"gateway RPC protocol is %q, expected %q",
			response.GetProtocolVersion(),
			gatewayv1.ProtocolVersion,
		)
	}
	return nil
}

func (client *Client) CreateSession(
	ctx context.Context,
	request CreateSessionRequest,
) (gateway.Session, error) {
	maxTurns, err := int32Value("max turns", request.MaxTurns)
	if err != nil {
		return gateway.Session{}, err
	}
	settings, err := json.Marshal(request.Settings)
	if err != nil {
		return gateway.Session{}, fmt.Errorf("encode trajectory settings: %w", err)
	}
	response, err := client.remote.CreateTrajectory(
		ctx,
		&gatewayv1.CreateTrajectoryRequest{
			UserId: client.userID, Id: request.ID,
			Provider: request.Provider, System: request.System,
			MaxTurns: maxTurns, WorkspaceRoot: request.WorkspaceRoot,
			RuntimeConfigJson: request.RuntimeConfig,
			SettingsJson:      settings,
		},
	)
	return decodeResponse[gateway.Session]("create trajectory", response, err)
}

// UpdateSession applies one CAS-protected control-plane patch. Callers should
// refresh and retry on ErrSessionConflict rather than overwriting another
// attached frontend's change.
func (client *Client) UpdateSession(
	ctx context.Context,
	trajectoryID string,
	expectedRevision uint64,
	patch gateway.SessionPatch,
) (gateway.Session, error) {
	payload, err := json.Marshal(patch)
	if err != nil {
		return gateway.Session{}, fmt.Errorf("encode trajectory patch: %w", err)
	}
	response, err := client.remote.UpdateTrajectory(
		ctx,
		&gatewayv1.UpdateTrajectoryRequest{
			UserId:           client.userID,
			TrajectoryId:     trajectoryID,
			ExpectedRevision: expectedRevision,
			PatchJson:        payload,
		},
	)
	return decodeResponse[gateway.Session]("update trajectory", response, err)
}

func (client *Client) GetSession(
	ctx context.Context,
	trajectoryID string,
) (gateway.Session, error) {
	response, err := client.remote.GetTrajectory(
		ctx,
		&gatewayv1.GetTrajectoryRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
		},
	)
	return decodeResponse[gateway.Session]("get trajectory", response, err)
}

func (client *Client) ListSessions(
	ctx context.Context,
	request sdk.PageRequest,
) (gateway.SessionPage, error) {
	limit, err := int32Value("trajectory page limit", request.Limit)
	if err != nil {
		return gateway.SessionPage{}, err
	}
	response, err := client.remote.ListTrajectories(
		ctx,
		&gatewayv1.ListTrajectoriesRequest{
			UserId: client.userID, After: request.After, Limit: limit,
		},
	)
	return decodeResponse[gateway.SessionPage]("list trajectories", response, err)
}

func (client *Client) LoadTrajectory(
	ctx context.Context,
	trajectoryID string,
	head string,
) (sdk.Trajectory, error) {
	response, err := client.remote.LoadTrajectory(
		ctx,
		&gatewayv1.LoadTrajectoryRequest{
			UserId: client.userID, TrajectoryId: trajectoryID, Head: head,
		},
	)
	return decodeResponse[sdk.Trajectory]("load trajectory", response, err)
}

func (client *Client) ListConversation(
	ctx context.Context,
	trajectoryID string,
	head string,
	request gateway.ConversationQuery,
) (gateway.ConversationPage, error) {
	limit, err := int32Value("conversation page limit", request.Limit)
	if err != nil {
		return gateway.ConversationPage{}, err
	}
	response, err := client.remote.ListConversation(
		ctx,
		&gatewayv1.ListConversationRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			Head: head, After: request.After, Limit: limit,
		},
	)
	return decodeResponse[gateway.ConversationPage](
		"list trajectory conversation",
		response,
		err,
	)
}

func (client *Client) ListTrajectoryEntries(
	ctx context.Context,
	trajectoryID string,
	head string,
	request gateway.TrajectoryEntryQuery,
) (gateway.TrajectoryEntryPage, error) {
	limit, err := int32Value("trajectory entry page limit", request.Limit)
	if err != nil {
		return gateway.TrajectoryEntryPage{}, err
	}
	response, err := client.remote.ListTrajectoryEntries(
		ctx,
		&gatewayv1.ListTrajectoryEntriesRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			Head: head, After: request.After, Limit: limit,
		},
	)
	return decodeResponse[gateway.TrajectoryEntryPage](
		"list trajectory entries",
		response,
		err,
	)
}

func (client *Client) RollbackTrajectory(
	ctx context.Context,
	trajectoryID string,
	checkpointID string,
) (sdk.Trajectory, error) {
	response, err := client.remote.RollbackTrajectory(
		ctx,
		&gatewayv1.RollbackTrajectoryRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			CheckpointId: checkpointID,
		},
	)
	return decodeResponse[sdk.Trajectory]("roll back trajectory", response, err)
}

func (client *Client) AttachPlugin(
	ctx context.Context,
	trajectoryID string,
	selector string,
	expectedRevision uint64,
) (gateway.Session, error) {
	response, err := client.remote.AttachPlugin(
		ctx,
		&gatewayv1.AttachPluginRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			Selector: selector, ExpectedRevision: expectedRevision,
		},
	)
	return decodeResponse[gateway.Session]("attach trajectory plugin", response, err)
}

func (client *Client) DetachPlugin(
	ctx context.Context,
	trajectoryID string,
	name string,
	expectedRevision uint64,
) (gateway.Session, error) {
	response, err := client.remote.DetachPlugin(
		ctx,
		&gatewayv1.DetachPluginRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			Name: name, ExpectedRevision: expectedRevision,
		},
	)
	return decodeResponse[gateway.Session]("detach trajectory plugin", response, err)
}

func (client *Client) SubmitMessage(
	ctx context.Context,
	trajectoryID string,
	content string,
) (gateway.Execution, error) {
	response, err := client.remote.SubmitMessage(
		ctx,
		&gatewayv1.SubmitMessageRequest{
			UserId: client.userID, TrajectoryId: trajectoryID, Content: content,
		},
	)
	return decodeResponse[gateway.Execution]("submit trajectory message", response, err)
}

func (client *Client) EnqueueContextInjection(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) (gateway.Execution, error) {
	raw, err := json.Marshal(injection)
	if err != nil {
		return gateway.Execution{}, fmt.Errorf("encode context injection: %w", err)
	}
	response, err := client.remote.EnqueueContextInjection(
		ctx,
		&gatewayv1.EnqueueContextInjectionRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			ExecutionId: executionID, InjectionJson: raw,
		},
	)
	return decodeResponse[gateway.Execution](
		"enqueue trajectory context injection", response, err,
	)
}

func (client *Client) EnqueueInput(
	ctx context.Context,
	trajectoryID string,
	inputID string,
	content string,
) (gateway.AgentInput, error) {
	response, err := client.remote.EnqueueInput(
		ctx,
		&gatewayv1.EnqueueInputRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			InputId: inputID, Content: content,
		},
	)
	return decodeResponse[gateway.AgentInput]("enqueue trajectory input", response, err)
}

func (client *Client) GetInput(
	ctx context.Context,
	trajectoryID string,
	inputID string,
) (gateway.AgentInput, error) {
	response, err := client.remote.GetInput(
		ctx,
		&gatewayv1.GetInputRequest{
			UserId: client.userID, TrajectoryId: trajectoryID, InputId: inputID,
		},
	)
	return decodeResponse[gateway.AgentInput]("get trajectory input", response, err)
}

func (client *Client) ListInputs(
	ctx context.Context,
	trajectoryID string,
	request gateway.InputQuery,
) (gateway.InputPage, error) {
	limit, err := int32Value("input page limit", request.Limit)
	if err != nil {
		return gateway.InputPage{}, err
	}
	response, err := client.remote.ListInputs(
		ctx,
		&gatewayv1.ListInputsRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			After: request.After, Limit: limit,
		},
	)
	return decodeResponse[gateway.InputPage]("list trajectory inputs", response, err)
}

func (client *Client) CancelInput(
	ctx context.Context,
	trajectoryID string,
	inputID string,
	expectedRevision uint64,
) (gateway.AgentInput, error) {
	response, err := client.remote.CancelInput(
		ctx,
		&gatewayv1.CancelInputRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			InputId: inputID, ExpectedRevision: expectedRevision,
		},
	)
	return decodeResponse[gateway.AgentInput]("cancel trajectory input", response, err)
}

func (client *Client) PauseSession(
	ctx context.Context,
	trajectoryID string,
	expectedRevision uint64,
) (gateway.Session, error) {
	return client.setPaused(ctx, trajectoryID, expectedRevision, true)
}

func (client *Client) ResumeSession(
	ctx context.Context,
	trajectoryID string,
	expectedRevision uint64,
) (gateway.Session, error) {
	return client.setPaused(ctx, trajectoryID, expectedRevision, false)
}

func (client *Client) setPaused(
	ctx context.Context,
	trajectoryID string,
	expectedRevision uint64,
	paused bool,
) (gateway.Session, error) {
	response, err := client.remote.SetPaused(
		ctx,
		&gatewayv1.SetPausedRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			Paused: paused, ExpectedRevision: expectedRevision,
		},
	)
	return decodeResponse[gateway.Session]("set trajectory paused", response, err)
}

func (client *Client) ListEvents(
	ctx context.Context,
	trajectoryID string,
	request gateway.EventQuery,
) (gateway.EventPage, error) {
	limit, err := int32Value("event page limit", request.Limit)
	if err != nil {
		return gateway.EventPage{}, err
	}
	response, err := client.remote.ListEvents(
		ctx,
		&gatewayv1.ListEventsRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			After: request.After, Limit: limit,
		},
	)
	return decodeResponse[gateway.EventPage]("list trajectory events", response, err)
}

func (client *Client) GetEventCursor(
	ctx context.Context,
	trajectoryID string,
) (gateway.EventCursor, error) {
	response, err := client.remote.GetEventCursor(
		ctx,
		&gatewayv1.GetEventCursorRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
		},
	)
	return decodeResponse[gateway.EventCursor](
		"get trajectory event cursor",
		response,
		err,
	)
}

func (client *Client) GetInteraction(
	ctx context.Context,
	trajectoryID string,
	interactionID string,
) (gateway.Interaction, error) {
	response, err := client.remote.GetInteraction(
		ctx,
		&gatewayv1.GetInteractionRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			InteractionId: interactionID,
		},
	)
	return decodeResponse[gateway.Interaction]("get trajectory interaction", response, err)
}

func (client *Client) ListInteractions(
	ctx context.Context,
	trajectoryID string,
	request gateway.InteractionQuery,
) (gateway.InteractionPage, error) {
	limit, err := int32Value("interaction page limit", request.Limit)
	if err != nil {
		return gateway.InteractionPage{}, err
	}
	response, err := client.remote.ListInteractions(
		ctx,
		&gatewayv1.ListInteractionsRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			After: request.After, Limit: limit, State: string(request.State),
		},
	)
	return decodeResponse[gateway.InteractionPage](
		"list trajectory interactions", response, err,
	)
}

func (client *Client) ResolveInteraction(
	ctx context.Context,
	trajectoryID string,
	interactionID string,
	expectedRevision uint64,
	answer gateway.InteractionAnswer,
) (gateway.Interaction, error) {
	response, err := client.remote.ResolveInteraction(
		ctx,
		&gatewayv1.ResolveInteractionRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			InteractionId:    interactionID,
			ExpectedRevision: expectedRevision,
			OptionId:         answer.OptionID, Text: answer.Text,
		},
	)
	return decodeResponse[gateway.Interaction](
		"resolve trajectory interaction", response, err,
	)
}

func (client *Client) GetExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
) (gateway.Execution, error) {
	response, err := client.remote.GetExecution(
		ctx,
		&gatewayv1.GetExecutionRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			ExecutionId: executionID,
		},
	)
	return decodeResponse[gateway.Execution]("get trajectory execution", response, err)
}

func (client *Client) CancelExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
) (gateway.Execution, error) {
	response, err := client.remote.CancelExecution(
		ctx,
		&gatewayv1.CancelExecutionRequest{
			UserId: client.userID, TrajectoryId: trajectoryID,
			ExecutionId: executionID,
		},
	)
	return decodeResponse[gateway.Execution]("cancel trajectory execution", response, err)
}

func decodeResponse[T any](
	action string,
	response *gatewayv1.JsonValue,
	err error,
) (T, error) {
	var value T
	if err != nil {
		return value, fmt.Errorf("%s: %w", action, err)
	}
	if response == nil || len(response.GetJson()) == 0 {
		return value, fmt.Errorf("%s: gateway RPC returned an empty value", action)
	}
	if err := json.Unmarshal(response.GetJson(), &value); err != nil {
		return value, fmt.Errorf("%s: decode gateway RPC value: %w", action, err)
	}
	return value, nil
}

func int32Value(name string, value int) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%s is outside the RPC integer range", name)
	}
	return int32(value), nil
}

type View struct {
	trajectory gateway.Session
	stream     grpc.BidiStreamingClient[gatewayv1.ViewRequest, gatewayv1.ViewResponse]
	context    context.Context
	cancel     context.CancelFunc
	sendMu     sync.Mutex
	pendingMu  sync.Mutex
	pending    map[string]chan viewResult
	events     chan gateway.AgentEvent
	done       chan struct{}
	finishOnce sync.Once
	errorMu    sync.Mutex
	terminal   error
	cursor     atomic.Uint64
}

type viewResult struct {
	value *gatewayv1.JsonValue
	err   error
}

func (client *Client) OpenView(
	ctx context.Context,
	trajectoryID string,
	afterEvent uint64,
) (*View, error) {
	viewContext, cancel := context.WithCancel(ctx)
	stream, err := client.remote.Connect(viewContext)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("connect trajectory view: %w", err)
	}
	if err := stream.Send(&gatewayv1.ViewRequest{
		Frame: &gatewayv1.ViewRequest_Open{Open: &gatewayv1.OpenView{
			UserId: client.userID, TrajectoryId: trajectoryID,
			AfterEvent: afterEvent,
		}},
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("open trajectory view: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("read trajectory view readiness: %w", err)
	}
	ready := first.GetReady()
	if ready == nil {
		cancel()
		return nil, errors.New("trajectory view did not return readiness first")
	}
	trajectory, err := decodeResponse[gateway.Session](
		"decode trajectory view readiness", ready.GetTrajectory(), nil,
	)
	if err != nil {
		cancel()
		return nil, err
	}
	if trajectory.ID != trajectoryID {
		cancel()
		return nil, fmt.Errorf(
			"trajectory view opened %q, expected %q", trajectory.ID, trajectoryID,
		)
	}
	view := &View{
		trajectory: trajectory, stream: stream,
		context: viewContext, cancel: cancel,
		pending: make(map[string]chan viewResult),
		events:  make(chan gateway.AgentEvent, 256),
		done:    make(chan struct{}),
	}
	view.cursor.Store(ready.GetCursor())
	go view.receive()
	return view, nil
}

func (view *View) Trajectory() gateway.Session { return view.trajectory }

func (view *View) Cursor() uint64 {
	if view == nil {
		return 0
	}
	return view.cursor.Load()
}

func (view *View) Next() (gateway.AgentEvent, error) {
	if view == nil {
		return gateway.AgentEvent{}, errors.New("trajectory view is nil")
	}
	event, ok := <-view.events
	if ok {
		return event, nil
	}
	return gateway.AgentEvent{}, view.terminalError()
}

func (view *View) Close() error {
	if view == nil {
		return nil
	}
	view.cancel()
	select {
	case <-view.done:
	default:
	}
	err := view.terminalError()
	if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (view *View) EnqueueInput(
	ctx context.Context,
	inputID string,
	content string,
) (gateway.AgentInput, error) {
	return viewCommand[gateway.AgentInput](view, ctx, &gatewayv1.ViewCommand{
		RequestId: sdk.NewID(),
		Command: &gatewayv1.ViewCommand_EnqueueInput{
			EnqueueInput: &gatewayv1.ViewEnqueueInput{
				InputId: inputID, Content: content,
			},
		},
	})
}

func (view *View) CancelInput(
	ctx context.Context,
	inputID string,
	expectedRevision uint64,
) (gateway.AgentInput, error) {
	return viewCommand[gateway.AgentInput](view, ctx, &gatewayv1.ViewCommand{
		RequestId: sdk.NewID(),
		Command: &gatewayv1.ViewCommand_CancelInput{
			CancelInput: &gatewayv1.ViewCancelInput{
				InputId: inputID, ExpectedRevision: expectedRevision,
			},
		},
	})
}

func (view *View) ResolveInteraction(
	ctx context.Context,
	interactionID string,
	expectedRevision uint64,
	answer gateway.InteractionAnswer,
) (gateway.Interaction, error) {
	return viewCommand[gateway.Interaction](view, ctx, &gatewayv1.ViewCommand{
		RequestId: sdk.NewID(),
		Command: &gatewayv1.ViewCommand_ResolveInteraction{
			ResolveInteraction: &gatewayv1.ViewResolveInteraction{
				InteractionId:    interactionID,
				ExpectedRevision: expectedRevision,
				OptionId:         answer.OptionID, Text: answer.Text,
			},
		},
	})
}

func (view *View) SetPaused(
	ctx context.Context,
	paused bool,
	expectedRevision uint64,
) (gateway.Session, error) {
	return viewCommand[gateway.Session](view, ctx, &gatewayv1.ViewCommand{
		RequestId: sdk.NewID(),
		Command: &gatewayv1.ViewCommand_SetPaused{
			SetPaused: &gatewayv1.ViewSetPaused{
				Paused: paused, ExpectedRevision: expectedRevision,
			},
		},
	})
}

func (view *View) CancelExecution(
	ctx context.Context,
	executionID string,
) (gateway.Execution, error) {
	return viewCommand[gateway.Execution](view, ctx, &gatewayv1.ViewCommand{
		RequestId: sdk.NewID(),
		Command: &gatewayv1.ViewCommand_CancelExecution{
			CancelExecution: &gatewayv1.ViewCancelExecution{
				ExecutionId: executionID,
			},
		},
	})
}

func viewCommand[T any](
	view *View,
	ctx context.Context,
	command *gatewayv1.ViewCommand,
) (T, error) {
	var zero T
	response := make(chan viewResult, 1)
	view.pendingMu.Lock()
	select {
	case <-view.done:
		view.pendingMu.Unlock()
		return zero, view.terminalError()
	default:
	}
	view.pending[command.GetRequestId()] = response
	view.pendingMu.Unlock()

	view.sendMu.Lock()
	err := view.stream.Send(&gatewayv1.ViewRequest{
		Frame: &gatewayv1.ViewRequest_Command{Command: command},
	})
	view.sendMu.Unlock()
	if err != nil {
		view.removePending(command.GetRequestId())
		return zero, fmt.Errorf("send trajectory view command: %w", err)
	}

	select {
	case <-ctx.Done():
		view.removePending(command.GetRequestId())
		return zero, ctx.Err()
	case result := <-response:
		if result.err != nil {
			return zero, result.err
		}
		return decodeResponse[T]("decode trajectory view command", result.value, nil)
	}
}

func (view *View) receive() {
	for {
		response, err := view.stream.Recv()
		if err != nil {
			view.finish(err)
			return
		}
		switch frame := response.GetFrame().(type) {
		case *gatewayv1.ViewResponse_Event:
			event, err := decodeResponse[gateway.AgentEvent](
				"decode trajectory view event", frame.Event.GetEvent(), nil,
			)
			if err != nil {
				view.finish(err)
				return
			}
			if event.SessionID != view.trajectory.ID || event.Sequence == 0 {
				view.finish(errors.New("gateway returned an invalid trajectory event"))
				return
			}
			if event.Sequence <= view.cursor.Load() {
				continue
			}
			view.cursor.Store(event.Sequence)
			select {
			case <-view.context.Done():
				view.finish(view.context.Err())
				return
			case view.events <- event:
			}
		case *gatewayv1.ViewResponse_Result:
			view.complete(frame.Result.GetRequestId(), viewResult{
				value: frame.Result.GetValue(),
			})
		case *gatewayv1.ViewResponse_Failure:
			code := parseCode(frame.Failure.GetCode())
			view.complete(frame.Failure.GetRequestId(), viewResult{
				err: status.Error(code, frame.Failure.GetMessage()),
			})
		default:
			view.finish(errors.New("gateway returned an unexpected trajectory view frame"))
			return
		}
	}
}

func (view *View) complete(requestID string, result viewResult) {
	view.pendingMu.Lock()
	target := view.pending[requestID]
	delete(view.pending, requestID)
	view.pendingMu.Unlock()
	if target != nil {
		target <- result
	}
}

func (view *View) removePending(requestID string) {
	view.pendingMu.Lock()
	delete(view.pending, requestID)
	view.pendingMu.Unlock()
}

func (view *View) finish(err error) {
	view.finishOnce.Do(func() {
		view.errorMu.Lock()
		view.terminal = err
		view.errorMu.Unlock()
		view.cancel()
		view.pendingMu.Lock()
		for requestID, target := range view.pending {
			delete(view.pending, requestID)
			target <- viewResult{err: err}
		}
		view.pendingMu.Unlock()
		close(view.events)
		close(view.done)
	})
}

func (view *View) terminalError() error {
	view.errorMu.Lock()
	defer view.errorMu.Unlock()
	if view.terminal == nil {
		return context.Canceled
	}
	return view.terminal
}

func parseCode(value string) codes.Code {
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		if strings.EqualFold(code.String(), value) {
			return code
		}
	}
	return codes.Unknown
}
