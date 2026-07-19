package pluginrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerConfig struct {
	Plugin            sdk.Plugin
	Operations        sdk.OperationStore
	Inbox             sdk.DeliveryStore
	Logger            *slog.Logger
	InboxWorkers      int
	InboxLease        time.Duration
	InboxPoll         time.Duration
	SubscriberTimeout time.Duration
	InboxMaxAttempts  int
	OperationLease    time.Duration
}

type Server interface {
	pluginv1.PluginServiceServer
	Close(context.Context) error
}

type server struct {
	pluginv1.UnimplementedPluginServiceServer
	manifest          sdk.Manifest
	registrar         *plugincontract.Registrar
	pluginCloser      interface{ Close(context.Context) error }
	operations        sdk.OperationStore
	inbox             sdk.DeliveryStore
	logger            *slog.Logger
	inboxWorkers      int
	inboxLease        time.Duration
	inboxPoll         time.Duration
	subscriberTimeout time.Duration
	inboxMaxAttempts  int
	operationLease    time.Duration
	operationWorkerID string
	context           context.Context
	cancel            context.CancelFunc
	wait              sync.WaitGroup
	lifecycleMu       sync.Mutex
	closed            bool
	closeDone         chan struct{}
	closeErr          error
	operationInflight operationworker.Inflight
}

func NewServer(ctx context.Context, config ServerConfig) (Server, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.Plugin == nil {
		return nil, errors.New("plugin is nil")
	}
	if config.Operations == nil {
		return nil, errors.New("operation store is nil")
	}
	if config.Inbox == nil {
		return nil, errors.New("inbox delivery store is nil")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.InboxWorkers == 0 {
		config.InboxWorkers = 2
	}
	if config.InboxLease == 0 {
		config.InboxLease = 30 * time.Second
	}
	if config.InboxPoll == 0 {
		config.InboxPoll = 25 * time.Millisecond
	}
	if config.SubscriberTimeout == 0 {
		config.SubscriberTimeout = 5 * time.Second
	}
	if config.InboxMaxAttempts == 0 {
		config.InboxMaxAttempts = 8
	}
	if config.OperationLease == 0 {
		config.OperationLease = 30 * time.Second
	}
	if config.InboxWorkers < 1 || config.InboxLease <= 0 ||
		config.InboxPoll <= 0 || config.SubscriberTimeout <= 0 ||
		config.InboxMaxAttempts < 1 || config.OperationLease <= 0 {
		return nil, errors.New("RPC server worker settings must be positive")
	}
	if config.SubscriberTimeout >= config.InboxLease {
		return nil, errors.New(
			"subscriber timeout must be shorter than the inbox lease",
		)
	}
	manifest := sdk.CloneManifest(config.Plugin.Manifest())
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("validate plugin manifest: %w", err)
	}
	registrar := plugincontract.NewRegistrar()
	if err := config.Plugin.Install(ctx, registrar); err != nil {
		return nil, fmt.Errorf("install plugin into RPC server: %w", err)
	}
	if err := registrar.ValidateManifest(manifest); err != nil {
		return nil, err
	}
	if err := validateRPCContributions(registrar); err != nil {
		return nil, err
	}
	serverContext, cancel := context.WithCancel(lifecycle.Detached(ctx))
	server := &server{
		manifest:          manifest,
		registrar:         registrar,
		operations:        config.Operations,
		inbox:             config.Inbox,
		logger:            config.Logger,
		inboxWorkers:      config.InboxWorkers,
		inboxLease:        config.InboxLease,
		inboxPoll:         config.InboxPoll,
		subscriberTimeout: config.SubscriberTimeout,
		inboxMaxAttempts:  config.InboxMaxAttempts,
		operationLease:    config.OperationLease,
		operationWorkerID: fmt.Sprintf("plugin-%s-%d", manifest.Name, time.Now().UnixNano()),
		context:           serverContext,
		cancel:            cancel,
		closeDone:         make(chan struct{}),
		operationInflight: operationworker.NewInflight(serverContext),
	}
	server.pluginCloser, _ = config.Plugin.(interface {
		Close(context.Context) error
	})
	if err := server.recoverOperations(ctx); err != nil {
		cancel()
		return nil, err
	}
	if len(registrar.Subscribers) > 0 {
		for worker := range server.inboxWorkers {
			server.wait.Add(1)
			go server.inboxLoop(worker)
		}
	}
	return server, nil
}

func (server *server) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	server.lifecycleMu.Lock()
	if !server.closed {
		server.closed = true
		server.cancel()
		go func() {
			server.wait.Wait()
			if server.pluginCloser != nil {
				closeCtx, cancel := lifecycle.WithDetachedTimeout(
					ctx,
					10*time.Second,
				)
				server.closeErr = server.closePlugin(closeCtx)
				cancel()
			}
			close(server.closeDone)
		}()
	}
	done := server.closeDone
	server.lifecycleMu.Unlock()
	select {
	case <-done:
		return server.closeErr
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return server.closeErr
	}
}

func (server *server) closePlugin(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"close plugin %q panic: %v\n%s",
				server.manifest.Name,
				recovered,
				debug.Stack(),
			)
		}
	}()
	return server.pluginCloser.Close(ctx)
}

func (server *server) beginRPC() error {
	server.lifecycleMu.Lock()
	defer server.lifecycleMu.Unlock()
	if server.closed {
		return status.Error(codes.Unavailable, "plugin RPC server is closed")
	}
	server.wait.Add(1)
	return nil
}

func (server *server) Describe(
	context.Context,
	*pluginv1.DescribeRequest,
) (*pluginv1.DescribeResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	response := &pluginv1.DescribeResponse{Manifest: toProtoManifest(server.manifest)}
	for _, name := range sortedKeys(server.registrar.Providers) {
		response.Providers = append(
			response.Providers,
			toProtoProviderSpec(server.registrar.Providers[name].Spec),
		)
	}
	for _, name := range sortedKeys(server.registrar.Tools) {
		spec, err := toProtoToolSpec(server.registrar.Tools[name].Spec)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Tools = append(response.Tools, spec)
	}
	for _, name := range sortedKeys(server.registrar.Hooks) {
		response.Hooks = append(
			response.Hooks,
			toProtoHookSpec(server.registrar.Hooks[name].Spec),
		)
	}
	for _, name := range sortedKeys(server.registrar.Subscribers) {
		response.Subscribers = append(
			response.Subscribers,
			toProtoSubscriberSpec(server.registrar.Subscribers[name].Spec),
		)
	}
	for _, name := range sortedKeys(server.registrar.Capabilities) {
		spec, err := toProtoCapabilitySpec(
			server.registrar.Capabilities[name].Spec,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Capabilities = append(response.Capabilities, spec)
	}
	for _, name := range sortedKeys(server.registrar.Events) {
		response.Events = append(
			response.Events,
			toProtoEventContract(server.registrar.Events[name]),
		)
	}
	return response, nil
}

func (server *server) SubmitOperation(
	ctx context.Context,
	request *pluginv1.SubmitOperationRequest,
) (*pluginv1.SubmitOperationResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	kind := fromProtoOperationKind(request.GetKind())
	operationRequest := request.GetRequest()
	if operationRequest == nil || operationRequest.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "operation request and idempotency key are required")
	}
	input, err := structToRaw(operationRequest.GetInput())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	sdkRequest := sdk.OperationRequest{
		IdempotencyKey: operationRequest.GetIdempotencyKey(),
		Input:          input,
		Invocation: fromProtoInvocation(
			operationRequest.GetInvocation(),
		),
	}
	if err := sdk.ValidateInvocation(sdkRequest.Invocation); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resource, err := server.operationResource(kind, request.GetResource())
	if err != nil {
		return nil, err
	}
	operation, err := resource.submit(ctx, sdkRequest)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.SubmitOperationResponse{Operation: converted}, nil
}

func (server *server) PollOperation(
	ctx context.Context,
	request *pluginv1.PollOperationRequest,
) (*pluginv1.PollOperationResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	kind := fromProtoOperationKind(request.GetKind())
	resource, err := server.operationResource(kind, request.GetResource())
	if err != nil {
		return nil, err
	}
	operation, err := resource.poll(
		ctx,
		request.GetId(),
		request.GetAfterRevision(),
	)
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.PollOperationResponse{Operation: converted}, nil
}

func (server *server) CancelOperation(
	ctx context.Context,
	request *pluginv1.CancelOperationRequest,
) (*pluginv1.CancelOperationResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	kind := fromProtoOperationKind(request.GetKind())
	resource, err := server.operationResource(kind, request.GetResource())
	if err != nil {
		return nil, err
	}
	operation, err := resource.cancel(ctx, request.GetId())
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.CancelOperationResponse{Operation: converted}, nil
}

type operationResource struct {
	submit func(context.Context, sdk.OperationRequest) (sdk.Operation, error)
	poll   func(context.Context, string, uint64) (sdk.Operation, error)
	cancel func(context.Context, string) (sdk.Operation, error)
}

func (server *server) operationResource(
	kind sdk.OperationKind,
	name string,
) (operationResource, error) {
	switch kind {
	case sdk.OperationKindProvider:
		provider, exists := server.registrar.Providers[name]
		if !exists {
			return operationResource{}, status.Errorf(codes.NotFound, "%s %q not found", kind, name)
		}
		if asynchronous, ok := provider.Value.(sdk.AsyncProvider); ok {
			return operationResource{
				submit: asynchronous.SubmitCompletion,
				poll:   asynchronous.PollCompletion,
				cancel: asynchronous.CancelCompletion,
			}, nil
		}
	case sdk.OperationKindTool:
		tool, exists := server.registrar.Tools[name]
		if !exists {
			return operationResource{}, status.Errorf(codes.NotFound, "%s %q not found", kind, name)
		}
		if asynchronous, ok := tool.Value.(sdk.AsyncTool); ok {
			return operationResource{
				submit: asynchronous.SubmitCall,
				poll:   asynchronous.PollCall,
				cancel: asynchronous.CancelCall,
			}, nil
		}
	case sdk.OperationKindCapability:
		capability, exists := server.registrar.Capabilities[name]
		if !exists {
			return operationResource{}, status.Errorf(codes.NotFound, "%s %q not found", kind, name)
		}
		if asynchronous, ok := capability.Value.(sdk.AsyncCapability); ok {
			return operationResource{
				submit: asynchronous.SubmitInvoke,
				poll:   asynchronous.PollInvoke,
				cancel: asynchronous.CancelInvoke,
			}, nil
		}
	default:
		return operationResource{}, status.Error(
			codes.InvalidArgument,
			"unsupported operation kind",
		)
	}
	return server.storedOperationResource(kind, name), nil
}

func (server *server) storedOperationResource(
	kind sdk.OperationKind,
	name string,
) operationResource {
	return operationResource{
		submit: func(ctx context.Context, request sdk.OperationRequest) (sdk.Operation, error) {
			return server.submitStored(ctx, kind, name, request)
		},
		poll: func(ctx context.Context, id string, _ uint64) (sdk.Operation, error) {
			return server.getStored(ctx, kind, name, id)
		},
		cancel: func(ctx context.Context, id string) (sdk.Operation, error) {
			return server.cancelStored(ctx, kind, name, id)
		},
	}
}

func (server *server) HandleHook(
	ctx context.Context,
	request *pluginv1.HandleHookRequest,
) (*pluginv1.HandleHookResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	hook, exists := server.registrar.Hooks[request.GetHook()]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "hook %q not found", request.GetHook())
	}
	event, err := fromProtoEvent(request.GetEvent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	effect, err := pluginpolicy.HandleHook(ctx, hook.Value, hook.Spec, event)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	converted, err := toProtoEffect(effect)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.HandleHookResponse{Effect: converted}, nil
}

func (server *server) Deliver(
	ctx context.Context,
	request *pluginv1.DeliverRequest,
) (*pluginv1.DeliverResponse, error) {
	if err := server.beginRPC(); err != nil {
		return nil, err
	}
	defer server.wait.Done()
	delivery, err := fromProtoDelivery(request.GetDelivery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target := server.resolveDeliveryTarget(delivery)
	switch target.state {
	case serverDeliveryTargetReady:
	case serverDeliveryTargetWrongPlugin:
		return nil, status.Error(codes.InvalidArgument, target.cause.Error())
	case serverDeliveryTargetMissing:
		return nil, status.Error(codes.NotFound, target.cause.Error())
	case serverDeliveryTargetStale:
		return nil, status.Error(codes.FailedPrecondition, target.cause.Error())
	}
	if err := server.inbox.Enqueue(ctx, delivery); err != nil {
		return nil, rpcError(err)
	}
	return &pluginv1.DeliverResponse{Accepted: true}, nil
}

func rpcError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, sdk.ErrOperationNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, sdk.ErrOperationConflict):
		return status.Error(codes.Aborted, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func sortedKeys[V any](values map[string]V) []string { return slices.Sorted(maps.Keys(values)) }

func validateRPCContributions(
	registrar *plugincontract.Registrar,
) error {
	for name, tool := range registrar.Tools {
		if _, err := toProtoToolSpec(tool.Spec); err != nil {
			return fmt.Errorf("encode tool %q for RPC: %w", name, err)
		}
	}
	for name, capability := range registrar.Capabilities {
		if _, err := toProtoCapabilitySpec(capability.Spec); err != nil {
			return fmt.Errorf(
				"encode capability %q for RPC: %w",
				name,
				err,
			)
		}
	}
	return nil
}
