package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerConfig struct {
	Plugin            sdk.Plugin
	Operations        sdk.OperationStore
	Inbox             sdk.OutboxStore
	Logger            *slog.Logger
	InboxWorkers      int
	InboxLease        time.Duration
	InboxPoll         time.Duration
	SubscriberTimeout time.Duration
	InboxMaxAttempts  int
}

type Server struct {
	pluginv1.UnimplementedPluginServiceServer
	manifest          sdk.Manifest
	registrar         *serverRegistrar
	operations        sdk.OperationStore
	inbox             sdk.OutboxStore
	logger            *slog.Logger
	inboxWorkers      int
	inboxLease        time.Duration
	inboxPoll         time.Duration
	subscriberTimeout time.Duration
	inboxMaxAttempts  int
	context           context.Context
	cancel            context.CancelFunc
	wait              sync.WaitGroup
	cancelMu          sync.Mutex
	operationCancels  map[string]context.CancelFunc
}

func NewServer(ctx context.Context, config ServerConfig) (*Server, error) {
	if config.Plugin == nil {
		return nil, errors.New("plugin is nil")
	}
	manifest := config.Plugin.Manifest()
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("validate plugin manifest: %w", err)
	}
	registrar := newServerRegistrar()
	if err := config.Plugin.Install(ctx, registrar); err != nil {
		return nil, fmt.Errorf("install plugin into RPC server: %w", err)
	}
	if err := registrar.validateManifest(manifest); err != nil {
		return nil, err
	}
	if config.Operations == nil {
		config.Operations = sdk.NewMemoryOperationStore()
	}
	if config.Inbox == nil {
		config.Inbox = sdk.NewMemoryOutboxStore()
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
	if config.InboxWorkers < 1 || config.InboxLease <= 0 ||
		config.InboxPoll <= 0 || config.SubscriberTimeout <= 0 ||
		config.InboxMaxAttempts < 1 {
		return nil, errors.New("RPC server worker settings must be positive")
	}
	serverContext, cancel := context.WithCancel(context.Background())
	server := &Server{
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
		context:           serverContext,
		cancel:            cancel,
		operationCancels:  make(map[string]context.CancelFunc),
	}
	if len(registrar.subscribers) > 0 {
		for worker := range server.inboxWorkers {
			server.wait.Add(1)
			go server.inboxLoop(worker)
		}
	}
	if err := server.recoverOperations(ctx); err != nil {
		cancel()
		return nil, err
	}
	return server, nil
}

func (server *Server) Close(ctx context.Context) error {
	server.cancel()
	done := make(chan struct{})
	go func() {
		server.wait.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (server *Server) Describe(
	context.Context,
	*pluginv1.DescribeRequest,
) (*pluginv1.DescribeResponse, error) {
	response := &pluginv1.DescribeResponse{Manifest: toProtoManifest(server.manifest)}
	for _, name := range sortedKeys(server.registrar.providers) {
		response.Providers = append(response.Providers, toProtoProviderSpec(server.registrar.providers[name].Spec()))
	}
	for _, name := range sortedKeys(server.registrar.tools) {
		spec, err := toProtoToolSpec(server.registrar.tools[name].Spec())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Tools = append(response.Tools, spec)
	}
	for _, name := range sortedKeys(server.registrar.hooks) {
		response.Hooks = append(response.Hooks, toProtoHookSpec(server.registrar.hooks[name].Spec()))
	}
	for _, name := range sortedKeys(server.registrar.subscribers) {
		response.Subscribers = append(response.Subscribers, toProtoSubscriberSpec(server.registrar.subscribers[name].Spec()))
	}
	for _, name := range sortedKeys(server.registrar.capabilities) {
		spec, err := toProtoCapabilitySpec(server.registrar.capabilities[name].Spec())
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Capabilities = append(response.Capabilities, spec)
	}
	for _, name := range sortedKeys(server.registrar.events) {
		response.Events = append(response.Events, toProtoEventContract(server.registrar.events[name]))
	}
	return response, nil
}

func (server *Server) SubmitOperation(
	ctx context.Context,
	request *pluginv1.SubmitOperationRequest,
) (*pluginv1.SubmitOperationResponse, error) {
	kind := fromProtoOperationKind(request.GetKind())
	resource := request.GetResource()
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
	}
	var operation sdk.Operation
	switch kind {
	case sdk.OperationKindProvider:
		provider := server.registrar.providers[resource]
		if provider == nil {
			return nil, status.Errorf(codes.NotFound, "provider %q not found", resource)
		}
		if asynchronous, ok := provider.(sdk.AsyncProvider); ok {
			operation, err = asynchronous.SubmitCompletion(ctx, sdkRequest)
		} else {
			operation, err = server.submitStored(ctx, kind, resource, sdkRequest)
		}
	case sdk.OperationKindTool:
		tool := server.registrar.tools[resource]
		if tool == nil {
			return nil, status.Errorf(codes.NotFound, "tool %q not found", resource)
		}
		if asynchronous, ok := tool.(sdk.AsyncTool); ok {
			operation, err = asynchronous.SubmitCall(ctx, sdkRequest)
		} else {
			operation, err = server.submitStored(ctx, kind, resource, sdkRequest)
		}
	case sdk.OperationKindCapability:
		capability := server.registrar.capabilities[resource]
		if capability == nil {
			return nil, status.Errorf(codes.NotFound, "capability %q not found", resource)
		}
		if asynchronous, ok := capability.(sdk.AsyncCapability); ok {
			operation, err = asynchronous.SubmitInvoke(ctx, sdkRequest)
		} else {
			operation, err = server.submitStored(ctx, kind, resource, sdkRequest)
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported operation kind")
	}
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.SubmitOperationResponse{Operation: converted}, nil
}

func (server *Server) PollOperation(
	ctx context.Context,
	request *pluginv1.PollOperationRequest,
) (*pluginv1.PollOperationResponse, error) {
	kind := fromProtoOperationKind(request.GetKind())
	resource := request.GetResource()
	var operation sdk.Operation
	var err error
	switch kind {
	case sdk.OperationKindProvider:
		provider := server.registrar.providers[resource]
		if provider == nil {
			return nil, status.Errorf(codes.NotFound, "provider %q not found", resource)
		}
		if asynchronous, ok := provider.(sdk.AsyncProvider); ok {
			operation, err = asynchronous.PollCompletion(ctx, request.GetId(), request.GetAfterRevision())
		} else {
			operation, err = server.getStored(ctx, kind, resource, request.GetId())
		}
	case sdk.OperationKindTool:
		tool := server.registrar.tools[resource]
		if tool == nil {
			return nil, status.Errorf(codes.NotFound, "tool %q not found", resource)
		}
		if asynchronous, ok := tool.(sdk.AsyncTool); ok {
			operation, err = asynchronous.PollCall(ctx, request.GetId(), request.GetAfterRevision())
		} else {
			operation, err = server.getStored(ctx, kind, resource, request.GetId())
		}
	case sdk.OperationKindCapability:
		capability := server.registrar.capabilities[resource]
		if capability == nil {
			return nil, status.Errorf(codes.NotFound, "capability %q not found", resource)
		}
		if asynchronous, ok := capability.(sdk.AsyncCapability); ok {
			operation, err = asynchronous.PollInvoke(ctx, request.GetId(), request.GetAfterRevision())
		} else {
			operation, err = server.getStored(ctx, kind, resource, request.GetId())
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported operation kind")
	}
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.PollOperationResponse{Operation: converted}, nil
}

func (server *Server) CancelOperation(
	ctx context.Context,
	request *pluginv1.CancelOperationRequest,
) (*pluginv1.CancelOperationResponse, error) {
	kind := fromProtoOperationKind(request.GetKind())
	resource := request.GetResource()
	var operation sdk.Operation
	var err error
	switch kind {
	case sdk.OperationKindProvider:
		provider := server.registrar.providers[resource]
		if provider == nil {
			return nil, status.Errorf(codes.NotFound, "provider %q not found", resource)
		}
		if asynchronous, ok := provider.(sdk.AsyncProvider); ok {
			operation, err = asynchronous.CancelCompletion(ctx, request.GetId())
		} else {
			operation, err = server.cancelStored(ctx, kind, resource, request.GetId())
		}
	case sdk.OperationKindTool:
		tool := server.registrar.tools[resource]
		if tool == nil {
			return nil, status.Errorf(codes.NotFound, "tool %q not found", resource)
		}
		if asynchronous, ok := tool.(sdk.AsyncTool); ok {
			operation, err = asynchronous.CancelCall(ctx, request.GetId())
		} else {
			operation, err = server.cancelStored(ctx, kind, resource, request.GetId())
		}
	case sdk.OperationKindCapability:
		capability := server.registrar.capabilities[resource]
		if capability == nil {
			return nil, status.Errorf(codes.NotFound, "capability %q not found", resource)
		}
		if asynchronous, ok := capability.(sdk.AsyncCapability); ok {
			operation, err = asynchronous.CancelInvoke(ctx, request.GetId())
		} else {
			operation, err = server.cancelStored(ctx, kind, resource, request.GetId())
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported operation kind")
	}
	if err != nil {
		return nil, rpcError(err)
	}
	converted, err := toProtoOperation(operation)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.CancelOperationResponse{Operation: converted}, nil
}

func (server *Server) HandleHook(
	ctx context.Context,
	request *pluginv1.HandleHookRequest,
) (*pluginv1.HandleHookResponse, error) {
	hook := server.registrar.hooks[request.GetHook()]
	if hook == nil {
		return nil, status.Errorf(codes.NotFound, "hook %q not found", request.GetHook())
	}
	event, err := fromProtoEvent(request.GetEvent())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	effect, err := hook.Handle(ctx, event)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	converted, err := toProtoEffect(effect)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pluginv1.HandleHookResponse{Effect: converted}, nil
}

func (server *Server) Deliver(
	ctx context.Context,
	request *pluginv1.DeliverRequest,
) (*pluginv1.DeliverResponse, error) {
	delivery, err := fromProtoDelivery(request.GetDelivery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if delivery.Plugin != server.manifest.Name {
		return nil, status.Errorf(codes.InvalidArgument, "delivery targets plugin %q", delivery.Plugin)
	}
	if server.registrar.subscribers[delivery.Subscription] == nil {
		return nil, status.Errorf(codes.NotFound, "subscriber %q not found", delivery.Subscription)
	}
	if err := server.inbox.Enqueue(ctx, delivery); err != nil {
		return nil, rpcError(err)
	}
	return &pluginv1.DeliverResponse{Accepted: true}, nil
}

func (server *Server) submitStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	record, created, err := server.operations.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:      kind,
		Resource:  resource,
		Input:     request.Input,
	})
	if err != nil {
		return sdk.Operation{}, err
	}
	if created {
		server.startOperation(ctx, record.Operation.ID)
	}
	return record.Operation, nil
}

func (server *Server) getStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	record, err := server.operations.Get(ctx, id)
	if err != nil {
		return sdk.Operation{}, err
	}
	if record.Kind != kind || record.Resource != resource {
		return sdk.Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
	}
	return record.Operation, nil
}

func (server *Server) cancelStored(
	ctx context.Context,
	kind sdk.OperationKind,
	resource string,
	id string,
) (sdk.Operation, error) {
	for {
		record, err := server.operations.Get(ctx, id)
		if err != nil {
			return sdk.Operation{}, err
		}
		if record.Kind != kind || record.Resource != resource {
			return sdk.Operation{}, fmt.Errorf("operation %q does not belong to %s %q", id, kind, resource)
		}
		if record.Operation.Terminal() {
			return record.Operation, nil
		}
		cancelled, err := server.operations.Transition(
			ctx,
			id,
			record.Operation.Revision,
			sdk.OperationCancelled,
			nil,
			"",
		)
		if errors.Is(err, sdk.ErrOperationConflict) {
			continue
		}
		if err != nil {
			return sdk.Operation{}, err
		}
		server.cancelMu.Lock()
		cancel := server.operationCancels[id]
		server.cancelMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return cancelled.Operation, nil
	}
}

func (server *Server) recoverOperations(ctx context.Context) error {
	records, err := server.operations.List(ctx)
	if err != nil {
		return fmt.Errorf("list operations for recovery: %w", err)
	}
	for _, record := range records {
		if !record.Operation.Terminal() {
			server.startOperation(ctx, record.Operation.ID)
		}
	}
	return nil
}

func (server *Server) startOperation(parent context.Context, id string) {
	server.wait.Add(1)
	go func() {
		defer server.wait.Done()
		server.executeStored(parent, id)
	}()
}

func (server *Server) executeStored(parent context.Context, id string) {
	operationContext, cancel := context.WithCancel(context.WithoutCancel(parent))
	stopServerCancel := context.AfterFunc(server.context, cancel)
	defer func() {
		stopServerCancel()
		cancel()
	}()
	record, err := server.operations.Get(operationContext, id)
	if err != nil || record.Operation.Terminal() {
		return
	}
	if record.Operation.State == sdk.OperationPending {
		record, err = server.operations.Transition(
			operationContext,
			id,
			record.Operation.Revision,
			sdk.OperationRunning,
			nil,
			"",
		)
		if err != nil {
			return
		}
	}
	server.cancelMu.Lock()
	server.operationCancels[id] = cancel
	server.cancelMu.Unlock()
	output, executeErr := server.executeLocal(operationContext, record)
	cancel()
	server.cancelMu.Lock()
	delete(server.operationCancels, id)
	server.cancelMu.Unlock()
	if errors.Is(executeErr, context.Canceled) && server.context.Err() != nil {
		return
	}
	state := sdk.OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = sdk.OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = server.operations.Transition(
		context.Background(),
		id,
		record.Operation.Revision,
		state,
		output,
		errorText,
	)
	if err != nil && !errors.Is(err, sdk.ErrOperationConflict) {
		server.logger.Error("complete stored operation", "operation_id", id, "error", err)
	}
}

func (server *Server) executeLocal(
	ctx context.Context,
	record sdk.OperationRecord,
) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("plugin operation panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	switch record.Kind {
	case sdk.OperationKindProvider:
		provider, ok := server.registrar.providers[record.Resource].(sdk.SyncProvider)
		if !ok {
			return nil, fmt.Errorf("provider %q is not synchronous", record.Resource)
		}
		var request sdk.ModelRequest
		if err := json.Unmarshal(record.Input, &request); err != nil {
			return nil, err
		}
		response, err := provider.Complete(ctx, request)
		if err != nil {
			return nil, err
		}
		return json.Marshal(response)
	case sdk.OperationKindTool:
		tool, ok := server.registrar.tools[record.Resource].(sdk.SyncTool)
		if !ok {
			return nil, fmt.Errorf("tool %q is not synchronous", record.Resource)
		}
		result, err := tool.Call(ctx, record.Input)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	case sdk.OperationKindCapability:
		capability, ok := server.registrar.capabilities[record.Resource].(sdk.SyncCapability)
		if !ok {
			return nil, fmt.Errorf("capability %q is not synchronous", record.Resource)
		}
		return capability.Invoke(ctx, record.Input)
	default:
		return nil, fmt.Errorf("unsupported stored operation kind %q", record.Kind)
	}
}

func (server *Server) inboxLoop(worker int) {
	defer server.wait.Done()
	for {
		if server.context.Err() != nil {
			return
		}
		delivery, err := server.inbox.Lease(server.context, time.Now().UTC(), server.inboxLease)
		if errors.Is(err, sdk.ErrNoDelivery) {
			if !wait(server.context, server.inboxPoll) {
				return
			}
			continue
		}
		if err != nil {
			server.logger.Warn("lease plugin inbox", "worker", worker, "error", err)
			if !wait(server.context, server.inboxPoll) {
				return
			}
			continue
		}
		server.receiveDelivery(delivery)
	}
}

func (server *Server) receiveDelivery(delivery sdk.Delivery) {
	subscriber := server.registrar.subscribers[delivery.Subscription]
	if subscriber == nil {
		server.retryDelivery(delivery, errors.New("subscriber disappeared"))
		return
	}
	timeout := server.subscriberTimeout
	if configured := subscriber.Spec().Timeout; configured > 0 && configured < timeout {
		timeout = configured
	}
	ctx, cancel := context.WithTimeout(server.context, timeout)
	err := safeReceive(ctx, subscriber, delivery)
	cancel()
	if err != nil {
		server.retryDelivery(delivery, err)
		return
	}
	if err := server.inbox.Ack(server.context, delivery.ID, delivery.LeaseToken, time.Now().UTC()); err != nil && !errors.Is(err, context.Canceled) {
		server.logger.Warn("ack plugin inbox", "delivery_id", delivery.ID, "error", err)
	}
}

func (server *Server) retryDelivery(delivery sdk.Delivery, cause error) {
	if server.context.Err() != nil {
		return
	}
	now := time.Now().UTC()
	var err error
	if delivery.Attempt >= server.inboxMaxAttempts {
		err = server.inbox.DeadLetter(server.context, delivery.ID, delivery.LeaseToken, now, cause.Error())
	} else {
		shift := min(max(delivery.Attempt-1, 0), 10)
		delay := min(server.inboxPoll*time.Duration(1<<shift), 30*time.Second)
		err = server.inbox.Retry(server.context, delivery.ID, delivery.LeaseToken, now.Add(delay), cause.Error())
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		server.logger.Warn("reschedule plugin inbox", "delivery_id", delivery.ID, "error", err)
	}
}

func safeReceive(ctx context.Context, subscriber sdk.Subscriber, delivery sdk.Delivery) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("subscriber panic: %v\n%s", recovered, debug.Stack())
		}
	}()
	return subscriber.Receive(ctx, delivery)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
	case errors.Is(err, sdk.ErrLeaseNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, sdk.ErrLeaseExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
