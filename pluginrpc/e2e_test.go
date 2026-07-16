package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
)

type e2eProvider struct {
	mu      sync.Mutex
	calls   int
	systems []string
}

func (provider *e2eProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "remote-model", Model: "remote-v1"}
}

func (provider *e2eProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.calls++
	if len(request.Messages) > 0 && request.Messages[0].Role == sdk.RoleSystem {
		provider.systems = append(provider.systems, request.Messages[0].Content)
	}
	if provider.calls == 1 {
		return sdk.ModelResponse{
			Model:        "remote-v1",
			FinishReason: "tool_calls",
			ToolCalls: []sdk.ToolCall{{
				ID:        "remote-tool-call",
				Name:      "remote-echo",
				Arguments: []byte(`{"value":"from-rpc"}`),
			}},
		}, nil
	}
	return sdk.ModelResponse{
		Content:      "remote-finished",
		Model:        "remote-v1",
		FinishReason: "stop",
	}, nil
}

type e2eTool struct{}

func (e2eTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "remote-echo",
		Description: "echoes a value through the RPC operation path",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"value": map[string]any{"type": "string"},
			},
		},
	}
}

type e2eCapability struct{}

func (e2eCapability) Spec() sdk.CapabilitySpec {
	return sdk.CapabilitySpec{
		Name: "remote-state", Description: "returns serializable remote state",
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
	}
}

func (e2eCapability) Invoke(
	_ context.Context,
	input json.RawMessage,
) (json.RawMessage, error) {
	var request map[string]any
	if err := json.Unmarshal(input, &request); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"transport": "rpc-operation",
		"input":     request,
	})
}

func (e2eTool) Call(
	_ context.Context,
	arguments json.RawMessage,
) (sdk.ToolResult, error) {
	var input struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(arguments, &input); err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{Content: input.Value}, nil
}

type e2ePlugin struct {
	provider *e2eProvider
	received chan sdk.Delivery
}

func newE2EPlugin() *e2ePlugin {
	return &e2ePlugin{
		provider: &e2eProvider{},
		received: make(chan sdk.Delivery, 1),
	}
}

func (plugin *e2ePlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "remote-e2e",
		Version:     "1.0.0",
		Description: "real TCP RPC end-to-end plugin",
		APIVersion:  sdk.APIVersion,
		Registers: []string{
			sdk.ProviderResource("remote-model"),
			sdk.ToolResource("remote-echo"),
			sdk.CapabilityResource("remote-state"),
			sdk.HookResource("remote-system"),
			sdk.SubscriberResource("remote-terminal-events"),
		},
	}
}

func (plugin *e2ePlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	hook := sdk.TypedHook[sdk.BeforeAgentStartPayload](
		sdk.HookSpec{
			Name:    "remote-system",
			Event:   sdk.EventBeforeAgentStart,
			Timeout: time.Second,
		},
		func(_ context.Context, _ sdk.BeforeAgentStartPayload) (sdk.Effect, error) {
			return sdk.Patch(map[string]any{"system": "system-from-remote-hook"})
		},
	)
	subscriber := sdk.SubscriberFunc{
		SubscriberSpec: sdk.SubscriberSpec{
			Name:   "remote-terminal-events",
			Events: []string{sdk.EventAgentEnd},
		},
		ReceiveFunc: func(_ context.Context, delivery sdk.Delivery) error {
			plugin.received <- delivery
			return nil
		},
	}
	return errors.Join(
		registrar.RegisterProvider(plugin.provider),
		registrar.RegisterTool(e2eTool{}),
		registrar.RegisterCapability(e2eCapability{}),
		registrar.RegisterHook(hook),
		registrar.RegisterSubscriber(subscriber),
	)
}

func TestRemotePluginRealTCPRunsSessionHookToolAndSubscriber(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	plugin := newE2EPlugin()
	serverAdapter, err := NewServer(ctx, ServerConfig{
		Plugin:       plugin,
		InboxPoll:    time.Millisecond,
		InboxWorkers: 2,
	})
	if err != nil {
		t.Fatalf("new plugin server: %v", err)
	}
	grpcServer, err := NewGRPCServer(serverAdapter, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = listener.Close()
		select {
		case serveErr := <-serveErrors:
			if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
				t.Errorf("serve: %v", serveErr)
			}
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := serverAdapter.Close(closeCtx); err != nil {
			t.Errorf("close adapter: %v", err)
		}
	})

	registry := sdk.NewPluginRegistry()
	if err := RegisterDrivers(registry, ClientConfig{}); err != nil {
		t.Fatal(err)
	}
	uri := "grpc://" + listener.Addr().String()
	if err := registry.Register(sdk.PluginReference{Name: "remote-e2e", URI: uri}); err != nil {
		t.Fatal(err)
	}
	runtime, err := sdk.NewRuntime(sdk.RuntimeConfig{
		OperationPoll:       time.Millisecond,
		DeliveryPoll:        time.Millisecond,
		DeliveryWorkers:     4,
		DeliveryMaxAttempts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	mount, err := runtime.MountRegistered(ctx, registry, "remote-e2e")
	if err != nil {
		t.Fatalf("mount remote plugin: %v", err)
	}
	if mount.Name() != "remote-e2e" {
		t.Fatalf("mount name = %q", mount.Name())
	}
	catalog := runtime.Catalog()
	if len(catalog.Providers) != 1 || len(catalog.Tools) != 1 ||
		len(catalog.Hooks) != 1 || len(catalog.Subscribers) != 1 ||
		len(catalog.Capabilities) != 1 {
		t.Fatalf("remote catalog = %#v", catalog)
	}

	session, err := runtime.NewSession(ctx, sdk.SessionConfig{
		ID:       "remote-session",
		Provider: "remote-model",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Prompt(ctx, "run over RPC")
	if err != nil {
		t.Fatalf("remote prompt: %v", err)
	}
	if result.Output != "remote-finished" || result.Turns != 2 || result.ToolCalls != 1 {
		t.Fatalf("remote result = %#v", result)
	}
	capabilityOutput, err := runtime.InvokeCapability(ctx, "remote-state", []byte(`{"value":"shared"}`))
	if err != nil {
		t.Fatalf("invoke remote capability: %v", err)
	}
	var capabilityState struct {
		Transport string         `json:"transport"`
		Input     map[string]any `json:"input"`
	}
	if err := json.Unmarshal(capabilityOutput, &capabilityState); err != nil {
		t.Fatal(err)
	}
	if capabilityState.Transport != "rpc-operation" || capabilityState.Input["value"] != "shared" {
		t.Fatalf("remote capability state = %#v", capabilityState)
	}
	plugin.provider.mu.Lock()
	systems := append([]string(nil), plugin.provider.systems...)
	plugin.provider.mu.Unlock()
	if len(systems) != 2 || systems[0] != "system-from-remote-hook" || systems[1] != "system-from-remote-hook" {
		t.Fatalf("provider systems = %v", systems)
	}
	select {
	case delivery := <-plugin.received:
		if delivery.Event.Name != sdk.EventAgentEnd || delivery.Event.SessionID != session.ID() {
			t.Fatalf("remote subscriber delivery = %#v", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("remote subscriber did not receive terminal event")
	}
}

type blockingRPCTool struct {
	entered chan struct{}
	release chan struct{}
}

func (tool *blockingRPCTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "blocking-tool",
		Description: "blocks until its operation is cancelled",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *blockingRPCTool) Call(
	ctx context.Context,
	_ json.RawMessage,
) (sdk.ToolResult, error) {
	close(tool.entered)
	select {
	case <-ctx.Done():
		return sdk.ToolResult{}, ctx.Err()
	case <-tool.release:
		return sdk.ToolResult{Content: "late success"}, nil
	}
}

func TestRemoteCancelWinsAgainstBlockingSyncToolCompletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tool := &blockingRPCTool{entered: make(chan struct{}), release: make(chan struct{})}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "cancel-e2e",
			Version:     "1.0.0",
			Description: "cancel race end-to-end plugin",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("blocking-tool")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(tool)
		},
	}
	operationStore := sdk.NewMemoryOperationStore()
	adapter, err := NewServer(ctx, ServerConfig{Plugin: plugin, Operations: operationStore})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer, err := NewGRPCServer(adapter, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	defer func() {
		grpcServer.GracefulStop()
		_ = listener.Close()
		<-serveErrors
		closeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := adapter.Close(closeCtx); err != nil {
			t.Errorf("close adapter: %v", err)
		}
	}()
	parsed, err := parseSourceURI("grpc://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dial(ctx, parsed, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	client := pluginv1.NewPluginServiceClient(connection)
	input, err := rawToStruct([]byte(`{"work":"slow"}`))
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := client.SubmitOperation(ctx, &pluginv1.SubmitOperationRequest{
		Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
		Resource: "blocking-tool",
		Request: &pluginv1.OperationRequest{
			IdempotencyKey: "cancel-entry",
			Input:          input,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	operationID := submitted.GetOperation().GetId()
	select {
	case <-tool.entered:
	case <-time.After(time.Second):
		t.Fatal("blocking tool did not start")
	}
	cancelled, err := client.CancelOperation(ctx, &pluginv1.CancelOperationRequest{
		Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
		Resource: "blocking-tool",
		Id:       operationID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.GetOperation().GetState() != pluginv1.OperationState_OPERATION_STATE_CANCELLED {
		t.Fatalf("cancel response = %#v", cancelled.GetOperation())
	}
	close(tool.release)
	eventuallyRPC(t, time.Second, func() bool {
		polled, pollErr := client.PollOperation(ctx, &pluginv1.PollOperationRequest{
			Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
			Resource: "blocking-tool",
			Id:       operationID,
		})
		return pollErr == nil && polled.GetOperation().GetState() ==
			pluginv1.OperationState_OPERATION_STATE_CANCELLED
	})
}
