package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/pluginrpc"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type gatewayE2EProvider struct {
	output   string
	entered  chan struct{}
	mu       sync.Mutex
	requests []sdk.ModelRequest
}

func (*gatewayE2EProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "switch-model", Model: "test"}
}

func (provider *gatewayE2EProvider) Complete(
	ctx context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	provider.requests = append(
		provider.requests,
		sdk.ModelRequest{
			Messages: append([]sdk.Message(nil), request.Messages...),
			Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
		},
	)
	provider.mu.Unlock()
	if latestUserContent(request.Messages) == "block" {
		select {
		case provider.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return sdk.ModelResponse{}, ctx.Err()
	}
	return sdk.ModelResponse{
		Content: provider.output, Model: "test", FinishReason: "stop",
	}, nil
}

type gatewayE2EPlugin struct {
	provider *gatewayE2EProvider
}

func (*gatewayE2EPlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "switch-plugin",
		Version:     "1.0.0",
		Description: "gateway composition switch test plugin",
		APIVersion:  sdk.APIVersion,
		Registers: []string{
			sdk.ProviderResource("switch-model"),
		},
	}
}

func (plugin *gatewayE2EPlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	return registrar.RegisterProvider(plugin.provider)
}

func TestGatewayCancelsChangesPluginAndResumesDurableContext(t *testing.T) {
	firstProvider := &gatewayE2EProvider{
		output: "context-from-a", entered: make(chan struct{}, 1),
	}
	secondProvider := &gatewayE2EProvider{
		output: "continued-by-b", entered: make(chan struct{}, 1),
	}
	firstURI := serveGatewayE2EPlugin(
		t,
		&gatewayE2EPlugin{provider: firstProvider},
	)
	secondURI := serveGatewayE2EPlugin(
		t,
		&gatewayE2EPlugin{provider: secondProvider},
	)

	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	for instanceID, uri := range map[string]string{
		"node-a": firstURI,
		"node-b": secondURI,
	} {
		if _, err := directory.Register(
			t.Context(),
			registry.PluginRegistration{
				Namespace:  registry.DefaultNamespace,
				Name:       "switch-plugin",
				InstanceID: instanceID,
				URI:        uri,
				Manifest:   (&gatewayE2EPlugin{}).Manifest(),
			},
			registry.LeaseOptions{TTL: time.Minute},
		); err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	sessionStore, err := gateway.NewFileSessionStore(root)
	if err != nil {
		t.Fatal(err)
	}
	stateFactory, err := gateway.NewFileSessionStateFactory(root + "/state")
	if err != nil {
		t.Fatal(err)
	}
	executions, err := gateway.NewRuntimeExecutionBackend(
		gateway.RuntimeExecutionConfig{
			States: stateFactory,
			Build: gatewayRuntimeBuilder(
				appconfig.Config{},
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				nil,
				nil,
				"test",
			),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := gateway.NewService(gateway.ServiceConfig{
		Store: sessionStore, Directory: directory,
		Executions:       executions,
		DefaultNamespace: registry.DefaultNamespace,
		DefaultProvider:  "switch-model",
		DefaultMaxTurns:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("close gateway service: %v", err)
		}
	})
	handler, err := gateway.NewHTTPHandler(
		service,
		gateway.HeaderAuthenticator,
	)
	if err != nil {
		t.Fatal(err)
	}

	session := gatewayE2ERequest[gateway.Session](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions",
		map[string]any{"id": "gateway-switch"},
		http.StatusCreated,
	)
	session = gatewayE2ERequest[gateway.Session](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/plugins",
		map[string]any{
			"selector":          "switch-plugin@node-a",
			"expected_revision": session.Revision,
		},
		http.StatusOK,
	)
	first := gatewayE2ERequest[gateway.Execution](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/messages",
		map[string]any{"content": "remember this"},
		http.StatusAccepted,
	)
	first = waitGatewayE2EExecution(t, handler, first.Execution.ID)
	if first.Result == nil || first.Result.Output != "context-from-a" {
		t.Fatalf("first result = %#v", first)
	}

	blocked := gatewayE2ERequest[gateway.Execution](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/messages",
		map[string]any{"content": "block"},
		http.StatusAccepted,
	)
	select {
	case <-firstProvider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking provider did not start")
	}
	cancelled := gatewayE2ERequest[gateway.Execution](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/executions/"+
			blocked.Execution.ID+"/cancel",
		nil,
		http.StatusOK,
	)
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", cancelled)
	}

	session = gatewayE2ERequest[gateway.Session](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/plugins",
		map[string]any{
			"selector":          "switch-plugin@node-b",
			"expected_revision": session.Revision,
		},
		http.StatusOK,
	)
	continued := gatewayE2ERequest[gateway.Execution](
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/gateway-switch/messages",
		map[string]any{"content": "continue"},
		http.StatusAccepted,
	)
	continued = waitGatewayE2EExecution(
		t,
		handler,
		continued.Execution.ID,
	)
	if continued.Result == nil ||
		continued.Result.Output != "continued-by-b" {
		t.Fatalf("continued result = %#v", continued)
	}
	secondProvider.mu.Lock()
	requests := append(
		[]sdk.ModelRequest(nil),
		secondProvider.requests...,
	)
	secondProvider.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("second provider requests = %#v", requests)
	}
	contents := messageContents(requests[0].Messages)
	if len(contents) != 3 ||
		contents[0] != "remember this" ||
		contents[1] != "context-from-a" ||
		contents[2] != "continue" {
		t.Fatalf("resumed message contents = %#v", contents)
	}
}

func serveGatewayE2EPlugin(
	t *testing.T,
	plugin sdk.Plugin,
) string {
	t.Helper()
	adapter, err := pluginrpc.NewServer(t.Context(), pluginrpc.ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
		InboxPoll:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := pluginrpc.NewGRPCServer(adapter, 0)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("serve plugin: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("plugin server did not stop")
		}
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := adapter.Close(ctx); err != nil {
			t.Errorf("close plugin adapter: %v", err)
		}
	})
	return "grpc://" + listener.Addr().String()
}

func gatewayE2ERequest[T any](
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	input any,
	status int,
) T {
	t.Helper()
	var body bytes.Buffer
	if input != nil {
		if err := json.NewEncoder(&body).Encode(input); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, &body)
	request.Header.Set(gateway.UserHeader, "user-a")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != status {
		t.Fatalf(
			"%s %s status=%d body=%s",
			method,
			path,
			response.Code,
			response.Body.String(),
		)
	}
	var result T
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode %s %s: %v", method, path, err)
	}
	return result
}

func waitGatewayE2EExecution(
	t *testing.T,
	handler http.Handler,
	executionID string,
) gateway.Execution {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		execution := gatewayE2ERequest[gateway.Execution](
			t,
			handler,
			http.MethodGet,
			"/v1/sessions/gateway-switch/executions/"+executionID,
			nil,
			http.StatusOK,
		)
		if execution.Execution.Terminal() {
			return execution
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution %s did not finish", executionID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func latestUserContent(messages []sdk.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == sdk.RoleUser {
			return messages[index].Content
		}
	}
	return ""
}

func messageContents(messages []sdk.Message) []string {
	result := make([]string, 0, len(messages))
	for _, message := range messages {
		if message.Role == sdk.RoleUser ||
			message.Role == sdk.RoleAssistant {
			result = append(result, message.Content)
		}
	}
	return result
}
