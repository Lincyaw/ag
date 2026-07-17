package pluginrpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverConfigTool struct{}

type mutableServerSpecTool struct {
	spec sdk.ToolSpec
}

type closingServerPlugin struct {
	closes   atomic.Int64
	closeErr error
}

func (*closingServerPlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "closing-server-plugin",
		Version:     "1.0.0",
		Description: "verifies RPC server plugin ownership",
		APIVersion:  sdk.APIVersion,
	}
}

func (*closingServerPlugin) Install(context.Context, sdk.Registrar) error {
	return nil
}

func (plugin *closingServerPlugin) Close(context.Context) error {
	plugin.closes.Add(1)
	return plugin.closeErr
}

func (serverConfigTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "server-config",
		Description: "exercises server lifecycle configuration",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (serverConfigTool) Call(
	context.Context,
	json.RawMessage,
) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "ok"}, nil
}

func (tool *mutableServerSpecTool) Spec() sdk.ToolSpec {
	return tool.spec
}

func (*mutableServerSpecTool) Call(
	context.Context,
	json.RawMessage,
) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "ok"}, nil
}

func TestServerClosesOwnedPluginOnce(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("plugin close failed")
	plugin := &closingServerPlugin{closeErr: closeErr}
	server, err := NewServer(t.Context(), ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		if err := server.Close(t.Context()); !errors.Is(err, closeErr) {
			t.Fatalf("close attempt %d error = %v", attempt, err)
		}
	}
	if got := plugin.closes.Load(); got != 1 {
		t.Fatalf("plugin close calls = %d, want 1", got)
	}
}

func TestNewServerRejectsMissingStoresBeforePluginInstall(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		operations sdk.OperationStore
		inbox      sdk.DeliveryStore
	}{
		{
			name:  "operations",
			inbox: sdkstorage.NewMemoryDeliveryStore(),
		},
		{
			name:       "inbox",
			operations: sdkstorage.NewMemoryOperationStore(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var installed atomic.Bool
			plugin := sdk.PluginFunc{
				PluginManifest: sdk.Manifest{
					Name:        "invalid-server-config-" + test.name,
					Version:     "1.0.0",
					Description: "tracks whether invalid construction installs",
					APIVersion:  sdk.APIVersion,
				},
				InstallFunc: func(context.Context, sdk.Registrar) error {
					installed.Store(true)
					return nil
				},
			}
			if _, err := NewServer(context.Background(), ServerConfig{
				Plugin:     plugin,
				Operations: test.operations,
				Inbox:      test.inbox,
			}); err == nil {
				t.Fatal("NewServer unexpectedly accepted a missing store")
			}
			if installed.Load() {
				t.Fatal("plugin was installed before server config validation")
			}
		})
	}
}

func TestNewServerRejectsInvalidWorkersBeforePluginInstall(t *testing.T) {
	t.Parallel()
	var installed atomic.Bool
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "invalid-workers",
			Version:     "1.0.0",
			Description: "tracks invalid worker configuration",
			APIVersion:  sdk.APIVersion,
		},
		InstallFunc: func(context.Context, sdk.Registrar) error {
			installed.Store(true)
			return nil
		},
	}
	if _, err := NewServer(context.Background(), ServerConfig{
		Plugin:       plugin,
		Operations:   sdkstorage.NewMemoryOperationStore(),
		Inbox:        sdkstorage.NewMemoryDeliveryStore(),
		InboxWorkers: -1,
	}); err == nil {
		t.Fatal("NewServer unexpectedly accepted invalid workers")
	}
	if installed.Load() {
		t.Fatal("plugin was installed before worker config validation")
	}
}

func TestClosedServerRejectsStoredOperationBeforePersistence(t *testing.T) {
	t.Parallel()
	operations := sdkstorage.NewMemoryOperationStore()
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "closed-server",
			Version:     "1.0.0",
			Description: "verifies the operation lifecycle gate",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("server-config")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(serverConfigTool{})
		},
	}
	service, err := NewServer(context.Background(), ServerConfig{
		Plugin:     plugin,
		Operations: operations,
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := service.(*server)
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := server.submitStored(
		context.Background(),
		sdk.OperationKindTool,
		"server-config",
		sdk.OperationRequest{IdempotencyKey: "after-close", Input: []byte(`{}`)},
	); err == nil {
		t.Fatal("closed server accepted a stored operation")
	}
	records, err := operations.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("operations persisted after close = %#v", records)
	}
}

func TestClosedServerRejectsRPCMethods(t *testing.T) {
	t.Parallel()
	service, err := NewServer(context.Background(), ServerConfig{
		Plugin: sdk.PluginFunc{
			PluginManifest: sdk.Manifest{
				Name:        "closed-rpc-server",
				Version:     "1.0.0",
				Description: "verifies the RPC lifecycle gate",
				APIVersion:  sdk.APIVersion,
			},
			InstallFunc: func(context.Context, sdk.Registrar) error {
				return nil
			},
		},
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	requests := map[string]func() error{
		"describe": func() error {
			_, err := service.Describe(
				context.Background(),
				&pluginv1.DescribeRequest{},
			)
			return err
		},
		"submit": func() error {
			_, err := service.SubmitOperation(
				context.Background(),
				&pluginv1.SubmitOperationRequest{},
			)
			return err
		},
		"poll": func() error {
			_, err := service.PollOperation(
				context.Background(),
				&pluginv1.PollOperationRequest{},
			)
			return err
		},
		"cancel": func() error {
			_, err := service.CancelOperation(
				context.Background(),
				&pluginv1.CancelOperationRequest{},
			)
			return err
		},
		"hook": func() error {
			_, err := service.HandleHook(
				context.Background(),
				&pluginv1.HandleHookRequest{},
			)
			return err
		},
		"deliver": func() error {
			_, err := service.Deliver(
				context.Background(),
				&pluginv1.DeliverRequest{},
			)
			return err
		},
	}
	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			if code := status.Code(request()); code != codes.Unavailable {
				t.Fatalf("status = %s", code)
			}
		})
	}
}

func TestNewServerFailsUnrecoverableStoredOperations(t *testing.T) {
	t.Parallel()
	operations := sdkstorage.NewMemoryOperationStore()
	record, _, err := operations.Submit(t.Context(), sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "stale-operation"},
		Kind:      sdk.OperationKindTool,
		Resource:  "server-config",
		ResourceRevision: sdk.ResourceRevision(
			sdk.Manifest{Name: "old-plugin", Version: "1.0.0"},
			string(sdk.OperationKindTool),
			"server-config",
			serverConfigTool{}.Spec(),
		),
		Input: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "new-plugin",
			Version:     "2.0.0",
			Description: "cannot execute operations from the old resource revision",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("server-config")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(serverConfigTool{})
		},
	}
	server, err := NewServer(t.Context(), ServerConfig{
		Plugin:     plugin,
		Operations: operations,
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(context.Background()); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	recovered, err := operations.Get(t.Context(), record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Operation.State != sdk.OperationFailed ||
		!strings.Contains(recovered.Operation.Error, "does not match current revision") {
		t.Fatalf("stale operation after recovery = %#v", recovered.Operation)
	}
}

func TestServerDescribeUsesFrozenDefensiveSpecs(t *testing.T) {
	t.Parallel()
	valueSchema := map[string]any{"type": "string"}
	tool := &mutableServerSpecTool{spec: sdk.ToolSpec{
		Name:        "frozen-server-tool",
		Description: "verifies frozen RPC descriptions",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"value": valueSchema,
			},
		},
	}}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "frozen-server",
			Version:     "1.0.0",
			Description: "freezes resource specs during server construction",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("frozen-server-tool")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(tool)
		},
	}
	server, err := NewServer(context.Background(), ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(context.Background()); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	tool.spec.Name = "changed"
	valueSchema["type"] = "number"
	plugin.PluginManifest.Registers[0] = sdk.ToolResource("changed")
	first, err := server.Describe(context.Background(), &pluginv1.DescribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Tools) != 1 || first.Tools[0].GetName() != "frozen-server-tool" {
		t.Fatalf("describe tools after plugin mutation = %#v", first.Tools)
	}
	if got := first.GetManifest().GetRegisters(); len(got) != 1 ||
		got[0] != sdk.ToolResource("frozen-server-tool") {
		t.Fatalf("describe manifest after plugin mutation = %#v", got)
	}
	value := first.Tools[0].GetParameters().AsMap()["properties"].(map[string]any)["value"].(map[string]any)
	if value["type"] != "string" {
		t.Fatalf("frozen RPC schema = %#v", value)
	}

	first.Tools[0].Name = "response-mutated"
	second, err := server.Describe(context.Background(), &pluginv1.DescribeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if second.Tools[0].GetName() != "frozen-server-tool" {
		t.Fatalf("describe response mutation leaked: %#v", second.Tools[0])
	}
}

func TestServerDeadLettersPersistedDeliveryForAnotherPlugin(t *testing.T) {
	t.Parallel()
	var received atomic.Bool
	registrar := newServerRegistrar()
	err := registrar.RegisterSubscriber(sdk.SubscriberFunc{
		SubscriberSpec: sdk.SubscriberSpec{
			Name:   "target",
			Events: []string{"example.event"},
		},
		ReceiveFunc: func(context.Context, sdk.Delivery) error {
			received.Store(true)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := sdkstorage.NewMemoryDeliveryStore()
	err = inbox.Enqueue(context.Background(), sdk.Delivery{
		ID:           "misrouted",
		Plugin:       "another-plugin",
		Subscription: "target",
		Event: sdk.Event{
			ID: "event", Name: "example.event", Payload: json.RawMessage(`{}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := inbox.Lease(context.Background(), time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	server := &server{
		manifest:  sdk.Manifest{Name: "expected-plugin", Version: "1.0.0"},
		registrar: registrar,
		inbox:     inbox,
		logger:    slog.Default(),
		context:   context.Background(),
	}
	server.receiveDelivery(delivery)

	records, err := inbox.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].State != sdk.DeliveryDeadLetter {
		t.Fatalf("misrouted delivery state = %#v", records)
	}
	if received.Load() {
		t.Fatal("misrouted delivery reached subscriber")
	}
}
