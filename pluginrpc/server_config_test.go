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

	"github.com/lincyaw/ag/internal/deliveryworker"
	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/internal/plugincontract"
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
	closes     atomic.Int64
	closeErr   error
	closePanic any
}

type panicGetOperationStore struct {
	sdk.OperationStore
	entered chan<- struct{}
}

func (store panicGetOperationStore) Get(
	context.Context,
	string,
) (sdk.OperationRecord, error) {
	select {
	case store.entered <- struct{}{}:
	default:
	}
	panic("broken recovery get")
}

func TestServerRejectsSameProcessAgentRegistration(t *testing.T) {
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "agent-only",
			Version:     "1.0.0",
			Description: "same-process agent must not cross RPC",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.AgentResource("worker")},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "worker",
				Description: "same-process worker",
			})
		},
	}
	_, err := NewServer(t.Context(), ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err == nil || !strings.Contains(err.Error(), "same-process runtime") {
		t.Fatalf("NewServer() error = %v", err)
	}
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
	if plugin.closePanic != nil {
		panic(plugin.closePanic)
	}
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

func TestServerClosePanicReturnsErrorAndCompletesOnce(t *testing.T) {
	t.Parallel()
	plugin := &closingServerPlugin{closePanic: "broken plugin close"}
	server, err := NewServer(t.Context(), ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := server.Close(closeCtx)
		cancel()
		if err == nil ||
			!strings.Contains(err.Error(), "close plugin \"closing-server-plugin\" panic") ||
			!strings.Contains(err.Error(), "broken plugin close") {
			t.Fatalf("close attempt %d error = %v", attempt, err)
		}
	}
	if got := plugin.closes.Load(); got != 1 {
		t.Fatalf("plugin close calls = %d, want 1", got)
	}
}

func TestDelayedRecoveryPanicCompletesServerWait(t *testing.T) {
	t.Parallel()
	service, err := NewServer(t.Context(), ServerConfig{
		Plugin:     &closingServerPlugin{},
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := service.(*server)
	entered := make(chan struct{}, 1)
	server.operations = panicGetOperationStore{
		OperationStore: server.operations,
		entered:        entered,
	}
	if !server.reserveOperation() {
		t.Fatal("reserve operation rejected before server close")
	}
	server.startReservedRecovery(
		context.Background(),
		operationworker.RecoveryCandidate{OperationID: "panic-recovery"},
	)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("delayed recovery did not enter operation lookup")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeCtx); err != nil {
		t.Fatalf("server close after recovery panic: %v", err)
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

func TestServerHandleHookHonorsHookTimeout(t *testing.T) {
	t.Parallel()
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "hook-timeout",
			Version:     "1.0.0",
			Description: "verifies hook invocation timeout policy",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.HookResource("slow-hook")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterHook(sdk.HookFunc{
				HookSpec: sdk.HookSpec{
					Name:    "slow-hook",
					Event:   "example.event",
					Timeout: 10 * time.Millisecond,
				},
				HandleFunc: func(
					ctx context.Context,
					_ sdk.Event,
				) (sdk.Effect, error) {
					<-ctx.Done()
					return sdk.Effect{}, ctx.Err()
				},
			})
		},
	}
	service, err := NewServer(context.Background(), ServerConfig{
		Plugin:     plugin,
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Errorf("close server: %v", err)
		}
	})
	event, err := toProtoEvent(sdk.Event{
		ID:      "timeout-event",
		Name:    "example.event",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = service.HandleHook(ctx, &pluginv1.HandleHookRequest{
		Hook:  "slow-hook",
		Event: event,
	})
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("hook timeout status = %s, error = %v", code, err)
	}
	if ctx.Err() != nil {
		t.Fatal("server waited for the caller context instead of the hook timeout")
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
	cancelled, err := server.CancelOperation(
		t.Context(),
		&pluginv1.CancelOperationRequest{
			Kind:     pluginv1.OperationKind_OPERATION_KIND_TOOL,
			Resource: "server-config",
			Id:       record.Operation.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.GetOperation().GetState() !=
		pluginv1.OperationState_OPERATION_STATE_FAILED {
		t.Fatalf("cancel stale terminal operation = %#v", cancelled.GetOperation())
	}
}

func TestServerReschedulesExistingNonTerminalIdempotentSubmit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	operations := sdkstorage.NewMemoryOperationStore()
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "reschedule-existing",
			Version:     "1.0.0",
			Description: "reschedules idempotent pending operations",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("server-config")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterTool(serverConfigTool{})
		},
	}
	service, err := NewServer(ctx, ServerConfig{
		Plugin:     plugin,
		Operations: operations,
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := service.(*server)
	t.Cleanup(func() {
		if err := server.Close(context.Background()); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	request := sdk.OperationRequest{
		IdempotencyKey: "existing-pending",
		Input:          json.RawMessage(`{}`),
	}
	target := server.operationTarget(sdk.OperationKindTool, "server-config")
	existing, _, err := operations.Submit(ctx, target.Record(request))
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := server.submitStored(
		ctx,
		sdk.OperationKindTool,
		"server-config",
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if submitted.ID != existing.Operation.ID {
		t.Fatalf(
			"submitted operation ID = %q, want existing %q",
			submitted.ID,
			existing.Operation.ID,
		)
	}

	deadline := time.Now().Add(time.Second)
	for {
		record, err := operations.Get(ctx, existing.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		if record.Operation.Terminal() {
			if record.Operation.State != sdk.OperationSucceeded {
				t.Fatalf("operation after reschedule = %#v", record.Operation)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation was not rescheduled: %#v", record.Operation)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServerDelayedRecoveryIgnoresStartupContextCancellation(
	t *testing.T,
) {
	t.Parallel()
	operations := sdkstorage.NewMemoryOperationStore()
	manifest := sdk.Manifest{
		Name:        "delayed-recovery",
		Version:     "1.0.0",
		Description: "recovers operations after an existing lease expires",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.ToolResource("server-config")},
	}
	record, _, err := operations.Submit(context.Background(), sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "delayed-recovery"},
		Kind:      sdk.OperationKindTool,
		Resource:  "server-config",
		ResourceRevision: sdk.ResourceRevision(
			manifest,
			string(sdk.OperationKindTool),
			"server-config",
			serverConfigTool{}.Spec(),
		),
		Input: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.Claim(
		context.Background(),
		record.Operation.ID,
		"old-worker",
		time.Now().UTC(),
		50*time.Millisecond,
	); err != nil {
		t.Fatal(err)
	}
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	service, err := NewServer(startupCtx, ServerConfig{
		Plugin: sdk.PluginFunc{
			PluginManifest: manifest,
			InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
				return registrar.RegisterTool(serverConfigTool{})
			},
		},
		Operations:     operations,
		Inbox:          sdkstorage.NewMemoryDeliveryStore(),
		OperationLease: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelStartup()
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Errorf("close server: %v", err)
		}
	})

	deadline := time.Now().Add(2 * time.Second)
	for {
		recovered, err := operations.Get(context.Background(), record.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		if recovered.Operation.Terminal() {
			if recovered.Operation.State != sdk.OperationSucceeded {
				t.Fatalf("delayed recovery = %#v", recovered.Operation)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("operation did not recover: %#v", recovered.Operation)
		}
		time.Sleep(time.Millisecond)
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
	registrar := plugincontract.NewRegistrar()
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
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	server := &server{
		manifest:          sdk.Manifest{Name: "expected-plugin", Version: "1.0.0"},
		registrar:         registrar,
		inbox:             inbox,
		logger:            slog.Default(),
		context:           serverCtx,
		inboxLease:        time.Minute,
		inboxPoll:         time.Millisecond,
		inboxMaxAttempts:  3,
		subscriberTimeout: time.Second,
	}
	runner := deliveryworker.Runner{
		Store:       inbox,
		Logger:      server.logger,
		Context:     serverCtx,
		Queue:       "plugin inbox",
		Lease:       server.inboxLease,
		Poll:        server.inboxPoll,
		MaxAttempts: server.inboxMaxAttempts,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runner.Run(0, server.receiveDelivery)
	}()

	eventuallyRPC(t, time.Second, func() bool {
		records, err := inbox.List(context.Background())
		if err != nil {
			return false
		}
		return len(records) == 1 && records[0].State == sdk.DeliveryDeadLetter
	})
	cancelServer()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
	if received.Load() {
		t.Fatal("misrouted delivery reached subscriber")
	}
}
