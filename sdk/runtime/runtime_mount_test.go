package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type mountTestTool struct{}

func (mountTestTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "dependency-tool",
		Description: "dependency used to exercise transactional unmount",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (mountTestTool) Call(context.Context, json.RawMessage) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "ok"}, nil
}

type closeCountingPlugin struct {
	manifest sdk.Manifest
	install  func(sdk.Registrar) error
	closes   atomic.Int64
	closed   chan struct{}
	once     sync.Once
}

type closeCountingBackend struct {
	sdk.StateBackend
	closes       atomic.Int64
	healthChecks atomic.Int64
	health       error
}

type blockingCloseBackend struct {
	sdk.StateBackend
	closes  atomic.Int64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (backend *closeCountingBackend) Close(ctx context.Context) error {
	backend.closes.Add(1)
	return backend.StateBackend.Close(ctx)
}

func (backend *closeCountingBackend) Health(ctx context.Context) error {
	backend.healthChecks.Add(1)
	if backend.health != nil {
		return backend.health
	}
	return backend.StateBackend.Health(ctx)
}

func (backend *blockingCloseBackend) Close(context.Context) error {
	backend.closes.Add(1)
	backend.once.Do(func() { close(backend.started) })
	<-backend.release
	return nil
}

func (plugin *closeCountingPlugin) Manifest() sdk.Manifest { return plugin.manifest }

func (plugin *closeCountingPlugin) Install(_ context.Context, registrar sdk.Registrar) error {
	return plugin.install(registrar)
}

func (plugin *closeCountingPlugin) Close(context.Context) error {
	plugin.closes.Add(1)
	plugin.once.Do(func() { close(plugin.closed) })
	return nil
}

func TestUnmountCanRetryAfterDependencyFailureAndClosesOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	}()
	dependency := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "dependency",
			Version:     "1.0.0",
			Description: "owns a required tool",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("dependency-tool")},
		},
		install: func(registrar sdk.Registrar) error {
			return registrar.RegisterTool(mountTestTool{})
		},
		closed: make(chan struct{}),
	}
	dependent := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "dependent",
			Version:     "1.0.0",
			Description: "requires the dependency tool",
			APIVersion:  sdk.APIVersion,
			Requires:    []string{sdk.ToolResource("dependency-tool")},
			Registers:   []string{sdk.SubscriberResource("dependent-events")},
		},
		install: func(registrar sdk.Registrar) error {
			return registrar.RegisterSubscriber(sdk.SubscriberFunc{
				SubscriberSpec: sdk.SubscriberSpec{Name: "dependent-events", Events: []string{sdk.EventAgentEnd}},
				ReceiveFunc:    func(context.Context, sdk.Delivery) error { return nil },
			})
		},
		closed: make(chan struct{}),
	}
	dependencyMount, err := runtime.Mount(ctx, sdk.Local(dependency))
	if err != nil {
		t.Fatal(err)
	}
	dependentMount, err := runtime.Mount(ctx, sdk.Local(dependent))
	if err != nil {
		t.Fatal(err)
	}
	if err := dependencyMount.Unmount(ctx); err == nil {
		t.Fatal("dependency unmount unexpectedly succeeded while required")
	}
	select {
	case <-dependency.closed:
		t.Fatal("failed unmount closed the dependency")
	default:
	}
	if err := dependentMount.Unmount(ctx); err != nil {
		t.Fatalf("unmount dependent: %v", err)
	}
	if err := dependencyMount.Unmount(ctx); err != nil {
		t.Fatalf("retry dependency unmount: %v", err)
	}

	const retries = 32
	var wait sync.WaitGroup
	var retryErrors atomic.Int64
	for range retries {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := dependencyMount.Unmount(ctx); err != nil {
				retryErrors.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := retryErrors.Load(); got != 0 {
		t.Fatalf("concurrent idempotent unmount errors = %d", got)
	}
	select {
	case <-dependencyMount.Done():
	case <-time.After(time.Second):
		t.Fatal("dependency connection did not close")
	}
	if got := dependency.closes.Load(); got != 1 {
		t.Fatalf("dependency close count = %d, want 1", got)
	}
	if err := errors.Join(dependencyMount.Unmount(ctx), dependentMount.Unmount(ctx)); err != nil {
		t.Fatalf("final idempotent unmount: %v", err)
	}
}

func TestMountHonorsConflictsRegardlessOfOrder(t *testing.T) {
	t.Parallel()
	for _, conflictFirst := range []bool{true, false} {
		name := "resource_then_conflict"
		if conflictFirst {
			name = "conflict_then_resource"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Errorf("close runtime: %v", err)
				}
			})
			conflict := sdk.PluginFunc{
				PluginManifest: sdk.Manifest{
					Name:        "conflict",
					Version:     "1.0.0",
					Description: "rejects the dependency tool",
					APIVersion:  sdk.APIVersion,
					Conflicts:   []string{sdk.ToolResource("dependency-tool")},
				},
				InstallFunc: func(context.Context, sdk.Registrar) error { return nil },
			}
			resource := sdk.PluginFunc{
				PluginManifest: sdk.Manifest{
					Name:        "resource",
					Version:     "1.0.0",
					Description: "registers the conflicting tool",
					APIVersion:  sdk.APIVersion,
					Registers:   []string{sdk.ToolResource("dependency-tool")},
				},
				InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
					return registrar.RegisterTool(mountTestTool{})
				},
			}
			first, second := sdk.Plugin(conflict), sdk.Plugin(resource)
			if !conflictFirst {
				first, second = second, first
			}
			if _, err := runtime.Mount(context.Background(), sdk.Local(first)); err != nil {
				t.Fatalf("mount first plugin: %v", err)
			}
			if _, err := runtime.Mount(context.Background(), sdk.Local(second)); err == nil ||
				!strings.Contains(err.Error(), "conflicts with resource") {
				t.Fatalf("mount conflicting plugin error = %v", err)
			}
			if plugins := runtime.Catalog().Plugins; len(plugins) != 1 {
				t.Fatalf("mounted plugins after conflict = %#v", plugins)
			}
		})
	}
}

func TestHookWithoutTimeoutUsesRuntimeDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const hookTimeout = 20 * time.Millisecond
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:     newTestStateBackend(),
		HookTimeout: hookTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	}()

	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "bounded-hook",
			Version:     "1.0.0",
			Description: "verifies the runtime hook timeout default",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.HookResource("bounded-hook")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterHook(sdk.HookFunc{
				HookSpec: sdk.HookSpec{
					Name:  "bounded-hook",
					Event: sdk.EventBeforeAgentStart,
				},
				HandleFunc: func(ctx context.Context, _ sdk.Event) (sdk.Effect, error) {
					<-ctx.Done()
					return sdk.Effect{}, ctx.Err()
				},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	hooks := runtime.Catalog().Hooks
	if len(hooks) != 1 ||
		hooks[0].Timeout != hookTimeout ||
		hooks[0].FailurePolicy != sdk.FailurePolicyFailClosed {
		t.Fatalf(
			"normalized hooks = %#v, want timeout %s and fail-closed policy",
			hooks,
			hookTimeout,
		)
	}
	started := time.Now()
	_, err = runtime.Emit(
		ctx,
		sdk.EventBeforeAgentStart,
		"timeout-test",
		sdk.BeforeAgentStartPayload{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("emit error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("zero-timeout hook blocked for %s", elapsed)
	}
}

func TestHooksCannotMutateSharedEventPayload(t *testing.T) {
	t.Parallel()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	var observed sdk.BeforeAgentStartPayload
	mutator := sdk.HookFunc{
		HookSpec: sdk.HookSpec{
			Name: "mutator", Event: sdk.EventBeforeAgentStart,
			Priority: sdk.PriorityPre,
		},
		HandleFunc: func(_ context.Context, event sdk.Event) (sdk.Effect, error) {
			event.Payload[0] = '['
			return sdk.Effect{}, nil
		},
	}
	observer := sdk.HookFunc{
		HookSpec: sdk.HookSpec{
			Name: "observer", Event: sdk.EventBeforeAgentStart,
			Priority: sdk.PriorityNormal,
		},
		HandleFunc: func(_ context.Context, event sdk.Event) (sdk.Effect, error) {
			return sdk.Effect{}, json.Unmarshal(event.Payload, &observed)
		},
	}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "isolated-hooks",
			Version:     "1.0.0",
			Description: "verifies that hooks receive isolated event payloads",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.HookResource("mutator"),
				sdk.HookResource("observer"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterHook(mutator),
				registrar.RegisterHook(observer),
			)
		},
	}
	if _, err := runtime.Mount(t.Context(), sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.Emit(
		t.Context(),
		sdk.EventBeforeAgentStart,
		"isolated-hooks",
		sdk.BeforeAgentStartPayload{System: "original"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if observed.System != "original" {
		t.Fatalf("observer payload = %#v", observed)
	}
	var final sdk.BeforeAgentStartPayload
	if err := json.Unmarshal(result.Event.Payload, &final); err != nil {
		t.Fatalf("final event payload was mutated: %v", err)
	}
	if final.System != "original" {
		t.Fatalf("final payload = %#v", final)
	}
}

func TestCloneEffectSnapshotsMutableValues(t *testing.T) {
	t.Parallel()
	effect := sdk.Effect{
		Patch: map[string]json.RawMessage{"field": json.RawMessage(`{"value":1}`)},
		Block: &sdk.Block{Reason: "original"},
		Action: &sdk.Action{
			Kind:  sdk.ActionInject,
			Cause: &sdk.Cause{Code: "original"},
			Messages: []sdk.Message{{
				Content: "original",
				ToolCalls: []sdk.ToolCall{{
					ID: "call", Arguments: json.RawMessage(`{"value":1}`),
				}},
			}},
		},
	}
	cloned := cloneEffect(effect)

	effect.Patch["field"][0] = '['
	effect.Block.Reason = "changed"
	effect.Action.Cause.Code = "changed"
	effect.Action.Messages[0].Content = "changed"
	effect.Action.Messages[0].ToolCalls[0].Arguments[0] = '['

	if string(cloned.Patch["field"]) != `{"value":1}` ||
		cloned.Block.Reason != "original" ||
		cloned.Action.Cause.Code != "original" ||
		cloned.Action.Messages[0].Content != "original" ||
		string(cloned.Action.Messages[0].ToolCalls[0].Arguments) != `{"value":1}` {
		t.Fatalf("cloned effect changed with plugin-owned value: %#v", cloned)
	}
}

func TestRuntimeHonorsStorageOwnership(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		ownership StorageOwnership
		wantClose int64
	}{
		{name: "owned", ownership: StorageOwned, wantClose: 1},
		{name: "borrowed", ownership: StorageBorrowed, wantClose: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			backend := &closeCountingBackend{
				StateBackend: newTestStateBackend(),
			}
			runtime, err := NewRuntime(RuntimeConfig{
				Storage:          backend,
				StorageOwnership: test.ownership,
			})
			if err != nil {
				t.Fatal(err)
			}
			closeCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			defer cancel()
			if err := runtime.Close(closeCtx); err != nil {
				t.Fatal(err)
			}
			if got := backend.closes.Load(); got != test.wantClose {
				t.Fatalf("backend close count = %d, want %d", got, test.wantClose)
			}
		})
	}
}

func TestRuntimeConstructionFailureLeavesStorageWithCaller(t *testing.T) {
	t.Parallel()
	backend := &closeCountingBackend{
		StateBackend: newTestStateBackend(),
		health:       errors.New("unhealthy"),
	}
	if _, err := NewRuntime(RuntimeConfig{Storage: backend}); err == nil {
		t.Fatal("NewRuntime unexpectedly accepted an unhealthy backend")
	}
	if got := backend.closes.Load(); got != 0 {
		t.Fatalf("constructor closed caller-owned backend %d times", got)
	}
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := backend.closes.Load(); got != 1 {
		t.Fatalf("caller close count = %d, want 1", got)
	}
}

func TestRuntimeValidatesConfigBeforeTouchingStorage(t *testing.T) {
	t.Parallel()
	backend := &closeCountingBackend{
		StateBackend: newTestStateBackend(),
		health:       errors.New("health should not be checked"),
	}
	_, err := NewRuntime(RuntimeConfig{
		Storage:         backend,
		DeliveryWorkers: -1,
	})
	if err == nil || err.Error() != "delivery and operation settings must be positive" {
		t.Fatalf("constructor error = %v", err)
	}
	if got := backend.healthChecks.Load(); got != 0 {
		t.Fatalf("backend health checks = %d, want 0", got)
	}
}

func TestRuntimeCloseContinuesAfterCallerTimeout(t *testing.T) {
	t.Parallel()
	backend := &blockingCloseBackend{
		StateBackend: newTestStateBackend(),
		started:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	runtime, err := NewRuntime(RuntimeConfig{Storage: backend})
	if err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("first close error = %v, want context canceled", err)
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("runtime did not continue closing its storage")
	}
	close(backend.release)

	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("retry close: %v", err)
	}
	if got := backend.closes.Load(); got != 1 {
		t.Fatalf("backend close count = %d, want 1", got)
	}
}
