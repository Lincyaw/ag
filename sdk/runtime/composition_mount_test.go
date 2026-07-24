package runtime

// Composition tests cover atomic publication and plugin ownership.

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

type contextHealthBackend struct {
	*closeCountingBackend
}

type blockingCloseBackend struct {
	sdk.StateBackend
	closes  atomic.Int64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type panicRuntimeCloseBackend struct {
	sdk.StateBackend
	closes atomic.Int64
}

type capabilityOverrideBackend struct {
	sdk.StateBackend
	capabilities sdk.StorageCapabilities
}

func (backend *capabilityOverrideBackend) Capabilities() sdk.StorageCapabilities {
	return backend.capabilities
}

type hiddenAtomicStateBackend struct {
	*atomicTestBackend
	capabilities sdk.StorageCapabilities
}

func (backend *hiddenAtomicStateBackend) Capabilities() sdk.StorageCapabilities {
	return backend.capabilities
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

func (backend *contextHealthBackend) Health(ctx context.Context) error {
	backend.healthChecks.Add(1)
	return ctx.Err()
}

func (backend *blockingCloseBackend) Close(context.Context) error {
	backend.closes.Add(1)
	backend.once.Do(func() { close(backend.started) })
	<-backend.release
	return nil
}

func (backend *panicRuntimeCloseBackend) Close(context.Context) error {
	backend.closes.Add(1)
	panic("broken runtime storage close")
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

type panicClosePlugin struct {
	manifest sdk.Manifest
}

func (plugin *panicClosePlugin) Manifest() sdk.Manifest {
	return plugin.manifest
}

func (plugin *panicClosePlugin) Install(context.Context, sdk.Registrar) error {
	return nil
}

func (plugin *panicClosePlugin) Close(context.Context) error {
	panic("broken plugin close")
}

type deadlineClosePlugin struct {
	manifest sdk.Manifest
	minimum  time.Duration
}

func (plugin *deadlineClosePlugin) Manifest() sdk.Manifest {
	return plugin.manifest
}

func (plugin *deadlineClosePlugin) Install(context.Context, sdk.Registrar) error {
	return errors.New("install failed")
}

func (plugin *deadlineClosePlugin) Close(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("plugin close deadline missing")
	}
	if time.Until(deadline) < plugin.minimum {
		return errors.New("plugin close deadline shorter than configured budget")
	}
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

func TestUnmountClosePanicStillCompletesMount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
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
	plugin := &panicClosePlugin{
		manifest: sdk.Manifest{
			Name:        "panic-close",
			Version:     "1.0.0",
			Description: "panics while closing",
			APIVersion:  sdk.APIVersion,
		},
	}
	mount, err := runtime.Mount(ctx, sdk.Local(plugin))
	if err != nil {
		t.Fatal(err)
	}
	if err := mount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mount.Done():
	case <-time.After(time.Second):
		t.Fatal("mount close did not complete after panic")
	}
	err = mount.state.closeError()
	if err == nil ||
		!strings.Contains(err.Error(), "close plugin \"panic-close\" connection panic") ||
		!strings.Contains(err.Error(), "broken plugin close") {
		t.Fatalf("mount close error = %v", err)
	}
}

func TestMountFailureClosePanicReturnsJoinedError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
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
	plugin := &panicClosePlugin{
		manifest: sdk.Manifest{
			Version:     "1.0.0",
			Description: "invalid manifest that panics while cleaning up",
			APIVersion:  sdk.APIVersion,
		},
	}
	_, err = runtime.Mount(ctx, sdk.Local(plugin))
	if err == nil ||
		!strings.Contains(err.Error(), "validate plugin manifest") ||
		!strings.Contains(err.Error(), "close plugin \"<unknown>\" connection panic") ||
		!strings.Contains(err.Error(), "broken plugin close") {
		t.Fatalf("mount error = %v", err)
	}
}

func TestMountFailureUsesConfiguredPluginCloseTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:            newTestStateBackend(),
		PluginCloseTimeout: 2500 * time.Millisecond,
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
	plugin := &deadlineClosePlugin{
		manifest: sdk.Manifest{
			Name:        "configured-close-timeout",
			Version:     "1.0.0",
			Description: "fails install after observing close timeout",
			APIVersion:  sdk.APIVersion,
		},
		minimum: 1500 * time.Millisecond,
	}
	_, err = runtime.Mount(ctx, sdk.Local(plugin))
	if err == nil || !strings.Contains(err.Error(), "install failed") {
		t.Fatalf("mount error = %v, want install failure", err)
	}
	if strings.Contains(err.Error(), "plugin close deadline") {
		t.Fatalf("mount close used wrong deadline: %v", err)
	}
}

func TestSnapshotLeaseReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
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
	plugin := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "lease-idempotent",
			Version:     "1.0.0",
			Description: "verifies idempotent snapshot release",
			APIVersion:  sdk.APIVersion,
		},
		install: func(sdk.Registrar) error { return nil },
		closed:  make(chan struct{}),
	}
	mount, err := runtime.Mount(ctx, sdk.Local(plugin))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	lease.release()
	lease.release()
	if err := mount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mount.Done():
	case <-time.After(time.Second):
		t.Fatal("plugin connection did not close")
	}
	if plugin.closes.Load() != 1 {
		t.Fatalf("plugin close count = %d, want 1", plugin.closes.Load())
	}
}

func TestPluginCannotOverrideBuiltinEventContract(t *testing.T) {
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
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "event-override",
			Version:     "1.0.0",
			Description: "attempts to redefine a runtime-owned event",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.EventResource(sdk.EventTrajectoryAppend)},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterEvent(sdk.EventContract{
				Name:          sdk.EventTrajectoryAppend,
				MutableFields: []string{"trajectory_id"},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err == nil ||
		!strings.Contains(
			err.Error(),
			"cannot register existing resource \"event:trajectory_appended\"",
		) {
		t.Fatalf("mount error = %v", err)
	}
}

func TestPluginLifecycleEventUsesCommittedGeneration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	events := make(chan sdk.Event, 2)
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          newTestStateBackend(),
		StorageOwnership: StorageBorrowed,
		EventObserver: func(_ context.Context, event sdk.Event) {
			switch event.Name {
			case sdk.EventPluginMounted, sdk.EventPluginUnmounted:
				events <- event
			}
		},
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

	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "lifecycle-subject",
			Version:     "1.0.0",
			Description: "verifies lifecycle event generation",
			APIVersion:  sdk.APIVersion,
		},
		InstallFunc: func(context.Context, sdk.Registrar) error {
			return nil
		},
	}
	mount, err := runtime.Mount(ctx, sdk.Local(plugin))
	if err != nil {
		t.Fatal(err)
	}
	mounted := requirePluginLifecycleEvent(
		t,
		events,
		sdk.EventPluginMounted,
		"lifecycle-subject",
	)
	if got := runtime.Catalog().Generation; mounted.Generation != got {
		t.Fatalf("mounted event generation = %d, catalog generation = %d", mounted.Generation, got)
	}

	if err := mount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	unmounted := requirePluginLifecycleEvent(
		t,
		events,
		sdk.EventPluginUnmounted,
		"lifecycle-subject",
	)
	if got := runtime.Catalog().Generation; unmounted.Generation != got {
		t.Fatalf("unmounted event generation = %d, catalog generation = %d", unmounted.Generation, got)
	}
}

func requirePluginLifecycleEvent(
	t *testing.T,
	events <-chan sdk.Event,
	name string,
	plugin string,
) sdk.Event {
	t.Helper()
	select {
	case event := <-events:
		if event.Name != name {
			t.Fatalf("event name = %q, want %q", event.Name, name)
		}
		var payload sdk.PluginLifecyclePayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Name != plugin {
			t.Fatalf("payload plugin = %q, want %q", payload.Name, plugin)
		}
		if payload.Generation != event.Generation {
			t.Fatalf(
				"payload generation = %d, event generation = %d",
				payload.Generation,
				event.Generation,
			)
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("%s event was not observed", name)
		return sdk.Event{}
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

func TestNewRuntimeContextUsesStartupContextForStorageHealth(t *testing.T) {
	t.Parallel()
	backend := &contextHealthBackend{
		closeCountingBackend: &closeCountingBackend{
			StateBackend: newTestStateBackend(),
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := NewRuntimeContext(
		ctx,
		RuntimeConfig{Storage: backend},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("NewRuntimeContext() error = %v, want context canceled", err)
	}
	if got := backend.healthChecks.Load(); got != 1 {
		t.Fatalf("backend health checks = %d, want 1", got)
	}
	if got := backend.closes.Load(); got != 0 {
		t.Fatalf("constructor closed caller-owned backend %d times", got)
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
	if err == nil || err.Error() != "runtime lifecycle settings must be positive" {
		t.Fatalf("constructor error = %v", err)
	}
	if got := backend.healthChecks.Load(); got != 0 {
		t.Fatalf("backend health checks = %d, want 0", got)
	}
}

func TestRuntimeRejectsMissingRequiredStorageCapabilities(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		backend sdk.StateBackend
		want    string
	}{
		{
			name: "missing operation fencing",
			backend: stateBackendWithCapabilities(func(
				capabilities sdk.StorageCapabilities,
			) sdk.StorageCapabilities {
				capabilities.OperationFencing = false
				return capabilities
			}),
			want: "state backend must advertise operation fencing",
		},
		{
			name: "missing named queues",
			backend: stateBackendWithCapabilities(func(
				capabilities sdk.StorageCapabilities,
			) sdk.StorageCapabilities {
				capabilities.NamedQueues = false
				return capabilities
			}),
			want: "state backend must advertise named delivery queues",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewRuntime(RuntimeConfig{Storage: test.backend})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewRuntime() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRuntimeDiscoversAtomicStateFromMethodSet(t *testing.T) {
	t.Parallel()
	backend := hiddenAtomicStateTestBackend()
	runtime, err := NewRuntime(RuntimeConfig{Storage: backend})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if !sdk.InspectStorageCapabilities(backend).AtomicState {
		t.Fatal("atomic state capability was not discovered from method set")
	}
	nonAtomic := stateBackendWithCapabilities(func(
		capabilities sdk.StorageCapabilities,
	) sdk.StorageCapabilities {
		capabilities.AtomicState = true
		return capabilities
	})
	if sdk.InspectStorageCapabilities(nonAtomic).AtomicState {
		t.Fatal("atomic state capability trusted a descriptive flag")
	}
	plainRuntime, err := NewRuntime(RuntimeConfig{Storage: nonAtomic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := plainRuntime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
}

func stateBackendWithCapabilities(
	update func(sdk.StorageCapabilities) sdk.StorageCapabilities,
) sdk.StateBackend {
	backend := newTestStateBackend()
	return &capabilityOverrideBackend{
		StateBackend: backend,
		capabilities: update(backend.Capabilities()),
	}
}

func hiddenAtomicStateTestBackend() sdk.StateBackend {
	backend := newTestStateBackend()
	capabilities := backend.Capabilities()
	return &hiddenAtomicStateBackend{
		atomicTestBackend: &atomicTestBackend{StateBackend: backend},
		capabilities:      capabilities,
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

func TestRuntimeClosePanicCompletesShutdown(t *testing.T) {
	t.Parallel()
	backend := &panicRuntimeCloseBackend{
		StateBackend: newTestStateBackend(),
	}
	runtime, err := NewRuntime(RuntimeConfig{Storage: backend})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = runtime.Close(ctx)
	cancel()
	if err == nil ||
		!strings.Contains(err.Error(), "runtime close panic") ||
		!strings.Contains(err.Error(), "broken runtime storage close") {
		t.Fatalf("close error = %v, want storage close panic", err)
	}
	if got := backend.closes.Load(); got != 1 {
		t.Fatalf("backend close count = %d, want 1", got)
	}

	retryCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	retryErr := runtime.Close(retryCtx)
	if retryErr == nil ||
		!strings.Contains(retryErr.Error(), "runtime close panic") ||
		!strings.Contains(retryErr.Error(), "broken runtime storage close") {
		t.Fatalf("retry close error = %v, want stored storage close panic", retryErr)
	}
	if got := backend.closes.Load(); got != 1 {
		t.Fatalf("backend close count after retry = %d, want 1", got)
	}
}

func TestRuntimeRequestCloseStartsShutdownWithoutWaiting(t *testing.T) {
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

	done := make(chan struct{})
	go func() {
		runtime.RequestClose(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestClose waited for cleanup")
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("RequestClose did not start runtime cleanup")
	}
	close(backend.release)

	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("wait for requested close: %v", err)
	}
}
