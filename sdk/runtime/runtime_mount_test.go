package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mountTestTool struct{}

func (mountTestTool) Spec() ToolSpec {
	return ToolSpec{
		Name:        "dependency-tool",
		Description: "dependency used to exercise transactional unmount",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (mountTestTool) Call(context.Context, json.RawMessage) (ToolResult, error) {
	return ToolResult{Content: "ok"}, nil
}

type closeCountingPlugin struct {
	manifest Manifest
	install  func(Registrar) error
	closes   atomic.Int64
	closed   chan struct{}
	once     sync.Once
}

type closeCountingBackend struct {
	StateBackend
	closes atomic.Int64
}

func (backend *closeCountingBackend) Close(ctx context.Context) error {
	backend.closes.Add(1)
	return backend.StateBackend.Close(ctx)
}

func (plugin *closeCountingPlugin) Manifest() Manifest { return plugin.manifest }

func (plugin *closeCountingPlugin) Install(_ context.Context, registrar Registrar) error {
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
	runtime, err := NewRuntime(RuntimeConfig{})
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
		manifest: Manifest{
			Name:        "dependency",
			Version:     "1.0.0",
			Description: "owns a required tool",
			APIVersion:  APIVersion,
			Registers:   []string{ToolResource("dependency-tool")},
		},
		install: func(registrar Registrar) error {
			return registrar.RegisterTool(mountTestTool{})
		},
		closed: make(chan struct{}),
	}
	dependent := &closeCountingPlugin{
		manifest: Manifest{
			Name:        "dependent",
			Version:     "1.0.0",
			Description: "requires the dependency tool",
			APIVersion:  APIVersion,
			Requires:    []string{ToolResource("dependency-tool")},
			Registers:   []string{SubscriberResource("dependent-events")},
		},
		install: func(registrar Registrar) error {
			return registrar.RegisterSubscriber(SubscriberFunc{
				SubscriberSpec: SubscriberSpec{Name: "dependent-events", Events: []string{EventAgentEnd}},
				ReceiveFunc:    func(context.Context, Delivery) error { return nil },
			})
		},
		closed: make(chan struct{}),
	}
	dependencyMount, err := runtime.Mount(ctx, Local(dependency))
	if err != nil {
		t.Fatal(err)
	}
	dependentMount, err := runtime.Mount(ctx, Local(dependent))
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

func TestHookWithoutTimeoutUsesRuntimeDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const hookTimeout = 20 * time.Millisecond
	runtime, err := NewRuntime(RuntimeConfig{HookTimeout: hookTimeout})
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

	plugin := PluginFunc{
		PluginManifest: Manifest{
			Name:        "bounded-hook",
			Version:     "1.0.0",
			Description: "verifies the runtime hook timeout default",
			APIVersion:  APIVersion,
			Registers:   []string{HookResource("bounded-hook")},
		},
		InstallFunc: func(_ context.Context, registrar Registrar) error {
			return registrar.RegisterHook(HookFunc{
				HookSpec: HookSpec{
					Name:  "bounded-hook",
					Event: EventBeforeAgentStart,
				},
				HandleFunc: func(ctx context.Context, _ Event) (Effect, error) {
					<-ctx.Done()
					return Effect{}, ctx.Err()
				},
			})
		},
	}
	if _, err := runtime.Mount(ctx, Local(plugin)); err != nil {
		t.Fatal(err)
	}
	hooks := runtime.Catalog().Hooks
	if len(hooks) != 1 || hooks[0].Timeout != hookTimeout {
		t.Fatalf("normalized hooks = %#v, want timeout %s", hooks, hookTimeout)
	}
	started := time.Now()
	_, err = runtime.Emit(
		ctx,
		EventBeforeAgentStart,
		"timeout-test",
		BeforeAgentStartPayload{},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("emit error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("zero-timeout hook blocked for %s", elapsed)
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
				StateBackend: NewMemoryStateBackend(),
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
