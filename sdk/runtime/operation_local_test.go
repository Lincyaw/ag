package runtime

// Operation tests cover local durable execution adapters.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/internal/operationworker"
	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type syncOperationProvider struct{ calls atomic.Int64 }

func (*syncOperationProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "sync-model", Model: "sync-v1"}
}

func (provider *syncOperationProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	call := provider.calls.Add(1)
	if call == 1 {
		return sdk.ModelResponse{
			Model: "sync-v1", FinishReason: "tool_calls",
			ToolCalls: []sdk.ToolCall{{
				ID: "blocking-call", Name: "sync-block", Arguments: []byte(`{"wait":true}`),
			}},
		}, nil
	}
	return sdk.ModelResponse{Content: "done", Model: "sync-v1", FinishReason: "stop"}, nil
}

type leaseOperationProvider struct{}

func (leaseOperationProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "lease-model", Model: "lease-v1"}
}

func (leaseOperationProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{
		Model:        "lease-v1",
		FinishReason: "tool_calls",
		ToolCalls: []sdk.ToolCall{{
			ID:        "lease-call",
			Name:      "lease-block",
			Arguments: []byte(`{"wait":true}`),
		}},
	}, nil
}

type blockingSyncOperationTool struct {
	name      string
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	calls     atomic.Int64
	once      sync.Once
	observed  any
}

func (tool *blockingSyncOperationTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{Name: tool.name, Description: "blocks for async adapter tests", Parameters: map[string]any{"type": "object"}}
}

func (tool *blockingSyncOperationTool) Call(
	ctx context.Context,
	_ json.RawMessage,
) (sdk.ToolResult, error) {
	tool.calls.Add(1)
	tool.observed = ctx.Value(operationContextKey{})
	tool.once.Do(func() { close(tool.entered) })
	select {
	case <-ctx.Done():
		if tool.cancelled != nil {
			close(tool.cancelled)
		}
		return sdk.ToolResult{}, ctx.Err()
	case <-tool.release:
		return sdk.ToolResult{Content: "released"}, nil
	}
}

type stubbornSyncOperationTool struct {
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
	cancel    sync.Once
}

func (tool *stubbornSyncOperationTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "lease-block",
		Description: "holds the operation lease after caller cancellation",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *stubbornSyncOperationTool) Call(
	ctx context.Context,
	_ json.RawMessage,
) (sdk.ToolResult, error) {
	tool.once.Do(func() { close(tool.entered) })
	<-ctx.Done()
	tool.cancel.Do(func() { close(tool.cancelled) })
	<-tool.release
	return sdk.ToolResult{}, ctx.Err()
}

type countingSyncCapability struct {
	calls atomic.Int64
}

func (capability *countingSyncCapability) Spec() sdk.CapabilitySpec {
	return sdk.CapabilitySpec{
		Name:         "stateful-capability",
		Description:  "records capability invocation attempts",
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
	}
}

func (capability *countingSyncCapability) Invoke(
	context.Context,
	json.RawMessage,
) (json.RawMessage, error) {
	return json.Marshal(struct {
		Call int64 `json:"call"`
	}{
		Call: capability.calls.Add(1),
	})
}

type blockingAsyncCapability struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	key     string
}

func (capability *blockingAsyncCapability) Spec() sdk.CapabilitySpec {
	return sdk.CapabilitySpec{
		Name:         "blocking-capability",
		Description:  "blocks while the runtime awaits async capability work",
		InputSchema:  map[string]any{"type": "object"},
		OutputSchema: map[string]any{"type": "object"},
	}
}

func (capability *blockingAsyncCapability) SubmitInvoke(
	_ context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	capability.mu.Lock()
	capability.key = request.IdempotencyKey
	capability.mu.Unlock()
	return sdk.Operation{
		ID:             "blocking-capability-operation",
		IdempotencyKey: request.IdempotencyKey,
		State:          sdk.OperationRunning,
		Revision:       1,
	}, nil
}

func (capability *blockingAsyncCapability) PollInvoke(
	ctx context.Context,
	id string,
	revision uint64,
) (sdk.Operation, error) {
	capability.once.Do(func() { close(capability.entered) })
	select {
	case <-ctx.Done():
		return sdk.Operation{}, ctx.Err()
	case <-capability.release:
		capability.mu.Lock()
		key := capability.key
		capability.mu.Unlock()
		return sdk.Operation{
			ID:             id,
			IdempotencyKey: key,
			State:          sdk.OperationSucceeded,
			Revision:       revision + 1,
			Output:         json.RawMessage(`{"ok":true}`),
		}, nil
	}
}

func (capability *blockingAsyncCapability) CancelInvoke(
	_ context.Context,
	id string,
) (sdk.Operation, error) {
	capability.mu.Lock()
	key := capability.key
	capability.mu.Unlock()
	return sdk.Operation{
		ID:             id,
		IdempotencyKey: key,
		State:          sdk.OperationCancelled,
		Revision:       2,
	}, nil
}

func TestSyncResourcesAreAdaptedToOperationsAndCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	provider := &syncOperationProvider{}
	tool := &blockingSyncOperationTool{
		name: "sync-block", entered: make(chan struct{}),
		cancelled: make(chan struct{}), release: make(chan struct{}),
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       newTestStateBackend(),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeContext); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name: "sync-operation-test", Version: "1.0.0",
			Description: "sync resources adapted to operations", APIVersion: sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("sync-model"),
				sdk.ToolResource("sync-block"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(registrar.RegisterProvider(provider), registrar.RegisterTool(tool))
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "sync-operation-session", Provider: "sync-model"})
	if err != nil {
		t.Fatal(err)
	}
	promptContext := context.WithValue(ctx, operationContextKey{}, "trace-baggage")
	promptContext, cancelPrompt := context.WithCancel(promptContext)
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(promptContext, "block in the tool")
		promptDone <- promptErr
	}()
	select {
	case <-tool.entered:
	case <-time.After(time.Second):
		t.Fatal("sync tool did not start in operation worker")
	}
	if tool.observed != "trace-baggage" {
		t.Fatalf("operation context value = %#v", tool.observed)
	}
	records, err := runtime.operation.store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Kind != sdk.OperationKindProvider ||
		records[0].Operation.State != sdk.OperationSucceeded ||
		records[1].Kind != sdk.OperationKindTool || records[1].Operation.State != sdk.OperationRunning {
		t.Fatalf("operations before cancellation = %#v", records)
	}
	cancelPrompt()
	select {
	case promptErr := <-promptDone:
		if !errors.Is(promptErr, context.Canceled) {
			t.Fatalf("prompt error = %v", promptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not stop after cancellation")
	}
	select {
	case <-tool.cancelled:
	case <-time.After(time.Second):
		t.Fatal("tool context was not cancelled")
	}
	records, err = runtime.operation.store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if records[1].Operation.State != sdk.OperationCancelled {
		t.Fatalf("tool operation after cancellation = %#v", records[1])
	}
}

func TestLocalOperationCancelAllowsStaleResourceRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: newTestStateBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeContext); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	record, _, err := runtime.operation.store.Submit(
		ctx,
		sdk.OperationRecord{
			Operation:        sdk.Operation{IdempotencyKey: "stale-cancel"},
			Kind:             sdk.OperationKindTool,
			Resource:         "stale-tool",
			ResourceRevision: "old-revision",
			Input:            json.RawMessage(`{}`),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := runtime.cancelLocalOperation(
		ctx,
		operationworker.Target{
			Kind:             sdk.OperationKindTool,
			Resource:         "stale-tool",
			ResourceRevision: "current-revision",
		},
		record.Operation.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != sdk.OperationCancelled {
		t.Fatalf("cancelled stale operation = %#v", cancelled)
	}
}

func TestLocalOperationLeaseKeepsUnmountWaitingAfterCallerCancellation(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       newTestStateBackend(),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeContext); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	tool := &stubbornSyncOperationTool{
		entered:   make(chan struct{}),
		cancelled: make(chan struct{}),
		release:   make(chan struct{}),
	}
	plugin := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "operation-lease",
			Version:     "1.0.0",
			Description: "keeps local operation resources alive",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("lease-model"),
				sdk.ToolResource("lease-block"),
			},
		},
		install: func(registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterProvider(leaseOperationProvider{}),
				registrar.RegisterTool(tool),
			)
		},
		closed: make(chan struct{}),
	}
	mount, err := runtime.Mount(ctx, sdk.Local(plugin))
	if err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "operation-lease-session",
		Provider: "lease-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	promptCtx, cancelPrompt := context.WithCancel(ctx)
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(promptCtx, "hold the tool")
		promptDone <- promptErr
	}()
	select {
	case <-tool.entered:
	case <-time.After(time.Second):
		t.Fatal("sync tool did not start")
	}
	cancelPrompt()
	select {
	case promptErr := <-promptDone:
		if !errors.Is(promptErr, context.Canceled) {
			t.Fatalf("prompt error = %v", promptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not return after cancellation")
	}
	select {
	case <-tool.cancelled:
	case <-time.After(time.Second):
		t.Fatal("tool context was not cancelled")
	}
	if err := mount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-mount.Done():
		t.Fatal("mount closed while local operation was still running")
	default:
	}
	close(tool.release)
	select {
	case <-mount.Done():
	case <-time.After(time.Second):
		t.Fatal("mount did not close after local operation returned")
	}
	if plugin.closes.Load() != 1 {
		t.Fatalf("plugin close count = %d, want 1", plugin.closes.Load())
	}
}

func TestLocalOperationRejectsStaleResourceRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(nil, store),
		StorageOwnership: StorageBorrowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeContext); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	record, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation:        sdk.Operation{IdempotencyKey: "stale-revision"},
		Kind:             sdk.OperationKindTool,
		Resource:         "revisioned-tool",
		ResourceRevision: "old-revision",
		Input:            json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	target := localOperationTarget{
		runtime:          runtime,
		kind:             sdk.OperationKindTool,
		resource:         "revisioned-tool",
		resourceRevision: "current-revision",
	}
	if !runtime.operationHost().Run(
		ctx,
		record.Operation.ID,
		target.identity().Validate,
		func(context.Context, sdk.OperationRecord) (json.RawMessage, error) {
			t.Fatal("stale operation should fail before execution")
			return nil, nil
		},
	) {
		t.Fatal("operation host rejected stale operation")
	}
	failed, err := store.Get(ctx, record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Operation.State != sdk.OperationFailed ||
		!strings.Contains(failed.Operation.Error, "current-revision") {
		t.Fatalf("stale operation after validation = %#v", failed.Operation)
	}
}

type operationContextKey struct{}

func TestLocalOperationRecoversRunningRecordAfterRuntimeRestart(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	store, err := sdkstorage.NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	firstRuntime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(nil, store),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstTool := &blockingSyncOperationTool{
		name: "recover", entered: make(chan struct{}), release: make(chan struct{}),
	}
	firstAdapter := syncToolAdapter{
		synchronous: firstTool,
		spec:        firstTool.Spec(),
		target: localOperationTarget{
			runtime:  firstRuntime,
			kind:     sdk.OperationKindTool,
			resource: "recover",
		},
	}
	request := sdk.OperationRequest{IdempotencyKey: "same-entry", Input: []byte(`{"work":"once"}`)}
	initial, err := firstAdapter.SubmitCall(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstTool.entered:
	case <-time.After(time.Second):
		t.Fatal("first operation did not start")
	}
	closeContext, cancelClose := context.WithTimeout(context.Background(), time.Second)
	if err := firstRuntime.Close(closeContext); err != nil {
		t.Fatal(err)
	}
	cancelClose()
	reopened, err := sdkstorage.NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	running, err := reopened.Get(context.Background(), initial.ID)
	if err != nil {
		t.Fatal(err)
	}
	if running.Operation.State != sdk.OperationRunning {
		t.Fatalf("operation after shutdown = %#v", running.Operation)
	}

	secondRuntime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(nil, reopened),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := secondRuntime.Close(ctx); err != nil {
			t.Errorf("close second runtime: %v", err)
		}
	})
	secondTool := &blockingSyncOperationTool{
		name: "recover", entered: make(chan struct{}), release: make(chan struct{}),
	}
	close(secondTool.release)
	secondAdapter := syncToolAdapter{
		synchronous: secondTool,
		spec:        secondTool.Spec(),
		target: localOperationTarget{
			runtime:  secondRuntime,
			kind:     sdk.OperationKindTool,
			resource: "recover",
		},
	}
	recovered, err := secondAdapter.SubmitCall(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ID != initial.ID || recovered.State != sdk.OperationRunning {
		t.Fatalf("resubmitted operation = %#v, initial = %#v", recovered, initial)
	}
	eventuallyOperation(t, time.Second, func() bool {
		operation, pollErr := secondAdapter.PollCall(context.Background(), initial.ID, 0)
		return pollErr == nil && operation.State == sdk.OperationSucceeded
	})
	if firstTool.calls.Load() != 1 || secondTool.calls.Load() != 1 {
		t.Fatalf("at-least-once attempts first=%d second=%d", firstTool.calls.Load(), secondTool.calls.Load())
	}
}

func TestInvokeCapabilityWithRequestReusesIdempotencyKey(t *testing.T) {
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
	capability := &countingSyncCapability{}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "capability-idempotency",
			Version:     "1.0.0",
			Description: "verifies capability request idempotency",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.CapabilityResource("stateful-capability"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterCapability(capability)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	request := sdk.OperationRequest{
		IdempotencyKey: "capability-once",
		Input:          json.RawMessage(`{"value":"first"}`),
	}
	first, err := runtime.InvokeCapabilityWithRequest(
		ctx,
		"stateful-capability",
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.InvokeCapabilityWithRequest(
		ctx,
		"stateful-capability",
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != `{"call":1}` || string(second) != `{"call":1}` {
		t.Fatalf("capability outputs first=%s second=%s", first, second)
	}
	if calls := capability.calls.Load(); calls != 1 {
		t.Fatalf("capability calls = %d, want 1", calls)
	}
}

func TestInvokeCapabilityLeaseIsScopedToOwnerMount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       newTestStateBackend(),
		OperationPoll: time.Millisecond,
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
	capability := &blockingAsyncCapability{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	owner := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "capability-owner",
			Version:     "1.0.0",
			Description: "owns the blocking capability",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.CapabilityResource("blocking-capability")},
		},
		install: func(registrar sdk.Registrar) error {
			return registrar.RegisterCapability(capability)
		},
		closed: make(chan struct{}),
	}
	unrelated := &closeCountingPlugin{
		manifest: sdk.Manifest{
			Name:        "unrelated-plugin",
			Version:     "1.0.0",
			Description: "must not be leased by another plugin's capability",
			APIVersion:  sdk.APIVersion,
		},
		install: func(sdk.Registrar) error { return nil },
		closed:  make(chan struct{}),
	}
	ownerMount, err := runtime.Mount(ctx, sdk.Local(owner))
	if err != nil {
		t.Fatal(err)
	}
	unrelatedMount, err := runtime.Mount(ctx, sdk.Local(unrelated))
	if err != nil {
		t.Fatal(err)
	}
	invoked := make(chan error, 1)
	go func() {
		output, invokeErr := runtime.InvokeCapabilityWithRequest(
			ctx,
			"blocking-capability",
			sdk.OperationRequest{
				IdempotencyKey: "scoped-capability-lease",
				Input:          json.RawMessage(`{}`),
			},
		)
		if invokeErr != nil {
			invoked <- invokeErr
			return
		}
		if string(output) != `{"ok":true}` {
			invoked <- errors.New("unexpected capability output: " + string(output))
			return
		}
		invoked <- nil
	}()
	select {
	case <-capability.entered:
	case <-time.After(time.Second):
		t.Fatal("capability poll did not start")
	}

	if err := unrelatedMount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-unrelatedMount.Done():
	case <-time.After(time.Second):
		t.Fatal("unrelated mount stayed leased during capability invocation")
	}
	if unrelated.closes.Load() != 1 {
		t.Fatalf("unrelated plugin close count = %d, want 1", unrelated.closes.Load())
	}

	if err := ownerMount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ownerMount.Done():
		t.Fatal("owner mount closed while its capability was still running")
	default:
	}
	close(capability.release)
	select {
	case err := <-invoked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("capability invocation did not finish")
	}
	select {
	case <-ownerMount.Done():
	case <-time.After(time.Second):
		t.Fatal("owner mount did not close after capability invocation finished")
	}
	if owner.closes.Load() != 1 {
		t.Fatalf("owner plugin close count = %d, want 1", owner.closes.Load())
	}
}

func TestLocalOperationRejectsSubmissionAfterRuntimeClose(t *testing.T) {
	t.Parallel()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          newTestStateBackend(),
		StorageOwnership: StorageBorrowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}

	adapter := syncToolAdapter{
		synchronous: &blockingSyncOperationTool{
			name:    "closed",
			entered: make(chan struct{}),
			release: make(chan struct{}),
		},
		spec: sdk.ToolSpec{Name: "closed"},
		target: localOperationTarget{
			runtime:  runtime,
			kind:     sdk.OperationKindTool,
			resource: "closed",
		},
	}
	_, err = adapter.SubmitCall(context.Background(), sdk.OperationRequest{
		IdempotencyKey: "closed-runtime",
		Input:          []byte(`{}`),
	})
	if !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("submit error = %v, want runtime is closed", err)
	}
	records, err := runtime.operation.store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("persisted operations after close = %#v", records)
	}
}

func eventuallyOperation(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("operation did not reach expected state")
}
