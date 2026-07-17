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

type operationContextKey struct{}

func TestLocalOperationPanicBecomesStoredFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryOperationStore()
	record, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "panicking-operation"},
		Kind:      sdk.OperationKindTool,
		Resource:  "panicking-tool",
		Input:     json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{operation: operationRuntime{
		store: store, workerID: "panic-test", lease: time.Second,
	}}
	runtime.executeLocalOperation(
		context.Background(),
		record.Operation.ID,
		func(context.Context, json.RawMessage) (json.RawMessage, error) {
			panic("broken plugin")
		},
	)
	failed, err := store.Get(ctx, record.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Operation.State != sdk.OperationFailed || !strings.Contains(
		failed.Operation.Error,
		"plugin operation panic: broken plugin",
	) {
		t.Fatalf("operation after panic = %#v", failed.Operation)
	}
}

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
		runtime:     firstRuntime,
		synchronous: firstTool,
		spec:        firstTool.Spec(),
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
		runtime:     secondRuntime,
		synchronous: secondTool,
		spec:        secondTool.Spec(),
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
		runtime: runtime,
		synchronous: &blockingSyncOperationTool{
			name:    "closed",
			entered: make(chan struct{}),
			release: make(chan struct{}),
		},
		spec: sdk.ToolSpec{Name: "closed"},
	}
	_, err = adapter.SubmitCall(context.Background(), sdk.OperationRequest{
		IdempotencyKey: "closed-runtime",
		Input:          []byte(`{}`),
	})
	if err == nil || err.Error() != "runtime is closed" {
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
