package runtime

// Execution tests cover structured same-turn concurrency.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type concurrentToolProvider struct {
	mu       sync.Mutex
	requests []sdk.ModelRequest
}

func (*concurrentToolProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "concurrent-model", Model: "test"}
}

func (provider *concurrentToolProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.requests = append(provider.requests, request)
	if len(provider.requests) == 1 {
		return sdk.ModelResponse{
			ToolCalls: []sdk.ToolCall{
				{
					ID:        "first-call",
					Name:      "barrier",
					Arguments: json.RawMessage(`{"value":"first"}`),
				},
				{
					ID:        "second-call",
					Name:      "barrier",
					Arguments: json.RawMessage(`{"value":"second"}`),
				},
			},
		}, nil
	}
	return sdk.ModelResponse{Content: "done"}, nil
}

func (provider *concurrentToolProvider) Requests() []sdk.ModelRequest {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	result := make([]sdk.ModelRequest, len(provider.requests))
	copy(result, provider.requests)
	return result
}

type barrierTool struct {
	started       chan string
	firstRelease  chan struct{}
	secondRelease chan struct{}
	secondDone    chan struct{}
}

type cancellingTool struct {
	started   chan string
	cancelled chan string
}

func (*cancellingTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "cancel-group",
		Description: "blocks until its structured parent is cancelled",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *cancellingTool) Call(
	ctx context.Context,
	input json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(input, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	tool.started <- arguments.Value
	<-ctx.Done()
	tool.cancelled <- arguments.Value
	return sdk.ToolResult{}, ctx.Err()
}

type cancellingToolProvider struct{}

func (cancellingToolProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "cancel-model", Model: "test"}
}

func (cancellingToolProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{
		ToolCalls: []sdk.ToolCall{
			{
				ID:        "cancel-first",
				Name:      "cancel-group",
				Arguments: json.RawMessage(`{"value":"first"}`),
			},
			{
				ID:        "cancel-second",
				Name:      "cancel-group",
				Arguments: json.RawMessage(`{"value":"second"}`),
			},
		},
	}, nil
}

type panicSubmitProvider struct{}

func (panicSubmitProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "panic-submit-model", Model: "test"}
}

func (panicSubmitProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	for _, message := range request.Messages {
		if message.Role == sdk.RoleTool &&
			message.ToolCallID == "panic-submit" {
			if strings.Contains(message.Content, "submit exploded") {
				return sdk.ModelResponse{
					Content: "panic captured",
				}, nil
			}
			return sdk.ModelResponse{
				Content: "unexpected tool error: " + message.Content,
			}, nil
		}
	}
	return sdk.ModelResponse{
		ToolCalls: []sdk.ToolCall{{
			ID:        "panic-submit",
			Name:      "panic-submit",
			Arguments: json.RawMessage(`{}`),
		}},
	}, nil
}

type panicSubmitTool struct{}

func (panicSubmitTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "panic-submit",
		Description: "panics while submitting an async operation",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (panicSubmitTool) SubmitCall(
	context.Context,
	sdk.OperationRequest,
) (sdk.Operation, error) {
	panic("submit exploded")
}

func (panicSubmitTool) PollCall(
	context.Context,
	string,
	uint64,
) (sdk.Operation, error) {
	return sdk.Operation{}, nil
}

func (panicSubmitTool) CancelCall(
	context.Context,
	string,
) (sdk.Operation, error) {
	return sdk.Operation{}, nil
}

func (*barrierTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "barrier",
		Description: "exposes concurrent execution in tests",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *barrierTool) Call(
	ctx context.Context,
	input json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(input, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	select {
	case tool.started <- arguments.Value:
	case <-ctx.Done():
		return sdk.ToolResult{}, ctx.Err()
	}
	var release <-chan struct{}
	switch arguments.Value {
	case "first":
		release = tool.firstRelease
	case "second":
		release = tool.secondRelease
	default:
		return sdk.ToolResult{}, fmt.Errorf(
			"unexpected barrier value %q",
			arguments.Value,
		)
	}
	select {
	case <-release:
	case <-ctx.Done():
		return sdk.ToolResult{}, ctx.Err()
	}
	if arguments.Value == "second" {
		close(tool.secondDone)
	}
	return sdk.ToolResult{Content: arguments.Value}, nil
}

func TestToolCallsSubmitTogetherAwaitConcurrentlyAndJoinStably(
	t *testing.T,
) {
	ctx := context.Background()
	operations := sdkstorage.NewMemoryOperationStore()
	provider := &concurrentToolProvider{}
	tool := &barrierTool{
		started:       make(chan string, 2),
		firstRelease:  make(chan struct{}),
		secondRelease: make(chan struct{}),
		secondDone:    make(chan struct{}),
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       testStateBackendWithStores(nil, operations),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "concurrent-tools",
			Version:     "1.0.0",
			Description: "tests structured tool concurrency",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("concurrent-model"),
				sdk.ToolResource("barrier"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return registrar.RegisterTool(tool)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "concurrent-tool-session",
		Provider: "concurrent-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := session.Prompt(ctx, "run both")
		resultCh <- result
		errCh <- promptErr
	}()

	started := map[string]bool{}
	for len(started) < 2 {
		select {
		case value := <-tool.started:
			started[value] = true
		case <-time.After(time.Second):
			t.Fatalf("only these tool calls started: %#v", started)
		}
	}
	close(tool.secondRelease)
	select {
	case <-tool.secondDone:
	case <-time.After(time.Second):
		t.Fatal("second tool did not complete while first tool was blocked")
	}
	select {
	case err := <-errCh:
		t.Fatalf("prompt completed before the first sibling: %v", err)
	default:
	}
	close(tool.firstRelease)

	var result Result
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not complete")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if result.Output != "done" || result.ToolCalls != 2 {
		t.Fatalf("result = %#v", result)
	}
	requests := provider.Requests()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %d, want 2", len(requests))
	}
	messages := requests[1].Messages
	if len(messages) < 4 {
		t.Fatalf("second request messages = %#v", messages)
	}
	joined := messages[len(messages)-2:]
	if joined[0].ToolCallID != "first-call" ||
		joined[0].Content != "first" ||
		joined[1].ToolCallID != "second-call" ||
		joined[1].Content != "second" {
		t.Fatalf("tool join order = %#v", joined)
	}

	records, err := operations.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var toolRecords []sdk.OperationRecord
	for _, record := range records {
		if record.Kind == sdk.OperationKindTool {
			toolRecords = append(toolRecords, record)
		}
	}
	if len(toolRecords) != 2 {
		t.Fatalf("tool operation records = %#v", toolRecords)
	}
	left, right := toolRecords[0].Invocation, toolRecords[1].Invocation
	if left.GroupID == "" || left.GroupID != right.GroupID {
		t.Fatalf("tool invocation groups = %#v, %#v", left, right)
	}
	if left.RootID == "" || left.RootID != right.RootID ||
		left.SessionID != session.ID() ||
		right.SessionID != session.ID() {
		t.Fatalf("tool invocation lineage = %#v, %#v", left, right)
	}
	if len(left.Dependencies) != 1 ||
		len(right.Dependencies) != 1 ||
		left.Dependencies[0] != right.Dependencies[0] {
		t.Fatalf("tool invocation dependencies = %#v, %#v", left, right)
	}
}

func TestToolCallGroupCancellationReachesEverySibling(t *testing.T) {
	operations := sdkstorage.NewMemoryOperationStore()
	tool := &cancellingTool{
		started:   make(chan string, 2),
		cancelled: make(chan string, 2),
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       testStateBackendWithStores(nil, operations),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "cancel-tools",
			Version:     "1.0.0",
			Description: "tests structured cancellation",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("cancel-model"),
				sdk.ToolResource("cancel-group"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(
				cancellingToolProvider{},
			); err != nil {
				return err
			}
			return registrar.RegisterTool(tool)
		},
	}
	ctx := context.Background()
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "cancel-tool-session",
		Provider: "cancel-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	promptCtx, cancelPrompt := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(promptCtx, "cancel both")
		errCh <- promptErr
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-tool.started:
		case <-time.After(time.Second):
			t.Fatal("tool sibling did not start")
		}
	}
	cancelPrompt()
	cancelled := map[string]bool{}
	for len(cancelled) < 2 {
		select {
		case value := <-tool.cancelled:
			cancelled[value] = true
		case <-time.After(time.Second):
			t.Fatalf(
				"only these tool siblings were cancelled: %#v",
				cancelled,
			)
		}
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("cancelled prompt returned no error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled prompt did not return")
	}
	records, err := operations.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancelledOperations := 0
	for _, record := range records {
		if record.Kind == sdk.OperationKindTool &&
			record.Operation.State == sdk.OperationCancelled {
			cancelledOperations++
		}
	}
	if cancelledOperations != 2 {
		t.Fatalf(
			"cancelled tool operations = %d, records=%#v",
			cancelledOperations,
			records,
		)
	}
}

func TestToolCallSubmitPanicBecomesToolFailure(t *testing.T) {
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: newTestStateBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "panic-submit-tools",
			Version:     "1.0.0",
			Description: "tests structured fan-out panic capture",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("panic-submit-model"),
				sdk.ToolResource("panic-submit"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(
				panicSubmitProvider{},
			); err != nil {
				return err
			}
			return registrar.RegisterTool(panicSubmitTool{})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "panic-submit-session",
		Provider: "panic-submit-model",
		MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Prompt(ctx, "recover panic")
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "panic captured" || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}
}
