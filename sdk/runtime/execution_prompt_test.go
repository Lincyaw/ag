package runtime

// Execution tests cover the synchronous agent turn loop.

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

type invalidResponseProvider struct{}

func (invalidResponseProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "invalid-response", Model: "test"}
}

func (invalidResponseProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{
		ToolCalls: []sdk.ToolCall{
			{ID: "call-1", Name: "first", Arguments: json.RawMessage(`{}`)},
			{ID: "call-1", Name: "second", Arguments: json.RawMessage(`{}`)},
		},
	}, nil
}

type observerProvider struct{}

func (observerProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "observer-provider", Model: "test"}
}

func (observerProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{Content: "observer result"}, nil
}

func TestEventObserverDoesNotAffectExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	var observed atomic.Int64
	var first atomic.Bool
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: newTestStateBackend(),
		EventObserver: func(context.Context, sdk.Event) {
			observed.Add(1)
			if first.CompareAndSwap(false, true) {
				close(entered)
			}
			<-release
			panic("observer failure")
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
			Name:        "observer-provider",
			Version:     "1.0.0",
			Description: "provider for observer tests",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ProviderResource("observer-provider")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterProvider(observerProvider{})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "observer-session",
		Provider: "observer-provider",
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("event observer was not called")
	}
	resultC := make(chan Result, 1)
	errC := make(chan error, 1)
	go func() {
		result, err := session.Prompt(ctx, "run despite observer panic")
		if err != nil {
			errC <- err
			return
		}
		resultC <- result
	}()
	var result Result
	select {
	case err := <-errC:
		t.Fatal(err)
	case result = <-resultC:
	case <-time.After(time.Second):
		t.Fatal("prompt waited for event observer")
	}
	if result.Output != "observer result" || observed.Load() == 0 {
		t.Fatalf("result = %#v observed = %d", result, observed.Load())
	}
}

func TestRuntimeCloseCancelsEventObserverContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	var enterOnce sync.Once
	var cancelOnce sync.Once
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: newTestStateBackend(),
		EventObserver: func(ctx context.Context, _ sdk.Event) {
			enterOnce.Do(func() { close(entered) })
			<-ctx.Done()
			cancelOnce.Do(func() { close(cancelled) })
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Mount(ctx, sdk.Local(sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "observer-close-plugin",
			Version:     "1.0.0",
			Description: "triggers observer close cancellation",
			APIVersion:  sdk.APIVersion,
		},
		InstallFunc: func(context.Context, sdk.Registrar) error {
			return nil
		},
	})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("event observer was not called")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Close(closeCtx); err != nil {
		t.Fatalf("close runtime: %v", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("runtime close returned before cancelling event observer context")
	}
}

func TestEventObserverWaitStoppedIsBounded(t *testing.T) {
	t.Parallel()
	var observer eventObserverRuntime
	release := make(chan struct{})
	observer.wait.Add(1)
	go func() {
		defer observer.wait.Done()
		<-release
	}()
	defer close(release)

	err := observer.waitStopped(context.Background(), 10*time.Millisecond)
	if err == nil || !strings.Contains(
		err.Error(),
		"runtime event observers did not stop",
	) {
		t.Fatalf("waitStopped() error = %v", err)
	}
}

func TestPromptBlockCommitsWithoutCallingProvider(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(trajectories, nil),
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

	hook := sdk.TypedHook[sdk.BeforeAgentStartPayload](
		sdk.HookSpec{
			Name:  "block-prompt",
			Event: sdk.EventBeforeAgentStart,
		},
		func(context.Context, sdk.BeforeAgentStartPayload) (sdk.Effect, error) {
			return sdk.BlockWith("blocked by policy", "policy"), nil
		},
	)
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "prompt-policy",
			Version:     "1.0.0",
			Description: "blocks a prompt before provider selection",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.HookResource("block-prompt")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterHook(hook)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "blocked-prompt"})
	if err != nil {
		t.Fatal(err)
	}

	result, err := session.Prompt(ctx, "do not run")
	if err != nil {
		t.Fatalf("blocked prompt: %v", err)
	}
	if result.Cause.Code != "prompt_blocked" ||
		result.Cause.Detail != "blocked by policy" {
		t.Fatalf("blocked result cause = %#v", result.Cause)
	}
	if len(result.Messages) != 1 || result.Messages[0].Content != "do not run" {
		t.Fatalf("blocked result messages = %#v", result.Messages)
	}
	if messages := session.Messages(); len(messages) != 1 ||
		messages[0].Content != "do not run" {
		t.Fatalf("committed session messages = %#v", messages)
	}

	trajectory, err := trajectories.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	wantKinds := []sdk.TrajectoryKind{
		sdk.TrajectoryKindUserMessage,
		sdk.TrajectoryKindCheckpoint,
		sdk.TrajectoryKindTerminal,
	}
	if len(branch) != len(wantKinds) {
		t.Fatalf("blocked trajectory branch = %#v", branch)
	}
	for index, want := range wantKinds {
		if branch[index].Kind != want {
			t.Fatalf(
				"blocked trajectory entry %d kind = %q, want %q",
				index,
				branch[index].Kind,
				want,
			)
		}
	}
}

func TestDecideInjectCannotOverrideFinalTurnLimit(t *testing.T) {
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
	hook := sdk.TypedHook[sdk.DecidePayload](
		sdk.HookSpec{
			Name:  "inject-on-final-turn",
			Event: sdk.EventDecide,
		},
		func(context.Context, sdk.DecidePayload) (sdk.Effect, error) {
			return sdk.Inject(sdk.Message{
				Role:    sdk.RoleUser,
				Content: "continue past the cap",
			}), nil
		},
	)
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "final-turn-policy",
			Version:     "1.0.0",
			Description: "tests final turn action precedence",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("observer-provider"),
				sdk.HookResource("inject-on-final-turn"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterProvider(observerProvider{}),
				registrar.RegisterHook(hook),
			)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "final-turn-inject",
		Provider: "observer-provider",
		MaxTurns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := session.Prompt(ctx, "stop cleanly")
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if result.Cause.Code != "max_turns" || !result.Cause.Final {
		t.Fatalf("result cause = %#v", result.Cause)
	}
	if len(result.Messages) != 2 ||
		result.Messages[1].Content != "observer result" {
		t.Fatalf("result messages = %#v", result.Messages)
	}
}

func TestPromptRejectsInvalidProviderResponseAndRestoresSession(t *testing.T) {
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
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "invalid-response-plugin",
			Version:     "1.0.0",
			Description: "returns an invalid provider response",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ProviderResource("invalid-response")},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterProvider(invalidResponseProvider{})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "invalid-response-session",
		Provider: "invalid-response",
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := session.Prompt(ctx, "trigger invalid response")
	if err == nil || !strings.Contains(
		err.Error(),
		`tool call ID "call-1" is duplicated`,
	) {
		t.Fatalf("prompt error = %v", err)
	}
	if result.Cause.Code != "provider_error" {
		t.Fatalf("result cause = %#v", result.Cause)
	}
	if messages := session.Messages(); len(messages) != 0 {
		t.Fatalf("session retained failed prompt messages: %#v", messages)
	}
}

func TestValidateModelResponseToolCalls(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		calls    []sdk.ToolCall
		expected string
	}{
		{
			name: "valid",
			calls: []sdk.ToolCall{{
				ID: "call-1", Name: "tool", Arguments: json.RawMessage(`{"value":1}`),
			}},
		},
		{
			name:     "empty ID",
			calls:    []sdk.ToolCall{{Name: "tool", Arguments: json.RawMessage(`{}`)}},
			expected: "tool call 0 has an empty ID",
		},
		{
			name: "duplicate ID",
			calls: []sdk.ToolCall{
				{ID: "call-1", Name: "first", Arguments: json.RawMessage(`{}`)},
				{ID: "call-1", Name: "second", Arguments: json.RawMessage(`{}`)},
			},
			expected: `tool call ID "call-1" is duplicated`,
		},
		{
			name: "invalid arguments",
			calls: []sdk.ToolCall{{
				ID: "call-1", Name: "tool", Arguments: json.RawMessage(`{`),
			}},
			expected: `tool call "call-1" arguments are invalid JSON`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateModelResponse(sdk.ModelResponse{ToolCalls: test.calls})
			if test.expected == "" {
				if err != nil {
					t.Fatalf("validate response: %v", err)
				}
				return
			}
			if err == nil || err.Error() != test.expected {
				t.Fatalf("validation error = %v, want %q", err, test.expected)
			}
		})
	}
}
