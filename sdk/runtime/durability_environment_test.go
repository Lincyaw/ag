package runtime

// Durability tests cover resume-environment compatibility.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type executionEnvironmentProvider struct{}

func (executionEnvironmentProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "environment", Model: "test"}
}

func (executionEnvironmentProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{
		Content: "recovered",
		Model:   "test",
	}, nil
}

type executionBaseMessageProvider struct {
	mu       sync.Mutex
	requests []sdk.ModelRequest
}

func (provider *executionBaseMessageProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "base-message", Model: "test"}
}

func (provider *executionBaseMessageProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	provider.requests = append(provider.requests, sdk.ModelRequest{
		Messages: sdk.CloneMessages(request.Messages),
		Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
	})
	provider.mu.Unlock()
	return sdk.ModelResponse{
		Content:      "recovered",
		Model:        "test",
		FinishReason: "stop",
	}, nil
}

func (provider *executionBaseMessageProvider) Requests() []sdk.ModelRequest {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	result := make([]sdk.ModelRequest, len(provider.requests))
	for index, request := range provider.requests {
		result[index] = sdk.ModelRequest{
			Messages: sdk.CloneMessages(request.Messages),
			Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
		}
	}
	return result
}

func TestRecoverExecutionUsesInputBaseMessagesAfterCheckpoint(
	t *testing.T,
) {
	ctx := t.Context()
	backend := sdkstorage.NewMemoryStateBackend()
	provider := &executionBaseMessageProvider{}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "base-message-recovery",
			Version:     "1.0.0",
			Description: "records recovered execution base messages",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("base-message"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(provider)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	config := SessionConfig{
		ID:       "base-message-recovery",
		Provider: "base-message",
		System:   "recover base messages",
		MaxTurns: 1,
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	environment, err := newTrajectoryEnvironment(runtime, lease.snapshot, config)
	lease.release()
	if err != nil {
		t.Fatal(err)
	}
	store := backend.Trajectories()
	if err := store.Create(ctx, sdk.Trajectory{
		ID:          config.ID,
		Environment: environment,
	}); err != nil {
		t.Fatal(err)
	}
	checkpointPayload, err := json.Marshal(durability.Checkpoint{
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "checkpoint base",
		}},
		System:   config.System,
		Provider: config.Provider,
		Turns:    1,
		Action:   sdk.Action{Kind: sdk.ActionStep},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := sdk.TrajectoryEntry{
		ID:        "base-checkpoint",
		Kind:      sdk.TrajectoryKindCheckpoint,
		Timestamp: time.Now().UTC(),
		Payload:   checkpointPayload,
	}
	if _, err := store.Append(ctx, config.ID, "", checkpoint); err != nil {
		t.Fatal(err)
	}
	toolCall := durability.ToolCall{
		Turn: 1,
		Call: sdk.ToolCall{
			ID:        "post-checkpoint-tool",
			Name:      "noop",
			Arguments: []byte(`{}`),
		},
		OperationKey: "post-checkpoint-tool",
	}
	toolPayload, err := json.Marshal(toolCall)
	if err != nil {
		t.Fatal(err)
	}
	postCheckpointHead := sdk.TrajectoryEntry{
		ID:        "post-checkpoint-head",
		ParentID:  checkpoint.ID,
		Kind:      sdk.TrajectoryKindToolCall,
		Timestamp: time.Now().UTC(),
		Fields:    durability.EntryFields(toolCall),
		Payload:   toolPayload,
	}
	if _, err := store.Append(
		ctx,
		config.ID,
		checkpoint.ID,
		postCheckpointHead,
	); err != nil {
		t.Fatal(err)
	}
	baseMessages := []sdk.Message{
		{Role: sdk.RoleUser, Content: "checkpoint base"},
		{Role: sdk.RoleAssistant, Content: "post checkpoint base"},
	}
	userMessage := sdk.Message{
		Role:    sdk.RoleUser,
		Content: "recover pending input",
	}
	inputPayload, err := json.Marshal(durability.NewExecutionInput(
		userMessage,
		environment,
		baseMessages,
	))
	if err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "pending-input",
		ParentID:  postCheckpointHead.ID,
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   inputPayload,
	}
	if _, err := store.BeginExecution(
		ctx,
		config.ID,
		postCheckpointHead.ID,
		sdk.TrajectoryExecutionStart{
			ID:       "pending-execution",
			Provider: config.Provider,
			System:   config.System,
			MaxTurns: config.MaxTurns,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := runtime.RecoverExecution(ctx, config.ID); err != nil {
		t.Fatal(err)
	}
	requests := provider.Requests()
	if len(requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(requests))
	}
	messages := requests[0].Messages
	if len(messages) != 4 ||
		messages[0].Role != sdk.RoleSystem ||
		messages[1].Content != "checkpoint base" ||
		messages[2].Content != "post checkpoint base" ||
		messages[3].Content != "recover pending input" {
		t.Fatalf("recovered provider messages = %#v", messages)
	}
}

func TestRecoverExecutionProjectsLegacyBaseMessagesFromBaseBranch(
	t *testing.T,
) {
	ctx := t.Context()
	backend := sdkstorage.NewMemoryStateBackend()
	provider := &executionBaseMessageProvider{}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "legacy-base-recovery",
			Version:     "1.0.0",
			Description: "records recovered legacy execution base messages",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("base-message"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(provider)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	config := SessionConfig{
		ID:       "legacy-base-recovery",
		Provider: "base-message",
		System:   "recover legacy base messages",
		MaxTurns: 1,
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	environment, err := newTrajectoryEnvironment(runtime, lease.snapshot, config)
	lease.release()
	if err != nil {
		t.Fatal(err)
	}
	store := backend.Trajectories()
	if err := store.Create(ctx, sdk.Trajectory{
		ID:          config.ID,
		Environment: environment,
	}); err != nil {
		t.Fatal(err)
	}
	checkpointPayload, err := json.Marshal(durability.Checkpoint{
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "checkpoint base",
		}},
		System:   config.System,
		Provider: config.Provider,
		Turns:    1,
		Action:   sdk.Action{Kind: sdk.ActionStep},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := sdk.TrajectoryEntry{
		ID:        "legacy-base-checkpoint",
		Kind:      sdk.TrajectoryKindCheckpoint,
		Timestamp: time.Now().UTC(),
		Payload:   checkpointPayload,
	}
	head, err := store.Append(ctx, config.ID, "", checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	response := sdk.AfterProviderPayload{
		Turn:     1,
		Provider: config.Provider,
		Response: &sdk.ModelResponse{
			Content:      "post checkpoint assistant",
			Model:        "test",
			FinishReason: "tool_calls",
		},
	}
	responsePayload, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	postCheckpointHead := sdk.TrajectoryEntry{
		ID:        "legacy-post-checkpoint-response",
		ParentID:  head,
		Kind:      sdk.TrajectoryKindProviderResponse,
		Timestamp: time.Now().UTC(),
		Fields:    durability.EntryFields(response),
		Payload:   responsePayload,
	}
	head, err = store.Append(ctx, config.ID, head, postCheckpointHead)
	if err != nil {
		t.Fatal(err)
	}
	userMessage := sdk.Message{
		Role:    sdk.RoleUser,
		Content: "recover legacy pending input",
	}
	inputPayload, err := json.Marshal(userMessage)
	if err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "legacy-pending-input",
		ParentID:  head,
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   inputPayload,
	}
	if _, err := store.BeginExecution(
		ctx,
		config.ID,
		head,
		sdk.TrajectoryExecutionStart{
			ID:       "legacy-pending-execution",
			Provider: config.Provider,
			System:   config.System,
			MaxTurns: config.MaxTurns,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := runtime.RecoverExecution(ctx, config.ID); err != nil {
		t.Fatal(err)
	}
	requests := provider.Requests()
	if len(requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(requests))
	}
	messages := requests[0].Messages
	if len(messages) != 4 ||
		messages[0].Role != sdk.RoleSystem ||
		messages[1].Content != "checkpoint base" ||
		messages[2].Content != "post checkpoint assistant" ||
		messages[3].Content != "recover legacy pending input" {
		t.Fatalf("recovered provider messages = %#v", messages)
	}
}

func TestRecoverExecutionUsesItsOwnCompositionSnapshot(t *testing.T) {
	backend := sdkstorage.NewMemoryStateBackend()
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	initial := newExecutionEnvironmentRuntime(t, backend, false)
	session, err := initial.NewSession(t.Context(), SessionConfig{
		ID: "execution-environment", Provider: "environment", MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Prompt(t.Context(), "first composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, initial)

	changed := newExecutionEnvironmentRuntime(t, backend, true)
	session, err = changed.ResumeSession(
		t.Context(),
		"execution-environment",
		SessionConfig{
			Provider: "environment", MaxTurns: 2,
			ResumePolicy: ResumeCurrent,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(
		t.Context(),
		"recover under changed composition",
	)
	if err != nil {
		t.Fatal(err)
	}
	executionID := submission.Execution().ID
	closeExecutionEnvironmentRuntime(t, changed)

	recovery := newExecutionEnvironmentRuntime(t, backend, true)
	mountEnvironmentMarker(t, recovery, "recovery-extra-marker")
	result, err := recovery.RecoverExecution(
		t.Context(),
		"execution-environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "recovered" {
		t.Fatalf("recovery result = %#v", result)
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		t.Context(),
		"execution-environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != executionID ||
		metadata.Execution.State != sdk.TrajectoryExecutionSucceeded {
		t.Fatalf("recovered execution = %#v", metadata.Execution)
	}
	closeExecutionEnvironmentRuntime(t, recovery)
}

func TestResumeExactRebuildsRecordedCompositionWithExtraMountedPlugins(t *testing.T) {
	backend := sdkstorage.NewMemoryStateBackend()
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	initial := newExecutionEnvironmentRuntime(t, backend, false)
	session, err := initial.NewSession(t.Context(), SessionConfig{
		ID: "exact-rebuild", Provider: "environment", MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Prompt(t.Context(), "initial composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, initial)

	changed := newExecutionEnvironmentRuntime(t, backend, true)
	resumed, err := changed.ResumeSession(
		t.Context(),
		"exact-rebuild",
		SessionConfig{
			Provider:     "environment",
			MaxTurns:     2,
			ResumePolicy: ResumeExact,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Prompt(t.Context(), "still exact"); err != nil {
		t.Fatal(err)
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		t.Context(),
		"exact-rebuild",
	)
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := backend.Trajectories().FindLatest(
		t.Context(),
		"exact-rebuild",
		metadata.Head,
		sdk.TrajectoryKindUserMessage,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("latest user message was not found")
	}
	executionInput, err := durability.DecodeExecutionInput(
		"exact-rebuild",
		entry,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !executionInput.HasEnvironment {
		t.Fatal("latest user message did not record an execution environment")
	}
	environment := executionInput.Environment
	for _, plugin := range environment.Plugins {
		if plugin.Name == "composition-marker" {
			t.Fatalf(
				"exact resume execution environment included extra plugin: %#v",
				environment.Plugins,
			)
		}
	}
	closeExecutionEnvironmentRuntime(t, changed)
}

func TestResumeExactUsesLatestCheckpointExecutionEnvironment(t *testing.T) {
	backend := sdkstorage.NewMemoryStateBackend()
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	initial := newExecutionEnvironmentRuntime(t, backend, false)
	session, err := initial.NewSession(t.Context(), SessionConfig{
		ID: "exact-latest-checkpoint", Provider: "environment", MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Prompt(t.Context(), "initial composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, initial)

	changed := newExecutionEnvironmentRuntime(t, backend, false)
	mountExecutionEnvironmentMarker(t, changed, "composition-marker")
	current, err := changed.ResumeSession(
		t.Context(),
		"exact-latest-checkpoint",
		SessionConfig{
			Provider:     "environment",
			MaxTurns:     2,
			ResumePolicy: ResumeCurrent,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.Prompt(t.Context(), "current composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, changed)

	missing := newExecutionEnvironmentRuntime(t, backend, false)
	_, err = missing.ResumeSession(
		t.Context(),
		"exact-latest-checkpoint",
		SessionConfig{
			Provider:     "environment",
			MaxTurns:     2,
			ResumePolicy: ResumeExact,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "composition-marker") {
		t.Fatalf(
			"ResumeExact() error = %v, want latest checkpoint composition",
			err,
		)
	}
	closeExecutionEnvironmentRuntime(t, missing)

	exact := newExecutionEnvironmentRuntime(t, backend, false)
	mountExecutionEnvironmentMarker(t, exact, "composition-marker")
	resumed, err := exact.ResumeSession(
		t.Context(),
		"exact-latest-checkpoint",
		SessionConfig{
			Provider:     "environment",
			MaxTurns:     2,
			ResumePolicy: ResumeExact,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Prompt(t.Context(), "exact composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, exact)
}

func TestResumeSnapshotRejectsResourceSnapshotWithoutOwnerPlugin(t *testing.T) {
	backend := sdkstorage.NewMemoryStateBackend()
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	runtime := newExecutionEnvironmentRuntime(t, backend, false)
	defer closeExecutionEnvironmentRuntime(t, runtime)

	incomplete, err := sdk.FinalizeTrajectoryEnvironment(
		sdk.TrajectoryEnvironment{
			SDKAPIVersion: sdk.APIVersion,
			Providers: []sdk.ProviderSpec{{
				Name:  "environment",
				Model: "test",
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()

	_, err = runtime.resolveResumeSnapshot(
		lease.snapshot,
		sdk.TrajectoryEnvironment{},
		newResumeEnvironment(incomplete),
		SessionConfig{ID: "incomplete-owner", Provider: "environment", MaxTurns: 1},
	)
	if !errors.Is(err, ErrResumeEnvironmentMismatch) {
		t.Fatalf("resolve incomplete owner snapshot error = %v", err)
	}
}

func newExecutionEnvironmentRuntime(
	t *testing.T,
	backend sdk.StateBackend,
	withMarker bool,
) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
		TrajectoryLease:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "environment-provider",
			Version:     "1.0.0",
			Description: "execution environment test provider",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("environment"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(executionEnvironmentProvider{})
		},
	}
	if _, err := runtime.Mount(t.Context(), sdk.Local(provider)); err != nil {
		t.Fatal(err)
	}
	if withMarker {
		mountEnvironmentMarker(t, runtime, "composition-marker")
	}
	return runtime
}

func mountEnvironmentMarker(t *testing.T, runtime *Runtime, name string) {
	t.Helper()
	marker := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        name,
			Version:     "1.0.0",
			Description: "changes the runtime composition digest",
			APIVersion:  sdk.APIVersion,
		},
		InstallFunc: func(context.Context, sdk.Registrar) error {
			return nil
		},
	}
	if _, err := runtime.Mount(
		t.Context(),
		sdk.Local(marker),
	); err != nil {
		t.Fatal(err)
	}
}

func mountExecutionEnvironmentMarker(t *testing.T, runtime *Runtime, name string) {
	t.Helper()
	marker := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        name,
			Version:     "1.0.0",
			Description: "changes the trajectory execution environment",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.HookResource(name)},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return registrar.RegisterHook(sdk.HookFunc{
				HookSpec: sdk.HookSpec{
					Name:  name,
					Event: sdk.EventBeforeProvider,
				},
				HandleFunc: func(context.Context, sdk.Event) (sdk.Effect, error) {
					return sdk.Effect{}, nil
				},
			})
		},
	}
	if _, err := runtime.Mount(
		t.Context(),
		sdk.Local(marker),
	); err != nil {
		t.Fatal(err)
	}
}

func closeExecutionEnvironmentRuntime(t *testing.T, runtime *Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
