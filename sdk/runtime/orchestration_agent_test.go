package runtime

// Orchestration tests cover recursive agent invocation.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestMountRejectsAgentUnavailableResources(t *testing.T) {
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(nil, nil),
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
	tests := []struct {
		name string
		spec sdk.AgentSpec
		want string
	}{
		{
			name: "missing-provider",
			spec: sdk.AgentSpec{
				Name:        "provider-agent",
				Description: "references a missing provider",
				Provider:    "missing-provider",
			},
			want: `references unavailable provider "missing-provider"`,
		},
		{
			name: "missing-tool",
			spec: sdk.AgentSpec{
				Name:        "tool-agent",
				Description: "references a missing tool",
				Tools:       []string{"missing-tool"},
			},
			want: `references unavailable tool "missing-tool"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plugin := sdk.PluginFunc{
				PluginManifest: sdk.Manifest{
					Name:        test.name,
					Version:     "1.0.0",
					Description: "tests agent resource validation",
					APIVersion:  sdk.APIVersion,
					Registers: []string{
						sdk.AgentResource(test.spec.Name),
					},
				},
				InstallFunc: func(
					_ context.Context,
					registrar sdk.Registrar,
				) error {
					return sdk.RegisterAgent(
						registrar,
						test.spec,
					)
				},
			}
			if _, err := runtime.Mount(
				ctx,
				sdk.Local(plugin),
			); err == nil || !strings.Contains(
				err.Error(),
				test.want,
			) {
				t.Fatalf("mount error = %v, want %q", err, test.want)
			}
		})
	}
}

type nestedAgentProvider struct {
	mu            sync.Mutex
	requests      []sdk.ModelRequest
	failOnContent string
}

func (provider *nestedAgentProvider) observedRequests() []sdk.ModelRequest {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return append([]sdk.ModelRequest(nil), provider.requests...)
}

func TestAgentCannotExpandInheritedTurnLimit(t *testing.T) {
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(nil, nil),
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
	provider := &nestedAgentProvider{}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "agent-turn-limit",
			Version:     "1.0.0",
			Description: "tests inherited child limits",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.AgentResource("expansive-agent"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "expansive-agent",
				Description: "asks for more turns than its parent",
				MaxTurns:    2,
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "limited-parent",
		Provider: "nested-model",
		MaxTurns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	invoker := &scopedAgentInvoker{
		runtime:        runtime,
		snapshot:       runtime.current.Load(),
		parentSession:  session,
		parentProvider: "nested-model",
	}
	if _, err := invoker.InvokeAgent(ctx, sdk.AgentRequest{
		Agent:  "expansive-agent",
		Prompt: "expand the budget",
	}); err == nil || !strings.Contains(
		err.Error(),
		"exceeds inherited limit",
	) {
		t.Fatalf("invoke error = %v", err)
	}
}

func TestAgentInvocationRequiresStableIdentity(t *testing.T) {
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(nil, nil),
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
	provider := &nestedAgentProvider{}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "agent-stable-identity",
			Version:     "1.0.0",
			Description: "tests child agent identity validation",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.AgentResource("researcher"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "researcher",
				Description: "requires a stable invocation coordinate",
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "identity-parent",
		Provider: "nested-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	invoker := &scopedAgentInvoker{
		runtime:        runtime,
		snapshot:       runtime.current.Load(),
		parentSession:  session,
		parentProvider: "nested-model",
	}

	if _, err := invoker.InvokeAgent(ctx, sdk.AgentRequest{
		Agent:  "researcher",
		Prompt: "inspect",
	}); err == nil || !strings.Contains(
		err.Error(),
		"agent idempotency key is required",
	) {
		t.Fatalf("invoke error = %v", err)
	}
}

func TestExistingForkedAgentSessionMustMatchTrajectoryAncestry(
	t *testing.T,
) {
	parent := &Session{config: SessionConfig{ID: "parent-session"}}
	invoker := &scopedAgentInvoker{
		parentSession: parent,
		forkAnchor: trajectoryForkAnchor{
			parentEntryID:          "expected-fork-head",
			originForkInvocationID: "tool-invocation",
		},
	}
	metadata := sdk.TrajectoryMetadata{
		ID:            "child-session",
		ParentID:      "parent-session",
		ParentEntryID: "other-fork-head",
		Environment: sdk.TrajectoryEnvironment{
			ParentSessionID:        "parent-session",
			OriginInvocationID:     "agent-invocation",
			OriginForkInvocationID: "tool-invocation",
			OriginMode:             sdk.AgentSessionFork,
		},
	}
	err := validateExistingAgentSessionForTest(
		t,
		invoker,
		metadata,
		sdk.AgentRequest{
			SessionID: "child-session",
			Mode:      sdk.AgentSessionFork,
		},
		sdk.Invocation{
			ID:       "agent-invocation",
			ParentID: "tool-invocation",
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"already forks trajectory",
	) {
		t.Fatalf("fork ancestry error = %v", err)
	}
}

func TestExistingAgentSessionMustMatchInvocationRoot(t *testing.T) {
	parent := &Session{config: SessionConfig{ID: "parent-session"}}
	invoker := &scopedAgentInvoker{parentSession: parent}
	metadata := sdk.TrajectoryMetadata{
		ID: "child-session",
		Environment: sdk.TrajectoryEnvironment{
			ParentSessionID:        "parent-session",
			OriginInvocationID:     "agent-invocation",
			OriginInvocationRootID: "other-root",
			OriginMode:             sdk.AgentSessionNew,
		},
	}
	err := validateExistingAgentSessionForTest(
		t,
		invoker,
		metadata,
		sdk.AgentRequest{
			SessionID: "child-session",
			Mode:      sdk.AgentSessionNew,
		},
		sdk.Invocation{
			ID:     "agent-invocation",
			RootID: "expected-root",
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"already belongs to invocation root",
	) {
		t.Fatalf("invocation root error = %v", err)
	}
}

func TestResumeAgentSessionAllowsFreshInvocationOnExistingTrajectory(
	t *testing.T,
) {
	parent := &Session{config: SessionConfig{ID: "parent-session"}}
	invoker := &scopedAgentInvoker{parentSession: parent}
	metadata := sdk.TrajectoryMetadata{
		ID: "child-session",
		Environment: sdk.TrajectoryEnvironment{
			ParentSessionID:        "parent-session",
			OriginInvocationID:     "original-agent-invocation",
			OriginInvocationRootID: "original-root",
			OriginMode:             sdk.AgentSessionNew,
		},
	}
	err := validateExistingAgentSessionForTest(
		t,
		invoker,
		metadata,
		sdk.AgentRequest{
			SessionID: "child-session",
			Mode:      sdk.AgentSessionResume,
		},
		sdk.Invocation{
			ID:     "resume-agent-invocation",
			RootID: "resume-root",
		},
	)
	if err != nil {
		t.Fatalf("resume validation error = %v", err)
	}
}

func TestResumeAgentRequestDefaultsToPromptScopedIdempotency(
	t *testing.T,
) {
	first := sdk.AgentRequest{
		Agent:     "researcher",
		Prompt:    "first follow-up",
		SessionID: "child-session",
		Mode:      sdk.AgentSessionResume,
	}
	second := sdk.AgentRequest{
		Agent:     "researcher",
		Prompt:    "second follow-up",
		SessionID: "child-session",
		Mode:      sdk.AgentSessionResume,
	}
	for _, request := range []*sdk.AgentRequest{&first, &second} {
		if err := validateAgentRequest(request); err != nil {
			t.Fatal(err)
		}
		if err := ensureAgentIdempotencyKey(request); err != nil {
			t.Fatal(err)
		}
	}
	if first.IdempotencyKey == "" ||
		first.IdempotencyKey == first.SessionID {
		t.Fatalf("first idempotency key = %q", first.IdempotencyKey)
	}
	if first.IdempotencyKey == second.IdempotencyKey {
		t.Fatalf(
			"resume prompts share idempotency key %q",
			first.IdempotencyKey,
		)
	}
}

func validateExistingAgentSessionForTest(
	t *testing.T,
	invoker *scopedAgentInvoker,
	metadata sdk.TrajectoryMetadata,
	request sdk.AgentRequest,
	invocation sdk.Invocation,
) error {
	t.Helper()
	binding, err := invoker.bindAgentSession(
		request,
		sdk.AgentSpec{},
		"",
		invocation,
	)
	if err != nil {
		return err
	}
	return binding.validateExisting(metadata)
}

func (*nestedAgentProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "nested-model", Model: "test"}
}

func (provider *nestedAgentProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	provider.requests = append(provider.requests, request)
	provider.mu.Unlock()
	if provider.failOnContent != "" {
		for _, message := range request.Messages {
			if message.Content == provider.failOnContent {
				return sdk.ModelResponse{}, fmt.Errorf(
					"provider failed on %q",
					provider.failOnContent,
				)
			}
		}
	}
	if len(request.Messages) > 0 &&
		request.Messages[0].Role == sdk.RoleSystem &&
		request.Messages[0].Content == "child system" {
		return sdk.ModelResponse{Content: "child answer"}, nil
	}
	for _, message := range request.Messages {
		if message.Role == sdk.RoleTool &&
			message.ToolCallID == "delegate-call" {
			return sdk.ModelResponse{
				Content: "root received " + message.Content,
			}, nil
		}
	}
	return sdk.ModelResponse{
		ToolCalls: []sdk.ToolCall{{
			ID:        "delegate-call",
			Name:      "delegate",
			Arguments: json.RawMessage(`{"question":"inspect"}`),
		}},
	}, nil
}

type delegateAgentTool struct{}

func (delegateAgentTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "delegate",
		Description: "delegates work to a registered child agent",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (delegateAgentTool) Call(
	ctx context.Context,
	input json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments struct {
		Question string `json:"question"`
	}
	if err := json.Unmarshal(input, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	result, err := sdk.InvokeAgent(ctx, sdk.AgentRequest{
		Agent:          "researcher",
		Prompt:         arguments.Question,
		Mode:           sdk.AgentSessionFork,
		IdempotencyKey: "research-once",
	})
	if err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{Content: result.Output}, nil
}

func TestToolCanInvokeForkedAgentWithRecursiveInvocationGraph(
	t *testing.T,
) {
	ctx := context.Background()
	operations := sdkstorage.NewMemoryOperationStore()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	provider := &nestedAgentProvider{}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(
			trajectories,
			operations,
		),
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
			Name:        "nested-agents",
			Version:     "1.0.0",
			Description: "tests recursive agent invocations",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.ToolResource("delegate"),
				sdk.AgentResource("researcher"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			if err := registrar.RegisterTool(
				delegateAgentTool{},
			); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "researcher",
				Description: "answers delegated research questions",
				System:      "child system",
				Tools:       []string{},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "root-agent-session",
		Provider: "nested-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.Prompt(ctx, "delegate this")
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "root received child answer" {
		t.Fatalf("root output = %q", result.Output)
	}

	records, err := operations.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rootTool sdk.OperationRecord
	var agent sdk.OperationRecord
	var childProvider sdk.OperationRecord
	for _, record := range records {
		switch record.Kind {
		case sdk.OperationKindAgent:
			agent = record
		case sdk.OperationKindTool:
			rootTool = record
		case sdk.OperationKindProvider:
			if record.Invocation.SessionID != session.ID() {
				childProvider = record
			}
		}
	}
	if agent.Operation.ID == "" || rootTool.Operation.ID == "" ||
		childProvider.Operation.ID == "" {
		t.Fatalf("recursive operation records = %#v", records)
	}
	if agent.Invocation.ParentID != rootTool.Invocation.ID {
		t.Fatalf(
			"agent parent = %q, want tool invocation %q",
			agent.Invocation.ParentID,
			rootTool.Invocation.ID,
		)
	}
	if childProvider.Invocation.ParentID != agent.Invocation.ID {
		t.Fatalf(
			"child provider parent = %q, want agent invocation %q",
			childProvider.Invocation.ParentID,
			agent.Invocation.ID,
		)
	}
	if agent.Invocation.RootID != rootTool.Invocation.RootID ||
		childProvider.Invocation.RootID != rootTool.Invocation.RootID {
		t.Fatalf(
			"recursive roots differ: tool=%q agent=%q child=%q",
			rootTool.Invocation.RootID,
			agent.Invocation.RootID,
			childProvider.Invocation.RootID,
		)
	}
	if agent.Invocation.TargetSessionID == "" ||
		childProvider.Invocation.SessionID !=
			agent.Invocation.TargetSessionID {
		t.Fatalf(
			"target session linkage: agent=%#v child=%#v",
			agent.Invocation,
			childProvider.Invocation,
		)
	}
	graph, err := sdk.LoadInvocationGraph(
		ctx,
		operations,
		rootTool.Invocation.RootID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(graph.Operations) != len(records) {
		t.Fatalf(
			"loaded invocation graph has %d operations, want %d",
			len(graph.Operations),
			len(records),
		)
	}
	child, err := trajectories.LoadMetadata(
		ctx,
		agent.Invocation.TargetSessionID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentID != session.ID() ||
		child.ParentEntryID == "" {
		t.Fatalf("fork metadata = %#v", child)
	}
	if child.Environment.ParentSessionID != session.ID() ||
		child.Environment.OriginInvocationID != agent.Invocation.ID ||
		child.Environment.OriginInvocationRootID !=
			agent.Invocation.RootID ||
		child.Environment.OriginForkInvocationID !=
			rootTool.Invocation.ID ||
		child.Environment.OriginMode != sdk.AgentSessionFork {
		t.Fatalf(
			"child structured lineage = %#v",
			child.Environment,
		)
	}
	if len(child.Environment.Tools) != 0 {
		t.Fatalf(
			"child tool allowlist was not attenuated: %#v",
			child.Environment.Tools,
		)
	}
	if len(child.Environment.Agents) != 0 {
		t.Fatalf(
			"child agent resources were recorded without a tool entrypoint: %#v",
			child.Environment.Agents,
		)
	}
	if len(child.Environment.Providers) != 1 ||
		child.Environment.Providers[0].Name != "nested-model" {
		t.Fatalf(
			"child provider inheritance = %#v",
			child.Environment.Providers,
		)
	}
	if got := fmt.Sprint(agent.Invocation); !strings.Contains(
		got,
		agent.Invocation.TargetSessionID,
	) {
		t.Fatalf("agent invocation is not printable: %s", got)
	}
}

func TestExistingForkedAgentSessionResumesTrajectoryCheckpoint(
	t *testing.T,
) {
	ctx := context.Background()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	provider := &nestedAgentProvider{}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(
			trajectories,
			sdkstorage.NewMemoryOperationStore(),
		),
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
			Name:        "existing-fork-resume",
			Version:     "1.0.0",
			Description: "tests existing forked agent resume",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.AgentResource("researcher"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "researcher",
				Description: "answers delegated research questions",
				System:      "child system",
				Tools:       []string{},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	parent, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "parent-existing-fork",
		Provider: "nested-model",
		MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Prompt(ctx, "parent context"); err != nil {
		t.Fatal(err)
	}

	request := sdk.AgentRequest{
		Agent:          "researcher",
		Prompt:         "child prompt",
		SessionID:      "child-existing-fork",
		Mode:           sdk.AgentSessionFork,
		IdempotencyKey: "child-existing-fork",
	}
	invocation := sdk.Invocation{
		ID:          parent.executionOperationKey("agent", "seed"),
		RootID:      "root-invocation",
		ParentID:    "fork-tool",
		SessionID:   parent.ID(),
		ExecutionID: "parent-execution",
	}
	spec := sdk.AgentSpec{
		Name:        "researcher",
		Description: "answers delegated research questions",
		System:      "child system",
		Tools:       []string{},
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	childSnapshot, providerName, err := narrowAgentSnapshot(
		lease.snapshot,
		parent.config.Provider,
		spec,
	)
	if err != nil {
		lease.release()
		t.Fatal(err)
	}
	initialInvoker := &scopedAgentInvoker{
		runtime:          runtime,
		snapshot:         lease.snapshot,
		parentSession:    parent,
		parentInvocation: invocation,
		parentProvider:   parent.config.Provider,
		forkAnchor: trajectoryForkAnchor{
			parentEntryID:          parent.head,
			originForkInvocationID: "fork-tool",
		},
	}
	if _, err := initialInvoker.newAgentSession(
		ctx,
		request,
		spec,
		childSnapshot,
		providerName,
		invocation,
	); err != nil {
		lease.release()
		t.Fatal(err)
	}
	lease.release()

	resumeLease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	resumeSnapshot, providerName, err := narrowAgentSnapshot(
		resumeLease.snapshot,
		parent.config.Provider,
		spec,
	)
	if err != nil {
		resumeLease.release()
		t.Fatal(err)
	}
	resumeInvoker := &scopedAgentInvoker{
		runtime:          runtime,
		snapshot:         resumeLease.snapshot,
		parentSession:    parent,
		parentInvocation: invocation,
		parentProvider:   parent.config.Provider,
		forkAnchor: trajectoryForkAnchor{
			parentEntryID:          parent.head,
			originForkInvocationID: "fork-tool",
		},
	}
	if _, err := resumeInvoker.executeAgentSession(
		ctx,
		request,
		spec,
		resumeSnapshot,
		providerName,
		invocation,
	); err != nil {
		resumeLease.release()
		t.Fatal(err)
	}
	resumeLease.release()

	var childRequest sdk.ModelRequest
	for _, observed := range provider.observedRequests() {
		for _, message := range observed.Messages {
			if message.Content == "child prompt" {
				childRequest = observed
			}
		}
	}
	if childRequest.Messages == nil {
		t.Fatal("child provider request was not observed")
	}
	contents := fmt.Sprint(childRequest.Messages)
	if !strings.Contains(contents, "parent context") {
		t.Fatalf("child request did not resume parent checkpoint: %s", contents)
	}
	if strings.Contains(contents, "stale parent state") {
		t.Fatalf("child request used caller parent messages: %s", contents)
	}
}

func TestNewForkedAgentSessionInitializesFromParentTrajectoryBranch(
	t *testing.T,
) {
	ctx := context.Background()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	provider := &nestedAgentProvider{}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(
			trajectories,
			sdkstorage.NewMemoryOperationStore(),
		),
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
			Name:        "new-fork-branch",
			Version:     "1.0.0",
			Description: "tests fork branch projection",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.AgentResource("researcher"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "researcher",
				Description: "answers delegated research questions",
				System:      "child system",
				Tools:       []string{},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	parent, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "parent-branch-fork",
		Provider: "nested-model",
		MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointRaw, err := json.Marshal(durability.Checkpoint{
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "checkpoint context",
		}},
		Provider: "nested-model",
		System:   "parent system",
		Turns:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := sdk.TrajectoryEntry{
		ID:        "branch-base-checkpoint",
		Kind:      sdk.TrajectoryKindCheckpoint,
		Timestamp: time.Now().UTC(),
		Payload:   checkpointRaw,
	}
	head, err := trajectories.Append(ctx, parent.ID(), "", checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	response := sdk.AfterProviderPayload{
		Turn:     1,
		Provider: "nested-model",
		Response: &sdk.ModelResponse{
			Content: "post checkpoint assistant",
			ToolCalls: []sdk.ToolCall{{
				ID:        "branch-call",
				Name:      "delegate",
				Arguments: json.RawMessage(`{"question":"inspect"}`),
			}},
		},
	}
	responseRaw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	forkEntry := sdk.TrajectoryEntry{
		ID:        "branch-provider-response",
		ParentID:  head,
		Kind:      sdk.TrajectoryKindProviderResponse,
		Timestamp: time.Now().UTC(),
		Fields:    durability.EntryFields(response),
		Payload:   responseRaw,
	}
	head, err = trajectories.Append(ctx, parent.ID(), head, forkEntry)
	if err != nil {
		t.Fatal(err)
	}
	parent.head = head

	spec := sdk.AgentSpec{
		Name:        "researcher",
		Description: "answers delegated research questions",
		System:      "child system",
		Tools:       []string{},
	}
	snapshot := runtime.current.Load()
	childSnapshot, providerName, err := narrowAgentSnapshot(
		snapshot,
		parent.config.Provider,
		spec,
	)
	if err != nil {
		t.Fatal(err)
	}
	invocation := sdk.Invocation{
		ID:          parent.executionOperationKey("agent", "branch-fork"),
		RootID:      "root-invocation",
		ParentID:    "branch-call",
		SessionID:   parent.ID(),
		ExecutionID: "parent-execution",
	}
	invoker := &scopedAgentInvoker{
		runtime:          runtime,
		snapshot:         snapshot,
		parentSession:    parent,
		parentInvocation: invocation,
		parentProvider:   parent.config.Provider,
		forkAnchor: trajectoryForkAnchor{
			parentEntryID:          head,
			originForkInvocationID: "branch-call",
		},
	}
	child, err := invoker.newAgentSession(
		ctx,
		sdk.AgentRequest{
			Agent:          "researcher",
			Prompt:         "child prompt",
			SessionID:      "child-branch-fork",
			Mode:           sdk.AgentSessionFork,
			IdempotencyKey: "child-branch-fork",
		},
		spec,
		childSnapshot,
		providerName,
		invocation,
	)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := runtime.ResumeSession(
		ctx,
		child.ID(),
		SessionConfig{
			System:   child.config.System,
			MaxTurns: child.config.MaxTurns,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.head != head {
		t.Fatalf(
			"resumed fork head = %q, want fork anchor %q",
			resumed.head,
			head,
		)
	}
	if resumed.config.Provider != providerName {
		t.Fatalf(
			"resumed fork provider = %q, want recorded provider %q",
			resumed.config.Provider,
			providerName,
		)
	}
	resumedContents := fmt.Sprint(resumed.Messages())
	if !strings.Contains(resumedContents, "checkpoint context") ||
		!strings.Contains(resumedContents, "post checkpoint assistant") {
		t.Fatalf(
			"resumed fork did not project inherited branch: %s",
			resumedContents,
		)
	}
	if _, err := child.Prompt(ctx, "child prompt"); err != nil {
		t.Fatal(err)
	}

	var childRequest sdk.ModelRequest
	for _, observed := range provider.observedRequests() {
		for _, message := range observed.Messages {
			if message.Content == "child prompt" {
				childRequest = observed
			}
		}
	}
	if childRequest.Messages == nil {
		t.Fatal("child provider request was not observed")
	}
	contents := fmt.Sprint(childRequest.Messages)
	if !strings.Contains(contents, "checkpoint context") ||
		!strings.Contains(contents, "post checkpoint assistant") {
		t.Fatalf("child request did not project parent branch: %s", contents)
	}
}

func TestForkedAgentFailureRestoresToExecutionBaseHead(t *testing.T) {
	ctx := context.Background()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	provider := &nestedAgentProvider{failOnContent: "failing child prompt"}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(
			trajectories,
			sdkstorage.NewMemoryOperationStore(),
		),
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
			Name:        "fork-failure-base-head",
			Version:     "1.0.0",
			Description: "tests fork failure restore anchor",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("nested-model"),
				sdk.AgentResource("researcher"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "researcher",
				Description: "answers delegated research questions",
				System:      "child system",
				Tools:       []string{},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	parent, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "parent-fork-failure",
		Provider: "nested-model",
		MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpointRaw, err := json.Marshal(durability.Checkpoint{
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "checkpoint context",
		}},
		Provider: "nested-model",
		System:   "parent system",
		Turns:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := sdk.TrajectoryEntry{
		ID:        "failure-base-checkpoint",
		Kind:      sdk.TrajectoryKindCheckpoint,
		Timestamp: time.Now().UTC(),
		Payload:   checkpointRaw,
	}
	head, err := trajectories.Append(ctx, parent.ID(), "", checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	response := sdk.AfterProviderPayload{
		Turn:     1,
		Provider: "nested-model",
		Response: &sdk.ModelResponse{
			Content: "post checkpoint assistant",
		},
	}
	responseRaw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	forkAnchor := sdk.TrajectoryEntry{
		ID:        "failure-base-provider-response",
		ParentID:  head,
		Kind:      sdk.TrajectoryKindProviderResponse,
		Timestamp: time.Now().UTC(),
		Fields:    durability.EntryFields(response),
		Payload:   responseRaw,
	}
	head, err = trajectories.Append(ctx, parent.ID(), head, forkAnchor)
	if err != nil {
		t.Fatal(err)
	}
	parent.head = head

	spec := sdk.AgentSpec{
		Name:        "researcher",
		Description: "answers delegated research questions",
		System:      "child system",
		Tools:       []string{},
	}
	snapshot := runtime.current.Load()
	childSnapshot, providerName, err := narrowAgentSnapshot(
		snapshot,
		parent.config.Provider,
		spec,
	)
	if err != nil {
		t.Fatal(err)
	}
	invocation := sdk.Invocation{
		ID:          parent.executionOperationKey("agent", "fork-failure"),
		RootID:      "root-invocation",
		ParentID:    "delegate-call",
		SessionID:   parent.ID(),
		ExecutionID: "parent-execution",
	}
	invoker := &scopedAgentInvoker{
		runtime:          runtime,
		snapshot:         snapshot,
		parentSession:    parent,
		parentInvocation: invocation,
		parentProvider:   parent.config.Provider,
		forkAnchor: trajectoryForkAnchor{
			parentEntryID:          head,
			originForkInvocationID: "delegate-call",
		},
	}
	child, err := invoker.newAgentSession(
		ctx,
		sdk.AgentRequest{
			Agent:          "researcher",
			Prompt:         "failing child prompt",
			SessionID:      "child-fork-failure",
			Mode:           sdk.AgentSessionFork,
			IdempotencyKey: "child-fork-failure",
		},
		spec,
		childSnapshot,
		providerName,
		invocation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.Prompt(ctx, "failing child prompt"); err == nil ||
		!strings.Contains(err.Error(), "provider failed") {
		t.Fatalf("child prompt error = %v", err)
	}
	if child.head == "" {
		t.Fatal("failed child session did not record a restore")
	}
	trajectory, err := trajectories.Load(ctx, child.ID())
	if err != nil {
		t.Fatal(err)
	}
	if trajectory.Execution == nil ||
		trajectory.Execution.State != sdk.TrajectoryExecutionFailed {
		t.Fatalf("child execution = %#v", trajectory.Execution)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	last := branch[len(branch)-1]
	if last.Kind != sdk.TrajectoryKindRestore || last.ParentID != head {
		t.Fatalf("failure restore = %#v, want parent fork anchor %q", last, head)
	}
	contents := fmt.Sprint(child.Messages())
	if !strings.Contains(contents, "checkpoint context") ||
		!strings.Contains(contents, "post checkpoint assistant") ||
		strings.Contains(contents, "failing child prompt") {
		t.Fatalf("failed child messages = %s", contents)
	}
}
