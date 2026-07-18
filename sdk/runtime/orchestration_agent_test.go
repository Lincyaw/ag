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
	mu       sync.Mutex
	requests []sdk.ModelRequest
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
