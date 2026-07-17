package runtime

// Orchestration tests cover workflow DAG scheduling.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type workflowProvider struct {
	started       chan string
	firstRelease  chan struct{}
	secondRelease chan struct{}
	secondDone    chan struct{}
	doneOnce      sync.Once
}

func TestValidateWorkflowRejectsCycleBeforeSubmission(t *testing.T) {
	t.Parallel()
	request := sdk.WorkflowRequest{
		Name: "cycle",
		Nodes: []sdk.WorkflowNode{
			{
				ID: "left",
				Agent: sdk.AgentRequest{
					Agent:  "worker",
					Prompt: "left",
				},
				DependsOn: []string{"right"},
			},
			{
				ID: "right",
				Agent: sdk.AgentRequest{
					Agent:  "worker",
					Prompt: "right",
				},
				DependsOn: []string{"left"},
			},
		},
	}
	if err := validateWorkflowRequest(&request); err == nil ||
		!strings.Contains(err.Error(), "contains a cycle") {
		t.Fatalf("validation error = %v", err)
	}
}

func (*workflowProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "workflow-model", Model: "test"}
}

func (provider *workflowProvider) Complete(
	ctx context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	if len(request.Messages) > 0 &&
		request.Messages[0].Role == sdk.RoleSystem &&
		request.Messages[0].Content == "workflow worker" {
		prompt := ""
		for _, message := range request.Messages {
			if message.Role == sdk.RoleUser {
				prompt = message.Content
			}
		}
		switch prompt {
		case "first", "second":
			select {
			case provider.started <- prompt:
			case <-ctx.Done():
				return sdk.ModelResponse{}, ctx.Err()
			}
			release := provider.firstRelease
			if prompt == "second" {
				release = provider.secondRelease
			}
			select {
			case <-release:
			case <-ctx.Done():
				return sdk.ModelResponse{}, ctx.Err()
			}
			if prompt == "second" {
				provider.doneOnce.Do(
					func() { close(provider.secondDone) },
				)
			}
			return sdk.ModelResponse{
				Content: prompt + " output",
			}, nil
		default:
			if strings.Contains(prompt, "first: first output") &&
				strings.Contains(prompt, "second: second output") {
				return sdk.ModelResponse{Content: "combined"}, nil
			}
			return sdk.ModelResponse{Content: "bad dependencies"}, nil
		}
	}
	for _, message := range request.Messages {
		if message.Role == sdk.RoleTool &&
			message.ToolCallID == "workflow-call" {
			return sdk.ModelResponse{
				Content: "root " + message.Content,
			}, nil
		}
	}
	return sdk.ModelResponse{
		ToolCalls: []sdk.ToolCall{{
			ID:        "workflow-call",
			Name:      "run-workflow",
			Arguments: json.RawMessage(`{}`),
		}},
	}, nil
}

type workflowTool struct{}

func (workflowTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "run-workflow",
		Description: "runs a fanout and reduce agent workflow",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (workflowTool) Call(
	ctx context.Context,
	_ json.RawMessage,
) (sdk.ToolResult, error) {
	result, err := sdk.ExecuteWorkflow(ctx, sdk.WorkflowRequest{
		Name:           "fanout-reduce",
		IdempotencyKey: "workflow-once",
		Nodes: []sdk.WorkflowNode{
			{
				ID: "first",
				Agent: sdk.AgentRequest{
					Agent:  "worker",
					Prompt: "first",
				},
			},
			{
				ID: "second",
				Agent: sdk.AgentRequest{
					Agent:  "worker",
					Prompt: "second",
				},
			},
			{
				ID: "reduce",
				Agent: sdk.AgentRequest{
					Agent:  "worker",
					Prompt: "reduce",
				},
				DependsOn:                []string{"first", "second"},
				IncludeDependencyOutputs: true,
			},
		},
	})
	if err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{
		Content: result.Nodes[2].Result.Output,
	}, nil
}

func TestWorkflowRunsReadyAgentsConcurrentlyThenHonorsDependencies(
	t *testing.T,
) {
	ctx := context.Background()
	operations := sdkstorage.NewMemoryOperationStore()
	trajectories := sdkstorage.NewMemoryTrajectoryStore()
	provider := &workflowProvider{
		started:       make(chan string, 2),
		firstRelease:  make(chan struct{}),
		secondRelease: make(chan struct{}),
		secondDone:    make(chan struct{}),
	}
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
			Name:        "workflow-agents",
			Version:     "1.0.0",
			Description: "tests structured agent workflows",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("workflow-model"),
				sdk.ToolResource("run-workflow"),
				sdk.AgentResource("worker"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			if err := registrar.RegisterProvider(provider); err != nil {
				return err
			}
			if err := registrar.RegisterTool(workflowTool{}); err != nil {
				return err
			}
			return sdk.RegisterAgent(registrar, sdk.AgentSpec{
				Name:        "worker",
				Description: "workflow worker",
				System:      "workflow worker",
				Tools:       []string{},
			})
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "workflow-root",
		Provider: "workflow-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		result, promptErr := session.Prompt(ctx, "run workflow")
		resultCh <- result
		errCh <- promptErr
	}()

	started := map[string]bool{}
	for len(started) < 2 {
		select {
		case value := <-provider.started:
			started[value] = true
		case <-time.After(time.Second):
			t.Fatalf("only these workflow agents started: %#v", started)
		}
	}
	close(provider.secondRelease)
	select {
	case <-provider.secondDone:
	case <-time.After(time.Second):
		t.Fatal("second workflow agent did not finish independently")
	}
	select {
	case result := <-resultCh:
		t.Fatalf("workflow joined before first agent: %#v", result)
	default:
	}
	close(provider.firstRelease)

	var result Result
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("workflow prompt did not complete")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if result.Output != "root combined" {
		t.Fatalf("workflow output = %q", result.Output)
	}

	records, err := operations.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var workflow sdk.OperationRecord
	var agents []sdk.OperationRecord
	for _, record := range records {
		switch record.Kind {
		case sdk.OperationKindWorkflow:
			workflow = record
		case sdk.OperationKindAgent:
			agents = append(agents, record)
		}
	}
	if workflow.Operation.ID == "" || len(agents) != 3 {
		t.Fatalf("workflow operation graph = %#v", records)
	}
	var roots []sdk.OperationRecord
	var reducer sdk.OperationRecord
	for _, agent := range agents {
		if agent.Invocation.ParentID != workflow.Invocation.ID {
			t.Fatalf(
				"agent parent = %q, want workflow %q",
				agent.Invocation.ParentID,
				workflow.Invocation.ID,
			)
		}
		if len(agent.Invocation.Dependencies) == 0 {
			roots = append(roots, agent)
		} else {
			reducer = agent
		}
	}
	if len(roots) != 2 || reducer.Operation.ID == "" {
		t.Fatalf("workflow dependency records = %#v", agents)
	}
	if roots[0].Invocation.GroupID == "" ||
		roots[0].Invocation.GroupID != roots[1].Invocation.GroupID {
		t.Fatalf("fanout groups = %#v", roots)
	}
	if reducer.Invocation.GroupID == roots[0].Invocation.GroupID ||
		len(reducer.Invocation.Dependencies) != 2 {
		t.Fatalf("reducer invocation = %#v", reducer.Invocation)
	}
	for _, agent := range agents {
		metadata, loadErr := trajectories.LoadMetadata(
			ctx,
			agent.Invocation.TargetSessionID,
		)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if metadata.ParentID != "" ||
			metadata.Environment.ParentSessionID != session.ID() ||
			metadata.Environment.OriginMode !=
				sdk.AgentSessionNew {
			t.Fatalf(
				"new agent trajectory lineage = %#v",
				metadata,
			)
		}
	}
}
