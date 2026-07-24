package subagent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

type recordingInvoker struct {
	request sdk.AgentRequest
}

func (invoker *recordingInvoker) InvokeAgent(
	_ context.Context,
	request sdk.AgentRequest,
) (sdk.AgentResult, error) {
	invoker.request = request
	return sdk.AgentResult{
		SessionID: request.SessionID,
		Output:    "child result",
		Turns:     2,
		ToolCalls: 1,
	}, nil
}

func TestDefaultSubagentRegistersAndDispatches(t *testing.T) {
	registrar := plugincontract.NewAgentRegistrar()
	installed := New(Config{})
	if err := installed.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	agent, exists := registrar.Agents["general"]
	if !exists {
		t.Fatal("general agent was not registered")
	}
	if agent.Tools != nil {
		t.Fatalf("default agent tools = %#v, want inherited nil allowlist", agent.Tools)
	}

	invoker := &recordingInvoker{}
	ctx := sdk.WithAgentInvoker(t.Context(), invoker)
	tool := registrar.Tools["dispatch_agent"].Value.(sdk.SyncTool)
	result, err := tool.Call(ctx, json.RawMessage(
		`{"agent":"general","task":"inspect the implementation","mode":"fork"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || !strings.Contains(result.Content, "child result") {
		t.Fatalf("dispatch_agent = %#v", result)
	}
	if invoker.request.Agent != "general" ||
		invoker.request.Prompt != "inspect the implementation" ||
		invoker.request.Mode != sdk.AgentSessionFork ||
		!strings.HasPrefix(invoker.request.SessionID, "subagent-") {
		t.Fatalf("agent request = %#v", invoker.request)
	}
}

func TestConfiguredSubagentPreservesEmptyToolAllowlist(t *testing.T) {
	registrar := plugincontract.NewAgentRegistrar()
	installed := New(Config{Agents: []Agent{{
		Name: "reviewer", Description: "reviews changes", Tools: []string{},
	}}})
	if err := installed.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	agent := registrar.Agents["reviewer"]
	if agent.Tools == nil || len(agent.Tools) != 0 {
		t.Fatalf("agent tools = %#v, want non-nil empty allowlist", agent.Tools)
	}
}

func TestDispatchRejectsUnknownAgent(t *testing.T) {
	registrar := plugincontract.NewAgentRegistrar()
	if err := New(Config{}).Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["dispatch_agent"].Value.(sdk.SyncTool)
	result, err := tool.Call(t.Context(), json.RawMessage(
		`{"agent":"missing","task":"work"}`,
	))
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError || !strings.Contains(result.Content, "unknown agent") {
		t.Fatalf("dispatch_agent = %#v", result)
	}
}
