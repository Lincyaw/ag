// Package subagent exposes the SDK's structured child-agent runtime as tools.
package subagent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type Agent struct {
	Name        string
	Description string
	Provider    string
	System      string
	MaxTurns    int
	Tools       []string
}

type Config struct {
	Agents []Agent
}

type plugin struct {
	agents []Agent
}

func New(config Config) sdk.Plugin {
	agents := append([]Agent(nil), config.Agents...)
	for index := range agents {
		if agents[index].Tools != nil {
			agents[index].Tools = append(
				make([]string, 0, len(agents[index].Tools)),
				agents[index].Tools...,
			)
		}
	}
	if len(agents) == 0 {
		agents = []Agent{{
			Name:        "general",
			Description: "A general-purpose worker for delegated independent tasks.",
			System:      "You are a focused subagent. Complete the delegated task and return a concise, evidence-based result to the parent agent.",
		}}
	}
	return &plugin{agents: agents}
}

func (plugin *plugin) Manifest() sdk.Manifest {
	registers := []string{sdk.ToolResource("dispatch_agent")}
	for _, agent := range plugin.agents {
		registers = append(registers, sdk.AgentResource(agent.Name))
	}
	return sdk.Manifest{
		Name:        "subagent",
		Version:     "1.0.0",
		Description: "dispatch isolated, durable child-agent sessions",
		APIVersion:  sdk.APIVersion,
		Registers:   registers,
	}
}

func (plugin *plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	seen := make(map[string]struct{}, len(plugin.agents))
	names := make([]string, 0, len(plugin.agents))
	for _, agent := range plugin.agents {
		if _, exists := seen[agent.Name]; exists {
			return fmt.Errorf("subagent %q is configured twice", agent.Name)
		}
		seen[agent.Name] = struct{}{}
		if err := sdk.RegisterAgent(registrar, sdk.AgentSpec{
			Name:        agent.Name,
			Description: agent.Description,
			Provider:    agent.Provider,
			System:      agent.System,
			MaxTurns:    agent.MaxTurns,
			Tools:       cloneTools(agent.Tools),
		}); err != nil {
			return err
		}
		names = append(names, agent.Name)
	}
	sort.Strings(names)
	return registrar.RegisterTool(dispatchTool{agents: names})
}

type dispatchTool struct{ agents []string }

func (tool dispatchTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "dispatch_agent",
		Description: "Delegate an independent task to an isolated child agent and wait for its result. Available agents: " +
			strings.Join(tool.agents, ", ") + ".",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"agent": map[string]any{
					"type": "string", "enum": tool.agents,
				},
				"task": map[string]any{
					"type": "string", "description": "Self-contained task for the child agent.",
				},
				"mode": map[string]any{
					"type": "string", "enum": []string{"new", "fork", "resume"},
					"description": "new starts empty context; fork inherits the parent transcript; resume continues session_id.",
				},
				"session_id": map[string]any{
					"type": "string", "description": "Required for resume; optional stable ID for new/fork.",
				},
			},
			"required":             []string{"agent", "task"},
			"additionalProperties": false,
		},
	}
}

func (tool dispatchTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var args struct {
		Agent     string `json:"agent"`
		Task      string `json:"task"`
		Mode      string `json:"mode"`
		SessionID string `json:"session_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		return dispatchFailure(fmt.Errorf("decode arguments: %w", err)), nil
	}
	if !contains(tool.agents, args.Agent) {
		return dispatchFailure(fmt.Errorf(
			"unknown agent %q; available: %s", args.Agent, strings.Join(tool.agents, ", "),
		)), nil
	}
	if strings.TrimSpace(args.Task) == "" {
		return dispatchFailure(errors.New("task is required")), nil
	}
	mode := sdk.AgentSessionMode(args.Mode)
	if mode == "" {
		mode = sdk.AgentSessionNew
	}
	if mode != sdk.AgentSessionResume && args.SessionID == "" {
		id, err := randomSessionID()
		if err != nil {
			return sdk.ToolResult{}, err
		}
		args.SessionID = id
	}
	result, err := sdk.InvokeAgent(ctx, sdk.AgentRequest{
		Agent: args.Agent, Prompt: args.Task,
		SessionID: args.SessionID, Mode: mode,
	})
	if err != nil {
		return dispatchFailure(err), nil
	}
	payload, err := json.Marshal(struct {
		SessionID    string `json:"session_id"`
		Output       string `json:"output"`
		Turns        int    `json:"turns"`
		ToolCalls    int    `json:"tool_calls"`
		InputTokens  int64  `json:"input_tokens,omitempty"`
		OutputTokens int64  `json:"output_tokens,omitempty"`
	}{
		SessionID: result.SessionID, Output: result.Output,
		Turns: result.Turns, ToolCalls: result.ToolCalls,
		InputTokens: result.InputTokens, OutputTokens: result.OutputTokens,
	})
	if err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{Content: string(payload)}, nil
}

func randomSessionID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate subagent session ID: %w", err)
	}
	return "subagent-" + hex.EncodeToString(value[:]), nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func cloneTools(tools []string) []string {
	if tools == nil {
		return nil
	}
	return append(make([]string, 0, len(tools)), tools...)
}

func dispatchFailure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: "error: " + err.Error(), IsError: true}
}
