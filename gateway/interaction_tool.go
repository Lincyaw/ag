package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

const GatewayAskUserTool = "ask_user"

func NewInteractionPlugin(manager *InteractionManager) sdk.Plugin {
	return interactionPlugin{manager: manager}
}

type interactionPlugin struct {
	manager *InteractionManager
}

func (interactionPlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "gateway_interaction",
		Version:     "1.0.0",
		Description: "gateway-managed user questions that durably suspend and resume tool execution",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.ToolResource(GatewayAskUserTool)},
	}
}

func (plugin interactionPlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	if plugin.manager == nil {
		return errors.New("gateway interaction manager is nil")
	}
	return registrar.RegisterTool(askUserTool{manager: plugin.manager})
}

type askUserTool struct {
	manager *InteractionManager
}

func (askUserTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        GatewayAskUserTool,
		Description: "Ask the user a blocking question when their decision or missing information is required. Execution resumes after the user answers.",
		Concurrency: sdk.ToolConcurrencyExclusive,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"question": map[string]any{"type": "string"},
				"options": map[string]any{
					"type": "array",
					"items": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"id":          map[string]any{"type": "string"},
							"label":       map[string]any{"type": "string"},
							"description": map[string]any{"type": "string"},
						},
						"required": []string{"id", "label"},
					},
				},
			},
			"required": []string{"question"},
		},
	}
}

func (tool askUserTool) Call(
	ctx context.Context,
	raw json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments struct {
		Question string              `json:"question"`
		Options  []InteractionOption `json:"options,omitempty"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&arguments); err != nil {
		return sdk.ToolResult{}, fmt.Errorf("decode ask_user arguments: %w", err)
	}
	invocation, ok := sdk.InvocationFromContext(ctx)
	if !ok || invocation.SessionID == "" || invocation.ExecutionID == "" {
		return sdk.ToolResult{}, errors.New(
			"ask_user requires gateway execution context",
		)
	}
	request := Interaction{
		ID:        interactionID(invocation, arguments.Question, arguments.Options),
		SessionID: invocation.SessionID, ExecutionID: invocation.ExecutionID,
		Kind: InteractionQuestion, Prompt: arguments.Question,
		Options: arguments.Options,
	}
	answer, err := tool.manager.Request(ctx, request)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	content := strings.TrimSpace(answer.Text)
	if answer.OptionID != "" {
		content = answer.OptionID
		for _, option := range arguments.Options {
			if option.ID == answer.OptionID {
				content = option.Label
				break
			}
		}
	}
	return sdk.ToolResult{Content: content}, nil
}

func interactionID(
	invocation sdk.Invocation,
	question string,
	options []InteractionOption,
) string {
	raw, _ := json.Marshal(struct {
		InvocationID string
		ExecutionID  string
		Question     string
		Options      []InteractionOption
	}{invocation.ID, invocation.ExecutionID, question, options})
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("interaction-%x", digest[:12])
}
