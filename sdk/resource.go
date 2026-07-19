// Package sdk defines the shared language and ports used by runtimes,
// plugins, presenters, and infrastructure adapters.
package sdk

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	// IsError is meaningful for RoleTool messages and preserves the tool_result
	// error bit across runtime checkpoints, providers, and replay.
	IsError bool `json:"is_error,omitempty"`
}

func ToolMessage(toolCallID string, result ToolResult) Message {
	return Message{
		Role:       RoleTool,
		Content:    result.Content,
		ToolCallID: toolCallID,
		IsError:    result.IsError,
	}
}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type ModelRequest struct {
	Messages []Message  `json:"messages"`
	Tools    []ToolSpec `json:"tools"`
}

type ModelResponse struct {
	Content      string     `json:"content,omitempty"`
	ToolCalls    []ToolCall `json:"tool_calls,omitempty"`
	Model        string     `json:"model,omitempty"`
	FinishReason string     `json:"finish_reason,omitempty"`
	Usage        Usage      `json:"usage"`
}

type ProviderSpec struct {
	Name  string `json:"name"`
	Model string `json:"model"`
}

type Provider interface {
	Spec() ProviderSpec
}

type SyncProvider interface {
	Provider
	Complete(context.Context, ModelRequest) (ModelResponse, error)
}

type AsyncProvider interface {
	Provider
	SubmitCompletion(context.Context, OperationRequest) (Operation, error)
	PollCompletion(context.Context, string, uint64) (Operation, error)
	CancelCompletion(context.Context, string) (Operation, error)
}

type ToolInterruptBehavior string

const (
	ToolInterruptBlock  ToolInterruptBehavior = "block"
	ToolInterruptCancel ToolInterruptBehavior = "cancel"
)

type ToolSpec struct {
	Name              string                `json:"name"`
	Description       string                `json:"description"`
	Parameters        map[string]any        `json:"parameters"`
	InterruptBehavior ToolInterruptBehavior `json:"interrupt_behavior,omitempty"`
}

func CloneToolSpec(spec ToolSpec) ToolSpec {
	spec.Parameters = cloneJSONMap(spec.Parameters)
	return spec
}

func (spec ToolSpec) EffectiveInterruptBehavior() ToolInterruptBehavior {
	if spec.InterruptBehavior == "" {
		return ToolInterruptBlock
	}
	return spec.InterruptBehavior
}

func (behavior ToolInterruptBehavior) Valid() bool {
	switch behavior {
	case "", ToolInterruptBlock, ToolInterruptCancel:
		return true
	default:
		return false
	}
}

type ToolResult struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

type Tool interface {
	Spec() ToolSpec
}

type SyncTool interface {
	Tool
	Call(context.Context, json.RawMessage) (ToolResult, error)
}

type AsyncTool interface {
	Tool
	SubmitCall(context.Context, OperationRequest) (Operation, error)
	PollCall(context.Context, string, uint64) (Operation, error)
	CancelCall(context.Context, string) (Operation, error)
}

type CapabilitySpec struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema"`
}

func CloneCapabilitySpec(spec CapabilitySpec) CapabilitySpec {
	spec.InputSchema = cloneJSONMap(spec.InputSchema)
	spec.OutputSchema = cloneJSONMap(spec.OutputSchema)
	return spec
}

type Capability interface {
	Spec() CapabilitySpec
}

type SyncCapability interface {
	Capability
	Invoke(context.Context, json.RawMessage) (json.RawMessage, error)
}

type AsyncCapability interface {
	Capability
	SubmitInvoke(context.Context, OperationRequest) (Operation, error)
	PollInvoke(context.Context, string, uint64) (Operation, error)
	CancelInvoke(context.Context, string) (Operation, error)
}

func cloneJSONMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for name, value := range source {
		result[name] = cloneJSONValue(value)
	}
	return result
}

func cloneJSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneJSONMap(value)
	case map[string]string:
		return maps.Clone(value)
	case []any:
		result := make([]any, len(value))
		for index := range value {
			result[index] = cloneJSONValue(value[index])
		}
		return result
	case []string:
		return slices.Clone(value)
	case json.RawMessage:
		return slices.Clone(value)
	case []byte:
		return slices.Clone(value)
	default:
		return value
	}
}
