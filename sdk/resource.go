// Package sdk defines the shared language and ports used by runtimes,
// plugins, presenters, and infrastructure adapters.
package sdk

import (
	"context"
	"encoding/json"
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

type ToolSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
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
