package sdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// AgentSpec declares one same-process agent resource and its execution policy.
type AgentSpec struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Provider    string `json:"provider,omitempty"`
	System      string `json:"system,omitempty"`
	MaxTurns    int    `json:"max_turns,omitempty"`
	// Tools is an allowlist. Nil inherits every tool visible to the caller;
	// a non-nil empty slice exposes no tools.
	Tools []string `json:"tools"`
}

func CloneAgentSpec(spec AgentSpec) AgentSpec {
	if spec.Tools != nil {
		tools := make([]string, len(spec.Tools))
		copy(tools, spec.Tools)
		spec.Tools = tools
	}
	return spec
}

type AgentSessionMode string

const (
	AgentSessionNew    AgentSessionMode = "new"
	AgentSessionFork   AgentSessionMode = "fork"
	AgentSessionResume AgentSessionMode = "resume"
)

type AgentRequest struct {
	Agent          string           `json:"agent"`
	Prompt         string           `json:"prompt"`
	SessionID      string           `json:"session_id,omitempty"`
	Mode           AgentSessionMode `json:"mode,omitempty"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
	Group          string           `json:"group,omitempty"`
	Dependencies   []string         `json:"dependencies,omitempty"`
	Ordinal        uint32           `json:"ordinal,omitempty"`
}

func DefaultAgentResumeIdempotencyKey(sessionID string, prompt string) string {
	sum := sha256.Sum256([]byte(sessionID + "\x00" + prompt))
	return "resume-" + hex.EncodeToString(sum[:])[:24]
}

type AgentResult struct {
	InvocationID string    `json:"invocation_id"`
	SessionID    string    `json:"session_id"`
	Output       string    `json:"output"`
	Messages     []Message `json:"messages"`
	Turns        int       `json:"turns"`
	ToolCalls    int       `json:"tool_calls"`
	Generation   uint64    `json:"generation"`
	Cause        Cause     `json:"cause"`
}

type AgentInvoker interface {
	InvokeAgent(context.Context, AgentRequest) (AgentResult, error)
}

type agentInvokerContextKey struct{}

func WithAgentInvoker(
	ctx context.Context,
	invoker AgentInvoker,
) context.Context {
	return context.WithValue(ctx, agentInvokerContextKey{}, invoker)
}

func InvokeAgent(
	ctx context.Context,
	request AgentRequest,
) (AgentResult, error) {
	invoker, _ := ctx.Value(agentInvokerContextKey{}).(AgentInvoker)
	if invoker == nil {
		return AgentResult{}, errors.New(
			"agent invocation is unavailable outside a structured runtime call",
		)
	}
	return invoker.InvokeAgent(ctx, request)
}
