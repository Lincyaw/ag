package sdk

import (
	"context"
	"errors"
)

type WorkflowNode struct {
	ID                       string       `json:"id"`
	Agent                    AgentRequest `json:"agent"`
	DependsOn                []string     `json:"depends_on,omitempty"`
	IncludeDependencyOutputs bool         `json:"include_dependency_outputs,omitempty"`
}

type WorkflowRequest struct {
	Name           string         `json:"name"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Nodes          []WorkflowNode `json:"nodes"`
	MaxConcurrency int            `json:"max_concurrency,omitempty"`
	Group          string         `json:"group,omitempty"`
	Dependencies   []string       `json:"dependencies,omitempty"`
	Ordinal        uint32         `json:"ordinal,omitempty"`
}

type WorkflowNodeResult struct {
	ID     string      `json:"id"`
	Result AgentResult `json:"result"`
}

type WorkflowResult struct {
	InvocationID string               `json:"invocation_id"`
	Nodes        []WorkflowNodeResult `json:"nodes"`
}

type WorkflowInvoker interface {
	ExecuteWorkflow(
		context.Context,
		WorkflowRequest,
	) (WorkflowResult, error)
}

type workflowInvokerContextKey struct{}

func WithWorkflowInvoker(
	ctx context.Context,
	invoker WorkflowInvoker,
) context.Context {
	return context.WithValue(ctx, workflowInvokerContextKey{}, invoker)
}

func ExecuteWorkflow(
	ctx context.Context,
	request WorkflowRequest,
) (WorkflowResult, error) {
	invoker, _ := ctx.Value(
		workflowInvokerContextKey{},
	).(WorkflowInvoker)
	if invoker == nil {
		return WorkflowResult{}, errors.New(
			"workflow execution is unavailable outside a structured runtime call",
		)
	}
	return invoker.ExecuteWorkflow(ctx, request)
}
