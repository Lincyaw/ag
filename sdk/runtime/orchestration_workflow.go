package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

// ExecuteWorkflow schedules one validated invocation DAG.
func (invoker *scopedAgentInvoker) ExecuteWorkflow(
	ctx context.Context,
	request sdk.WorkflowRequest,
) (sdk.WorkflowResult, error) {
	if invoker == nil || invoker.runtime == nil ||
		invoker.snapshot == nil || invoker.parentSession == nil {
		return sdk.WorkflowResult{}, errors.New(
			"workflow invoker is not initialized",
		)
	}
	request = cloneWorkflowRequest(request)
	if err := validateWorkflowRequest(&request); err != nil {
		return sdk.WorkflowResult{}, err
	}
	if request.IdempotencyKey == "" {
		return sdk.WorkflowResult{}, errors.New(
			"workflow idempotency key is required",
		)
	}
	coordinate := request.Name + "/" + request.IdempotencyKey
	groupCoordinate := ""
	if request.Group != "" {
		groupCoordinate = "workflows/" + request.Group
	}
	invocation := invoker.childInvocation(childInvocationConfig{
		kind:            "workflow",
		coordinate:      coordinate,
		groupCoordinate: groupCoordinate,
		dependencies:    request.Dependencies,
		ordinal:         request.Ordinal,
	})
	if err := sdk.ValidateInvocation(invocation); err != nil {
		return sdk.WorkflowResult{}, err
	}
	target := localOperationTarget{
		runtime:  invoker.runtime,
		kind:     sdk.OperationKindWorkflow,
		resource: request.Name,
		snapshot: invoker.snapshot,
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return sdk.WorkflowResult{}, fmt.Errorf(
			"encode workflow %q request: %w",
			request.Name,
			err,
		)
	}
	operationCtx := sdk.WithAgentInvoker(ctx, nil)
	operationCtx = sdk.WithWorkflowInvoker(operationCtx, nil)
	operationRequest := sdk.OperationRequest{
		IdempotencyKey: invocation.ID,
		Input:          raw,
		Invocation:     invocation,
	}
	initial, err := target.submit(
		operationCtx,
		operationRequest,
		func(
			executionCtx context.Context,
			_ json.RawMessage,
		) (json.RawMessage, error) {
			result, runErr := invoker.runWorkflow(
				executionCtx,
				request,
				invocation,
			)
			if runErr != nil {
				return nil, runErr
			}
			return json.Marshal(result)
		},
	)
	if err != nil {
		return sdk.WorkflowResult{}, fmt.Errorf(
			"submit workflow %q: %w",
			request.Name,
			err,
		)
	}
	result, err := awaitOperationRequestJSON[sdk.WorkflowResult](
		invoker.runtime,
		ctx,
		operationRequest,
		initial,
		target.poll,
		target.cancel,
		fmt.Sprintf("workflow %q", request.Name),
		fmt.Sprintf("workflow %q result", request.Name),
	)
	if err != nil {
		return sdk.WorkflowResult{}, err
	}
	return result, nil
}

func cloneWorkflowRequest(
	request sdk.WorkflowRequest,
) sdk.WorkflowRequest {
	request.Dependencies = append(
		[]string(nil),
		request.Dependencies...,
	)
	nodes := make([]sdk.WorkflowNode, len(request.Nodes))
	for index, node := range request.Nodes {
		nodes[index] = node
		nodes[index].DependsOn = append(
			[]string(nil),
			node.DependsOn...,
		)
		nodes[index].Agent.Dependencies = append(
			[]string(nil),
			node.Agent.Dependencies...,
		)
	}
	request.Nodes = nodes
	return request
}

func validateWorkflowRequest(request *sdk.WorkflowRequest) error {
	if request == nil {
		return errors.New("workflow request is nil")
	}
	if err := sdk.ValidateResourceName(
		"workflow",
		request.Name,
	); err != nil {
		return err
	}
	if request.IdempotencyKey != "" {
		if err := sdk.ValidateResourceName(
			"workflow idempotency key",
			request.IdempotencyKey,
		); err != nil {
			return err
		}
	}
	if len(request.Nodes) == 0 {
		return fmt.Errorf(
			"workflow %q contains no nodes",
			request.Name,
		)
	}
	if request.MaxConcurrency < 0 {
		return fmt.Errorf(
			"workflow %q max concurrency cannot be negative",
			request.Name,
		)
	}
	nodes := make(map[string]struct{}, len(request.Nodes))
	for index := range request.Nodes {
		node := &request.Nodes[index]
		if err := sdk.ValidateResourceName(
			"workflow node",
			node.ID,
		); err != nil {
			return err
		}
		if _, duplicate := nodes[node.ID]; duplicate {
			return fmt.Errorf(
				"workflow %q contains duplicate node %q",
				request.Name,
				node.ID,
			)
		}
		nodes[node.ID] = struct{}{}
		if err := validateAgentRequest(&node.Agent); err != nil {
			return fmt.Errorf(
				"workflow node %q: %w",
				node.ID,
				err,
			)
		}
	}
	for _, node := range request.Nodes {
		seen := make(map[string]struct{}, len(node.DependsOn))
		for _, dependency := range node.DependsOn {
			if dependency == node.ID {
				return fmt.Errorf(
					"workflow node %q cannot depend on itself",
					node.ID,
				)
			}
			if _, exists := nodes[dependency]; !exists {
				return fmt.Errorf(
					"workflow node %q depends on unknown node %q",
					node.ID,
					dependency,
				)
			}
			if _, duplicate := seen[dependency]; duplicate {
				return fmt.Errorf(
					"workflow node %q contains duplicate dependency %q",
					node.ID,
					dependency,
				)
			}
			seen[dependency] = struct{}{}
		}
	}
	indegree := make(map[string]int, len(request.Nodes))
	dependents := make(map[string][]string, len(request.Nodes))
	for _, node := range request.Nodes {
		indegree[node.ID] = len(node.DependsOn)
		for _, dependency := range node.DependsOn {
			dependents[dependency] = append(
				dependents[dependency],
				node.ID,
			)
		}
	}
	ready := make([]string, 0, len(request.Nodes))
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	visited := 0
	for len(ready) > 0 {
		id := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++
		for _, dependent := range dependents[id] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if visited != len(request.Nodes) {
		return fmt.Errorf(
			"workflow %q dependency graph contains a cycle",
			request.Name,
		)
	}
	return nil
}

func (invoker *scopedAgentInvoker) runWorkflow(
	ctx context.Context,
	request sdk.WorkflowRequest,
	invocation sdk.Invocation,
) (sdk.WorkflowResult, error) {
	index := make(map[string]int, len(request.Nodes))
	for nodeIndex, node := range request.Nodes {
		index[node.ID] = nodeIndex
	}
	results := make([]sdk.WorkflowNodeResult, len(request.Nodes))
	completed := make([]bool, len(request.Nodes))
	remaining := len(request.Nodes)
	wave := 0
	childInvoker := *invoker
	childInvoker.parentInvocation = invocation
	for remaining > 0 {
		ready := make([]int, 0, remaining)
		for nodeIndex, node := range request.Nodes {
			if completed[nodeIndex] {
				continue
			}
			dependenciesReady := true
			for _, dependency := range node.DependsOn {
				if !completed[index[dependency]] {
					dependenciesReady = false
					break
				}
			}
			if dependenciesReady {
				ready = append(ready, nodeIndex)
			}
		}
		if len(ready) == 0 {
			return sdk.WorkflowResult{}, fmt.Errorf(
				"workflow %q dependency graph contains a cycle",
				request.Name,
			)
		}
		outcomes := make([]struct {
			result sdk.AgentResult
			err    error
		}, len(ready))
		errs := runParallelIndexed(
			ctx,
			len(ready),
			parallelIndexedOptions{
				Limit:         request.MaxConcurrency,
				CancelOnError: true,
			},
			func(ctx context.Context, readyIndex int) error {
				nodeIndex := ready[readyIndex]
				node := request.Nodes[nodeIndex]
				agentRequest := node.Agent
				if agentRequest.IdempotencyKey == "" {
					agentRequest.IdempotencyKey = node.ID
				}
				agentRequest.Group = fmt.Sprintf(
					"%s-wave-%d",
					invocation.ID,
					wave,
				)
				agentRequest.Ordinal = uint32(nodeIndex)
				agentRequest.Dependencies = make(
					[]string,
					0,
					len(node.DependsOn),
				)
				for _, dependency := range node.DependsOn {
					dependencyResult :=
						results[index[dependency]].Result
					agentRequest.Dependencies = append(
						agentRequest.Dependencies,
						dependencyResult.InvocationID,
					)
				}
				if node.IncludeDependencyOutputs {
					agentRequest.Prompt =
						appendWorkflowDependencyOutputs(
							agentRequest.Prompt,
							node.DependsOn,
							index,
							results,
						)
				}
				outcomes[readyIndex].result,
					outcomes[readyIndex].err =
					childInvoker.InvokeAgent(
						ctx,
						agentRequest,
					)
				return outcomes[readyIndex].err
			},
		)
		for readyIndex, err := range errs {
			if err != nil {
				outcomes[readyIndex].err = err
			}
		}
		var waveErrors []error
		for readyIndex, nodeIndex := range ready {
			if outcomes[readyIndex].err != nil {
				waveErrors = append(
					waveErrors,
					fmt.Errorf(
						"workflow node %q: %w",
						request.Nodes[nodeIndex].ID,
						outcomes[readyIndex].err,
					),
				)
				continue
			}
			results[nodeIndex] = sdk.WorkflowNodeResult{
				ID:     request.Nodes[nodeIndex].ID,
				Result: outcomes[readyIndex].result,
			}
			completed[nodeIndex] = true
			remaining--
		}
		if len(waveErrors) != 0 {
			return sdk.WorkflowResult{}, errors.Join(
				waveErrors...,
			)
		}
		wave++
	}
	return sdk.WorkflowResult{
		InvocationID: invocation.ID,
		Nodes:        results,
	}, nil
}

func appendWorkflowDependencyOutputs(
	prompt string,
	dependencies []string,
	index map[string]int,
	results []sdk.WorkflowNodeResult,
) string {
	if len(dependencies) == 0 {
		return prompt
	}
	var builder strings.Builder
	builder.WriteString(prompt)
	builder.WriteString("\n\nDependency outputs:\n")
	for _, dependency := range dependencies {
		result := results[index[dependency]].Result
		builder.WriteString("- ")
		builder.WriteString(dependency)
		builder.WriteString(": ")
		builder.WriteString(result.Output)
		builder.WriteByte('\n')
	}
	return builder.String()
}
