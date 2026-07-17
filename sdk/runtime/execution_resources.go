package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// invokeProvider crosses the execution-to-resource operation boundary.
func (session *Session) invokeProvider(
	ctx context.Context,
	name string,
	provider sdk.Provider,
	operationKey string,
	invocation sdk.Invocation,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	if asynchronous, ok := provider.(sdk.AsyncProvider); ok {
		input, err := json.Marshal(request)
		if err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("encode provider %q request: %w", name, err)
		}
		initial, err := asynchronous.SubmitCompletion(ctx, sdk.OperationRequest{
			IdempotencyKey: operationKey,
			Input:          input,
			Invocation:     sdk.CloneInvocation(invocation),
		})
		if err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("submit provider %q completion: %w", name, err)
		}
		if initial.IdempotencyKey != operationKey {
			return sdk.ModelResponse{}, fmt.Errorf(
				"provider %q returned operation with unexpected idempotency key",
				name,
			)
		}
		operation, err := session.runtime.awaitOperation(
			ctx,
			initial,
			asynchronous.PollCompletion,
			asynchronous.CancelCompletion,
		)
		if err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("provider %q completion: %w", name, err)
		}
		var response sdk.ModelResponse
		if err := json.Unmarshal(operation.Output, &response); err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("decode provider %q response: %w", name, err)
		}
		return response, nil
	}
	return sdk.ModelResponse{}, fmt.Errorf("provider %q has no asynchronous execution implementation", name)
}

func validateModelResponse(response sdk.ModelResponse) error {
	seen := make(map[string]struct{}, len(response.ToolCalls))
	for index, call := range response.ToolCalls {
		if call.ID == "" {
			return fmt.Errorf("tool call %d has an empty ID", index)
		}
		if _, duplicate := seen[call.ID]; duplicate {
			return fmt.Errorf("tool call ID %q is duplicated", call.ID)
		}
		if !json.Valid(call.Arguments) {
			return fmt.Errorf("tool call %q arguments are invalid JSON", call.ID)
		}
		seen[call.ID] = struct{}{}
	}
	return nil
}

func selectProviderName(
	snapshot *registrySnapshot,
	requested string,
) (string, error) {
	if requested != "" {
		if _, exists := snapshot.providers[requested]; !exists {
			return "", fmt.Errorf("provider %q is not mounted", requested)
		}
		return requested, nil
	}
	if len(snapshot.providers) == 0 {
		return "", errors.New("no provider is mounted")
	}
	if len(snapshot.providers) > 1 {
		return "", errors.New(
			"multiple providers are mounted; session provider is required",
		)
	}
	for name := range snapshot.providers {
		return name, nil
	}
	panic("unreachable")
}

func snapshotToolSpecs(snapshot *registrySnapshot) []sdk.ToolSpec {
	specs := make([]sdk.ToolSpec, 0, len(snapshot.tools))
	for _, owned := range snapshot.tools {
		specs = append(specs, cloneToolSpec(owned.spec))
	}
	slices.SortFunc(specs, func(left, right sdk.ToolSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	return specs
}

func resolveAdvertisedTools(
	snapshot *registrySnapshot,
	specs []sdk.ToolSpec,
) ([]sdk.ToolSpec, map[string]sdk.Tool, error) {
	result := make([]sdk.ToolSpec, 0, len(specs))
	index := make(map[string]sdk.Tool, len(specs))
	for _, spec := range specs {
		if err := validateToolSpec(spec); err != nil {
			return nil, nil, err
		}
		owned, exists := snapshot.tools[spec.Name]
		if !exists {
			return nil, nil, fmt.Errorf(
				"before_provider advertised unregistered tool %q",
				spec.Name,
			)
		}
		if _, duplicate := index[spec.Name]; duplicate {
			return nil, nil, fmt.Errorf(
				"before_provider advertised tool %q twice",
				spec.Name,
			)
		}
		index[spec.Name] = owned.value
		result = append(result, spec)
	}
	return result, index, nil
}

type preparedToolCall struct {
	call          sdk.ToolCall
	operationKey  string
	invocation    sdk.Invocation
	asynchronous  sdk.AsyncTool
	initial       sdk.Operation
	failureKind   string
	failureReason string
	forkHead      string
}

type toolCallOutcome struct {
	result sdk.ToolResult
	err    error
}

func (session *Session) prepareToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	index int,
	rawCall sdk.ToolCall,
	toolIndex map[string]sdk.Tool,
	providerInvocationID string,
) (preparedToolCall, error) {
	before, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventBeforeTool,
		session.config.ID,
		sdk.BeforeToolPayload{Turn: turn, Call: rawCall},
	)
	if err != nil {
		return preparedToolCall{}, err
	}
	var payload sdk.BeforeToolPayload
	if err := decodePayload(before.Event, &payload); err != nil {
		return preparedToolCall{}, err
	}
	call := payload.Call
	operationKey := session.executionOperationKey(
		"tool",
		fmt.Sprintf("%d/%s", turn, call.ID),
	)
	prepared := preparedToolCall{
		call:         call,
		operationKey: operationKey,
		invocation: session.executionInvocation(
			"tool",
			fmt.Sprintf("%d/%s", turn, call.ID),
			fmt.Sprintf("tools/%d", turn),
			[]string{providerInvocationID},
			uint32(index),
		),
	}
	if err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindToolCall,
		snapshot.generation,
		durability.ToolCall{
			Turn:         turn,
			Call:         call,
			OperationKey: operationKey,
		},
	); err != nil {
		return preparedToolCall{}, err
	}
	prepared.forkHead = session.head
	if before.Block != nil {
		prepared.failureKind = before.Block.Kind
		prepared.failureReason = before.Block.Reason
		return prepared, nil
	}

	tool, exists := toolIndex[call.Name]
	if !exists {
		prepared.failureKind = "unknown_tool"
		prepared.failureReason = fmt.Sprintf(
			"unknown or unavailable tool %q",
			call.Name,
		)
		return prepared, nil
	}
	asynchronous, ok := tool.(sdk.AsyncTool)
	if !ok {
		prepared.failureKind = "execution_failed"
		prepared.failureReason = fmt.Sprintf(
			"tool %q has no asynchronous execution implementation",
			call.Name,
		)
		return prepared, nil
	}
	prepared.asynchronous = asynchronous
	return prepared, nil
}

func (session *Session) submitToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	providerName string,
	call *preparedToolCall,
) {
	if call.failureKind != "" {
		return
	}
	invoker := &scopedAgentInvoker{
		runtime:          session.runtime,
		snapshot:         snapshot,
		parentSession:    session,
		parentInvocation: call.invocation,
		parentProvider:   providerName,
		parentMessages:   cloneMessages(messages),
		forkHead:         call.forkHead,
	}
	ctx = sdk.WithAgentInvoker(ctx, invoker)
	ctx = sdk.WithWorkflowInvoker(ctx, invoker)
	initial, err := call.asynchronous.SubmitCall(ctx, sdk.OperationRequest{
		IdempotencyKey: call.operationKey,
		Input: append(
			json.RawMessage(nil),
			call.call.Arguments...,
		),
		Invocation: sdk.CloneInvocation(call.invocation),
	})
	if err != nil {
		call.failureKind = "execution_failed"
		call.failureReason = fmt.Sprintf(
			"submit tool %q call: %v",
			call.call.Name,
			err,
		)
		return
	}
	if initial.IdempotencyKey != call.operationKey {
		call.failureKind = "execution_failed"
		call.failureReason = fmt.Sprintf(
			"tool %q returned operation with unexpected idempotency key",
			call.call.Name,
		)
		return
	}
	call.initial = initial
}

func (session *Session) submitToolCalls(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	providerName string,
	calls []preparedToolCall,
) {
	var wait sync.WaitGroup
	for index := range calls {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			session.submitToolCall(
				ctx,
				snapshot,
				messages,
				providerName,
				&calls[index],
			)
		}(index)
	}
	wait.Wait()
}

func (session *Session) awaitToolCalls(
	ctx context.Context,
	calls []preparedToolCall,
) []toolCallOutcome {
	outcomes := make([]toolCallOutcome, len(calls))
	var wait sync.WaitGroup
	for index := range calls {
		if calls[index].failureKind != "" {
			outcomes[index].result = sdk.ToolResult{
				Content: calls[index].failureReason,
				IsError: true,
			}
			continue
		}
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			call := calls[index]
			operation, err := session.runtime.awaitOperation(
				ctx,
				call.initial,
				call.asynchronous.PollCall,
				call.asynchronous.CancelCall,
			)
			if err != nil {
				outcomes[index].err = fmt.Errorf(
					"tool %q call: %w",
					call.call.Name,
					err,
				)
				return
			}
			if err := json.Unmarshal(
				operation.Output,
				&outcomes[index].result,
			); err != nil {
				outcomes[index].err = fmt.Errorf(
					"decode tool %q result: %w",
					call.call.Name,
					err,
				)
			}
		}(index)
	}
	wait.Wait()
	return outcomes
}

func (session *Session) finalizeToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call preparedToolCall,
	outcome toolCallOutcome,
) (sdk.ToolCall, sdk.ToolResult, error) {
	if outcome.err != nil {
		outcome.result = sdk.ToolResult{
			Content: outcome.err.Error(),
			IsError: true,
		}
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call.call,
			"execution_failed",
			outcome.err.Error(),
			outcome.result,
		)
	}
	if call.failureKind != "" {
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call.call,
			call.failureKind,
			call.failureReason,
			outcome.result,
		)
	}
	return session.afterTool(
		ctx,
		snapshot,
		turn,
		call.call,
		outcome.result,
	)
}

func (session *Session) afterToolError(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call sdk.ToolCall,
	kind string,
	reason string,
	result sdk.ToolResult,
) (sdk.ToolCall, sdk.ToolResult, error) {
	dispatched, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventToolError,
		session.config.ID,
		sdk.ToolErrorPayload{
			Turn:   turn,
			Call:   call,
			Kind:   kind,
			Reason: reason,
			Result: result,
		},
	)
	if err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	var payload sdk.ToolErrorPayload
	if err := decodePayload(dispatched.Event, &payload); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	return session.afterTool(ctx, snapshot, turn, call, payload.Result)
}

func (session *Session) afterTool(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call sdk.ToolCall,
	result sdk.ToolResult,
) (sdk.ToolCall, sdk.ToolResult, error) {
	dispatched, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventAfterTool,
		session.config.ID,
		sdk.AfterToolPayload{Turn: turn, Call: call, Result: result},
	)
	if err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	var payload sdk.AfterToolPayload
	if err := decodePayload(dispatched.Event, &payload); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	if err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindToolResult,
		snapshot.generation,
		payload,
	); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	return call, payload.Result, nil
}
