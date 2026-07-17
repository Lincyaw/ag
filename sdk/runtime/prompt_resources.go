package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

func (session *Session) complete(
	ctx context.Context,
	name string,
	provider sdk.Provider,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	if asynchronous, ok := provider.(sdk.AsyncProvider); ok {
		input, err := json.Marshal(request)
		if err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("encode provider %q request: %w", name, err)
		}
		initial, err := asynchronous.SubmitCompletion(ctx, sdk.OperationRequest{
			IdempotencyKey: session.head,
			Input:          input,
		})
		if err != nil {
			return sdk.ModelResponse{}, fmt.Errorf("submit provider %q completion: %w", name, err)
		}
		if initial.IdempotencyKey != session.head {
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

func (session *Session) executeTool(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	rawCall sdk.ToolCall,
	toolIndex map[string]sdk.Tool,
) (sdk.ToolCall, sdk.ToolResult, error) {
	before, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventBeforeTool,
		session.config.ID,
		sdk.BeforeToolPayload{Turn: turn, Call: rawCall},
	)
	if err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	var payload sdk.BeforeToolPayload
	if err := decodePayload(before.Event, &payload); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	call := payload.Call
	if err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindToolCall,
		snapshot.generation,
		sdk.BeforeToolPayload{Turn: turn, Call: call},
	); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	if before.Block != nil {
		result := sdk.ToolResult{
			Content: before.Block.Reason,
			IsError: true,
		}
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call,
			before.Block.Kind,
			before.Block.Reason,
			result,
		)
	}

	tool, exists := toolIndex[call.Name]
	if !exists {
		result := sdk.ToolResult{
			Content: fmt.Sprintf("unknown or unavailable tool %q", call.Name),
			IsError: true,
		}
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call,
			"unknown_tool",
			result.Content,
			result,
		)
	}

	result, callErr := session.callTool(ctx, call.Name, tool, call.Arguments)
	if callErr != nil {
		result = sdk.ToolResult{Content: callErr.Error(), IsError: true}
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call,
			"execution_failed",
			callErr.Error(),
			result,
		)
	}
	return session.afterTool(ctx, snapshot, turn, call, result)
}

func (session *Session) callTool(
	ctx context.Context,
	name string,
	tool sdk.Tool,
	arguments json.RawMessage,
) (sdk.ToolResult, error) {
	if asynchronous, ok := tool.(sdk.AsyncTool); ok {
		initial, err := asynchronous.SubmitCall(ctx, sdk.OperationRequest{
			IdempotencyKey: session.head,
			Input:          append(json.RawMessage(nil), arguments...),
		})
		if err != nil {
			return sdk.ToolResult{}, fmt.Errorf("submit tool %q call: %w", name, err)
		}
		if initial.IdempotencyKey != session.head {
			return sdk.ToolResult{}, fmt.Errorf(
				"tool %q returned operation with unexpected idempotency key",
				name,
			)
		}
		operation, err := session.runtime.awaitOperation(
			ctx,
			initial,
			asynchronous.PollCall,
			asynchronous.CancelCall,
		)
		if err != nil {
			return sdk.ToolResult{}, fmt.Errorf("tool %q call: %w", name, err)
		}
		var result sdk.ToolResult
		if err := json.Unmarshal(operation.Output, &result); err != nil {
			return sdk.ToolResult{}, fmt.Errorf("decode tool %q result: %w", name, err)
		}
		return result, nil
	}
	return sdk.ToolResult{}, fmt.Errorf("tool %q has no asynchronous execution implementation", name)
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
