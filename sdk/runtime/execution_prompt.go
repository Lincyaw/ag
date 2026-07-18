package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// Prompt executes one durable agent input synchronously.
func (session *Session) Prompt(
	ctx context.Context,
	prompt string,
) (result Result, returnErr error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if strings.TrimSpace(prompt) == "" {
		return Result{}, errors.New("prompt is empty")
	}
	if err := session.runtime.beginTrajectoryWork(); err != nil {
		return Result{}, err
	}
	defer session.runtime.endTrajectoryWork()
	execution := newPromptExecution(session, prompt)
	if err := session.beginExecution(ctx, execution.userMessage); err != nil {
		return Result{}, err
	}
	if err := session.claimExecution(ctx); err != nil {
		return Result{}, err
	}
	execution.mutated = true
	defer func() {
		if !execution.mutated || returnErr == nil {
			return
		}
		restoreCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		returnErr = errors.Join(
			returnErr,
			session.failExecution(restoreCtx, returnErr),
		)
	}()
	executionCtx, stopHeartbeat := session.executionHeartbeat(ctx)
	defer func() {
		returnErr = errors.Join(returnErr, stopHeartbeat())
	}()

	var done bool
	result, done, returnErr = execution.start(executionCtx)
	if returnErr != nil || done {
		return result, returnErr
	}
	return execution.runTurns(executionCtx)
}

type promptExecution struct {
	session      *Session
	userMessage  sdk.Message
	messages     []sdk.Message
	system       string
	dependencies []string
	result       Result
	mutated      bool
}

func newPromptExecution(session *Session, prompt string) *promptExecution {
	userMessage := sdk.Message{Role: sdk.RoleUser, Content: prompt}
	messages := append(cloneMessages(session.messages), userMessage)
	return &promptExecution{
		session:     session,
		userMessage: userMessage,
		messages:    messages,
		system:      session.config.System,
	}
}

func (execution *promptExecution) start(
	ctx context.Context,
) (Result, bool, error) {
	session := execution.session
	lease, err := session.acquireSnapshot()
	if err != nil {
		return Result{}, false, err
	}
	defer lease.release()
	startDispatch, err := session.runtime.dispatch(
		ctx,
		lease.snapshot,
		sdk.EventBeforeAgentStart,
		session.config.ID,
		sdk.BeforeAgentStartPayload{
			Messages: execution.messages,
			System:   execution.system,
		},
	)
	if err != nil {
		return Result{}, false, err
	}
	var beforeStart sdk.BeforeAgentStartPayload
	if err := decodePayload(startDispatch.Event, &beforeStart); err != nil {
		return Result{}, false, err
	}
	execution.messages = cloneMessages(beforeStart.Messages)
	execution.system = beforeStart.System
	if startDispatch.Block != nil {
		cause := sdk.Cause{
			Code:   "prompt_blocked",
			Detail: startDispatch.Block.Reason,
		}
		if err := session.checkpointTrajectory(
			ctx,
			lease.snapshot.generation,
			execution.messages,
			execution.result,
			sdk.Action{Kind: sdk.ActionStop, Cause: &cause},
			execution.system,
		); err != nil {
			return Result{}, false, err
		}
		execution.result, err = session.finish(
			ctx,
			lease.snapshot,
			execution.messages,
			execution.result,
			cause,
		)
		session.messages = cloneMessages(execution.messages)
		return execution.result, true, err
	}
	if _, err := session.runtime.dispatch(
		ctx,
		lease.snapshot,
		sdk.EventAgentStart,
		session.config.ID,
		sdk.AgentStartPayload{
			Messages: execution.messages,
			System:   execution.system,
		},
	); err != nil {
		return Result{}, false, err
	}
	if err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindAgentStart,
		lease.snapshot.generation,
		sdk.AgentStartPayload{
			Messages: execution.messages,
			System:   execution.system,
		},
	); err != nil {
		return Result{}, false, err
	}
	return Result{}, false, nil
}

func (execution *promptExecution) runTurns(
	ctx context.Context,
) (Result, error) {
	return execution.runTurnsFrom(ctx, 0)
}

func (execution *promptExecution) runTurnsFrom(
	ctx context.Context,
	startTurn int,
) (Result, error) {
	for turn := startTurn; turn < execution.session.config.MaxTurns; turn++ {
		if err := ctx.Err(); err != nil {
			cause := sdk.Cause{Code: "cancelled", Detail: err.Error(), Final: true}
			execution.result.Cause = cause
			execution.result.Messages = cloneMessages(execution.messages)
			execution.session.messages = cloneMessages(execution.messages)
			return execution.result, err
		}
		result, done, err := execution.executeTurn(ctx, turn)
		if err != nil || done {
			return result, err
		}
	}
	return Result{}, errors.New("agent loop exited without a terminal action")
}

type providerCall struct {
	name         string
	provider     sdk.Provider
	operationKey string
	invocation   sdk.Invocation
	request      sdk.ModelRequest
	tools        map[string]sdk.Tool
}

func (execution *promptExecution) executeTurn(
	ctx context.Context,
	turn int,
) (Result, bool, error) {
	session := execution.session
	lease, err := session.acquireSnapshot()
	if err != nil {
		return Result{}, false, err
	}
	defer lease.release()
	execution.result.Generation = lease.snapshot.generation

	call, err := execution.prepareProviderCall(ctx, lease.snapshot, turn)
	if err != nil {
		return Result{}, false, err
	}
	response, err := execution.callProvider(ctx, lease.snapshot, turn, call)
	if err != nil {
		result, finishErr := execution.finishWithError(
			ctx,
			lease.snapshot,
			"provider_error",
			err,
		)
		return result, true, finishErr
	}

	execution.messages = append(execution.messages, sdk.Message{
		Role:      sdk.RoleAssistant,
		Content:   response.Content,
		ToolCalls: cloneToolCalls(response.ToolCalls),
	})
	execution.result.Output = response.Content
	execution.result.Turns = turn + 1

	toolResults, err := execution.executeTools(
		ctx,
		lease.snapshot,
		turn,
		response.ToolCalls,
		call.tools,
		call.invocation.ID,
		call.name,
	)
	if err != nil {
		result, finishErr := execution.finishWithError(
			ctx,
			lease.snapshot,
			"hook_error",
			err,
		)
		return result, true, finishErr
	}
	action, err := execution.decide(
		ctx,
		lease.snapshot,
		turn,
		response,
		toolResults,
	)
	if err != nil {
		return Result{}, false, err
	}
	return execution.applyAction(ctx, lease.snapshot, action)
}

func (execution *promptExecution) prepareProviderCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
) (providerCall, error) {
	session := execution.session
	if _, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventTurnStart,
		session.config.ID,
		sdk.TurnStartPayload{Turn: turn},
	); err != nil {
		return providerCall{}, err
	}
	providerName, err := selectProviderName(snapshot, session.config.Provider)
	if err != nil {
		return providerCall{}, err
	}
	beforeProvider, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventBeforeProvider,
		session.config.ID,
		sdk.BeforeProviderPayload{
			Turn:     turn,
			Messages: cloneMessages(execution.messages),
			Provider: providerName,
			System:   execution.system,
			Tools:    snapshotToolSpecs(snapshot),
		},
	)
	if err != nil {
		return providerCall{}, err
	}
	var payload sdk.BeforeProviderPayload
	if err := decodePayload(beforeProvider.Event, &payload); err != nil {
		return providerCall{}, err
	}
	ownedProvider, exists := snapshot.providers[payload.Provider]
	if !exists {
		return providerCall{}, fmt.Errorf(
			"before_provider selected unregistered provider %q",
			payload.Provider,
		)
	}
	advertisedTools, toolIndex, err := resolveAdvertisedTools(
		snapshot,
		payload.Tools,
	)
	if err != nil {
		return providerCall{}, err
	}
	requestMessages := cloneMessages(payload.Messages)
	if payload.System != "" {
		requestMessages = append(
			[]sdk.Message{{Role: sdk.RoleSystem, Content: payload.System}},
			requestMessages...,
		)
	}
	call := providerCall{
		name:         payload.Provider,
		provider:     ownedProvider.value,
		operationKey: session.executionOperationKey("provider", fmt.Sprint(turn)),
		invocation: session.executionInvocation(
			"provider",
			fmt.Sprint(turn),
			fmt.Sprintf("turn/%d", turn),
			slices.Clone(execution.dependencies),
			0,
		),
		request: sdk.ModelRequest{
			Messages: requestMessages,
			Tools:    advertisedTools,
		},
		tools: toolIndex,
	}
	if err := session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindProviderRequest,
		snapshot.generation,
		durability.ProviderRequest{
			Turn:         turn,
			Provider:     call.name,
			Model:        ownedProvider.spec.Model,
			OperationKey: call.operationKey,
			Request:      call.request,
		},
	); err != nil {
		return providerCall{}, err
	}
	return call, nil
}

func (execution *promptExecution) callProvider(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call providerCall,
) (sdk.ModelResponse, error) {
	response, callErr := execution.session.invokeProvider(
		ctx,
		call.name,
		call.provider,
		call.operationKey,
		call.invocation,
		call.request,
	)
	if callErr == nil {
		callErr = validateModelResponse(response)
	}
	after := sdk.AfterProviderPayload{
		Turn:     turn,
		Provider: call.name,
	}
	if callErr != nil {
		after.Error = callErr.Error()
	} else {
		after.Response = &response
	}
	_, dispatchErr := execution.session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventAfterProvider,
		execution.session.config.ID,
		after,
	)
	trajectoryErr := execution.session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindProviderResponse,
		snapshot.generation,
		after,
	)
	return response, errors.Join(callErr, dispatchErr, trajectoryErr)
}

func (execution *promptExecution) executeTools(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	calls []sdk.ToolCall,
	tools map[string]sdk.Tool,
	providerInvocationID string,
	providerName string,
) ([]sdk.ToolResult, error) {
	prepared := make([]preparedToolCall, len(calls))
	for index, rawCall := range calls {
		call, err := execution.session.prepareToolCall(
			ctx,
			snapshot,
			turn,
			index,
			rawCall,
			tools,
			providerInvocationID,
		)
		if err != nil {
			return nil, err
		}
		prepared[index] = call
	}
	transformed := make([]sdk.ToolCall, len(prepared))
	for index := range prepared {
		transformed[index] = prepared[index].call
	}
	if err := validateModelResponse(
		sdk.ModelResponse{ToolCalls: transformed},
	); err != nil {
		return nil, fmt.Errorf(
			"before_tool produced invalid calls: %w",
			err,
		)
	}
	execution.session.submitToolCalls(
		ctx,
		snapshot,
		execution.messages,
		providerName,
		prepared,
	)
	outcomes := execution.session.awaitToolCalls(ctx, prepared)
	results := make([]sdk.ToolResult, len(prepared))
	dependencies := make([]string, len(prepared))
	for index, call := range prepared {
		finalCall, result, err := execution.session.finalizeToolCall(
			ctx,
			snapshot,
			turn,
			call,
			outcomes[index],
		)
		if err != nil {
			return nil, err
		}
		execution.messages = append(execution.messages, sdk.Message{
			Role:       sdk.RoleTool,
			Content:    result.Content,
			ToolCallID: finalCall.ID,
		})
		execution.result.ToolCalls++
		results[index] = result
		dependencies[index] = call.invocation.ID
	}
	if len(dependencies) == 0 {
		execution.dependencies = []string{
			providerInvocationID,
		}
	} else {
		execution.dependencies = dependencies
	}
	return results, nil
}

func (execution *promptExecution) decide(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	response sdk.ModelResponse,
	toolResults []sdk.ToolResult,
) (sdk.Action, error) {
	defaultAction := sdk.Action{
		Kind:  sdk.ActionStop,
		Cause: &sdk.Cause{Code: "model_end"},
	}
	if len(response.ToolCalls) > 0 {
		defaultAction = sdk.Action{Kind: sdk.ActionStep}
		if turn+1 >= execution.session.config.MaxTurns {
			defaultAction = sdk.Action{
				Kind: sdk.ActionStop,
				Cause: &sdk.Cause{
					Code:  "max_turns",
					Final: true,
				},
			}
		}
	}
	decision, err := execution.session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventDecide,
		execution.session.config.ID,
		sdk.DecidePayload{
			Turn:        turn,
			Default:     defaultAction,
			Response:    response,
			ToolResults: toolResults,
		},
	)
	if err != nil {
		return sdk.Action{}, err
	}
	action := resolveAction(defaultAction, decision.Actions)
	if turn+1 >= execution.session.config.MaxTurns &&
		action.Kind != sdk.ActionStop {
		action = sdk.Action{
			Kind: sdk.ActionStop,
			Cause: &sdk.Cause{
				Code:  "max_turns",
				Final: true,
			},
		}
	}
	if err := execution.session.appendTrajectory(
		ctx,
		sdk.TrajectoryKindDecision,
		snapshot.generation,
		durability.Decision{Turn: turn, Action: action},
	); err != nil {
		return sdk.Action{}, err
	}
	if _, err := execution.session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventTurnEnd,
		execution.session.config.ID,
		sdk.TurnEndPayload{
			Turn:     turn,
			Messages: cloneMessages(execution.messages),
			Action:   action,
		},
	); err != nil {
		return sdk.Action{}, err
	}
	return action, nil
}

func (execution *promptExecution) applyAction(
	ctx context.Context,
	snapshot *registrySnapshot,
	action sdk.Action,
) (Result, bool, error) {
	if action.Kind == sdk.ActionInject {
		execution.messages = append(
			execution.messages,
			cloneMessages(action.Messages)...,
		)
	}
	switch action.Kind {
	case sdk.ActionInject, sdk.ActionStep, sdk.ActionStop:
	default:
		return Result{}, false, fmt.Errorf(
			"unknown resolved action %q",
			action.Kind,
		)
	}
	if err := execution.session.checkpointTrajectory(
		ctx,
		snapshot.generation,
		execution.messages,
		execution.result,
		action,
		execution.system,
		execution.dependencies...,
	); err != nil {
		return Result{}, false, err
	}
	if action.Kind != sdk.ActionStop {
		return Result{}, false, nil
	}
	cause := sdk.Cause{Code: "model_end"}
	if action.Cause != nil {
		cause = *action.Cause
	}
	var err error
	execution.result, err = execution.session.finish(
		ctx,
		snapshot,
		execution.messages,
		execution.result,
		cause,
	)
	execution.session.messages = cloneMessages(execution.messages)
	return execution.result, true, err
}

func (execution *promptExecution) finishWithError(
	ctx context.Context,
	snapshot *registrySnapshot,
	code string,
	cause error,
) (Result, error) {
	var finishErr error
	execution.result, finishErr = execution.session.finish(
		ctx,
		snapshot,
		execution.messages,
		execution.result,
		sdk.Cause{Code: code, Detail: cause.Error()},
	)
	execution.session.messages = cloneMessages(execution.messages)
	return execution.result, errors.Join(cause, finishErr)
}

func (session *Session) finish(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	result Result,
	cause sdk.Cause,
) (Result, error) {
	result.Messages = cloneMessages(messages)
	result.Cause = cause
	end := sdk.AgentEndPayload{
		Messages: cloneMessages(messages),
		Cause:    cause,
	}
	state := executionStateForCause(cause)
	commitState := state
	if state == sdk.TrajectoryExecutionFailed {
		// Keep the lease active until Prompt's failure defer atomically restores
		// the last checkpoint and marks the execution failed.
		commitState = ""
	}
	if err := session.appendTrajectoryState(
		ctx,
		sdk.TrajectoryKindTerminal,
		snapshot.generation,
		end,
		commitState,
		"",
	); err != nil {
		return result, err
	}
	if _, err := session.runtime.dispatch(
		ctx,
		snapshot,
		sdk.EventAgentEnd,
		session.config.ID,
		end,
	); err != nil {
		session.runtime.logger.WarnContext(
			ctx,
			"agent end event failed",
			"session_id",
			session.config.ID,
			"error",
			err,
		)
	}
	return result, nil
}

func resolveAction(defaultAction sdk.Action, actions []sdk.Action) sdk.Action {
	if defaultAction.Kind == sdk.ActionStop &&
		defaultAction.Cause != nil &&
		defaultAction.Cause.Final {
		return defaultAction
	}
	var injected []sdk.Message
	for _, action := range actions {
		if action.Kind == sdk.ActionInject {
			injected = append(injected, cloneMessages(action.Messages)...)
		}
	}
	if len(injected) > 0 {
		return sdk.Action{Kind: sdk.ActionInject, Messages: injected}
	}
	for index := len(actions) - 1; index >= 0; index-- {
		if actions[index].Kind == sdk.ActionStop {
			return actions[index]
		}
	}
	for _, action := range actions {
		if action.Kind == sdk.ActionStep {
			return sdk.Action{Kind: sdk.ActionStep}
		}
	}
	return defaultAction
}

func decodePayload(event sdk.Event, target any) error {
	if err := json.Unmarshal(event.Payload, target); err != nil {
		return fmt.Errorf("decode %s event payload: %w", event.Name, err)
	}
	return nil
}

func cloneMessages(messages []sdk.Message) []sdk.Message {
	result := make([]sdk.Message, len(messages))
	for index, message := range messages {
		result[index] = message
		result[index].ToolCalls = cloneToolCalls(message.ToolCalls)
	}
	return result
}

func cloneToolCalls(calls []sdk.ToolCall) []sdk.ToolCall {
	result := make([]sdk.ToolCall, len(calls))
	for index, call := range calls {
		result[index] = call
		result[index].Arguments = append(json.RawMessage(nil), call.Arguments...)
	}
	return result
}
