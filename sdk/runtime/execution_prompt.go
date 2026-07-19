package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// Prompt executes one durable agent input synchronously.
func (session *Session) Prompt(
	ctx context.Context,
	prompt string,
) (Result, error) {
	if session == nil {
		return Result{}, errors.New("session is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	submission, err := session.submitPromptLocked(ctx, prompt)
	if err != nil {
		return Result{}, err
	}
	return submission.runLocked(ctx)
}

type promptExecution struct {
	session          *Session
	userMessage      sdk.Message
	messages         []sdk.Message
	system           string
	dependencies     []string
	providerAttempts map[int]int
	result           Result
}

type promptTurnTransition uint8

const (
	promptTurnAdvance promptTurnTransition = iota
	promptTurnRetry
	promptTurnDone
)

func newPromptUserMessage(prompt string) sdk.Message {
	return sdk.Message{Role: sdk.RoleUser, Content: prompt}
}

func newPromptExecutionFromInput(
	session *Session,
	input durability.ExecutionInput,
) (*promptExecution, error) {
	return newPromptExecutionFromAcceptedMessages(
		session,
		input.BaseMessages,
		input.Message,
	)
}

func newPromptExecutionFromAcceptedMessages(
	session *Session,
	baseMessages []sdk.Message,
	userMessage sdk.Message,
) (*promptExecution, error) {
	if userMessage.Role != sdk.RoleUser || userMessage.Content == "" {
		return nil, errors.New(
			"trajectory execution input is not a user message",
		)
	}
	message := sdk.CloneMessages([]sdk.Message{userMessage})[0]
	return &promptExecution{
		session:     session,
		userMessage: message,
		messages: append(
			sdk.CloneMessages(baseMessages),
			message,
		),
		system: session.config.System,
	}, nil
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
	beforeStart, startDispatch, err := dispatchMutableExecutionEvent(
		session.runtime,
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
	execution.messages = sdk.CloneMessages(beforeStart.Messages)
	execution.system = beforeStart.System
	if startDispatch.Block != nil {
		cause := sdk.Cause{
			Code:   sdk.CausePromptBlocked,
			Detail: startDispatch.Block.Reason,
		}
		if err := session.checkpointTrajectory(
			ctx,
			lease.snapshot,
			trajectoryCheckpointCommit{
				Messages: execution.messages,
				Result:   execution.result,
				Action: sdk.Action{
					Kind:  sdk.ActionStop,
					Cause: &cause,
				},
				System: execution.system,
				Audit:  trajectoryAudits(startDispatch.Audit),
			},
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
		session.applyMessageProjection(execution.messages)
		return execution.result, true, err
	}
	agentStart := sdk.AgentStartPayload{
		Messages: execution.messages,
		System:   execution.system,
	}
	if err := session.appendTrajectoryWithExecutionEvent(
		ctx,
		lease.snapshot,
		sdk.TrajectoryKindAgentStart,
		agentStart,
		trajectoryAudits(startDispatch.Audit),
		sdk.EventAgentStart,
		agentStart,
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

func (execution *promptExecution) runFromStart(
	ctx context.Context,
) (Result, error) {
	result, done, err := execution.start(ctx)
	if err != nil || done {
		return result, err
	}
	return execution.runTurns(ctx)
}

func (execution *promptExecution) runTurnsFrom(
	ctx context.Context,
	startTurn int,
) (Result, error) {
	for turn := startTurn; turn < execution.session.config.MaxTurns; {
		if err := ctx.Err(); err != nil {
			cause := sdk.Cause{
				Code:   sdk.CauseCancelled,
				Detail: err.Error(),
				Final:  true,
			}
			execution.result.Cause = cause
			execution.result.Messages = sdk.CloneMessages(execution.messages)
			execution.session.applyMessageProjection(execution.messages)
			return execution.result, err
		}
		result, transition, err := execution.executeTurn(ctx, turn)
		if err != nil {
			return result, err
		}
		switch transition {
		case promptTurnDone:
			return result, nil
		case promptTurnRetry:
			continue
		case promptTurnAdvance:
			turn++
		default:
			return Result{}, fmt.Errorf(
				"unknown prompt turn transition %d",
				transition,
			)
		}
	}
	return Result{}, errors.New("agent loop exited without a terminal action")
}

type providerCall struct {
	name       string
	provider   sdk.AsyncProvider
	invocation sdk.Invocation
	request    sdk.ModelRequest
	tools      map[string]advertisedTool
}

func (execution *promptExecution) executeTurn(
	ctx context.Context,
	turn int,
) (Result, promptTurnTransition, error) {
	session := execution.session
	lease, err := session.acquireSnapshot()
	if err != nil {
		return Result{}, promptTurnDone, err
	}
	defer lease.release()
	execution.result.Generation = lease.snapshot.generation
	if _, err := execution.checkpointQueuedContext(
		ctx,
		lease.snapshot,
		contextInjectionBeforeProvider,
	); err != nil {
		return Result{}, promptTurnDone, err
	}

	call, err := execution.prepareProviderCall(ctx, lease.snapshot, turn)
	if err != nil {
		return Result{}, promptTurnDone, err
	}
	providerCtx, stopProviderInterrupt := session.
		contextInjectionInterruptContext(ctx, true)
	response, err := execution.callProvider(
		ctx,
		providerCtx,
		lease.snapshot,
		turn,
		call,
	)
	providerInterrupted := errors.Is(
		context.Cause(providerCtx),
		errContextInjectionInterrupt,
	)
	stopProviderInterrupt()
	if providerInterrupted {
		if _, checkpointErr := execution.checkpointQueuedContext(
			ctx,
			lease.snapshot,
			contextInjectionBeforeProvider,
		); checkpointErr != nil {
			return Result{}, promptTurnDone, checkpointErr
		}
		return Result{}, promptTurnRetry, nil
	}
	if err != nil {
		result, finishErr := execution.finishWithError(
			sdk.CauseProviderError,
			err,
		)
		return result, promptTurnDone, finishErr
	}

	execution.messages = append(execution.messages, sdk.Message{
		Role:      sdk.RoleAssistant,
		Content:   response.Content,
		ToolCalls: sdk.CloneToolCalls(response.ToolCalls),
	})
	execution.result.Output = response.Content
	execution.result.Turns = turn + 1

	toolResults, interrupted, err := execution.executeTools(
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
			sdk.CauseHookError,
			err,
		)
		return result, promptTurnDone, finishErr
	}
	if interrupted {
		if _, err := execution.checkpointQueuedContext(
			ctx,
			lease.snapshot,
			contextInjectionBeforeProvider,
		); err != nil {
			return Result{}, promptTurnDone, err
		}
		if turn+1 >= execution.session.config.MaxTurns {
			result, done, err := execution.applyAction(
				ctx,
				lease.snapshot,
				sdk.Action{
					Kind: sdk.ActionStop,
					Cause: &sdk.Cause{
						Code:  sdk.CauseMaxTurns,
						Final: true,
					},
				},
			)
			return result, promptTransitionFromDone(done), err
		}
		return Result{}, promptTurnAdvance, nil
	}
	action, err := execution.decide(
		ctx,
		lease.snapshot,
		turn,
		response,
		toolResults,
	)
	if err != nil {
		return Result{}, promptTurnDone, err
	}
	result, done, err := execution.applyAction(ctx, lease.snapshot, action)
	return result, promptTransitionFromDone(done), err
}

func promptTransitionFromDone(done bool) promptTurnTransition {
	if done {
		return promptTurnDone
	}
	return promptTurnAdvance
}

func (execution *promptExecution) prepareProviderCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
) (providerCall, error) {
	session := execution.session
	providerName, err := selectProviderName(snapshot, session.config.Provider)
	if err != nil {
		return providerCall{}, err
	}
	payload, beforeProvider, err := dispatchMutableExecutionEvent(
		session.runtime,
		ctx,
		snapshot,
		sdk.EventBeforeProvider,
		session.config.ID,
		sdk.BeforeProviderPayload{
			Turn:     turn,
			Messages: sdk.CloneMessages(execution.messages),
			Provider: providerName,
			System:   execution.system,
			Tools:    snapshotToolSpecs(snapshot),
		},
	)
	if err != nil {
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
	requestMessages := sdk.CloneMessages(payload.Messages)
	if payload.System != "" {
		requestMessages = append(
			[]sdk.Message{{Role: sdk.RoleSystem, Content: payload.System}},
			requestMessages...,
		)
	}
	attempt, err := execution.nextProviderAttempt(ctx, turn)
	if err != nil {
		return providerCall{}, err
	}
	invocation := session.executionInvocation(
		"provider",
		providerInvocationCoordinate(turn, attempt),
		fmt.Sprintf("turn/%d", turn),
		slices.Clone(execution.dependencies),
		0,
	)
	call := providerCall{
		name:       payload.Provider,
		provider:   ownedProvider.value,
		invocation: invocation,
		request: sdk.ModelRequest{
			Messages: requestMessages,
			Tools:    advertisedTools,
		},
		tools: toolIndex,
	}
	if err := session.appendTrajectoryWithExecutionEvent(
		ctx,
		snapshot,
		sdk.TrajectoryKindProviderRequest,
		durability.ProviderRequest{
			Turn:          turn,
			Provider:      call.name,
			Model:         ownedProvider.spec.Model,
			OperationKey:  call.invocation.ID,
			CorrelationID: call.invocation.ID,
			Request:       call.request,
		},
		trajectoryAudits(beforeProvider.Audit),
		sdk.EventTurnStart,
		sdk.TurnStartPayload{Turn: turn},
	); err != nil {
		return providerCall{}, err
	}
	return call, nil
}

func (execution *promptExecution) callProvider(
	ctx context.Context,
	operationCtx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call providerCall,
) (sdk.ModelResponse, error) {
	response, callErr := execution.session.invokeProvider(
		operationCtx,
		call.name,
		call.provider,
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
	trajectoryErr := execution.session.appendTrajectoryWithExecutionEvent(
		ctx,
		snapshot,
		sdk.TrajectoryKindProviderResponse,
		durability.ProviderResponse{
			AfterProviderPayload: after,
			CorrelationID:        call.invocation.ID,
		},
		nil,
		sdk.EventAfterProvider,
		after,
	)
	return response, errors.Join(callErr, trajectoryErr)
}

func (execution *promptExecution) nextProviderAttempt(
	ctx context.Context,
	turn int,
) (int, error) {
	session := execution.session
	if session != nil && session.runtime != nil {
		executionID, _ := session.activeExecution()
		analyzer, ok := session.runtime.trajectories.(sdk.TrajectoryAnalyzer)
		if ok && executionID != "" {
			entries, err := analyzer.AnalyzeEntries(
				ctx,
				sdk.TrajectoryEntryQuery{
					TrajectoryID: session.config.ID,
					ExecutionID:  executionID,
					Kind:         sdk.TrajectoryKindProviderRequest,
					Limit:        sdk.MaxPageSize,
				},
			)
			if err != nil {
				return 0, err
			}
			attempt := 0
			for _, entry := range entries {
				if entry.Fields.Turn != nil && *entry.Fields.Turn == turn {
					attempt++
				}
			}
			return attempt, nil
		}
	}
	if execution.providerAttempts == nil {
		execution.providerAttempts = make(map[int]int)
	}
	attempt := execution.providerAttempts[turn]
	execution.providerAttempts[turn] = attempt + 1
	return attempt, nil
}

func providerInvocationCoordinate(turn int, attempt int) string {
	if attempt == 0 {
		return fmt.Sprint(turn)
	}
	return fmt.Sprintf("%d/%d", turn, attempt)
}

func (execution *promptExecution) executeTools(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	calls []sdk.ToolCall,
	tools map[string]advertisedTool,
	providerInvocationID string,
	providerName string,
) ([]sdk.ToolResult, bool, error) {
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
			return nil, false, err
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
		return nil, false, fmt.Errorf(
			"before_tool produced invalid calls: %w",
			err,
		)
	}
	toolCtx, stopToolInterrupt := execution.session.
		contextInjectionInterruptContext(
			ctx,
			toolCallsCancelOnContextInjection(prepared),
		)
	defer stopToolInterrupt()
	outcomes := make([]toolCallOutcome, len(prepared))
	interrupted := false
	for _, batch := range toolCallExecutionBatches(prepared) {
		batchCalls := prepared[batch.start:batch.end]
		execution.session.submitToolCalls(
			ctx,
			toolCtx,
			snapshot,
			providerName,
			batchCalls,
		)
		batchOutcomes := execution.session.awaitToolCalls(
			ctx,
			toolCtx,
			batchCalls,
		)
		copy(outcomes[batch.start:batch.end], batchOutcomes)
		interrupted = toolOutcomesInterrupted(batchOutcomes) ||
			errors.Is(context.Cause(toolCtx), errContextInjectionInterrupt)
		if interrupted && batch.end < len(prepared) {
			markInterruptedToolOutcomes(outcomes, batch.end)
			break
		}
	}
	results := make([]sdk.ToolResult, len(prepared))
	dependencies := make([]string, len(prepared))
	interrupted = interrupted || errors.Is(
		context.Cause(toolCtx),
		errContextInjectionInterrupt,
	)
	for index, call := range prepared {
		if errors.Is(outcomes[index].err, errContextInjectionInterrupt) {
			interrupted = true
		}
		finalCall, result, err := execution.session.finalizeToolCall(
			ctx,
			snapshot,
			turn,
			call,
			outcomes[index],
		)
		if err != nil {
			return nil, false, err
		}
		execution.messages = append(
			execution.messages,
			sdk.ToolMessage(finalCall.ID, result),
		)
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
	return results, interrupted, nil
}

func toolOutcomesInterrupted(outcomes []toolCallOutcome) bool {
	for _, outcome := range outcomes {
		if errors.Is(outcome.err, errContextInjectionInterrupt) {
			return true
		}
	}
	return false
}

func markInterruptedToolOutcomes(
	outcomes []toolCallOutcome,
	start int,
) {
	for index := start; index < len(outcomes); index++ {
		outcomes[index] = toolCallOutcome{
			err: errContextInjectionInterrupt,
		}
	}
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
		Cause: &sdk.Cause{Code: sdk.CauseModelEnd},
	}
	if len(response.ToolCalls) > 0 {
		defaultAction = sdk.Action{Kind: sdk.ActionStep}
		if turn+1 >= execution.session.config.MaxTurns {
			defaultAction = sdk.Action{
				Kind: sdk.ActionStop,
				Cause: &sdk.Cause{
					Code:  sdk.CauseMaxTurns,
					Final: true,
				},
			}
		}
	}
	decision, err := execution.session.runtime.dispatchExecutionEvent(
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
	action, resolution := resolveAction(
		defaultAction,
		decision.Actions,
		decision.actionSteps,
	)
	if turn+1 >= execution.session.config.MaxTurns &&
		action.Kind != sdk.ActionStop {
		action = sdk.Action{
			Kind: sdk.ActionStop,
			Cause: &sdk.Cause{
				Code:  sdk.CauseMaxTurns,
				Final: true,
			},
		}
		resolution = actionResolution(
			action,
			resolution.ActionSteps,
			"max_turns_override",
		)
	}
	decision.Audit.Resolution = resolution
	turnEnd := sdk.TurnEndPayload{
		Turn:     turn,
		Messages: sdk.CloneMessages(execution.messages),
		Action:   action,
	}
	if err := execution.session.appendTrajectoryWithExecutionEvent(
		ctx,
		snapshot,
		sdk.TrajectoryKindDecision,
		durability.Decision{Turn: turn, Action: action},
		trajectoryAudits(decision.Audit),
		sdk.EventTurnEnd,
		turnEnd,
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
			sdk.CloneMessages(action.Messages)...,
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
	if actionDrainsAfterTurn(action) {
		drained, err := execution.checkpointQueuedContext(
			ctx,
			snapshot,
			contextInjectionAfterTurn,
		)
		if err != nil {
			return Result{}, false, err
		}
		if drained {
			return Result{}, false, nil
		}
	}
	if err := execution.session.checkpointTrajectory(
		ctx,
		snapshot,
		trajectoryCheckpointCommit{
			Messages:     execution.messages,
			Result:       execution.result,
			Action:       action,
			System:       execution.system,
			Dependencies: execution.dependencies,
		},
	); err != nil {
		return Result{}, false, err
	}
	if action.Kind != sdk.ActionStop {
		return Result{}, false, nil
	}
	cause := sdk.Cause{Code: sdk.CauseModelEnd}
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
	execution.session.applyMessageProjection(execution.messages)
	return execution.result, true, err
}

func (execution *promptExecution) finishWithError(
	code string,
	cause error,
) (Result, error) {
	execution.result.Messages = sdk.CloneMessages(execution.messages)
	execution.result.Cause = sdk.Cause{Code: code, Detail: cause.Error()}
	execution.session.applyMessageProjection(execution.messages)
	return execution.result, cause
}

func (session *Session) finish(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []sdk.Message,
	result Result,
	cause sdk.Cause,
) (Result, error) {
	result.Messages = sdk.CloneMessages(messages)
	result.Cause = cause
	end := agentEndPayloadFromResult(result, messages, cause)
	state := executionStateForCause(cause)
	if state == sdk.TrajectoryExecutionFailed {
		return result, errors.New(
			"failed execution completion is owned by failure unwind",
		)
	}
	if err := session.appendTrajectoryStateWithExecutionEvent(
		ctx,
		snapshot,
		sdk.TrajectoryKindTerminal,
		end,
		state,
		"",
		nil,
		sdk.EventAgentEnd,
		end,
	); err != nil {
		return result, err
	}
	return result, nil
}

func resolveAction(
	defaultAction sdk.Action,
	actions []sdk.Action,
	actionSteps []int,
) (sdk.Action, sdk.EffectResolution) {
	if defaultAction.Kind == sdk.ActionStop &&
		defaultAction.Cause != nil &&
		defaultAction.Cause.Final {
		action := sdk.CloneAction(defaultAction)
		return action, actionResolution(action, nil, "default_final_stop")
	}
	var injected []sdk.Message
	var injectedSteps []int
	for _, action := range actions {
		if action.Kind == sdk.ActionInject {
			injected = append(injected, sdk.CloneMessages(action.Messages)...)
		}
	}
	for index, action := range actions {
		if action.Kind == sdk.ActionInject {
			injectedSteps = append(injectedSteps, actionStepAt(actionSteps, index))
		}
	}
	if len(injected) > 0 {
		action := sdk.Action{Kind: sdk.ActionInject, Messages: injected}
		return action, actionResolution(action, injectedSteps, "inject_merge")
	}
	for index, action := range actions {
		if action.Kind == sdk.ActionStop {
			action = sdk.CloneAction(action)
			return action, actionResolution(
				action,
				[]int{actionStepAt(actionSteps, index)},
				"first_stop",
			)
		}
	}
	for index, action := range actions {
		if action.Kind == sdk.ActionStep {
			action := sdk.Action{Kind: sdk.ActionStep}
			return action, actionResolution(
				action,
				[]int{actionStepAt(actionSteps, index)},
				"first_step",
			)
		}
	}
	action := sdk.CloneAction(defaultAction)
	return action, actionResolution(action, nil, "default")
}

func actionResolution(
	action sdk.Action,
	steps []int,
	rule string,
) sdk.EffectResolution {
	return sdk.EffectResolution{
		Outcome:     sdk.EffectResolutionAction,
		Action:      summarizeAction(&action),
		ActionSteps: slices.Clone(steps),
		ActionRule:  rule,
	}
}

func actionStepAt(steps []int, index int) int {
	if index >= 0 && index < len(steps) {
		return steps[index]
	}
	return index
}
