package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// invokeProvider crosses the execution-to-resource operation boundary.
func (session *Session) invokeProvider(
	ctx context.Context,
	name string,
	provider sdk.AsyncProvider,
	invocation sdk.Invocation,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	input, err := json.Marshal(request)
	if err != nil {
		return sdk.ModelResponse{}, fmt.Errorf("encode provider %q request: %w", name, err)
	}
	operationRequest := sdk.OperationRequest{
		IdempotencyKey: invocation.ID,
		Input:          input,
		Invocation:     invocation,
	}
	initial, err := provider.SubmitCompletion(
		ctx,
		sdk.CloneOperationRequest(operationRequest),
	)
	if err != nil {
		return sdk.ModelResponse{}, fmt.Errorf("submit provider %q completion: %w", name, err)
	}
	response, err := awaitOperationRequestJSON[sdk.ModelResponse](
		session.runtime,
		ctx,
		operationRequest,
		initial,
		provider.PollCompletion,
		provider.CancelCompletion,
		fmt.Sprintf("provider %q completion", name),
		fmt.Sprintf("provider %q response", name),
	)
	if err != nil {
		return sdk.ModelResponse{}, err
	}
	return response, nil
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
	return "", errors.New("no provider is mounted")
}

func snapshotToolSpecs(snapshot *registrySnapshot) []sdk.ToolSpec {
	specs := make([]sdk.ToolSpec, 0, len(snapshot.tools))
	for _, owned := range snapshot.tools {
		specs = append(specs, sdk.CloneToolSpec(owned.spec))
	}
	slices.SortFunc(specs, func(left, right sdk.ToolSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	return specs
}

type advertisedTool struct {
	value sdk.AsyncTool
	spec  sdk.ToolSpec
}

func resolveAdvertisedTools(
	snapshot *registrySnapshot,
	specs []sdk.ToolSpec,
) ([]sdk.ToolSpec, map[string]advertisedTool, error) {
	result := make([]sdk.ToolSpec, 0, len(specs))
	index := make(map[string]advertisedTool, len(specs))
	for _, spec := range specs {
		if err := plugincontract.ValidateToolSpec(spec); err != nil {
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
		advertised := sdk.CloneToolSpec(spec)
		advertised.InterruptBehavior = owned.spec.InterruptBehavior
		index[spec.Name] = advertisedTool{
			value: owned.value,
			spec:  sdk.CloneToolSpec(owned.spec),
		}
		result = append(result, advertised)
	}
	return result, index, nil
}

type preparedToolCall struct {
	call          sdk.ToolCall
	invocation    sdk.Invocation
	asynchronous  sdk.AsyncTool
	initial       sdk.Operation
	failureKind   string
	failureReason string
	forkAnchor    trajectoryForkAnchor
	interrupt     sdk.ToolInterruptBehavior
}

type toolCallOutcome struct {
	result sdk.ToolResult
	err    error
}

func (call preparedToolCall) operationRequest() sdk.OperationRequest {
	return sdk.OperationRequest{
		IdempotencyKey: call.invocation.ID,
		Input:          call.call.Arguments,
		Invocation:     call.invocation,
	}
}

func (session *Session) prepareToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	index int,
	rawCall sdk.ToolCall,
	toolIndex map[string]advertisedTool,
	providerInvocationID string,
) (preparedToolCall, error) {
	payload, before, err := dispatchMutableExecutionEvent(
		session.runtime,
		ctx,
		snapshot,
		sdk.EventBeforeTool,
		session.config.ID,
		sdk.BeforeToolPayload{Turn: turn, Call: rawCall},
	)
	if err != nil {
		return preparedToolCall{}, err
	}
	call := payload.Call
	coordinate := fmt.Sprintf("%d/%s", turn, call.ID)
	invocation := session.executionInvocation(
		"tool",
		coordinate,
		fmt.Sprintf("tools/%d", turn),
		[]string{providerInvocationID},
		uint32(index),
	)
	prepared := preparedToolCall{
		call:       call,
		invocation: invocation,
		interrupt:  sdk.ToolInterruptBlock,
	}
	if err := session.appendTrajectoryWithAudit(
		ctx,
		snapshot,
		sdk.TrajectoryKindToolCall,
		durability.ToolCall{
			Turn:         turn,
			Call:         call,
			OperationKey: invocation.ID,
		},
		trajectoryAudits(before.Audit),
	); err != nil {
		return preparedToolCall{}, err
	}
	prepared.forkAnchor = trajectoryForkAnchor{
		parentEntryID:          session.head,
		originForkInvocationID: invocation.ID,
	}
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
	prepared.interrupt = tool.spec.EffectiveInterruptBehavior()
	prepared.asynchronous = tool.value
	return prepared, nil
}

func (call preparedToolCall) cancelsOnContextInjection() bool {
	return call.interrupt == sdk.ToolInterruptCancel
}

func toolCallsCancelOnContextInjection(calls []preparedToolCall) bool {
	for _, call := range calls {
		if call.cancelsOnContextInjection() {
			return true
		}
	}
	return false
}

func (session *Session) submitToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
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
		forkAnchor:       call.forkAnchor,
	}
	ctx = sdk.WithAgentInvoker(ctx, invoker)
	ctx = sdk.WithWorkflowInvoker(ctx, invoker)
	request := call.operationRequest()
	initial, err := call.asynchronous.SubmitCall(
		ctx,
		sdk.CloneOperationRequest(request),
	)
	if err != nil {
		if errors.Is(context.Cause(ctx), errContextInjectionInterrupt) {
			call.failureKind = "interrupted"
			call.failureReason = errContextInjectionInterrupt.Error()
			return
		}
		call.failureKind = "execution_failed"
		call.failureReason = fmt.Sprintf(
			"submit tool %q call: %v",
			call.call.Name,
			err,
		)
		return
	}
	call.initial = initial
}

func (session *Session) submitToolCalls(
	ctx context.Context,
	interruptCtx context.Context,
	snapshot *registrySnapshot,
	providerName string,
	calls []preparedToolCall,
) {
	errs := runParallelIndexed(
		ctx,
		len(calls),
		parallelIndexedOptions{},
		func(_ context.Context, index int) error {
			callCtx := ctx
			if calls[index].cancelsOnContextInjection() {
				callCtx = interruptCtx
			}
			session.submitToolCall(
				callCtx,
				snapshot,
				providerName,
				&calls[index],
			)
			return nil
		},
	)
	for index, err := range errs {
		if err == nil {
			continue
		}
		calls[index].failureKind = "execution_failed"
		calls[index].failureReason = fmt.Sprintf(
			"submit tool %q call: %v",
			calls[index].call.Name,
			err,
		)
	}
}

func (session *Session) awaitToolCalls(
	ctx context.Context,
	interruptCtx context.Context,
	calls []preparedToolCall,
) []toolCallOutcome {
	outcomes := make([]toolCallOutcome, len(calls))
	errs := runParallelIndexed(
		ctx,
		len(calls),
		parallelIndexedOptions{},
		func(_ context.Context, index int) error {
			if calls[index].failureKind != "" {
				outcomes[index].result = sdk.ToolResult{
					Content: calls[index].failureReason,
					IsError: true,
				}
				return nil
			}
			call := calls[index]
			callCtx := ctx
			if call.cancelsOnContextInjection() {
				callCtx = interruptCtx
			}
			result, err := awaitOperationRequestJSON[sdk.ToolResult](
				session.runtime,
				callCtx,
				call.operationRequest(),
				call.initial,
				call.asynchronous.PollCall,
				call.asynchronous.CancelCall,
				fmt.Sprintf("tool %q call", call.call.Name),
				fmt.Sprintf("tool %q result", call.call.Name),
			)
			if err != nil {
				if errors.Is(
					context.Cause(callCtx),
					errContextInjectionInterrupt,
				) {
					err = errContextInjectionInterrupt
				}
				return err
			}
			outcomes[index].result = result
			return nil
		},
	)
	for index, err := range errs {
		if err != nil {
			outcomes[index].err = err
		}
	}
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
		if errors.Is(outcome.err, errContextInjectionInterrupt) {
			return session.afterToolError(
				ctx,
				snapshot,
				turn,
				call.call,
				"interrupted",
				outcome.err.Error(),
				outcome.result,
			)
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
	payload, dispatched, err := dispatchMutableExecutionEvent(
		session.runtime,
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
	return session.afterTool(
		ctx,
		snapshot,
		turn,
		call,
		payload.Result,
		dispatched.Audit,
	)
}

func (session *Session) afterTool(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call sdk.ToolCall,
	result sdk.ToolResult,
	audits ...sdk.EventAudit,
) (sdk.ToolCall, sdk.ToolResult, error) {
	payload, dispatched, err := dispatchMutableExecutionEvent(
		session.runtime,
		ctx,
		snapshot,
		sdk.EventAfterTool,
		session.config.ID,
		sdk.AfterToolPayload{Turn: turn, Call: call, Result: result},
	)
	if err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	combinedAudits := append([]sdk.EventAudit(nil), audits...)
	combinedAudits = append(combinedAudits, dispatched.Audit)
	if err := session.appendTrajectoryWithAudit(
		ctx,
		snapshot,
		sdk.TrajectoryKindToolResult,
		payload,
		trajectoryAudits(combinedAudits...),
	); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	return call, payload.Result, nil
}
