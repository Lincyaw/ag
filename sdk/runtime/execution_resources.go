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
		advertised.Concurrency = owned.spec.Concurrency
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
	correlationID string
	asynchronous  sdk.AsyncTool
	initial       sdk.Operation
	failure       *toolCallFailure
	forkAnchor    trajectoryForkAnchor
	interrupt     sdk.ToolInterruptBehavior
	concurrency   sdk.ToolConcurrency
}

type toolCallFailure struct {
	kind   sdk.ToolErrorKind
	reason string
}

func newToolCallFailure(
	kind sdk.ToolErrorKind,
	reason string,
) *toolCallFailure {
	if kind == "" {
		kind = sdk.ToolErrorBlocked
	}
	return &toolCallFailure{kind: kind, reason: reason}
}

type toolCallOutcome struct {
	result sdk.ToolResult
	err    error
}

type toolCallBatch struct {
	start int
	end   int
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
		call:          call,
		invocation:    invocation,
		correlationID: providerInvocationID,
		interrupt:     sdk.ToolInterruptBlock,
		concurrency:   sdk.ToolConcurrencyExclusive,
	}
	if err := session.appendTrajectoryWithAudit(
		ctx,
		snapshot,
		sdk.TrajectoryKindToolCall,
		durability.ToolCall{
			Turn:          turn,
			Call:          call,
			OperationKey:  invocation.ID,
			CorrelationID: providerInvocationID,
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
		prepared.failure = newToolCallFailure(
			sdk.ToolErrorKind(before.Block.Kind),
			before.Block.Reason,
		)
		return prepared, nil
	}

	tool, exists := toolIndex[call.Name]
	if !exists {
		prepared.failure = newToolCallFailure(
			sdk.ToolErrorUnknownTool,
			fmt.Sprintf("unknown or unavailable tool %q", call.Name),
		)
		return prepared, nil
	}
	prepared.interrupt = tool.spec.EffectiveInterruptBehavior()
	prepared.concurrency = tool.spec.EffectiveConcurrency()
	prepared.asynchronous = tool.value
	return prepared, nil
}

func (call preparedToolCall) cancelsOnContextInjection() bool {
	return call.interrupt == sdk.ToolInterruptCancel
}

func (call preparedToolCall) runsInParallel() bool {
	return call.concurrency == sdk.ToolConcurrencyParallel
}

func toolCallsCancelOnContextInjection(calls []preparedToolCall) bool {
	for _, call := range calls {
		if call.cancelsOnContextInjection() {
			return true
		}
	}
	return false
}

func toolCallExecutionBatches(
	calls []preparedToolCall,
) []toolCallBatch {
	if len(calls) == 0 {
		return nil
	}
	batches := make([]toolCallBatch, 0, len(calls))
	for index := 0; index < len(calls); {
		if !calls[index].runsInParallel() {
			batches = append(batches, toolCallBatch{
				start: index,
				end:   index + 1,
			})
			index++
			continue
		}
		start := index
		for index < len(calls) && calls[index].runsInParallel() {
			index++
		}
		batches = append(batches, toolCallBatch{
			start: start,
			end:   index,
		})
	}
	return batches
}

func (session *Session) submitToolCall(
	ctx context.Context,
	snapshot *registrySnapshot,
	providerName string,
	call *preparedToolCall,
) {
	if call.failure != nil {
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
			call.failure = newToolCallFailure(
				sdk.ToolErrorInterrupted,
				errContextInjectionInterrupt.Error(),
			)
			return
		}
		call.failure = newToolCallFailure(
			sdk.ToolErrorExecutionFailed,
			fmt.Sprintf("submit tool %q call: %v", call.call.Name, err),
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
		calls[index].failure = newToolCallFailure(
			sdk.ToolErrorExecutionFailed,
			fmt.Sprintf("submit tool %q call: %v", calls[index].call.Name, err),
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
			if calls[index].failure != nil {
				outcomes[index].result = sdk.ToolResult{
					Content: calls[index].failure.reason,
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
				call,
				sdk.ToolErrorInterrupted,
				outcome.err.Error(),
				outcome.result,
			)
		}
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call,
			sdk.ToolErrorExecutionFailed,
			outcome.err.Error(),
			outcome.result,
		)
	}
	if call.failure != nil {
		return session.afterToolError(
			ctx,
			snapshot,
			turn,
			call,
			call.failure.kind,
			call.failure.reason,
			outcome.result,
		)
	}
	return session.afterTool(
		ctx,
		snapshot,
		turn,
		call,
		outcome.result,
	)
}

func (session *Session) afterToolError(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call preparedToolCall,
	kind sdk.ToolErrorKind,
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
			Call:   call.call,
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
	call preparedToolCall,
	result sdk.ToolResult,
	audits ...sdk.EventAudit,
) (sdk.ToolCall, sdk.ToolResult, error) {
	payload, dispatched, err := dispatchMutableExecutionEvent(
		session.runtime,
		ctx,
		snapshot,
		sdk.EventAfterTool,
		session.config.ID,
		sdk.AfterToolPayload{
			Turn:   turn,
			Call:   call.call,
			Result: result,
		},
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
		durability.ToolResult{
			AfterToolPayload: payload,
			CorrelationID:    call.correlationID,
		},
		trajectoryAudits(combinedAudits...),
	); err != nil {
		return sdk.ToolCall{}, sdk.ToolResult{}, err
	}
	return call.call, payload.Result, nil
}
