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
)

// executionInvocation derives a stable causal node for one execution step.
func (session *Session) executionInvocation(
	kind string,
	coordinate string,
	groupCoordinate string,
	dependencies []string,
	ordinal uint32,
) sdk.Invocation {
	executionID, _ := session.activeExecution()
	invocation := sdk.Invocation{
		ID:           session.executionOperationKey(kind, coordinate),
		RootID:       executionID,
		ParentID:     executionID,
		SessionID:    session.config.ID,
		ExecutionID:  executionID,
		Dependencies: slices.Clone(dependencies),
		Ordinal:      ordinal,
	}
	if session.invocationRoot != "" {
		invocation.RootID = session.invocationRoot
	}
	if session.invocationParent != "" {
		invocation.ParentID = session.invocationParent
	}
	if groupCoordinate != "" {
		invocation.GroupID = session.executionOperationKey(
			"group",
			groupCoordinate,
		)
	}
	return invocation
}

type scopedAgentInvoker struct {
	runtime          *Runtime
	snapshot         *registrySnapshot
	parentSession    *Session
	parentInvocation sdk.Invocation
	parentProvider   string
	parentMessages   []sdk.Message
	forkHead         string
}

func (invoker *scopedAgentInvoker) InvokeAgent(
	ctx context.Context,
	request sdk.AgentRequest,
) (sdk.AgentResult, error) {
	if invoker == nil || invoker.runtime == nil ||
		invoker.snapshot == nil || invoker.parentSession == nil {
		return sdk.AgentResult{}, errors.New(
			"agent invoker is not initialized",
		)
	}
	if err := validateAgentRequest(&request); err != nil {
		return sdk.AgentResult{}, err
	}
	owned, exists := invoker.snapshot.agents[request.Agent]
	if !exists {
		return sdk.AgentResult{}, fmt.Errorf(
			"agent %q is not visible in the inherited environment",
			request.Agent,
		)
	}
	spec := sdk.CloneAgentSpec(owned.spec)
	if spec.MaxTurns > invoker.parentSession.config.MaxTurns {
		return sdk.AgentResult{}, fmt.Errorf(
			"agent %q max turns %d exceeds inherited limit %d",
			request.Agent,
			spec.MaxTurns,
			invoker.parentSession.config.MaxTurns,
		)
	}
	childSnapshot, providerName, err := narrowAgentSnapshot(
		invoker.snapshot,
		invoker.parentProvider,
		spec,
	)
	if err != nil {
		return sdk.AgentResult{}, err
	}
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = sdk.NewID()
	}
	invocationID := invoker.parentSession.executionOperationKey(
		"agent",
		invoker.parentInvocation.ID+"/"+request.Agent+"/"+
			request.IdempotencyKey,
	)
	if request.SessionID == "" {
		request.SessionID = "agent-" + invocationID[:24]
	}
	if err := sdk.ValidateResourceName(
		"agent session",
		request.SessionID,
	); err != nil {
		return sdk.AgentResult{}, err
	}
	invocation := sdk.Invocation{
		ID:              invocationID,
		RootID:          invoker.parentInvocation.RootID,
		ParentID:        invoker.parentInvocation.ID,
		SessionID:       invoker.parentInvocation.SessionID,
		TargetSessionID: request.SessionID,
		ExecutionID:     invoker.parentInvocation.ExecutionID,
		Dependencies:    append([]string(nil), request.Dependencies...),
		Ordinal:         request.Ordinal,
	}
	if invocation.RootID == "" {
		invocation.RootID = invoker.parentInvocation.ID
	}
	if request.Group != "" {
		invocation.GroupID =
			invoker.parentSession.executionOperationKey(
				"group",
				"agents/"+request.Group,
			)
	}
	if err := sdk.ValidateInvocation(invocation); err != nil {
		return sdk.AgentResult{}, err
	}
	requestRaw, err := json.Marshal(request)
	if err != nil {
		return sdk.AgentResult{}, fmt.Errorf(
			"encode agent %q request: %w",
			request.Agent,
			err,
		)
	}
	revision := sdk.ResourceRevision(
		owned.owner.manifest,
		string(sdk.OperationKindAgent),
		request.Agent,
		spec,
	)
	pin, err := invoker.runtime.acquireRegistrySnapshot(
		invoker.snapshot,
	)
	if err != nil {
		return sdk.AgentResult{}, err
	}
	defer pin.release()
	operationCtx := sdk.WithAgentInvoker(ctx, nil)
	operationCtx = sdk.WithWorkflowInvoker(operationCtx, nil)
	initial, err := invoker.runtime.submitLocalOperation(
		operationCtx,
		sdk.OperationKindAgent,
		request.Agent,
		revision,
		sdk.OperationRequest{
			IdempotencyKey: invocationID,
			Input:          requestRaw,
			Invocation:     invocation,
		},
		func(
			executionCtx context.Context,
			_ json.RawMessage,
		) (json.RawMessage, error) {
			result, executeErr := invoker.executeAgentSession(
				executionCtx,
				request,
				spec,
				childSnapshot,
				providerName,
				invocation,
			)
			if executeErr != nil {
				return nil, executeErr
			}
			return json.Marshal(agentRuntimeResult(
				invocation.ID,
				request.SessionID,
				result,
			))
		},
	)
	if err != nil {
		return sdk.AgentResult{}, fmt.Errorf(
			"submit agent %q invocation: %w",
			request.Agent,
			err,
		)
	}
	operation, err := invoker.runtime.awaitOperation(
		ctx,
		initial,
		func(
			pollCtx context.Context,
			id string,
			_ uint64,
		) (sdk.Operation, error) {
			return invoker.runtime.pollLocalOperation(
				pollCtx,
				sdk.OperationKindAgent,
				request.Agent,
				id,
			)
		},
		func(
			cancelCtx context.Context,
			id string,
		) (sdk.Operation, error) {
			return invoker.runtime.cancelLocalOperation(
				cancelCtx,
				sdk.OperationKindAgent,
				request.Agent,
				id,
			)
		},
	)
	if err != nil {
		return sdk.AgentResult{}, fmt.Errorf(
			"agent %q invocation: %w",
			request.Agent,
			err,
		)
	}
	var result sdk.AgentResult
	if err := json.Unmarshal(operation.Output, &result); err != nil {
		return sdk.AgentResult{}, fmt.Errorf(
			"decode agent %q result: %w",
			request.Agent,
			err,
		)
	}
	return result, nil
}

func validateAgentRequest(request *sdk.AgentRequest) error {
	if request == nil {
		return errors.New("agent request is nil")
	}
	if err := sdk.ValidateResourceName(
		"agent",
		request.Agent,
	); err != nil {
		return err
	}
	if strings.TrimSpace(request.Prompt) == "" {
		return errors.New("agent prompt is empty")
	}
	if request.Mode == "" {
		request.Mode = sdk.AgentSessionNew
	}
	switch request.Mode {
	case sdk.AgentSessionNew, sdk.AgentSessionFork:
	default:
		return fmt.Errorf(
			"unknown agent session mode %q",
			request.Mode,
		)
	}
	if request.IdempotencyKey != "" {
		if err := sdk.ValidateResourceName(
			"agent idempotency key",
			request.IdempotencyKey,
		); err != nil {
			return err
		}
	}
	return nil
}

func narrowAgentSnapshot(
	parent *registrySnapshot,
	parentProvider string,
	spec sdk.AgentSpec,
) (*registrySnapshot, string, error) {
	result := parent.clone()
	providerName := spec.Provider
	if providerName == "" {
		providerName = parentProvider
	}
	var err error
	providerName, err = selectProviderName(result, providerName)
	if err != nil {
		return nil, "", fmt.Errorf(
			"select provider for agent %q: %w",
			spec.Name,
			err,
		)
	}
	provider, exists := result.providers[providerName]
	if !exists {
		return nil, "", fmt.Errorf(
			"agent %q requested unavailable provider %q",
			spec.Name,
			providerName,
		)
	}
	result.providers = map[string]ownedResource[
		sdk.Provider,
		sdk.ProviderSpec,
	]{providerName: provider}
	if spec.Tools != nil {
		tools := make(
			map[string]ownedResource[sdk.Tool, sdk.ToolSpec],
			len(spec.Tools),
		)
		for _, name := range spec.Tools {
			tool, visible := parent.tools[name]
			if !visible {
				return nil, "", fmt.Errorf(
					"agent %q requested unavailable tool %q",
					spec.Name,
					name,
				)
			}
			tools[name] = tool
		}
		result.tools = tools
	}
	return result, providerName, nil
}

func (invoker *scopedAgentInvoker) newAgentSession(
	ctx context.Context,
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	snapshot *registrySnapshot,
	providerName string,
	invocation sdk.Invocation,
) (*Session, error) {
	config := SessionConfig{
		ID:       request.SessionID,
		Provider: providerName,
		System:   spec.System,
		MaxTurns: spec.MaxTurns,
	}
	if config.System == "" {
		config.System = invoker.parentSession.config.System
	}
	if config.MaxTurns == 0 {
		config.MaxTurns = invoker.parentSession.config.MaxTurns
	}
	if err := validateSessionConfig(invoker.runtime, &config); err != nil {
		return nil, err
	}
	environment, err := newTrajectoryEnvironment(
		invoker.runtime,
		snapshot,
		config,
	)
	if err != nil {
		return nil, err
	}
	environment.ParentSessionID = invoker.parentSession.ID()
	environment.OriginInvocationID = invocation.ID
	environment.OriginInvocationRootID = invocation.RootID
	environment.OriginMode = request.Mode
	trajectory := sdk.Trajectory{
		ID:          config.ID,
		Environment: environment,
	}
	var messages []sdk.Message
	var head string
	if request.Mode == sdk.AgentSessionFork {
		if invoker.forkHead == "" {
			return nil, errors.New(
				"cannot fork agent session without a parent trajectory head",
			)
		}
		trajectory.ParentID = invoker.parentSession.ID()
		trajectory.ParentEntryID = invoker.forkHead
		messages = cloneMessages(invoker.parentMessages)
		head = invoker.forkHead
	}
	if err := invoker.runtime.trajectories.Create(
		ctx,
		trajectory,
	); err != nil {
		return nil, fmt.Errorf(
			"create agent session trajectory %q: %w",
			config.ID,
			err,
		)
	}
	return &Session{
		runtime:          invoker.runtime,
		config:           config,
		messages:         messages,
		head:             head,
		pinnedSnapshot:   snapshot,
		invocationRoot:   invocation.RootID,
		invocationParent: invocation.ID,
	}, nil
}

func (invoker *scopedAgentInvoker) executeAgentSession(
	ctx context.Context,
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	snapshot *registrySnapshot,
	providerName string,
	invocation sdk.Invocation,
) (Result, error) {
	child, err := invoker.newAgentSession(
		ctx,
		request,
		spec,
		snapshot,
		providerName,
		invocation,
	)
	if err == nil {
		return child.Prompt(ctx, request.Prompt)
	}
	if !errors.Is(err, sdk.ErrTrajectoryExists) {
		return Result{}, err
	}
	metadata, loadErr := invoker.runtime.trajectories.LoadMetadata(
		ctx,
		request.SessionID,
	)
	if loadErr != nil {
		return Result{}, errors.Join(err, loadErr)
	}
	if metadata.Environment.OriginInvocationID != invocation.ID ||
		metadata.Environment.ParentSessionID !=
			invoker.parentSession.ID() {
		return Result{}, fmt.Errorf(
			"agent session %q already belongs to invocation %q from parent %q",
			request.SessionID,
			metadata.Environment.OriginInvocationID,
			metadata.Environment.ParentSessionID,
		)
	}
	if metadata.Execution != nil {
		if !metadata.Execution.Terminal() {
			if metadata.Execution.State ==
				sdk.TrajectoryExecutionRunning &&
				metadata.Execution.LeaseExpiresAt.After(
					time.Now(),
				) {
				timer := time.NewTimer(time.Until(
					metadata.Execution.LeaseExpiresAt,
				))
				defer timer.Stop()
				select {
				case <-ctx.Done():
					return Result{}, ctx.Err()
				case <-timer.C:
				}
			}
			return invoker.runtime.RecoverExecution(
				ctx,
				request.SessionID,
			)
		}
		if metadata.Execution.State !=
			sdk.TrajectoryExecutionSucceeded {
			return Result{}, fmt.Errorf(
				"agent session %q ended in state %q: %s",
				request.SessionID,
				metadata.Execution.State,
				metadata.Execution.LastError,
			)
		}
		result, resultErr := LoadExecutionResult(
			ctx,
			invoker.runtime.trajectories,
			metadata,
		)
		if resultErr != nil {
			return Result{}, resultErr
		}
		if result == nil {
			return Result{}, fmt.Errorf(
				"agent session %q succeeded without a checkpoint result",
				request.SessionID,
			)
		}
		return *result, nil
	}
	child = &Session{
		runtime: invoker.runtime,
		config: SessionConfig{
			ID:       request.SessionID,
			Provider: providerName,
			System:   spec.System,
			MaxTurns: spec.MaxTurns,
		},
		head:             metadata.Head,
		pinnedSnapshot:   snapshot,
		invocationRoot:   invocation.RootID,
		invocationParent: invocation.ID,
	}
	if child.config.System == "" {
		child.config.System = invoker.parentSession.config.System
	}
	if child.config.MaxTurns == 0 {
		child.config.MaxTurns =
			invoker.parentSession.config.MaxTurns
	}
	if request.Mode == sdk.AgentSessionFork {
		child.messages = cloneMessages(invoker.parentMessages)
	}
	return child.Prompt(ctx, request.Prompt)
}

func agentRuntimeResult(
	invocationID string,
	sessionID string,
	result Result,
) sdk.AgentResult {
	return sdk.AgentResult{
		InvocationID: invocationID,
		SessionID:    sessionID,
		Output:       result.Output,
		Messages:     cloneMessages(result.Messages),
		Turns:        result.Turns,
		ToolCalls:    result.ToolCalls,
		Generation:   result.Generation,
		Cause:        result.Cause,
	}
}
