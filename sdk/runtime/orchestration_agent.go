package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type scopedAgentInvoker struct {
	runtime          *Runtime
	snapshot         *registrySnapshot
	parentSession    *Session
	parentInvocation sdk.Invocation
	parentProvider   string
	forkAnchor       trajectoryForkAnchor
}

type agentSessionBinding struct {
	request    sdk.AgentRequest
	config     SessionConfig
	lineage    trajectorySessionLineage
	invocation sdk.Invocation
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
	if err := ensureAgentIdempotencyKey(&request); err != nil {
		return sdk.AgentResult{}, err
	}
	coordinate := request.Agent + "/" + request.IdempotencyKey
	groupCoordinate := ""
	if request.Group != "" {
		groupCoordinate = "agents/" + request.Group
	}
	invocation := invoker.childInvocation(childInvocationConfig{
		kind:            "agent",
		coordinate:      coordinate,
		groupCoordinate: groupCoordinate,
		targetSessionID: request.SessionID,
		dependencies:    request.Dependencies,
		ordinal:         request.Ordinal,
	})
	if request.SessionID == "" {
		request.SessionID = "agent-" + invocation.ID[:24]
		invocation.TargetSessionID = request.SessionID
	}
	if err := sdk.ValidateResourceName(
		"agent session",
		request.SessionID,
	); err != nil {
		return sdk.AgentResult{}, err
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
	target := localOperationTarget{
		runtime:          invoker.runtime,
		kind:             sdk.OperationKindAgent,
		resource:         request.Agent,
		resourceRevision: owned.resourceRevision(request.Agent),
		snapshot:         childSnapshot,
	}
	operationCtx := sdk.WithAgentInvoker(ctx, nil)
	operationCtx = sdk.WithWorkflowInvoker(operationCtx, nil)
	operationRequest := sdk.OperationRequest{
		IdempotencyKey: invocation.ID,
		Input:          requestRaw,
		Invocation:     invocation,
	}
	initial, err := target.submit(
		operationCtx,
		operationRequest,
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
	result, err := awaitOperationRequestJSON[sdk.AgentResult](
		invoker.runtime,
		ctx,
		operationRequest,
		initial,
		target.poll,
		target.cancel,
		fmt.Sprintf("agent %q invocation", request.Agent),
		fmt.Sprintf("agent %q result", request.Agent),
	)
	if err != nil {
		return sdk.AgentResult{}, err
	}
	return result, nil
}

func (invoker *scopedAgentInvoker) bindAgentSession(
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	providerName string,
	invocation sdk.Invocation,
) (agentSessionBinding, error) {
	if request.Mode == "" {
		request.Mode = sdk.AgentSessionNew
	}
	lineage, err := newTrajectorySessionLineage(
		invoker.parentSession,
		request.Mode,
		invocation,
		invoker.forkAnchor,
	)
	if err != nil {
		return agentSessionBinding{}, err
	}
	config := invoker.agentSessionConfig(request, spec, providerName)
	return agentSessionBinding{
		request:    request,
		config:     config,
		lineage:    lineage,
		invocation: invocation,
	}, nil
}

func (binding agentSessionBinding) validated(
	runtime *Runtime,
) (agentSessionBinding, error) {
	if err := validateSessionConfig(runtime, &binding.config); err != nil {
		return agentSessionBinding{}, err
	}
	return binding, nil
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

func ensureAgentIdempotencyKey(request *sdk.AgentRequest) error {
	if request.IdempotencyKey == "" {
		if request.SessionID == "" {
			return errors.New(
				"agent idempotency key is required unless session ID is provided",
			)
		}
		request.IdempotencyKey = request.SessionID
	}
	return sdk.ValidateResourceName(
		"agent idempotency key",
		request.IdempotencyKey,
	)
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

func (binding agentSessionBinding) creation(
	snapshot *registrySnapshot,
) trajectorySessionCreation {
	return trajectorySessionCreation{
		Label:                "agent session",
		Config:               binding.config,
		Snapshot:             snapshot,
		Lineage:              &binding.lineage,
		PinExecutionSnapshot: true,
		Invocation:           &binding.invocation,
	}
}

func (binding agentSessionBinding) validateExisting(
	metadata sdk.TrajectoryMetadata,
) error {
	return binding.lineage.validateExisting(
		metadata,
		binding.request.SessionID,
	)
}

func (invoker *scopedAgentInvoker) newAgentSession(
	ctx context.Context,
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	snapshot *registrySnapshot,
	providerName string,
	invocation sdk.Invocation,
) (*Session, error) {
	binding, err := invoker.bindAgentSession(
		request,
		spec,
		providerName,
		invocation,
	)
	if err != nil {
		return nil, err
	}
	binding, err = binding.validated(invoker.runtime)
	if err != nil {
		return nil, err
	}
	return invoker.createAgentSession(ctx, snapshot, binding)
}

func (invoker *scopedAgentInvoker) createAgentSession(
	ctx context.Context,
	snapshot *registrySnapshot,
	binding agentSessionBinding,
) (*Session, error) {
	releaseWork, err := invoker.runtime.beginTrajectoryWork()
	if err != nil {
		return nil, err
	}
	defer releaseWork()
	return invoker.runtime.createTrajectorySessionFromSnapshot(
		ctx,
		binding.creation(snapshot),
	)
}

func (invoker *scopedAgentInvoker) executeAgentSession(
	ctx context.Context,
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	snapshot *registrySnapshot,
	providerName string,
	invocation sdk.Invocation,
) (Result, error) {
	binding, err := invoker.bindAgentSession(
		request,
		spec,
		providerName,
		invocation,
	)
	if err != nil {
		return Result{}, err
	}
	binding, err = binding.validated(invoker.runtime)
	if err != nil {
		return Result{}, err
	}
	child, err := invoker.createAgentSession(ctx, snapshot, binding)
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
	if err := binding.validateExisting(metadata); err != nil {
		return Result{}, err
	}
	if result, handled, err := invoker.runtime.continueExistingExecution(
		ctx,
		metadata,
	); handled || err != nil {
		return result, err
	}
	child, err = invoker.resumeAgentSession(
		ctx,
		binding,
	)
	if err != nil {
		return Result{}, err
	}
	return child.Prompt(ctx, request.Prompt)
}

func (invoker *scopedAgentInvoker) agentSessionConfig(
	request sdk.AgentRequest,
	spec sdk.AgentSpec,
	providerName string,
) SessionConfig {
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
	return config
}

func (invoker *scopedAgentInvoker) resumeAgentSession(
	ctx context.Context,
	binding agentSessionBinding,
) (*Session, error) {
	config := binding.config
	config.ResumePolicy = ResumeExact
	session, err := invoker.runtime.ResumeSession(
		ctx,
		binding.request.SessionID,
		config,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"resume agent session trajectory %q: %w",
			binding.request.SessionID,
			err,
		)
	}
	session.applyInvocationScope(binding.invocation)
	return session, nil
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
		Messages:     sdk.CloneMessages(result.Messages),
		Turns:        result.Turns,
		ToolCalls:    result.ToolCalls,
		Generation:   result.Generation,
		Cause:        result.Cause,
	}
}
