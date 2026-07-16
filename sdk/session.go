package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

type SessionConfig struct {
	ID       string
	Provider string
	System   string
	MaxTurns int
}

type Session struct {
	runtime  *Runtime
	config   SessionConfig
	mu       sync.Mutex
	messages []Message
	head     string
}

type Result struct {
	Output     string    `json:"output"`
	Messages   []Message `json:"messages"`
	Turns      int       `json:"turns"`
	ToolCalls  int       `json:"tool_calls"`
	Generation uint64    `json:"generation"`
	Cause      Cause     `json:"cause"`
}

type trajectoryCheckpoint struct {
	Messages  []Message `json:"messages"`
	System    string    `json:"system,omitempty"`
	Turns     int       `json:"turns"`
	ToolCalls int       `json:"tool_calls"`
	Action    Action    `json:"action"`
}

type trajectoryProviderRequest struct {
	Turn     int          `json:"turn"`
	Provider string       `json:"provider"`
	Request  ModelRequest `json:"request"`
}

type trajectoryProviderResponse struct {
	Turn     int            `json:"turn"`
	Provider string         `json:"provider"`
	Response *ModelResponse `json:"response,omitempty"`
	Error    string         `json:"error,omitempty"`
}

type trajectoryDecision struct {
	Turn   int    `json:"turn"`
	Action Action `json:"action"`
}

func latestTrajectoryCheckpoint(
	trajectory Trajectory,
) (string, *trajectoryCheckpoint, error) {
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		return "", nil, err
	}
	for index := len(branch) - 1; index >= 0; index-- {
		if branch[index].Kind != TrajectoryKindCheckpoint {
			continue
		}
		var checkpoint trajectoryCheckpoint
		if err := json.Unmarshal(branch[index].Payload, &checkpoint); err != nil {
			return "", nil, fmt.Errorf(
				"decode trajectory %q checkpoint %q: %w",
				trajectory.ID,
				branch[index].ID,
				err,
			)
		}
		checkpoint.Messages = cloneMessages(checkpoint.Messages)
		return branch[index].ID, &checkpoint, nil
	}
	return "", nil, nil
}

func checkpointMessages(checkpoint *trajectoryCheckpoint) []Message {
	if checkpoint == nil {
		return nil
	}
	return checkpoint.Messages
}

func (runtime *Runtime) NewSession(
	ctx context.Context,
	config SessionConfig,
) (*Session, error) {
	if err := validateSessionConfig(runtime, &config); err != nil {
		return nil, err
	}
	if err := runtime.trajectories.Create(ctx, Trajectory{ID: config.ID}); err != nil {
		return nil, fmt.Errorf("create session trajectory %q: %w", config.ID, err)
	}
	return &Session{runtime: runtime, config: config}, nil
}

func (runtime *Runtime) ResumeSession(
	ctx context.Context,
	id string,
	config SessionConfig,
) (*Session, error) {
	config.ID = id
	if err := validateSessionConfig(runtime, &config); err != nil {
		return nil, err
	}
	trajectory, err := runtime.trajectories.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load session trajectory %q: %w", id, err)
	}
	checkpointID, checkpoint, err := latestTrajectoryCheckpoint(trajectory)
	if err != nil {
		return nil, err
	}
	if checkpoint != nil {
		config.System = checkpoint.System
	}
	head := trajectory.Head
	if trajectory.Head != "" && checkpointID != trajectory.Head {
		payload, marshalErr := json.Marshal(map[string]string{
			"from": trajectory.Head,
			"to":   checkpointID,
		})
		if marshalErr != nil {
			return nil, marshalErr
		}
		restore := TrajectoryEntry{
			ID:        newDispatchID(),
			ParentID:  checkpointID,
			Kind:      TrajectoryKindRestore,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		}
		head, err = runtime.trajectories.Append(
			ctx,
			id,
			trajectory.Head,
			restore,
		)
		if err != nil {
			return nil, fmt.Errorf("restore session trajectory %q: %w", id, err)
		}
	}
	session := &Session{
		runtime:  runtime,
		config:   config,
		messages: cloneMessages(checkpointMessages(checkpoint)),
		head:     head,
	}
	runtime.emitTrajectoryEvent(ctx, EventTrajectoryRestore, TrajectoryEventPayload{
		TrajectoryID: id,
		EntryID:      head,
		EntryKind:    TrajectoryKindRestore,
		From:         trajectory.Head,
		To:           checkpointID,
	})
	return session, nil
}

func validateSessionConfig(runtime *Runtime, config *SessionConfig) error {
	if runtime == nil {
		return errors.New("runtime is nil")
	}
	if config.ID == "" {
		config.ID = newDispatchID()
	}
	if err := validateResourceName("session", config.ID); err != nil {
		return err
	}
	if config.MaxTurns == 0 {
		config.MaxTurns = 8
	}
	if config.MaxTurns < 1 {
		return errors.New("session max turns must be positive")
	}
	return nil
}

func (session *Session) ID() string {
	return session.config.ID
}

func (session *Session) Messages() []Message {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneMessages(session.messages)
}

func (runtime *Runtime) RollbackTrajectory(
	ctx context.Context,
	id string,
	checkpointID string,
) error {
	if runtime == nil {
		return errors.New("runtime is nil")
	}
	trajectory, err := runtime.trajectories.Load(ctx, id)
	if err != nil {
		return err
	}
	var target *TrajectoryEntry
	for index := range trajectory.Entries {
		entry := &trajectory.Entries[index]
		if entry.ID == checkpointID {
			target = entry
			break
		}
	}
	if target == nil {
		err := fmt.Errorf(
			"trajectory %q checkpoint %q not found",
			id,
			checkpointID,
		)
		return err
	}
	if target.Kind != TrajectoryKindCheckpoint {
		err := fmt.Errorf(
			"trajectory entry %q is %q, not a checkpoint",
			checkpointID,
			target.Kind,
		)
		return err
	}
	payload, err := json.Marshal(map[string]string{
		"from": trajectory.Head,
		"to":   checkpointID,
	})
	if err != nil {
		return err
	}
	entry := TrajectoryEntry{
		ID:        newDispatchID(),
		ParentID:  checkpointID,
		Kind:      TrajectoryKindRollback,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
	head, err := runtime.trajectories.Append(
		ctx,
		id,
		trajectory.Head,
		entry,
	)
	if err != nil {
		return err
	}
	runtime.emitTrajectoryEvent(ctx, EventTrajectoryRollback, TrajectoryEventPayload{
		TrajectoryID: id,
		EntryID:      head,
		EntryKind:    TrajectoryKindRollback,
		From:         trajectory.Head,
		To:           checkpointID,
	})
	runtime.logger.InfoContext(
		ctx,
		"trajectory rolled back",
		"trajectory_id",
		id,
		"from",
		trajectory.Head,
		"to",
		checkpointID,
	)
	return nil
}

func (session *Session) Rollback(
	ctx context.Context,
	checkpointID string,
) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.runtime.RollbackTrajectory(
		ctx,
		session.config.ID,
		checkpointID,
	); err != nil {
		return err
	}
	trajectory, err := session.runtime.trajectories.Load(ctx, session.config.ID)
	if err != nil {
		return err
	}
	_, checkpoint, err := latestTrajectoryCheckpoint(trajectory)
	if err != nil {
		return err
	}
	if checkpoint == nil {
		return errors.New("rollback target did not produce a checkpoint")
	}
	session.messages = cloneMessages(checkpoint.Messages)
	session.config.System = checkpoint.System
	session.head = trajectory.Head
	return nil
}

func (session *Session) Prompt(
	ctx context.Context,
	prompt string,
) (Result, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if strings.TrimSpace(prompt) == "" {
		return Result{}, errors.New("prompt is empty")
	}

	messages := cloneMessages(session.messages)
	userMessage := Message{Role: RoleUser, Content: prompt}
	messages = append(messages, userMessage)
	system := session.config.System
	result := Result{}

	startLease, err := session.runtime.acquireSnapshot()
	if err != nil {
		return Result{}, err
	}
	if err := session.appendTrajectory(
		ctx,
		TrajectoryKindUserMessage,
		startLease.snapshot.generation,
		userMessage,
	); err != nil {
		startLease.release()
		return Result{}, err
	}
	startDispatch, err := session.runtime.dispatch(
		ctx,
		startLease.snapshot,
		EventBeforeAgentStart,
		session.config.ID,
		BeforeAgentStartPayload{Messages: messages, System: system},
	)
	if err != nil {
		startLease.release()
		return Result{}, err
	}
	var beforeStart BeforeAgentStartPayload
	if err := decodePayload(startDispatch.Event, &beforeStart); err != nil {
		startLease.release()
		return Result{}, err
	}
	messages = cloneMessages(beforeStart.Messages)
	system = beforeStart.System
	if startDispatch.Block != nil {
		cause := Cause{
			Code:   "prompt_blocked",
			Detail: startDispatch.Block.Reason,
		}
		result, err = session.finish(
			ctx,
			startLease.snapshot,
			messages,
			result,
			cause,
		)
		startLease.release()
		session.messages = cloneMessages(messages)
		return result, err
	}
	if _, err := session.runtime.dispatch(
		ctx,
		startLease.snapshot,
		EventAgentStart,
		session.config.ID,
		AgentStartPayload{Messages: messages, System: system},
	); err != nil {
		startLease.release()
		return Result{}, err
	}
	if err := session.appendTrajectory(
		ctx,
		TrajectoryKindAgentStart,
		startLease.snapshot.generation,
		AgentStartPayload{Messages: messages, System: system},
	); err != nil {
		startLease.release()
		return Result{}, err
	}
	startLease.release()

	for turn := 0; turn < session.config.MaxTurns; turn++ {
		if err := ctx.Err(); err != nil {
			cause := Cause{Code: "cancelled", Detail: err.Error(), Final: true}
			result.Cause = cause
			result.Messages = cloneMessages(messages)
			session.messages = cloneMessages(messages)
			return result, err
		}

		lease, err := session.runtime.acquireSnapshot()
		if err != nil {
			return Result{}, err
		}
		result.Generation = lease.snapshot.generation
		if _, err := session.runtime.dispatch(
			ctx,
			lease.snapshot,
			EventTurnStart,
			session.config.ID,
			TurnStartPayload{Turn: turn},
		); err != nil {
			lease.release()
			return Result{}, err
		}

		providerName, provider, err := selectProvider(
			lease.snapshot,
			session.config.Provider,
		)
		if err != nil {
			lease.release()
			return Result{}, err
		}
		toolSpecs := snapshotToolSpecs(lease.snapshot)
		beforeProvider, err := session.runtime.dispatch(
			ctx,
			lease.snapshot,
			EventBeforeProvider,
			session.config.ID,
			BeforeProviderPayload{
				Turn:     turn,
				Messages: cloneMessages(messages),
				Provider: providerName,
				System:   system,
				Tools:    toolSpecs,
			},
		)
		if err != nil {
			lease.release()
			return Result{}, err
		}
		var providerPayload BeforeProviderPayload
		if err := decodePayload(beforeProvider.Event, &providerPayload); err != nil {
			lease.release()
			return Result{}, err
		}
		providerName = providerPayload.Provider
		ownedProvider, exists := lease.snapshot.providers[providerName]
		if !exists {
			lease.release()
			err := fmt.Errorf(
				"before_provider selected unregistered provider %q",
				providerName,
			)
			return Result{}, err
		}
		provider = ownedProvider.provider
		advertisedTools, toolIndex, err := resolveAdvertisedTools(
			lease.snapshot,
			providerPayload.Tools,
		)
		if err != nil {
			lease.release()
			return Result{}, err
		}

		requestMessages := cloneMessages(providerPayload.Messages)
		if providerPayload.System != "" {
			requestMessages = append(
				[]Message{{Role: RoleSystem, Content: providerPayload.System}},
				requestMessages...,
			)
		}
		request := ModelRequest{
			Messages: requestMessages,
			Tools:    advertisedTools,
		}
		if err := session.appendTrajectory(
			ctx,
			TrajectoryKindProviderRequest,
			lease.snapshot.generation,
			trajectoryProviderRequest{
				Turn:     turn,
				Provider: providerName,
				Request:  request,
			},
		); err != nil {
			lease.release()
			return Result{}, err
		}
		response, callErr := session.complete(ctx, provider, request)
		afterPayload := AfterProviderPayload{
			Turn:     turn,
			Provider: providerName,
		}
		if callErr != nil {
			afterPayload.Error = callErr.Error()
		} else {
			afterPayload.Response = &response
		}
		_, dispatchErr := session.runtime.dispatch(
			ctx,
			lease.snapshot,
			EventAfterProvider,
			session.config.ID,
			afterPayload,
		)
		trajectoryErr := session.appendTrajectory(
			ctx,
			TrajectoryKindProviderResponse,
			lease.snapshot.generation,
			trajectoryProviderResponse(afterPayload),
		)
		callErr = errors.Join(callErr, dispatchErr, trajectoryErr)
		if callErr != nil {
			cause := Cause{
				Code:   "provider_error",
				Detail: callErr.Error(),
			}
			var finishErr error
			result, finishErr = session.finish(
				ctx,
				lease.snapshot,
				messages,
				result,
				cause,
			)
			lease.release()
			session.messages = cloneMessages(messages)
			return result, errors.Join(callErr, finishErr)
		}

		assistant := Message{
			Role:      RoleAssistant,
			Content:   response.Content,
			ToolCalls: cloneToolCalls(response.ToolCalls),
		}
		messages = append(messages, assistant)
		result.Output = response.Content
		result.Turns = turn + 1

		toolResults := make([]ToolResult, 0, len(response.ToolCalls))
		for _, rawCall := range response.ToolCalls {
			call, toolResult, err := session.executeTool(
				ctx,
				lease.snapshot,
				turn,
				rawCall,
				toolIndex,
			)
			if err != nil {
				cause := Cause{Code: "hook_error", Detail: err.Error()}
				result, finishErr := session.finish(
					ctx,
					lease.snapshot,
					messages,
					result,
					cause,
				)
				lease.release()
				session.messages = cloneMessages(messages)
				return result, errors.Join(err, finishErr)
			}
			if len(response.ToolCalls) > 0 {
				messages = append(messages, Message{
					Role:       RoleTool,
					Content:    toolResult.Content,
					ToolCallID: call.ID,
				})
				result.ToolCalls++
				toolResults = append(toolResults, toolResult)
			}
		}

		defaultAction := Action{
			Kind:  ActionStop,
			Cause: &Cause{Code: "model_end"},
		}
		if len(response.ToolCalls) > 0 {
			defaultAction = Action{Kind: ActionStep}
			if turn+1 >= session.config.MaxTurns {
				defaultAction = Action{
					Kind: ActionStop,
					Cause: &Cause{
						Code:  "max_turns",
						Final: true,
					},
				}
			}
		}
		decision, err := session.runtime.dispatch(
			ctx,
			lease.snapshot,
			EventDecide,
			session.config.ID,
			DecidePayload{
				Turn:        turn,
				Default:     defaultAction,
				Response:    response,
				ToolResults: toolResults,
			},
		)
		if err != nil {
			lease.release()
			return Result{}, err
		}
		action := resolveAction(defaultAction, decision.Actions)
		if err := session.appendTrajectory(
			ctx,
			TrajectoryKindDecision,
			lease.snapshot.generation,
			trajectoryDecision{Turn: turn, Action: action},
		); err != nil {
			lease.release()
			return Result{}, err
		}
		if _, err := session.runtime.dispatch(
			ctx,
			lease.snapshot,
			EventTurnEnd,
			session.config.ID,
			TurnEndPayload{
				Turn:     turn,
				Messages: cloneMessages(messages),
				Action:   action,
			},
		); err != nil {
			lease.release()
			return Result{}, err
		}

		if action.Kind == ActionInject {
			messages = append(messages, cloneMessages(action.Messages)...)
		}
		switch action.Kind {
		case ActionInject, ActionStep, ActionStop:
			if err := session.checkpointTrajectory(
				ctx,
				lease.snapshot.generation,
				messages,
				result,
				action,
				system,
			); err != nil {
				lease.release()
				return Result{}, err
			}
		default:
			lease.release()
			return Result{}, fmt.Errorf("unknown resolved action %q", action.Kind)
		}

		switch action.Kind {
		case ActionInject:
			lease.release()
			continue
		case ActionStep:
			lease.release()
			continue
		case ActionStop:
			cause := Cause{Code: "model_end"}
			if action.Cause != nil {
				cause = *action.Cause
			}
			result, err = session.finish(
				ctx,
				lease.snapshot,
				messages,
				result,
				cause,
			)
			lease.release()
			session.messages = cloneMessages(messages)
			return result, err
		}
	}

	err = errors.New("agent loop exited without a terminal action")
	return Result{}, err
}

func (session *Session) complete(
	ctx context.Context,
	provider Provider,
	request ModelRequest,
) (ModelResponse, error) {
	spec := provider.Spec()
	if asynchronous, ok := provider.(AsyncProvider); ok {
		input, err := json.Marshal(request)
		if err != nil {
			return ModelResponse{}, fmt.Errorf("encode provider %q request: %w", spec.Name, err)
		}
		initial, err := asynchronous.SubmitCompletion(ctx, OperationRequest{
			IdempotencyKey: session.head,
			Input:          input,
		})
		if err != nil {
			return ModelResponse{}, fmt.Errorf("submit provider %q completion: %w", spec.Name, err)
		}
		if initial.IdempotencyKey != session.head {
			return ModelResponse{}, fmt.Errorf(
				"provider %q returned operation with unexpected idempotency key",
				spec.Name,
			)
		}
		operation, err := session.runtime.awaitOperation(
			ctx,
			initial,
			asynchronous.PollCompletion,
			asynchronous.CancelCompletion,
		)
		if err != nil {
			return ModelResponse{}, fmt.Errorf("provider %q completion: %w", spec.Name, err)
		}
		var response ModelResponse
		if err := json.Unmarshal(operation.Output, &response); err != nil {
			return ModelResponse{}, fmt.Errorf("decode provider %q response: %w", spec.Name, err)
		}
		return response, nil
	}
	synchronous, ok := provider.(SyncProvider)
	if !ok {
		return ModelResponse{}, fmt.Errorf("provider %q has no execution implementation", spec.Name)
	}
	response, err := synchronous.Complete(ctx, request)
	if err != nil {
		return ModelResponse{}, fmt.Errorf("provider %q completion: %w", spec.Name, err)
	}
	return response, nil
}

func (session *Session) executeTool(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	rawCall ToolCall,
	toolIndex map[string]Tool,
) (ToolCall, ToolResult, error) {
	before, err := session.runtime.dispatch(
		ctx,
		snapshot,
		EventBeforeTool,
		session.config.ID,
		BeforeToolPayload{Turn: turn, Call: rawCall},
	)
	if err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	var payload BeforeToolPayload
	if err := decodePayload(before.Event, &payload); err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	call := payload.Call
	if err := session.appendTrajectory(
		ctx,
		TrajectoryKindToolCall,
		snapshot.generation,
		BeforeToolPayload{Turn: turn, Call: call},
	); err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	if before.Block != nil {
		result := ToolResult{
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
		result := ToolResult{
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

	result, callErr := session.callTool(ctx, tool, call.Arguments)
	if callErr != nil {
		result = ToolResult{Content: callErr.Error(), IsError: true}
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
	tool Tool,
	arguments json.RawMessage,
) (ToolResult, error) {
	spec := tool.Spec()
	if asynchronous, ok := tool.(AsyncTool); ok {
		initial, err := asynchronous.SubmitCall(ctx, OperationRequest{
			IdempotencyKey: session.head,
			Input:          append(json.RawMessage(nil), arguments...),
		})
		if err != nil {
			return ToolResult{}, fmt.Errorf("submit tool %q call: %w", spec.Name, err)
		}
		if initial.IdempotencyKey != session.head {
			return ToolResult{}, fmt.Errorf(
				"tool %q returned operation with unexpected idempotency key",
				spec.Name,
			)
		}
		operation, err := session.runtime.awaitOperation(
			ctx,
			initial,
			asynchronous.PollCall,
			asynchronous.CancelCall,
		)
		if err != nil {
			return ToolResult{}, fmt.Errorf("tool %q call: %w", spec.Name, err)
		}
		var result ToolResult
		if err := json.Unmarshal(operation.Output, &result); err != nil {
			return ToolResult{}, fmt.Errorf("decode tool %q result: %w", spec.Name, err)
		}
		return result, nil
	}
	synchronous, ok := tool.(SyncTool)
	if !ok {
		return ToolResult{}, fmt.Errorf("tool %q has no execution implementation", spec.Name)
	}
	return synchronous.Call(ctx, arguments)
}

func (session *Session) afterToolError(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call ToolCall,
	kind string,
	reason string,
	result ToolResult,
) (ToolCall, ToolResult, error) {
	dispatched, err := session.runtime.dispatch(
		ctx,
		snapshot,
		EventToolError,
		session.config.ID,
		ToolErrorPayload{
			Turn:   turn,
			Call:   call,
			Kind:   kind,
			Reason: reason,
			Result: result,
		},
	)
	if err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	var payload ToolErrorPayload
	if err := decodePayload(dispatched.Event, &payload); err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	return session.afterTool(ctx, snapshot, turn, call, payload.Result)
}

func (session *Session) afterTool(
	ctx context.Context,
	snapshot *registrySnapshot,
	turn int,
	call ToolCall,
	result ToolResult,
) (ToolCall, ToolResult, error) {
	dispatched, err := session.runtime.dispatch(
		ctx,
		snapshot,
		EventAfterTool,
		session.config.ID,
		AfterToolPayload{Turn: turn, Call: call, Result: result},
	)
	if err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	var payload AfterToolPayload
	if err := decodePayload(dispatched.Event, &payload); err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	if err := session.appendTrajectory(
		ctx,
		TrajectoryKindToolResult,
		snapshot.generation,
		payload,
	); err != nil {
		return ToolCall{}, ToolResult{}, err
	}
	return call, payload.Result, nil
}

func (session *Session) finish(
	ctx context.Context,
	snapshot *registrySnapshot,
	messages []Message,
	result Result,
	cause Cause,
) (Result, error) {
	result.Messages = cloneMessages(messages)
	result.Cause = cause
	end := AgentEndPayload{
		Messages: cloneMessages(messages),
		Cause:    cause,
	}
	if err := session.appendTrajectory(
		ctx,
		TrajectoryKindTerminal,
		snapshot.generation,
		end,
	); err != nil {
		return result, err
	}
	if _, err := session.runtime.dispatch(
		ctx,
		snapshot,
		EventAgentEnd,
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

func (session *Session) appendTrajectory(
	ctx context.Context,
	kind string,
	generation uint64,
	payload any,
) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s trajectory entry: %w", kind, err)
	}
	entry := TrajectoryEntry{
		ID:         newDispatchID(),
		ParentID:   session.head,
		Kind:       kind,
		Timestamp:  time.Now().UTC(),
		Generation: generation,
		Payload:    raw,
	}
	head, err := session.runtime.trajectories.Append(
		ctx,
		session.config.ID,
		session.head,
		entry,
	)
	if err != nil {
		return fmt.Errorf("append %s trajectory entry: %w", kind, err)
	}
	session.head = head
	session.runtime.emitTrajectoryEvent(ctx, EventTrajectoryAppend, TrajectoryEventPayload{
		TrajectoryID: session.config.ID,
		EntryID:      entry.ID,
		EntryKind:    kind,
		Generation:   generation,
	})
	return nil
}

func (runtime *Runtime) emitTrajectoryEvent(
	ctx context.Context,
	eventName string,
	payload TrajectoryEventPayload,
) {
	if _, err := runtime.Emit(ctx, eventName, payload.TrajectoryID, payload); err != nil {
		runtime.logger.WarnContext(
			ctx,
			"trajectory event failed",
			"event",
			eventName,
			"trajectory_id",
			payload.TrajectoryID,
			"error",
			err,
		)
	}
}

func (session *Session) checkpointTrajectory(
	ctx context.Context,
	generation uint64,
	messages []Message,
	result Result,
	action Action,
	system string,
) error {
	return session.appendTrajectory(
		ctx,
		TrajectoryKindCheckpoint,
		generation,
		trajectoryCheckpoint{
			Messages:  cloneMessages(messages),
			System:    system,
			Turns:     result.Turns,
			ToolCalls: result.ToolCalls,
			Action:    action,
		},
	)
}

func selectProvider(
	snapshot *registrySnapshot,
	requested string,
) (string, Provider, error) {
	if requested != "" {
		owned, exists := snapshot.providers[requested]
		if !exists {
			return "", nil, fmt.Errorf("provider %q is not mounted", requested)
		}
		return requested, owned.provider, nil
	}
	if len(snapshot.providers) == 0 {
		return "", nil, errors.New("no provider is mounted")
	}
	if len(snapshot.providers) > 1 {
		return "", nil, errors.New(
			"multiple providers are mounted; session provider is required",
		)
	}
	for name, owned := range snapshot.providers {
		return name, owned.provider, nil
	}
	panic("unreachable")
}

func snapshotToolSpecs(snapshot *registrySnapshot) []ToolSpec {
	specs := make([]ToolSpec, 0, len(snapshot.tools))
	for _, owned := range snapshot.tools {
		specs = append(specs, owned.tool.Spec())
	}
	slices.SortFunc(specs, func(left, right ToolSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	return specs
}

func resolveAdvertisedTools(
	snapshot *registrySnapshot,
	specs []ToolSpec,
) ([]ToolSpec, map[string]Tool, error) {
	result := make([]ToolSpec, 0, len(specs))
	index := make(map[string]Tool, len(specs))
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
		index[spec.Name] = owned.tool
		result = append(result, spec)
	}
	return result, index, nil
}

func resolveAction(defaultAction Action, actions []Action) Action {
	if defaultAction.Kind == ActionStop &&
		defaultAction.Cause != nil &&
		defaultAction.Cause.Final {
		return defaultAction
	}
	var injected []Message
	for _, action := range actions {
		if action.Kind == ActionInject {
			injected = append(injected, cloneMessages(action.Messages)...)
		}
	}
	if len(injected) > 0 {
		return Action{Kind: ActionInject, Messages: injected}
	}
	for index := len(actions) - 1; index >= 0; index-- {
		if actions[index].Kind == ActionStop {
			return actions[index]
		}
	}
	for _, action := range actions {
		if action.Kind == ActionStep {
			return Action{Kind: ActionStep}
		}
	}
	return defaultAction
}

func decodePayload(event Event, target any) error {
	if err := json.Unmarshal(event.Payload, target); err != nil {
		return fmt.Errorf("decode %s event payload: %w", event.Name, err)
	}
	return nil
}

func cloneMessages(messages []Message) []Message {
	result := make([]Message, len(messages))
	for index, message := range messages {
		result[index] = message
		result[index].ToolCalls = cloneToolCalls(message.ToolCalls)
	}
	return result
}

func cloneToolCalls(calls []ToolCall) []ToolCall {
	result := make([]ToolCall, len(calls))
	for index, call := range calls {
		result[index] = call
		result[index].Arguments = append(json.RawMessage(nil), call.Arguments...)
	}
	return result
}
