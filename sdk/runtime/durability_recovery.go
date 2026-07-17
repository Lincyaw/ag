package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// RecoveredExecution identifies one trajectory resumed by runtime recovery.
type RecoveredExecution struct {
	TrajectoryID string `json:"trajectory_id"`
	Result       Result `json:"result"`
}

func LoadExecutionResult(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (*Result, error) {
	if metadata.Execution == nil || metadata.Head == "" {
		return nil, nil
	}
	entry, found, err := store.FindLatest(
		ctx,
		metadata.ID,
		metadata.Head,
		sdk.TrajectoryKindCheckpoint,
	)
	if err != nil || !found {
		return nil, err
	}
	if entry.Fields.ExecutionID != metadata.Execution.ID {
		return nil, nil
	}
	checkpoint, err := durability.DecodeCheckpoint(metadata.ID, entry)
	if err != nil {
		return nil, err
	}
	result := &Result{
		Output:     checkpoint.Output,
		Messages:   checkpoint.Messages,
		Turns:      checkpoint.Turns,
		ToolCalls:  checkpoint.ToolCalls,
		Generation: checkpoint.Generation,
	}
	if checkpoint.Action.Cause != nil {
		result.Cause = *checkpoint.Action.Cause
	}
	return result, nil
}

func (runtime *Runtime) RecoverExecutions(
	ctx context.Context,
) ([]RecoveredExecution, error) {
	recoverable, err := runtime.trajectories.ListRecoverable(
		ctx,
		time.Now().UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("list recoverable trajectory executions: %w", err)
	}
	results := make([]RecoveredExecution, 0, len(recoverable))
	var recoveryErrors []error
	for _, metadata := range recoverable {
		result, recoverErr := runtime.RecoverExecution(ctx, metadata.ID)
		if recoverErr != nil {
			if errors.Is(recoverErr, sdk.ErrTrajectoryClaimed) {
				continue
			}
			recoveryErrors = append(
				recoveryErrors,
				fmt.Errorf(
					"recover trajectory %s: %w",
					metadata.ID,
					recoverErr,
				),
			)
			continue
		}
		results = append(results, RecoveredExecution{
			TrajectoryID: metadata.ID,
			Result:       result,
		})
	}
	return results, errors.Join(recoveryErrors...)
}

func (runtime *Runtime) RecoverExecution(
	ctx context.Context,
	id string,
) (result Result, returnErr error) {
	if err := runtime.beginTrajectoryWork(); err != nil {
		return Result{}, err
	}
	defer runtime.endTrajectoryWork()
	metadata, err := runtime.trajectories.LoadMetadata(ctx, id)
	if err != nil {
		return Result{}, err
	}
	if metadata.Execution == nil || metadata.Execution.Terminal() {
		return Result{}, fmt.Errorf(
			"%w: trajectory %s has no recoverable execution",
			sdk.ErrTrajectoryExecution,
			id,
		)
	}
	storedExecution := *metadata.Execution
	inputEntry, err := runtime.trajectories.LoadEntry(
		ctx,
		id,
		storedExecution.InputEntryID,
	)
	if err != nil {
		return Result{}, err
	}
	config := SessionConfig{
		ID:           id,
		Provider:     storedExecution.Provider,
		System:       storedExecution.System,
		MaxTurns:     storedExecution.MaxTurns,
		ResumePolicy: ResumeExact,
	}
	if err := validateSessionConfig(runtime, &config); err != nil {
		return Result{}, err
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return Result{}, err
	}
	defer lease.release()
	executionSnapshot, err := snapshotForTrajectoryEnvironment(
		lease.snapshot,
		metadata.Environment,
	)
	if err != nil {
		return Result{}, err
	}
	current, environmentErr := newTrajectoryEnvironment(
		runtime,
		executionSnapshot,
		config,
	)
	if environmentErr != nil {
		return Result{}, environmentErr
	}
	recordedEnvironment, err := executionResumeEnvironment(
		metadata.Environment,
		inputEntry,
	)
	if err != nil {
		return Result{}, err
	}
	if err := validateResumeEnvironment(recordedEnvironment, current); err != nil {
		return Result{}, err
	}

	session := &Session{
		runtime: runtime,
		config:  config,
		head:    metadata.Head,
	}
	if metadata.Environment.OriginInvocationID != "" {
		session.pinnedSnapshot = executionSnapshot
		session.invocationParent =
			metadata.Environment.OriginInvocationID
		session.invocationRoot =
			metadata.Environment.OriginInvocationRootID
		if session.invocationRoot == "" {
			session.invocationRoot =
				metadata.Environment.OriginInvocationID
		}
	}
	if err := session.claimExecution(ctx); err != nil {
		return Result{}, err
	}
	defer func() {
		if returnErr == nil {
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

	checkpointEntry, checkpoint, err := durability.LatestCheckpoint(
		executionCtx,
		runtime.trajectories,
		metadata,
	)
	if err != nil {
		return Result{}, err
	}
	if checkpointEntry.Fields.ExecutionID != storedExecution.ID {
		checkpointEntry = sdk.TrajectoryEntry{}
		checkpoint = nil
	}
	if checkpoint != nil {
		if err := session.restoreExecutionHead(
			executionCtx,
			checkpointEntry.ID,
		); err != nil {
			return Result{}, err
		}
		session.messages = cloneMessages(checkpoint.Messages)
		session.config.System = checkpoint.System
		if checkpoint.Provider != "" {
			session.config.Provider = checkpoint.Provider
		}
		execution := &promptExecution{
			session:  session,
			messages: cloneMessages(checkpoint.Messages),
			system:   checkpoint.System,
			dependencies: append(
				[]string(nil),
				checkpoint.Dependencies...,
			),
			result: Result{
				Output:     checkpoint.Output,
				Messages:   cloneMessages(checkpoint.Messages),
				Turns:      checkpoint.Turns,
				ToolCalls:  checkpoint.ToolCalls,
				Generation: checkpoint.Generation,
			},
			mutated: true,
		}
		if execution.result.Generation == 0 {
			execution.result.Generation = checkpointEntry.Generation
		}
		if execution.result.Output == "" {
			execution.result.Output = latestAssistantOutput(
				checkpoint.Messages,
			)
		}
		if checkpoint.Action.Kind == sdk.ActionStop {
			cause := sdk.Cause{Code: "model_end"}
			if checkpoint.Action.Cause != nil {
				cause = *checkpoint.Action.Cause
			}
			snapshotLease, acquireErr := session.acquireSnapshot()
			if acquireErr != nil {
				return Result{}, acquireErr
			}
			result, err := session.finish(
				executionCtx,
				snapshotLease.snapshot,
				execution.messages,
				execution.result,
				cause,
			)
			snapshotLease.release()
			session.messages = cloneMessages(execution.messages)
			return result, err
		}
		return execution.runTurnsFrom(executionCtx, checkpoint.Turns)
	}

	if err := session.restoreExecutionHead(
		executionCtx,
		storedExecution.InputEntryID,
	); err != nil {
		return Result{}, err
	}
	baseCheckpoint, err := runtime.checkpointAtOrBefore(
		executionCtx,
		id,
		storedExecution.BaseHead,
	)
	if err != nil {
		return Result{}, err
	}
	session.messages = durability.Messages(baseCheckpoint)
	var userMessage sdk.Message
	if err := json.Unmarshal(inputEntry.Payload, &userMessage); err != nil {
		return Result{}, fmt.Errorf(
			"decode trajectory execution input %s: %w",
			inputEntry.ID,
			err,
		)
	}
	if userMessage.Role != sdk.RoleUser ||
		userMessage.Content == "" {
		return Result{}, fmt.Errorf(
			"trajectory execution input %s is not a user message",
			inputEntry.ID,
		)
	}
	execution := newPromptExecution(session, userMessage.Content)
	execution.mutated = true
	result, done, err := execution.start(executionCtx)
	if err != nil || done {
		return result, err
	}
	return execution.runTurnsFrom(executionCtx, 0)
}

func snapshotForTrajectoryEnvironment(
	current *registrySnapshot,
	environment sdk.TrajectoryEnvironment,
) (*registrySnapshot, error) {
	if environment.OriginInvocationID == "" {
		return current, nil
	}
	result := current.clone()
	result.providers = make(
		map[string]ownedResource[sdk.Provider, sdk.ProviderSpec],
		len(environment.Providers),
	)
	for _, spec := range environment.Providers {
		provider, exists := current.providers[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"recover agent trajectory: provider %q is unavailable",
				spec.Name,
			)
		}
		result.providers[spec.Name] = provider
	}
	result.tools = make(
		map[string]ownedResource[sdk.Tool, sdk.ToolSpec],
		len(environment.Tools),
	)
	for _, spec := range environment.Tools {
		tool, exists := current.tools[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"recover agent trajectory: tool %q is unavailable",
				spec.Name,
			)
		}
		result.tools[spec.Name] = tool
	}
	return result, nil
}

func executionResumeEnvironment(
	fallback sdk.TrajectoryEnvironment,
	input sdk.TrajectoryEntry,
) (sdk.TrajectoryEnvironment, error) {
	digest := input.Attributes[executionCompositionDigestAttribute]
	rawVersion := input.Attributes[executionSDKAPIVersionAttribute]
	if digest == "" && rawVersion == "" {
		return fallback, nil
	}
	if digest == "" || rawVersion == "" {
		return sdk.TrajectoryEnvironment{}, fmt.Errorf(
			"trajectory execution input %s has an incomplete runtime environment",
			input.ID,
		)
	}
	apiVersion, err := strconv.Atoi(rawVersion)
	if err != nil || apiVersion < 1 {
		return sdk.TrajectoryEnvironment{}, fmt.Errorf(
			"trajectory execution input %s has invalid SDK API version %q",
			input.ID,
			rawVersion,
		)
	}
	return sdk.TrajectoryEnvironment{
		SDKAPIVersion:     apiVersion,
		CompositionDigest: digest,
	}, nil
}

func (runtime *Runtime) checkpointAtOrBefore(
	ctx context.Context,
	trajectoryID string,
	head string,
) (*durability.Checkpoint, error) {
	if head == "" {
		return nil, nil
	}
	entry, found, err := runtime.trajectories.FindLatest(
		ctx,
		trajectoryID,
		head,
		sdk.TrajectoryKindCheckpoint,
	)
	if err != nil || !found {
		return nil, err
	}
	return durability.DecodeCheckpoint(trajectoryID, entry)
}

func (session *Session) restoreExecutionHead(
	ctx context.Context,
	target string,
) error {
	if session.head == target {
		return nil
	}
	restored, err := durability.HeadRestoresCheckpoint(
		ctx,
		session.runtime.trajectories,
		session.config.ID,
		session.head,
		target,
	)
	if err != nil {
		return err
	}
	if restored {
		return nil
	}
	from := session.head
	raw, err := json.Marshal(map[string]string{
		"from": from,
		"to":   target,
	})
	if err != nil {
		return err
	}
	entry := sdk.TrajectoryEntry{
		ID:        sdk.NewID(),
		ParentID:  target,
		Kind:      sdk.TrajectoryKindRestore,
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}
	if err := session.commitExecution(ctx, entry, "", ""); err != nil {
		return fmt.Errorf("restore recoverable trajectory execution: %w", err)
	}
	session.runtime.emitTrajectoryEvent(
		ctx,
		sdk.EventTrajectoryRestore,
		sdk.TrajectoryEventPayload{
			TrajectoryID: session.config.ID,
			EntryID:      entry.ID,
			EntryKind:    entry.Kind,
			From:         from,
			To:           target,
		},
	)
	return nil
}

func latestAssistantOutput(messages []sdk.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == sdk.RoleAssistant {
			return messages[index].Content
		}
	}
	return ""
}
