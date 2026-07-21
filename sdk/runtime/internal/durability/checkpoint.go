// Package durability owns the durable checkpoint language and the rules used
// to project and restore trajectory state.
package durability

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/lincyaw/ag/sdk"
)

// Checkpoint is the durable continuation state committed after an agent turn.
type Checkpoint struct {
	// Messages is retained for decoding legacy snapshot checkpoints. New
	// checkpoints project their conversation from the trajectory branch instead
	// of copying the complete message list into every turn boundary.
	Messages                    []sdk.Message          `json:"messages,omitempty"`
	MessageMode                 CheckpointMessageMode  `json:"message_mode,omitempty"`
	System                      string                 `json:"system,omitempty"`
	Provider                    string                 `json:"provider,omitempty"`
	Output                      string                 `json:"output,omitempty"`
	Turns                       int                    `json:"turns"`
	ToolCalls                   int                    `json:"tool_calls"`
	InputTokens                 int64                  `json:"input_tokens,omitempty"`
	OutputTokens                int64                  `json:"output_tokens,omitempty"`
	Generation                  uint64                 `json:"generation,omitempty"`
	Action                      sdk.Action             `json:"action"`
	ContextInjections           []sdk.ContextInjection `json:"context_injections,omitempty"`
	ConsumedContextInjectionIDs []string               `json:"consumed_context_injection_ids,omitempty"`
	Dependencies                []string               `json:"dependencies,omitempty"`
}

type CheckpointMessageMode string

const (
	// CheckpointMessagesSnapshot is the legacy representation. The empty wire
	// value deliberately means snapshot so existing checkpoints keep decoding.
	CheckpointMessagesSnapshot CheckpointMessageMode = ""
	// CheckpointMessagesBranch means the message projection is the sequence of
	// message-affecting entries reachable at this checkpoint's branch head.
	CheckpointMessagesBranch CheckpointMessageMode = "branch"
)

// ProviderRequest is the durable projection of one model invocation request.
type ProviderRequest struct {
	Turn          int    `json:"turn"`
	Provider      string `json:"provider"`
	Model         string `json:"model,omitempty"`
	OperationKey  string `json:"operation_key"`
	CorrelationID string `json:"correlation_id,omitempty"`
	MessageCount  int    `json:"message_count,omitempty"`
	ToolCount     int    `json:"tool_count,omitempty"`
	// Request is retained for decoding legacy trajectory entries. The durable
	// operation record owns the exact request needed for in-flight recovery;
	// new trajectory entries store only correlation and projection metadata.
	Request *sdk.ModelRequest `json:"request,omitempty"`
}

// ProviderResponse preserves event-payload compatibility while adding durable
// trajectory correlation metadata for one model round.
type ProviderResponse struct {
	sdk.AfterProviderPayload
	CorrelationID string `json:"correlation_id,omitempty"`
}

// ToolCall is the durable projection of one tool invocation request.
type ToolCall struct {
	Turn          int          `json:"turn"`
	Call          sdk.ToolCall `json:"call"`
	OperationKey  string       `json:"operation_key"`
	CorrelationID string       `json:"correlation_id,omitempty"`
}

// ToolResult preserves event-payload compatibility while adding durable
// trajectory correlation metadata for the provider round that emitted the call.
type ToolResult struct {
	sdk.AfterToolPayload
	CorrelationID string `json:"correlation_id,omitempty"`
}

// Decision records the action selected at the end of one agent turn.
type Decision struct {
	Turn   int        `json:"turn"`
	Action sdk.Action `json:"action"`
}

// EntryFields projects stable indexed fields from known trajectory payloads.
func EntryFields(payload any) sdk.TrajectoryEntryFields {
	var fields sdk.TrajectoryEntryFields
	setTurn := func(turn int) { fields.Turn = &turn }
	setError := func(isError bool) { fields.IsError = &isError }
	switch value := payload.(type) {
	case ProviderRequest:
		setTurn(value.Turn)
		fields.Provider = value.Provider
		fields.Model = value.Model
		fields.OperationKey = value.OperationKey
		fields.CorrelationID = value.CorrelationID
		if fields.CorrelationID == "" {
			fields.CorrelationID = value.OperationKey
		}
	case ProviderResponse:
		setTurn(value.Turn)
		fields.Provider = value.Provider
		fields.CorrelationID = value.CorrelationID
		setError(value.Error != "")
		if value.Response != nil {
			fields.Model = value.Response.Model
			fields.FinishReason = value.Response.FinishReason
			fields.InputTokens = value.Response.Usage.InputTokens
			fields.OutputTokens = value.Response.Usage.OutputTokens
		}
	case sdk.AfterProviderPayload:
		setTurn(value.Turn)
		fields.Provider = value.Provider
		setError(value.Error != "")
		if value.Response != nil {
			fields.Model = value.Response.Model
			fields.FinishReason = value.Response.FinishReason
			fields.InputTokens = value.Response.Usage.InputTokens
			fields.OutputTokens = value.Response.Usage.OutputTokens
		}
	case sdk.BeforeToolPayload:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
	case ToolCall:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
		fields.OperationKey = value.OperationKey
		fields.CorrelationID = value.CorrelationID
	case ToolResult:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
		fields.CorrelationID = value.CorrelationID
		setError(value.Result.IsError)
	case sdk.AfterToolPayload:
		setTurn(value.Turn)
		fields.ToolName = value.Call.Name
		fields.ToolCallID = value.Call.ID
		setError(value.Result.IsError)
	case Decision:
		setTurn(value.Turn)
		fields.ActionKind = value.Action.Kind
		if value.Action.Cause != nil {
			fields.CauseCode = value.Action.Cause.Code
		}
	case Checkpoint:
		if value.Turns > 0 {
			setTurn(value.Turns - 1)
		}
		fields.ActionKind = value.Action.Kind
		if value.Action.Cause != nil {
			fields.CauseCode = value.Action.Cause.Code
		}
	case sdk.AgentEndPayload:
		fields.CauseCode = value.Cause.Code
	}
	return fields
}

// LatestCheckpoint loads and validates the checkpoint cached by metadata.
func LatestCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (sdk.TrajectoryEntry, *Checkpoint, error) {
	if metadata.Checkpoint == "" {
		return sdk.TrajectoryEntry{}, nil, nil
	}
	entry, err := store.LoadEntry(ctx, metadata.ID, metadata.Checkpoint)
	if err != nil {
		return sdk.TrajectoryEntry{}, nil, err
	}
	if entry.Kind != sdk.TrajectoryKindCheckpoint {
		return sdk.TrajectoryEntry{}, nil, fmt.Errorf(
			"trajectory %q cached checkpoint %q is %q",
			metadata.ID,
			entry.ID,
			entry.Kind,
		)
	}
	checkpoint, err := DecodeCheckpoint(metadata.ID, entry)
	if err != nil {
		return sdk.TrajectoryEntry{}, nil, err
	}
	if err := HydrateCheckpointMessages(
		ctx,
		store,
		metadata.ID,
		entry,
		checkpoint,
	); err != nil {
		return sdk.TrajectoryEntry{}, nil, err
	}
	return entry, checkpoint, nil
}

// LatestExecutionCheckpoint returns the latest checkpoint on the active branch
// when that checkpoint belongs to the trajectory's current execution.
func LatestExecutionCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (sdk.TrajectoryEntry, *Checkpoint, bool, error) {
	if metadata.Execution == nil || metadata.Head == "" {
		return sdk.TrajectoryEntry{}, nil, false, nil
	}
	entry, found, err := store.FindLatest(
		ctx,
		metadata.ID,
		metadata.Head,
		sdk.TrajectoryKindCheckpoint,
	)
	if err != nil || !found {
		return sdk.TrajectoryEntry{}, nil, false, err
	}
	if entry.Fields.ExecutionID != metadata.Execution.ID {
		return sdk.TrajectoryEntry{}, nil, false, nil
	}
	checkpoint, err := DecodeCheckpoint(metadata.ID, entry)
	if err != nil {
		return sdk.TrajectoryEntry{}, nil, false, err
	}
	if err := HydrateCheckpointMessages(
		ctx,
		store,
		metadata.ID,
		entry,
		checkpoint,
	); err != nil {
		return sdk.TrajectoryEntry{}, nil, false, err
	}
	return entry, checkpoint, true, nil
}

// DecodeCheckpoint restores an owned checkpoint value from one trajectory
// entry.
func DecodeCheckpoint(
	trajectoryID string,
	entry sdk.TrajectoryEntry,
) (*Checkpoint, error) {
	var checkpoint Checkpoint
	if err := json.Unmarshal(entry.Payload, &checkpoint); err != nil {
		return nil, fmt.Errorf(
			"decode trajectory %q checkpoint %q: %w",
			trajectoryID,
			entry.ID,
			err,
		)
	}
	if checkpoint.MessageMode != CheckpointMessagesSnapshot &&
		checkpoint.MessageMode != CheckpointMessagesBranch {
		return nil, fmt.Errorf(
			"decode trajectory %q checkpoint %q: unknown message mode %q",
			trajectoryID,
			entry.ID,
			checkpoint.MessageMode,
		)
	}
	checkpoint.Messages = sdk.CloneMessages(checkpoint.Messages)
	checkpoint.ContextInjections = sdk.CloneContextInjections(
		checkpoint.ContextInjections,
	)
	checkpoint.ConsumedContextInjectionIDs = slices.Clone(
		checkpoint.ConsumedContextInjectionIDs,
	)
	checkpoint.Dependencies = slices.Clone(checkpoint.Dependencies)
	return &checkpoint, nil
}

// HydrateCheckpointMessages reconstructs a branch-backed checkpoint without
// changing its durable representation. Legacy snapshot checkpoints are already
// self-contained and remain a fast recovery anchor.
func HydrateCheckpointMessages(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	entry sdk.TrajectoryEntry,
	checkpoint *Checkpoint,
) error {
	if checkpoint == nil || checkpoint.MessageMode != CheckpointMessagesBranch {
		return nil
	}
	messages, err := LoadBranchMessages(ctx, store, trajectoryID, entry.ID)
	if err != nil {
		return err
	}
	checkpoint.Messages = messages
	return nil
}

// Messages returns an owned copy of the checkpoint conversation.
func Messages(checkpoint *Checkpoint) []sdk.Message {
	if checkpoint == nil {
		return nil
	}
	return sdk.CloneMessages(checkpoint.Messages)
}

// BranchBase is the durable projection of the model-visible state at one
// trajectory branch head.
type BranchBase struct {
	Head     string
	Messages []sdk.Message
}

const ForkToolResultPlaceholder = "Forked agent inherited this parent tool call without its result."

// SessionResumeBase is the durable trajectory projection used to resume a
// session from a stable branch head. Checkpoint fields are retained because
// exact resume also rebuilds provider/system state from checkpoint context.
type SessionResumeBase struct {
	BranchBase
	CheckpointEntry sdk.TrajectoryEntry
	Checkpoint      *Checkpoint
}

// LoadSessionResumeBase resolves the branch head and visible messages that a
// session should resume from. Failed and cancelled executions resume from their
// recorded execution base; forked trajectories without a local checkpoint resume
// from their fork anchor.
func LoadSessionResumeBase(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (SessionResumeBase, error) {
	checkpointEntry, checkpoint, err := LatestCheckpoint(ctx, store, metadata)
	if err != nil {
		return SessionResumeBase{}, err
	}
	head, preserveCheckpoint := sessionResumeBranchHead(
		metadata,
		checkpointEntry,
	)
	base := SessionResumeBase{
		BranchBase: BranchBase{
			Head:     checkpointEntry.ID,
			Messages: Messages(checkpoint),
		},
		CheckpointEntry: checkpointEntry,
		Checkpoint:      checkpoint,
	}
	if head != checkpointEntry.ID || !preserveCheckpoint {
		branchBase, err := LoadBranchBase(
			ctx,
			store,
			metadata.ID,
			head,
		)
		if err != nil {
			if !preserveCheckpoint {
				return SessionResumeBase{}, fmt.Errorf(
					"project forked trajectory %q base branch at %q: %w",
					metadata.ID,
					head,
					err,
				)
			}
			return SessionResumeBase{}, err
		}
		if !preserveCheckpoint {
			branchBase.Messages = CloseUnresolvedToolCalls(
				branchBase.Messages,
				ForkToolResultPlaceholder,
			)
		}
		base.BranchBase = branchBase
	}
	if !preserveCheckpoint {
		base.CheckpointEntry = sdk.TrajectoryEntry{}
		base.Checkpoint = nil
	}
	return base, nil
}

// ExecutionCompletionBase is the durable branch projection used when an active
// execution is failed or cancelled and the trajectory head must return to the
// execution's accepted base.
type ExecutionCompletionBase = BranchBase

func LoadExecutionCompletionBase(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (ExecutionCompletionBase, error) {
	if metadata.Execution == nil {
		return ExecutionCompletionBase{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			metadata.ID,
		)
	}
	return LoadBranchBase(
		ctx,
		store,
		metadata.ID,
		metadata.Execution.BaseHead,
	)
}

// ExecutionRecoveryBase is the durable replay point for one accepted execution.
// It is either the latest checkpoint owned by the execution or the accepted
// input entry plus the message state captured before that input.
type ExecutionRecoveryBase struct {
	Head            string
	Messages        []sdk.Message
	Message         sdk.Message
	CheckpointEntry sdk.TrajectoryEntry
	Checkpoint      *Checkpoint
}

func LoadExecutionRecoveryBase(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
	inputEntry sdk.TrajectoryEntry,
) (ExecutionRecoveryBase, error) {
	if metadata.Execution == nil {
		return ExecutionRecoveryBase{}, fmt.Errorf(
			"%w: trajectory %s has no execution",
			sdk.ErrTrajectoryExecution,
			metadata.ID,
		)
	}
	execution := *metadata.Execution
	if inputEntry.ID != execution.InputEntryID {
		return ExecutionRecoveryBase{}, fmt.Errorf(
			"trajectory %q recovery input entry = %q, want %q",
			metadata.ID,
			inputEntry.ID,
			execution.InputEntryID,
		)
	}
	checkpointEntry, checkpoint, found, err := LatestExecutionCheckpoint(
		ctx,
		store,
		metadata,
	)
	if err != nil {
		return ExecutionRecoveryBase{}, err
	}
	if found {
		return ExecutionRecoveryBase{
			Head:            checkpointEntry.ID,
			Messages:        Messages(checkpoint),
			CheckpointEntry: checkpointEntry,
			Checkpoint:      checkpoint,
		}, nil
	}
	input, err := LoadAcceptedExecutionInput(
		ctx,
		store,
		metadata.ID,
		execution.BaseHead,
		inputEntry,
	)
	if err != nil {
		return ExecutionRecoveryBase{}, err
	}
	return ExecutionRecoveryBase{
		Head:     inputEntry.ID,
		Messages: input.BaseMessages,
		Message:  input.Message,
	}, nil
}

func terminalExecutionResumeHead(
	metadata sdk.TrajectoryMetadata,
) (string, bool) {
	if metadata.Execution == nil {
		return "", false
	}
	switch metadata.Execution.State {
	case sdk.TrajectoryExecutionFailed,
		sdk.TrajectoryExecutionCancelled:
		return metadata.Execution.BaseHead, true
	default:
		return "", false
	}
}

// sessionResumeBranchHead chooses the branch head visible to a resumed session.
// preserveCheckpoint reports whether the latest checkpoint still belongs to the
// resumed trajectory's own continuation context. Fork anchors inherited from a
// parent seed messages, but they must not masquerade as child-owned checkpoints.
func sessionResumeBranchHead(
	metadata sdk.TrajectoryMetadata,
	checkpointEntry sdk.TrajectoryEntry,
) (head string, preserveCheckpoint bool) {
	if resumeHead, ok := terminalExecutionResumeHead(metadata); ok {
		return resumeHead, true
	}
	if metadata.ParentID != "" &&
		!trajectoryOwnsEntry(metadata, checkpointEntry) {
		return metadata.ParentEntryID, false
	}
	return checkpointEntry.ID, true
}

func trajectoryOwnsEntry(
	metadata sdk.TrajectoryMetadata,
	entry sdk.TrajectoryEntry,
) bool {
	return entry.ID != "" && entry.TrajectoryID == metadata.ID
}

func LoadBranchMessages(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	head string,
) ([]sdk.Message, error) {
	if head == "" {
		return nil, nil
	}
	if inspector, ok := store.(sdk.TrajectoryEntryInspector); ok {
		_, branch, err := inspector.InspectTrajectoryEntries(
			ctx, trajectoryID, head,
		)
		if err != nil {
			return nil, err
		}
		return LoadInspectedBranchMessages(
			ctx, store, trajectoryID, branch,
		)
	}
	branch, err := store.LoadBranch(ctx, trajectoryID, head)
	if err != nil {
		return nil, err
	}
	return BranchMessages(trajectoryID, branch)
}

func LoadBranchBase(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	head string,
) (BranchBase, error) {
	messages, err := LoadBranchMessages(ctx, store, trajectoryID, head)
	if err != nil {
		return BranchBase{}, err
	}
	return BranchBase{
		Head:     head,
		Messages: messages,
	}, nil
}

// CloseUnresolvedToolCalls appends placeholder tool results before a branch
// projection crosses from an assistant tool-call message to later context.
func CloseUnresolvedToolCalls(
	messages []sdk.Message,
	placeholder string,
) []sdk.Message {
	if placeholder == "" {
		placeholder = "Tool result omitted from trajectory branch projection."
	}
	result := make([]sdk.Message, 0, len(messages))
	var pending []string
	flush := func() {
		for _, id := range pending {
			result = append(result, sdk.ToolMessage(
				id,
				sdk.ToolResult{Content: placeholder},
			))
		}
		pending = pending[:0]
	}
	for _, message := range sdk.CloneMessages(messages) {
		if message.Role != sdk.RoleTool && len(pending) > 0 {
			flush()
		}
		result = append(result, message)
		switch message.Role {
		case sdk.RoleAssistant:
			for _, call := range message.ToolCalls {
				if call.ID != "" {
					pending = append(pending, call.ID)
				}
			}
		case sdk.RoleTool:
			pending = removePendingToolCall(pending, message.ToolCallID)
		}
	}
	flush()
	return sdk.CloneMessages(result)
}

func removePendingToolCall(pending []string, id string) []string {
	if id == "" || len(pending) == 0 {
		return pending
	}
	for index, candidate := range pending {
		if candidate != id {
			continue
		}
		copy(pending[index:], pending[index+1:])
		return pending[:len(pending)-1]
	}
	return pending
}

// BranchMessages projects the conversation visible at the supplied branch head.
// It starts from the latest legacy snapshot checkpoint on the branch and
// replays only trajectory entries that affect the model-visible message list.
// Branch-backed checkpoints are continuation markers, not replacement
// message snapshots.
func BranchMessages(
	trajectoryID string,
	branch []sdk.TrajectoryEntry,
) ([]sdk.Message, error) {
	var messages []sdk.Message
	start := 0
	for index := len(branch) - 1; index >= 0; index-- {
		entry := branch[index]
		if entry.Kind != sdk.TrajectoryKindCheckpoint {
			continue
		}
		checkpoint, err := DecodeCheckpoint(trajectoryID, entry)
		if err != nil {
			return nil, err
		}
		if checkpoint.MessageMode == CheckpointMessagesSnapshot {
			messages = Messages(checkpoint)
			start = index + 1
			break
		}
	}
	for _, entry := range branch[start:] {
		updated, err := branchMessagesAfterEntry(
			trajectoryID,
			messages,
			entry,
		)
		if err != nil {
			return nil, err
		}
		messages = updated
	}
	return sdk.CloneMessages(messages), nil
}

func branchMessagesAfterEntry(
	trajectoryID string,
	messages []sdk.Message,
	entry sdk.TrajectoryEntry,
) ([]sdk.Message, error) {
	switch entry.Kind {
	case sdk.TrajectoryKindUserMessage:
		input, err := DecodeExecutionInput(trajectoryID, entry)
		if err != nil {
			return nil, err
		}
		return input.MessagesAfter(messages), nil
	case sdk.TrajectoryKindAgentStart:
		var payload sdk.AgentStartPayload
		if err := decodeTrajectoryPayload(trajectoryID, entry, &payload); err != nil {
			return nil, err
		}
		return sdk.CloneMessages(payload.Messages), nil
	case sdk.TrajectoryKindProviderResponse:
		var payload sdk.AfterProviderPayload
		if err := decodeTrajectoryPayload(trajectoryID, entry, &payload); err != nil {
			return nil, err
		}
		if payload.Response == nil {
			return sdk.CloneMessages(messages), nil
		}
		return append(sdk.CloneMessages(messages), sdk.Message{
			Role:      sdk.RoleAssistant,
			Content:   payload.Response.Content,
			ToolCalls: sdk.CloneToolCalls(payload.Response.ToolCalls),
		}), nil
	case sdk.TrajectoryKindToolResult:
		var payload sdk.AfterToolPayload
		if err := decodeTrajectoryPayload(trajectoryID, entry, &payload); err != nil {
			return nil, err
		}
		return append(
			sdk.CloneMessages(messages),
			sdk.ToolMessage(payload.Call.ID, payload.Result),
		), nil
	case sdk.TrajectoryKindCheckpoint:
		checkpoint, err := DecodeCheckpoint(trajectoryID, entry)
		if err != nil {
			return nil, err
		}
		if checkpoint.MessageMode == CheckpointMessagesBranch {
			result := sdk.CloneMessages(messages)
			if checkpoint.Action.Kind == sdk.ActionInject {
				result = append(
					result,
					sdk.CloneMessages(checkpoint.Action.Messages)...,
				)
			}
			return result, nil
		}
		return Messages(checkpoint), nil
	case sdk.TrajectoryKindTerminal:
		var payload sdk.AgentEndPayload
		if err := decodeTrajectoryPayload(trajectoryID, entry, &payload); err != nil {
			return nil, err
		}
		if len(payload.Messages) == 0 {
			return sdk.CloneMessages(messages), nil
		}
		return sdk.CloneMessages(payload.Messages), nil
	default:
		return sdk.CloneMessages(messages), nil
	}
}

func decodeTrajectoryPayload(
	trajectoryID string,
	entry sdk.TrajectoryEntry,
	target any,
) error {
	if err := json.Unmarshal(entry.Payload, target); err != nil {
		return fmt.Errorf(
			"decode trajectory %q %s entry %q: %w",
			trajectoryID,
			entry.Kind,
			entry.ID,
			err,
		)
	}
	return nil
}

// HeadRestoresAnchor reports whether head already represents the requested
// branch anchor. The anchor is usually a checkpoint, but forked trajectories can
// also resume from their parent entry.
func HeadRestoresAnchor(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	head string,
	anchorID string,
) (bool, error) {
	if head == "" {
		return anchorID == "", nil
	}
	if head == anchorID {
		return true, nil
	}
	entry, err := store.LoadEntry(ctx, trajectoryID, head)
	if err != nil {
		return false, err
	}
	return (entry.Kind == sdk.TrajectoryKindRestore ||
		entry.Kind == sdk.TrajectoryKindRollback) &&
		entry.ParentID == anchorID, nil
}
