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
	Messages     []sdk.Message `json:"messages"`
	System       string        `json:"system,omitempty"`
	Provider     string        `json:"provider,omitempty"`
	Output       string        `json:"output,omitempty"`
	Turns        int           `json:"turns"`
	ToolCalls    int           `json:"tool_calls"`
	Generation   uint64        `json:"generation,omitempty"`
	Action       sdk.Action    `json:"action"`
	Dependencies []string      `json:"dependencies,omitempty"`
}

// ProviderRequest is the durable projection of one model invocation request.
type ProviderRequest struct {
	Turn         int              `json:"turn"`
	Provider     string           `json:"provider"`
	Model        string           `json:"model,omitempty"`
	OperationKey string           `json:"operation_key"`
	Request      sdk.ModelRequest `json:"request"`
}

// ToolCall is the durable projection of one tool invocation request.
type ToolCall struct {
	Turn         int          `json:"turn"`
	Call         sdk.ToolCall `json:"call"`
	OperationKey string       `json:"operation_key"`
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
	return entry, checkpoint, err
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
	checkpoint.Messages = cloneMessages(checkpoint.Messages)
	checkpoint.Dependencies = slices.Clone(checkpoint.Dependencies)
	return &checkpoint, nil
}

// Messages returns an owned copy of the checkpoint conversation.
func Messages(checkpoint *Checkpoint) []sdk.Message {
	if checkpoint == nil {
		return nil
	}
	return cloneMessages(checkpoint.Messages)
}

// HeadRestoresCheckpoint reports whether head already represents the requested
// checkpoint branch.
func HeadRestoresCheckpoint(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	head string,
	checkpointID string,
) (bool, error) {
	if head == "" {
		return checkpointID == "", nil
	}
	if head == checkpointID {
		return true, nil
	}
	entry, err := store.LoadEntry(ctx, trajectoryID, head)
	if err != nil {
		return false, err
	}
	return (entry.Kind == sdk.TrajectoryKindRestore ||
		entry.Kind == sdk.TrajectoryKindRollback) &&
		entry.ParentID == checkpointID, nil
}

func cloneMessages(messages []sdk.Message) []sdk.Message {
	result := make([]sdk.Message, len(messages))
	for index, message := range messages {
		result[index] = message
		result[index].ToolCalls = slices.Clone(message.ToolCalls)
	}
	return result
}
