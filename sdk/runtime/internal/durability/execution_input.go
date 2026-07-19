package durability

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

// ExecutionInput is the durable payload carried by a user_message entry.
// Older trajectories stored the sdk.Message directly; DecodeExecutionInput
// accepts both shapes.
type ExecutionInput struct {
	Message      sdk.Message               `json:"message"`
	BaseMessages []sdk.Message             `json:"base_messages,omitempty"`
	Environment  sdk.TrajectoryEnvironment `json:"environment,omitempty"`
}

type DecodedExecutionInput struct {
	Message        sdk.Message
	BaseMessages   []sdk.Message
	Environment    sdk.TrajectoryEnvironment
	HasEnvelope    bool
	HasEnvironment bool
}

// AcceptedExecutionInput is the durable prompt input projection used to recover
// an execution after its user_message entry has already been accepted.
type AcceptedExecutionInput struct {
	BaseMessages []sdk.Message
	Message      sdk.Message
}

func NewExecutionInput(
	message sdk.Message,
	environment sdk.TrajectoryEnvironment,
	baseMessages []sdk.Message,
) ExecutionInput {
	return ExecutionInput{
		Message:      sdk.CloneMessages([]sdk.Message{message})[0],
		BaseMessages: sdk.CloneMessages(baseMessages),
		Environment:  sdk.CloneTrajectoryEnvironment(environment),
	}
}

func DecodeExecutionInput(
	trajectoryID string,
	entry sdk.TrajectoryEntry,
) (DecodedExecutionInput, error) {
	if entry.Kind != sdk.TrajectoryKindUserMessage {
		return DecodedExecutionInput{}, fmt.Errorf(
			"trajectory %q entry %q is %q, not %q",
			trajectoryID,
			entry.ID,
			entry.Kind,
			sdk.TrajectoryKindUserMessage,
		)
	}
	var input ExecutionInput
	if err := json.Unmarshal(entry.Payload, &input); err == nil &&
		executionInputEnvelopeHasPayload(input) {
		return DecodedExecutionInput{
			Message:      sdk.CloneMessages([]sdk.Message{input.Message})[0],
			BaseMessages: sdk.CloneMessages(input.BaseMessages),
			Environment:  sdk.CloneTrajectoryEnvironment(input.Environment),
			HasEnvelope:  true,
			HasEnvironment: input.Environment.SDKAPIVersion != 0 ||
				input.Environment.CompositionDigest != "",
		}, nil
	}
	var legacy sdk.Message
	if err := json.Unmarshal(entry.Payload, &legacy); err != nil {
		return DecodedExecutionInput{}, fmt.Errorf(
			"decode trajectory %q user message %q: %w",
			trajectoryID,
			entry.ID,
			err,
		)
	}
	return DecodedExecutionInput{
		Message: sdk.CloneMessages([]sdk.Message{legacy})[0],
	}, nil
}

// LoadAcceptedExecutionInput resolves the prompt message and base conversation
// captured by an accepted user_message entry. Envelope inputs own their base
// snapshot; legacy inputs reconstruct it from the execution base branch.
func LoadAcceptedExecutionInput(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	baseHead string,
	entry sdk.TrajectoryEntry,
) (AcceptedExecutionInput, error) {
	input, err := DecodeExecutionInput(trajectoryID, entry)
	if err != nil {
		return AcceptedExecutionInput{}, err
	}
	if input.HasEnvelope {
		return AcceptedExecutionInput{
			BaseMessages: sdk.CloneMessages(input.BaseMessages),
			Message:      input.Message,
		}, nil
	}
	if baseHead == "" {
		return AcceptedExecutionInput{Message: input.Message}, nil
	}
	messages, err := LoadBranchMessages(ctx, store, trajectoryID, baseHead)
	if err != nil {
		return AcceptedExecutionInput{}, err
	}
	return AcceptedExecutionInput{
		BaseMessages: messages,
		Message:      input.Message,
	}, nil
}

// MessagesAfter projects the visible message state after applying the decoded
// input. Envelope inputs carry their own base snapshot; legacy inputs extend the
// branch state that came before them.
func (input DecodedExecutionInput) MessagesAfter(
	base []sdk.Message,
) []sdk.Message {
	messages := sdk.CloneMessages(base)
	if input.HasEnvelope {
		messages = sdk.CloneMessages(input.BaseMessages)
	}
	return append(messages, sdk.CloneMessages([]sdk.Message{input.Message})[0])
}

func executionInputEnvelopeHasMessage(message sdk.Message) bool {
	return message.Role != "" ||
		message.Content != "" ||
		len(message.ToolCalls) != 0 ||
		message.ToolCallID != ""
}

func executionInputEnvelopeHasPayload(input ExecutionInput) bool {
	return executionInputEnvelopeHasMessage(input.Message) ||
		len(input.BaseMessages) != 0 ||
		input.Environment.SDKAPIVersion != 0 ||
		input.Environment.CompositionDigest != ""
}
