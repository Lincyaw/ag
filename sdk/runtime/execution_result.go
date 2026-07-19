package runtime

import (
	"context"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// LoadExecutionResult projects the current terminal checkpoint into the runtime
// execution result read model.
func LoadExecutionResult(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (*Result, error) {
	entry, checkpoint, found, err := durability.LatestExecutionCheckpoint(
		ctx,
		store,
		metadata,
	)
	if err != nil || !found {
		return nil, err
	}
	return resultFromCheckpoint(entry, checkpoint), nil
}

func latestAssistantOutput(messages []sdk.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		if messages[index].Role == sdk.RoleAssistant {
			return messages[index].Content
		}
	}
	return ""
}

func resultFromCheckpoint(
	entry sdk.TrajectoryEntry,
	checkpoint *durability.Checkpoint,
) *Result {
	if checkpoint == nil {
		return nil
	}
	result := &Result{
		Output:     checkpoint.Output,
		Messages:   durability.Messages(checkpoint),
		Turns:      checkpoint.Turns,
		ToolCalls:  checkpoint.ToolCalls,
		Generation: checkpoint.Generation,
	}
	if result.Generation == 0 {
		result.Generation = entry.Generation
	}
	if result.Output == "" {
		result.Output = latestAssistantOutput(checkpoint.Messages)
	}
	if checkpoint.Action.Cause != nil {
		result.Cause = *checkpoint.Action.Cause
	}
	return result
}
