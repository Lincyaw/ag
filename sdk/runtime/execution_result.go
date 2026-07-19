package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// LoadExecutionResult projects the durable execution result read model.
func LoadExecutionResult(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (*Result, error) {
	if metadata.Execution != nil &&
		metadata.Execution.State != sdk.TrajectoryExecutionSucceeded {
		return LoadExecutionTerminalResult(ctx, store, metadata)
	}
	entry, checkpoint, found, err := durability.LatestExecutionCheckpoint(
		ctx,
		store,
		metadata,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return LoadExecutionTerminalResult(ctx, store, metadata)
	}
	return resultFromCheckpoint(entry, checkpoint), nil
}

// LoadExecutionTerminalResult projects a terminal trajectory entry into the
// runtime execution result read model. Terminal entries may be off the active
// branch after failure/cancellation restores the trajectory head.
func LoadExecutionTerminalResult(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (*Result, error) {
	if metadata.Execution == nil {
		return nil, nil
	}
	analyzer, ok := store.(sdk.TrajectoryAnalyzer)
	if !ok {
		return nil, nil
	}
	entries, err := analyzer.AnalyzeEntries(ctx, sdk.TrajectoryEntryQuery{
		TrajectoryID: metadata.ID,
		ExecutionID:  metadata.Execution.ID,
		Kind:         sdk.TrajectoryKindTerminal,
		Limit:        sdk.MaxPageSize,
	})
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	return resultFromTerminal(entries[len(entries)-1])
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

func resultFromTerminal(entry sdk.TrajectoryEntry) (*Result, error) {
	var end sdk.AgentEndPayload
	if err := json.Unmarshal(entry.Payload, &end); err != nil {
		return nil, fmt.Errorf(
			"decode trajectory %q terminal entry %q: %w",
			entry.TrajectoryID,
			entry.ID,
			err,
		)
	}
	output := end.Output
	if output == "" {
		output = latestAssistantOutput(end.Messages)
	}
	return &Result{
		Output:     output,
		Messages:   sdk.CloneMessages(end.Messages),
		Turns:      end.Turns,
		ToolCalls:  end.ToolCalls,
		Generation: entry.Generation,
		Cause:      end.Cause,
	}, nil
}
