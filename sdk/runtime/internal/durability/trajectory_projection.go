package durability

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

// LoadInspectedBranchMessages reconstructs model-visible messages from a
// payload-free branch index. Only checkpoint payloads needed to locate the
// latest legacy snapshot and message-producing deltas after it are fetched.
func LoadInspectedBranchMessages(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	branch []sdk.TrajectoryEntryInspection,
) ([]sdk.Message, error) {
	if store == nil {
		return nil, fmt.Errorf("load inspected branch messages: store is nil")
	}
	if trajectoryID == "" {
		return nil, fmt.Errorf("load inspected branch messages: trajectory ID is empty")
	}
	loaded := make(map[int]sdk.TrajectoryEntry)
	start := 0
	for index := len(branch) - 1; index >= 0; index-- {
		if branch[index].Kind != sdk.TrajectoryKindCheckpoint {
			continue
		}
		entry, err := store.LoadEntry(ctx, trajectoryID, branch[index].ID)
		if err != nil {
			return nil, err
		}
		checkpoint, err := DecodeCheckpoint(trajectoryID, entry)
		if err != nil {
			return nil, err
		}
		loaded[index] = entry
		if checkpoint.MessageMode == CheckpointMessagesSnapshot {
			start = index
			break
		}
	}

	entries := make([]sdk.TrajectoryEntry, 0, len(branch)-start)
	for index := start; index < len(branch); index++ {
		inspection := branch[index]
		if !messageProjectionEntryKind(inspection.Kind) {
			continue
		}
		entry, ok := loaded[index]
		if !ok {
			var err error
			entry, err = store.LoadEntry(ctx, trajectoryID, inspection.ID)
			if err != nil {
				return nil, err
			}
		}
		entries = append(entries, entry)
	}
	return BranchMessages(trajectoryID, entries)
}

func messageProjectionEntryKind(kind sdk.TrajectoryKind) bool {
	switch kind {
	case sdk.TrajectoryKindUserMessage,
		sdk.TrajectoryKindAgentStart,
		sdk.TrajectoryKindProviderResponse,
		sdk.TrajectoryKindToolResult,
		sdk.TrajectoryKindCheckpoint,
		sdk.TrajectoryKindTerminal:
		return true
	default:
		return false
	}
}
