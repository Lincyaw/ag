package runtime

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// ProjectTrajectoryMessages returns the model-visible conversation at the
// trajectory's active head. It is the stable read projection used by clients
// that need to reconstruct a session view without replaying runtime events.
func ProjectTrajectoryMessages(
	trajectory sdk.Trajectory,
) ([]sdk.Message, error) {
	if trajectory.Head == "" {
		return nil, nil
	}
	if trajectory.ID == "" {
		return nil, fmt.Errorf("project trajectory messages: trajectory ID is empty")
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		return nil, err
	}
	return ProjectTrajectoryBranchMessages(trajectory.ID, branch)
}

// ProjectTrajectoryBranchMessages projects an already resolved branch. It is
// useful for storage-backed control planes that load only message-affecting
// payloads instead of materializing the complete trajectory aggregate.
func ProjectTrajectoryBranchMessages(
	trajectoryID string,
	branch []sdk.TrajectoryEntry,
) ([]sdk.Message, error) {
	if trajectoryID == "" {
		return nil, fmt.Errorf("project trajectory branch messages: trajectory ID is empty")
	}
	return durability.BranchMessages(trajectoryID, branch)
}

// ProjectStoredTrajectoryMessages uses payload-free branch inspection to load
// only the entries required by the model-visible conversation projection. It
// starts at the latest legacy snapshot checkpoint, while branch-backed
// checkpoints and later message deltas remain append-only and compact.
func ProjectStoredTrajectoryMessages(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	branch []sdk.TrajectoryEntryInspection,
) ([]sdk.Message, error) {
	return durability.LoadInspectedBranchMessages(
		ctx, store, trajectoryID, branch,
	)
}
