package runtime

import (
	"errors"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

type trajectorySessionLineage struct {
	parentSessionID        string
	parentEntryID          string
	originInvocationID     string
	originInvocationRootID string
	originForkInvocationID string
	mode                   sdk.AgentSessionMode
}

func newTrajectorySessionLineage(
	parent *Session,
	mode sdk.AgentSessionMode,
	invocation sdk.Invocation,
	forkHead string,
	forkInvocationID string,
) (trajectorySessionLineage, error) {
	if parent == nil {
		return trajectorySessionLineage{}, errors.New(
			"trajectory lineage parent session is nil",
		)
	}
	lineage := trajectorySessionLineage{
		parentSessionID:        parent.ID(),
		originInvocationID:     invocation.ID,
		originInvocationRootID: invocation.RootID,
		mode:                   mode,
	}
	if lineage.originInvocationRootID == "" {
		lineage.originInvocationRootID = invocation.ID
	}
	if mode != sdk.AgentSessionFork {
		return lineage, nil
	}
	if forkHead == "" {
		return trajectorySessionLineage{}, errors.New(
			"cannot fork agent session without a parent trajectory head",
		)
	}
	lineage.parentEntryID = forkHead
	lineage.originForkInvocationID = forkInvocationID
	if lineage.originForkInvocationID == "" {
		lineage.originForkInvocationID = invocation.ParentID
	}
	return lineage, nil
}

func (lineage trajectorySessionLineage) applyEnvironment(
	environment *sdk.TrajectoryEnvironment,
) {
	environment.ParentSessionID = lineage.parentSessionID
	environment.OriginInvocationID = lineage.originInvocationID
	environment.OriginInvocationRootID = lineage.originInvocationRootID
	environment.OriginMode = lineage.mode
	environment.OriginForkInvocationID = lineage.originForkInvocationID
}

func (lineage trajectorySessionLineage) trajectory(
	id string,
	environment sdk.TrajectoryEnvironment,
) sdk.Trajectory {
	trajectory := sdk.Trajectory{
		ID:          id,
		Environment: environment,
	}
	if lineage.mode == sdk.AgentSessionFork {
		trajectory.ParentID = lineage.parentSessionID
		trajectory.ParentEntryID = lineage.parentEntryID
	}
	return trajectory
}

func (lineage trajectorySessionLineage) validateExisting(
	metadata sdk.TrajectoryMetadata,
	sessionID string,
) error {
	if metadata.Environment.OriginInvocationID != lineage.originInvocationID ||
		metadata.Environment.ParentSessionID != lineage.parentSessionID {
		return fmt.Errorf(
			"agent session %q already belongs to invocation %q from parent %q",
			sessionID,
			metadata.Environment.OriginInvocationID,
			metadata.Environment.ParentSessionID,
		)
	}
	if metadata.Environment.OriginInvocationRootID != "" &&
		metadata.Environment.OriginInvocationRootID !=
			lineage.originInvocationRootID {
		return fmt.Errorf(
			"agent session %q already belongs to invocation root %q",
			sessionID,
			metadata.Environment.OriginInvocationRootID,
		)
	}
	if metadata.Environment.OriginMode != "" &&
		metadata.Environment.OriginMode != lineage.mode {
		return fmt.Errorf(
			"agent session %q already belongs to %q mode",
			sessionID,
			metadata.Environment.OriginMode,
		)
	}
	switch lineage.mode {
	case sdk.AgentSessionFork:
		if metadata.ParentID != lineage.parentSessionID ||
			metadata.ParentEntryID != lineage.parentEntryID {
			return fmt.Errorf(
				"agent session %q already forks trajectory %q at %q, not %q at %q",
				sessionID,
				metadata.ParentID,
				metadata.ParentEntryID,
				lineage.parentSessionID,
				lineage.parentEntryID,
			)
		}
		if metadata.Environment.OriginForkInvocationID != "" &&
			metadata.Environment.OriginForkInvocationID !=
				lineage.originForkInvocationID {
			return fmt.Errorf(
				"agent session %q already forks from invocation %q",
				sessionID,
				metadata.Environment.OriginForkInvocationID,
			)
		}
	case sdk.AgentSessionNew:
		if metadata.ParentID != "" || metadata.ParentEntryID != "" {
			return fmt.Errorf(
				"agent session %q already forks trajectory %q at %q",
				sessionID,
				metadata.ParentID,
				metadata.ParentEntryID,
			)
		}
	}
	return nil
}
