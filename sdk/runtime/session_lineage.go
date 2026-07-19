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

// trajectoryForkAnchor is the parent branch entry a forked agent session must
// inherit; the origin fork invocation records which parent tool call opened it.
type trajectoryForkAnchor struct {
	parentEntryID          string
	originForkInvocationID string
}

func newTrajectorySessionLineage(
	parent *Session,
	mode sdk.AgentSessionMode,
	invocation sdk.Invocation,
	forkAnchor trajectoryForkAnchor,
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
	if forkAnchor.parentEntryID == "" {
		return trajectorySessionLineage{}, errors.New(
			"cannot fork agent session without a parent trajectory fork anchor",
		)
	}
	lineage.parentEntryID = forkAnchor.parentEntryID
	lineage.originForkInvocationID = forkAnchor.originForkInvocationID
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
	if lineage.mode == sdk.AgentSessionResume {
		return lineage.validateExistingResume(metadata, sessionID)
	}
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

func (lineage trajectorySessionLineage) validateExistingResume(
	metadata sdk.TrajectoryMetadata,
	sessionID string,
) error {
	if metadata.Environment.ParentSessionID != lineage.parentSessionID {
		return fmt.Errorf(
			"agent session %q belongs to parent %q, not %q",
			sessionID,
			metadata.Environment.ParentSessionID,
			lineage.parentSessionID,
		)
	}
	switch metadata.Environment.OriginMode {
	case sdk.AgentSessionNew:
		return lineage.validateExistingResumeNew(metadata, sessionID)
	case sdk.AgentSessionFork:
		return lineage.validateExistingResumeFork(metadata, sessionID)
	case "":
		if metadata.ParentID == "" && metadata.ParentEntryID == "" {
			return nil
		}
		return lineage.validateExistingResumeFork(metadata, sessionID)
	default:
		return fmt.Errorf(
			"agent session %q already belongs to %q mode",
			sessionID,
			metadata.Environment.OriginMode,
		)
	}
}

func (lineage trajectorySessionLineage) validateExistingResumeNew(
	metadata sdk.TrajectoryMetadata,
	sessionID string,
) error {
	if metadata.ParentID != "" || metadata.ParentEntryID != "" {
		return fmt.Errorf(
			"agent session %q already forks trajectory %q at %q",
			sessionID,
			metadata.ParentID,
			metadata.ParentEntryID,
		)
	}
	return nil
}

func (lineage trajectorySessionLineage) validateExistingResumeFork(
	metadata sdk.TrajectoryMetadata,
	sessionID string,
) error {
	if metadata.ParentID != lineage.parentSessionID ||
		metadata.ParentEntryID == "" {
		return fmt.Errorf(
			"agent session %q already forks trajectory %q at %q, not %q",
			sessionID,
			metadata.ParentID,
			metadata.ParentEntryID,
			lineage.parentSessionID,
		)
	}
	return nil
}
