package runtime

import (
	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

func (session *Session) applyMessageProjection(messages []sdk.Message) {
	session.messages = sdk.CloneMessages(messages)
}

func (session *Session) applyCheckpointConfig(
	checkpoint *durability.Checkpoint,
) {
	if checkpoint == nil {
		return
	}
	session.config.System = checkpoint.System
	if checkpoint.Provider != "" {
		session.config.Provider = checkpoint.Provider
	}
}

func (session *Session) applyCheckpointProjection(
	checkpoint *durability.Checkpoint,
) {
	if checkpoint == nil {
		return
	}
	session.applyMessageProjection(checkpoint.Messages)
	session.applyCheckpointConfig(checkpoint)
}

func (session *Session) applyExecutionBaseProjection(
	base durability.ExecutionCompletionBase,
	checkpoint *durability.Checkpoint,
) {
	session.applyMessageProjection(base.Messages)
	session.applyCheckpointConfig(checkpoint)
}

func exactResumeConfigProjection(
	config SessionConfig,
	checkpoint *durability.Checkpoint,
	recorded resumeEnvironment,
) SessionConfig {
	if checkpoint != nil {
		config.System = checkpoint.System
		if checkpoint.Provider != "" {
			config.Provider = checkpoint.Provider
		}
	}
	if (checkpoint == nil || checkpoint.Provider == "") &&
		recorded.environment.RequestedProvider != "" {
		config.Provider = recorded.environment.RequestedProvider
	}
	return config
}

type exactResumeProjection struct {
	Config SessionConfig
	Lease  *snapshotLease
}

func (projection exactResumeProjection) snapshot() *registrySnapshot {
	if projection.Lease == nil {
		return nil
	}
	return projection.Lease.snapshot
}

func (runtime *Runtime) acquireExactResumeProjection(
	fallback sdk.TrajectoryEnvironment,
	config SessionConfig,
	checkpoint *durability.Checkpoint,
	recorded resumeEnvironment,
) (exactResumeProjection, error) {
	config = exactResumeConfigProjection(config, checkpoint, recorded)
	currentLease, err := runtime.acquireSnapshot()
	if err != nil {
		return exactResumeProjection{}, err
	}
	defer currentLease.release()
	resumeLease, err := runtime.acquireResolvedResumeSnapshot(
		currentLease,
		fallback,
		recorded,
		config,
	)
	if err != nil {
		return exactResumeProjection{}, err
	}
	return exactResumeProjection{
		Config: config,
		Lease:  resumeLease,
	}, nil
}
