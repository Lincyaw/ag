package runtime

import (
	"slices"

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
	session.applyContextInjectionProjection(checkpoint)
}

func (session *Session) applyExecutionBaseProjection(
	base durability.ExecutionCompletionBase,
	checkpoint *durability.Checkpoint,
) {
	session.applyMessageProjection(base.Messages)
	session.applyCheckpointConfig(checkpoint)
	session.applyContextInjectionProjection(checkpoint)
}

func (session *Session) applyContextInjectionProjection(
	checkpoint *durability.Checkpoint,
) {
	if checkpoint == nil {
		session.consumedContext = nil
		session.contextInjections = nil
		return
	}
	session.contextInjections = sdk.CloneContextInjections(
		checkpoint.ContextInjections,
	)
	session.consumedContext = make(map[string]struct{})
	for _, id := range checkpoint.ConsumedContextInjectionIDs {
		if id == "" {
			continue
		}
		session.consumedContext[id] = struct{}{}
	}
	for _, injection := range session.contextInjections {
		if injection.ID == "" {
			continue
		}
		session.consumedContext[injection.ID] = struct{}{}
	}
	if len(session.consumedContext) == 0 {
		session.consumedContext = nil
	}
}

func (session *Session) consumedContextInjectionSet() map[string]struct{} {
	if len(session.consumedContext) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(session.consumedContext))
	for id := range session.consumedContext {
		result[id] = struct{}{}
	}
	return result
}

func (session *Session) markContextInjectionsConsumed(
	injections []sdk.ContextInjection,
) {
	if len(injections) == 0 {
		return
	}
	if session.consumedContext == nil {
		session.consumedContext = make(map[string]struct{}, len(injections))
	}
	for _, injection := range injections {
		if injection.ID == "" {
			continue
		}
		session.consumedContext[injection.ID] = struct{}{}
		if session.hasContextInjectionProjection(injection.ID) {
			continue
		}
		session.contextInjections = append(
			session.contextInjections,
			sdk.CloneContextInjection(injection),
		)
	}
}

func (session *Session) hasContextInjectionProjection(id string) bool {
	if id == "" {
		return false
	}
	for _, injection := range session.contextInjections {
		if injection.ID == id {
			return true
		}
	}
	return false
}

func (session *Session) contextInjectionProjection(
	injections []sdk.ContextInjection,
) []sdk.ContextInjection {
	result := sdk.CloneContextInjections(session.contextInjections)
	seen := make(map[string]struct{}, len(result)+len(injections))
	for _, injection := range result {
		if injection.ID != "" {
			seen[injection.ID] = struct{}{}
		}
	}
	for _, injection := range injections {
		if injection.ID == "" {
			continue
		}
		if _, ok := seen[injection.ID]; ok {
			continue
		}
		seen[injection.ID] = struct{}{}
		result = append(result, sdk.CloneContextInjection(injection))
	}
	return result
}

func (session *Session) consumedContextInjectionIDs(
	injections []sdk.ContextInjection,
) []string {
	ids := make([]string, 0, len(session.consumedContext)+len(injections))
	seen := make(map[string]struct{}, len(session.consumedContext)+len(injections))
	for id := range session.consumedContext {
		if id == "" {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for _, injection := range injections {
		if injection.ID == "" {
			continue
		}
		if _, ok := seen[injection.ID]; ok {
			continue
		}
		seen[injection.ID] = struct{}{}
		ids = append(ids, injection.ID)
	}
	slices.Sort(ids)
	return ids
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
