package runtime

import (
	"slices"

	"github.com/lincyaw/ag/sdk"
)

type invocationScope struct {
	rootID      string
	parentID    string
	sessionID   string
	executionID string
}

type causalInvocationScope struct {
	rootID   string
	parentID string
}

type invocationNodeConfig struct {
	id              string
	groupID         string
	targetSessionID string
	dependencies    []string
	ordinal         uint32
}

type childInvocationConfig struct {
	kind            string
	coordinate      string
	groupCoordinate string
	targetSessionID string
	dependencies    []string
	ordinal         uint32
}

// executionInvocation derives a stable causal node for one execution step.
func (session *Session) executionInvocation(
	kind string,
	coordinate string,
	groupCoordinate string,
	dependencies []string,
	ordinal uint32,
) sdk.Invocation {
	groupID := ""
	if groupCoordinate != "" {
		groupID = session.executionOperationKey("group", groupCoordinate)
	}
	return newExecutionInvocationScope(session).invocation(
		invocationNodeConfig{
			id:           session.executionOperationKey(kind, coordinate),
			groupID:      groupID,
			dependencies: dependencies,
			ordinal:      ordinal,
		},
	)
}

func (invoker *scopedAgentInvoker) childInvocation(
	config childInvocationConfig,
) sdk.Invocation {
	groupID := ""
	if config.groupCoordinate != "" {
		groupID = invoker.parentSession.executionOperationKey(
			"group",
			config.groupCoordinate,
		)
	}
	return newChildInvocationScope(invoker.parentInvocation).invocation(
		invocationNodeConfig{
			id: invoker.parentSession.executionOperationKey(
				config.kind,
				invoker.parentInvocation.ID+"/"+config.coordinate,
			),
			groupID:         groupID,
			targetSessionID: config.targetSessionID,
			dependencies:    config.dependencies,
			ordinal:         config.ordinal,
		},
	)
}

func newExecutionInvocationScope(session *Session) invocationScope {
	executionID, _ := session.activeExecution()
	scope := invocationScope{
		rootID:      executionID,
		parentID:    executionID,
		sessionID:   session.config.ID,
		executionID: executionID,
	}
	if session.causal.rootID != "" {
		scope.rootID = session.causal.rootID
	}
	if session.causal.parentID != "" {
		scope.parentID = session.causal.parentID
	}
	return scope
}

func newChildInvocationScope(parent sdk.Invocation) invocationScope {
	rootID := parent.RootID
	if rootID == "" {
		rootID = parent.ID
	}
	return invocationScope{
		rootID:      rootID,
		parentID:    parent.ID,
		sessionID:   parent.SessionID,
		executionID: parent.ExecutionID,
	}
}

func (session *Session) applyTrajectoryOrigin(
	environment sdk.TrajectoryEnvironment,
) {
	scope, ok := causalInvocationScopeFromTrajectoryOrigin(environment)
	if ok {
		session.causal = scope
	}
}

func (session *Session) applyInvocationScope(invocation sdk.Invocation) {
	session.causal = causalInvocationScopeFromInvocation(invocation)
}

func causalInvocationScopeFromTrajectoryOrigin(
	environment sdk.TrajectoryEnvironment,
) (causalInvocationScope, bool) {
	if environment.OriginInvocationID == "" {
		return causalInvocationScope{}, false
	}
	rootID := environment.OriginInvocationRootID
	if rootID == "" {
		rootID = environment.OriginInvocationID
	}
	return causalInvocationScope{
		rootID:   rootID,
		parentID: environment.OriginInvocationID,
	}, true
}

func causalInvocationScopeFromInvocation(
	invocation sdk.Invocation,
) causalInvocationScope {
	rootID := invocation.RootID
	if rootID == "" {
		rootID = invocation.ID
	}
	return causalInvocationScope{
		rootID:   rootID,
		parentID: invocation.ID,
	}
}

func (scope invocationScope) invocation(
	config invocationNodeConfig,
) sdk.Invocation {
	return sdk.Invocation{
		ID:              config.id,
		RootID:          scope.rootID,
		ParentID:        scope.parentID,
		GroupID:         config.groupID,
		SessionID:       scope.sessionID,
		TargetSessionID: config.targetSessionID,
		ExecutionID:     scope.executionID,
		Dependencies:    slices.Clone(config.dependencies),
		Ordinal:         config.ordinal,
	}
}
