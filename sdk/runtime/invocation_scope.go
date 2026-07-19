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

type sessionInvocationScope struct {
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
	if session.invocation.rootID != "" {
		scope.rootID = session.invocation.rootID
	}
	if session.invocation.parentID != "" {
		scope.parentID = session.invocation.parentID
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
	scope, ok := sessionInvocationScopeFromTrajectoryOrigin(environment)
	if ok {
		session.invocation = scope
	}
}

func (session *Session) applyInvocationScope(invocation sdk.Invocation) {
	session.invocation = sessionInvocationScopeFromInvocation(invocation)
}

func sessionInvocationScopeFromTrajectoryOrigin(
	environment sdk.TrajectoryEnvironment,
) (sessionInvocationScope, bool) {
	if environment.OriginInvocationID == "" {
		return sessionInvocationScope{}, false
	}
	rootID := environment.OriginInvocationRootID
	if rootID == "" {
		rootID = environment.OriginInvocationID
	}
	return sessionInvocationScope{
		rootID:   rootID,
		parentID: environment.OriginInvocationID,
	}, true
}

func sessionInvocationScopeFromInvocation(
	invocation sdk.Invocation,
) sessionInvocationScope {
	rootID := invocation.RootID
	if rootID == "" {
		rootID = invocation.ID
	}
	return sessionInvocationScope{
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
