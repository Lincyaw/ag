package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// ErrResumeEnvironmentMismatch rejects recovery under incompatible resources.
var ErrResumeEnvironmentMismatch = errors.New(
	"current runtime composition is incompatible with trajectory",
)

const (
	// Legacy execution-input attributes are read for trajectories created before
	// the durable user_message payload started carrying execution input state.
	executionEnvironmentAttribute       = "ag.runtime.environment"
	executionCompositionDigestAttribute = "ag.runtime.composition_digest"
	executionSDKAPIVersionAttribute     = "ag.runtime.sdk_api_version"
)

type resumeEnvironment struct {
	environment            sdk.TrajectoryEnvironment
	hasCompositionSnapshot bool
}

func newResumeEnvironment(
	environment sdk.TrajectoryEnvironment,
) resumeEnvironment {
	return resumeEnvironment{
		environment: environment,
		hasCompositionSnapshot: sdk.TrajectoryEnvironmentHasCompositionSnapshot(
			environment,
		),
	}
}

func newTrajectoryEnvironment(
	runtime *Runtime,
	snapshot *registrySnapshot,
	config SessionConfig,
) (sdk.TrajectoryEnvironment, error) {
	catalog := catalogFromSnapshot(snapshot)
	environment := sdk.TrajectoryEnvironment{
		SDKAPIVersion:     sdk.APIVersion,
		RuntimeVersion:    runtime.version,
		CreatedGeneration: catalog.Generation,
		RequestedProvider: config.Provider,
		SystemDigest:      sdk.TrajectorySystemDigest(config.System),
		Providers:         append([]sdk.ProviderSpec(nil), catalog.Providers...),
		Tools:             append([]sdk.ToolSpec(nil), catalog.Tools...),
		Agents:            append([]sdk.AgentSpec(nil), catalog.Agents...),
		Hooks:             append([]sdk.HookSpec(nil), catalog.Hooks...),
		Subscribers:       append([]sdk.SubscriberSpec(nil), catalog.Subscribers...),
		Capabilities:      append([]sdk.CapabilitySpec(nil), catalog.Capabilities...),
		Events:            append([]sdk.EventContract(nil), catalog.Events...),
	}
	for _, plugin := range catalog.Plugins {
		environment.Plugins = append(environment.Plugins, sdk.TrajectoryPlugin{
			Name:      plugin.Name,
			Version:   plugin.Version,
			Registers: append([]string(nil), plugin.Registers...),
		})
	}
	environment, err := sdk.FinalizeTrajectoryEnvironment(environment)
	if err != nil {
		return sdk.TrajectoryEnvironment{}, fmt.Errorf(
			"finalize trajectory environment: %w",
			err,
		)
	}
	return environment, nil
}

func newExecutionEnvironment(
	runtime *Runtime,
	snapshot *registrySnapshot,
	config SessionConfig,
) (*registrySnapshot, sdk.TrajectoryEnvironment, error) {
	executionSnapshot := executionSnapshotForSession(snapshot)
	environment, err := newTrajectoryEnvironment(runtime, executionSnapshot, config)
	if err != nil {
		return nil, sdk.TrajectoryEnvironment{}, err
	}
	return executionSnapshot, environment, nil
}

func executionSnapshotForSession(current *registrySnapshot) *registrySnapshot {
	result := &registrySnapshot{
		generation:   current.generation,
		plugins:      make(map[string]*mountState),
		providers:    make(map[string]ownedResource[sdk.Provider, sdk.ProviderSpec]),
		tools:        make(map[string]ownedResource[sdk.Tool, sdk.ToolSpec]),
		agents:       make(map[string]ownedAgent),
		hooks:        make(map[string][]ownedHook),
		subscribers:  make(map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec]),
		capabilities: make(map[string]ownedResource[sdk.Capability, sdk.CapabilitySpec]),
		events:       make(map[string]ownedEvent),
	}
	for name, event := range current.events {
		if builtinEventInTrajectoryEnvironment(name) {
			result.events[name] = event
		}
	}
	for name, provider := range current.providers {
		result.providers[name] = provider
		result.includePluginOwner(provider.owner)
	}
	for name, tool := range current.tools {
		result.tools[name] = tool
		result.includePluginOwner(tool.owner)
	}
	if executionCanInvokeAgents(result) {
		for name, agent := range current.agents {
			result.agents[name] = agent
			result.includePluginOwner(agent.owner)
		}
	}
	for event, hooks := range current.hooks {
		if !builtinEventInTrajectoryEnvironment(event) {
			continue
		}
		for _, hook := range hooks {
			result.hooks[event] = append(result.hooks[event], hook)
			result.includePluginOwner(hook.owner)
		}
	}
	for name, subscriber := range current.subscribers {
		if !subscriberObservesTrajectoryEnvironment(subscriber.spec) {
			continue
		}
		result.subscribers[name] = subscriber
		result.includePluginOwner(subscriber.owner)
	}
	return result
}

func executionCanInvokeAgents(snapshot *registrySnapshot) bool {
	// Structured agent/workflow invokers are installed only for tool calls.
	return len(snapshot.tools) != 0
}

func subscriberObservesTrajectoryEnvironment(spec sdk.SubscriberSpec) bool {
	for _, event := range spec.Events {
		if builtinEventInTrajectoryEnvironment(event) {
			return true
		}
	}
	return false
}

func validateResumeEnvironment(
	recorded sdk.TrajectoryEnvironment,
	current sdk.TrajectoryEnvironment,
) error {
	// Schema-zero trajectories predate environment snapshots. They remain
	// resumable, but cannot receive exact-composition guarantees retroactively.
	if recorded.SDKAPIVersion == 0 && recorded.CompositionDigest == "" {
		return nil
	}
	if recorded.SDKAPIVersion != current.SDKAPIVersion {
		return fmt.Errorf(
			"%w: SDK API version changed from %d to %d",
			ErrResumeEnvironmentMismatch,
			recorded.SDKAPIVersion,
			current.SDKAPIVersion,
		)
	}
	if recorded.CompositionDigest != current.CompositionDigest {
		return fmt.Errorf(
			"%w: composition digest changed from %s to %s",
			ErrResumeEnvironmentMismatch,
			recorded.CompositionDigest,
			current.CompositionDigest,
		)
	}
	return nil
}

func snapshotForTrajectoryEnvironment(
	current *registrySnapshot,
	environment sdk.TrajectoryEnvironment,
) (*registrySnapshot, error) {
	if environment.SDKAPIVersion == 0 && environment.CompositionDigest == "" {
		return current, nil
	}
	result := &registrySnapshot{
		generation: current.generation,
		plugins: make(
			map[string]*mountState,
			len(environment.Plugins),
		),
		providers: make(
			map[string]ownedResource[sdk.Provider, sdk.ProviderSpec],
			len(environment.Providers),
		),
		tools: make(
			map[string]ownedResource[sdk.Tool, sdk.ToolSpec],
			len(environment.Tools),
		),
		agents: make(map[string]ownedAgent, len(environment.Agents)),
		hooks:  make(map[string][]ownedHook),
		subscribers: make(
			map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec],
			len(environment.Subscribers),
		),
		capabilities: make(
			map[string]ownedResource[sdk.Capability, sdk.CapabilitySpec],
			len(environment.Capabilities),
		),
		events: make(map[string]ownedEvent, len(environment.Events)),
	}
	for _, plugin := range environment.Plugins {
		state, exists := current.plugins[plugin.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: plugin %q is unavailable",
				plugin.Name,
			)
		}
		result.includePluginOwner(state)
	}
	for _, spec := range environment.Providers {
		provider, exists := current.providers[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: provider %q is unavailable",
				spec.Name,
			)
		}
		result.providers[spec.Name] = provider
		result.includePluginOwner(provider.owner)
	}
	for _, spec := range environment.Tools {
		tool, exists := current.tools[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: tool %q is unavailable",
				spec.Name,
			)
		}
		result.tools[spec.Name] = tool
		result.includePluginOwner(tool.owner)
	}
	for _, spec := range environment.Agents {
		agent, exists := current.agents[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: agent %q is unavailable",
				spec.Name,
			)
		}
		result.agents[spec.Name] = agent
		result.includePluginOwner(agent.owner)
	}
	if err := copyEnvironmentHooks(result.hooks, current.hooks, environment.Hooks); err != nil {
		return nil, err
	}
	for _, hooks := range result.hooks {
		for _, hook := range hooks {
			result.includePluginOwner(hook.owner)
		}
	}
	for _, spec := range environment.Subscribers {
		subscriber, exists := current.subscribers[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: subscriber %q is unavailable",
				spec.Name,
			)
		}
		result.subscribers[spec.Name] = subscriber
		result.includePluginOwner(subscriber.owner)
	}
	for _, spec := range environment.Capabilities {
		capability, exists := current.capabilities[spec.Name]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory environment: capability %q is unavailable",
				spec.Name,
			)
		}
		result.capabilities[spec.Name] = capability
		result.includePluginOwner(capability.owner)
	}
	if err := copyEnvironmentEvents(result.events, current.events, environment.Events); err != nil {
		return nil, err
	}
	for _, event := range result.events {
		result.includePluginOwner(event.owner)
	}
	return result, nil
}

func copyEnvironmentHooks(
	dst map[string][]ownedHook,
	src map[string][]ownedHook,
	specs []sdk.HookSpec,
) error {
	hookNames := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		hookNames[spec.Name] = struct{}{}
	}
	for event, hooks := range src {
		for _, hook := range hooks {
			if _, include := hookNames[hook.spec.Name]; !include {
				continue
			}
			dst[event] = append(dst[event], hook)
			delete(hookNames, hook.spec.Name)
		}
	}
	for name := range hookNames {
		return fmt.Errorf(
			"trajectory environment: hook %q is unavailable",
			name,
		)
	}
	return nil
}

func copyEnvironmentEvents(
	dst map[string]ownedEvent,
	src map[string]ownedEvent,
	contracts []sdk.EventContract,
) error {
	for _, contract := range contracts {
		event, exists := src[contract.Name]
		if !exists {
			return fmt.Errorf(
				"trajectory environment: event %q is unavailable",
				contract.Name,
			)
		}
		event.contract = sdk.CloneEventContract(event.contract)
		dst[contract.Name] = event
	}
	return nil
}

func snapshotSourceForRecordedEnvironment(
	fallback sdk.TrajectoryEnvironment,
	recorded resumeEnvironment,
) sdk.TrajectoryEnvironment {
	if recorded.hasCompositionSnapshot {
		return recorded.environment
	}
	if recorded.environment.CompositionDigest != "" &&
		recorded.environment.CompositionDigest != fallback.CompositionDigest {
		return sdk.TrajectoryEnvironment{}
	}
	return fallback
}

func (runtime *Runtime) resolveResumeSnapshot(
	current *registrySnapshot,
	fallback sdk.TrajectoryEnvironment,
	recorded resumeEnvironment,
	config SessionConfig,
) (*registrySnapshot, error) {
	resumeSnapshot, err := snapshotForTrajectoryEnvironment(
		current,
		snapshotSourceForRecordedEnvironment(
			fallback,
			recorded,
		),
	)
	if err != nil {
		return nil, err
	}
	resolved, err := newTrajectoryEnvironment(
		runtime,
		resumeSnapshot,
		config,
	)
	if err != nil {
		return nil, err
	}
	if err := validateResumeEnvironment(recorded.environment, resolved); err != nil {
		return nil, err
	}
	return resumeSnapshot, nil
}

func (runtime *Runtime) acquireResolvedResumeSnapshot(
	current *snapshotLease,
	fallback sdk.TrajectoryEnvironment,
	recorded resumeEnvironment,
	config SessionConfig,
) (*snapshotLease, error) {
	if current == nil || current.snapshot == nil {
		return nil, errors.New("current runtime snapshot lease is nil")
	}
	resumeSnapshot, err := runtime.resolveResumeSnapshot(
		current.snapshot,
		fallback,
		recorded,
		config,
	)
	if err != nil {
		return nil, err
	}
	return runtime.acquireRegistrySnapshot(resumeSnapshot)
}

func checkpointResumeEnvironment(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
	checkpointEntry sdk.TrajectoryEntry,
) (resumeEnvironment, error) {
	if checkpointEntry.ID == "" ||
		checkpointEntry.Fields.ExecutionID == "" {
		return newResumeEnvironment(metadata.Environment), nil
	}
	input, err := executionInputEntryBefore(
		ctx,
		store,
		metadata.ID,
		checkpointEntry,
	)
	if err != nil {
		return resumeEnvironment{}, err
	}
	return executionResumeEnvironment(metadata.Environment, input)
}

func executionInputEntryBefore(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	from sdk.TrajectoryEntry,
) (sdk.TrajectoryEntry, error) {
	executionID := from.Fields.ExecutionID
	seen := make(map[string]struct{})
	entry := from
	for {
		if entry.Kind == sdk.TrajectoryKindUserMessage &&
			entry.Fields.ExecutionID == executionID {
			return entry, nil
		}
		if entry.ParentID == "" {
			return sdk.TrajectoryEntry{}, fmt.Errorf(
				"trajectory %q checkpoint %q has no input entry for execution %q",
				trajectoryID,
				from.ID,
				executionID,
			)
		}
		if _, cycle := seen[entry.ParentID]; cycle {
			return sdk.TrajectoryEntry{}, fmt.Errorf(
				"trajectory %q checkpoint %q ancestry contains a cycle at %q",
				trajectoryID,
				from.ID,
				entry.ParentID,
			)
		}
		seen[entry.ParentID] = struct{}{}
		next, err := store.LoadEntry(ctx, trajectoryID, entry.ParentID)
		if err != nil {
			return sdk.TrajectoryEntry{}, err
		}
		entry = next
	}
}

func executionResumeEnvironment(
	fallback sdk.TrajectoryEnvironment,
	input sdk.TrajectoryEntry,
) (resumeEnvironment, error) {
	if input.Kind == sdk.TrajectoryKindUserMessage {
		executionInput, err := durability.DecodeExecutionInput(
			input.TrajectoryID,
			input,
		)
		if err != nil {
			return resumeEnvironment{}, err
		}
		if executionInput.HasEnvironment {
			if executionInput.Environment.SDKAPIVersion < 1 ||
				executionInput.Environment.CompositionDigest == "" {
				return resumeEnvironment{}, fmt.Errorf(
					"trajectory execution input %s has invalid runtime environment",
					input.ID,
				)
			}
			return resumeEnvironment{
				environment:            executionInput.Environment,
				hasCompositionSnapshot: true,
			}, nil
		}
	}
	if rawEnvironment := input.Attributes[executionEnvironmentAttribute]; rawEnvironment != "" {
		var environment sdk.TrajectoryEnvironment
		if err := json.Unmarshal(
			[]byte(rawEnvironment),
			&environment,
		); err != nil {
			return resumeEnvironment{}, fmt.Errorf(
				"decode trajectory execution environment %s: %w",
				input.ID,
				err,
			)
		}
		if environment.SDKAPIVersion < 1 ||
			environment.CompositionDigest == "" {
			return resumeEnvironment{}, fmt.Errorf(
				"trajectory execution input %s has invalid runtime environment",
				input.ID,
			)
		}
		return resumeEnvironment{
			environment:            environment,
			hasCompositionSnapshot: true,
		}, nil
	}
	digest := input.Attributes[executionCompositionDigestAttribute]
	rawVersion := input.Attributes[executionSDKAPIVersionAttribute]
	if digest == "" && rawVersion == "" {
		return newResumeEnvironment(fallback), nil
	}
	if digest == "" || rawVersion == "" {
		return resumeEnvironment{}, fmt.Errorf(
			"trajectory execution input %s has an incomplete runtime environment",
			input.ID,
		)
	}
	apiVersion, err := strconv.Atoi(rawVersion)
	if err != nil || apiVersion < 1 {
		return resumeEnvironment{}, fmt.Errorf(
			"trajectory execution input %s has invalid SDK API version %q",
			input.ID,
			rawVersion,
		)
	}
	return resumeEnvironment{
		environment: sdk.TrajectoryEnvironment{
			SDKAPIVersion:     apiVersion,
			CompositionDigest: digest,
		},
	}, nil
}
