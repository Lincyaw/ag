package runtime

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

// ownedResource ties a published resource to its plugin lifecycle owner.
type ownedResource[Resource, Spec any] struct {
	value Resource
	spec  Spec
	owner *mountState
}

func ownResource[Resource, Spec any](
	owner *mountState,
	resource plugincontract.Contribution[Resource, Spec],
) ownedResource[Resource, Spec] {
	return ownedResource[Resource, Spec]{
		value: resource.Value,
		spec:  resource.Spec,
		owner: owner,
	}
}

func ownAsyncResource[Resource, Spec any](
	owner *mountState,
	kind string,
	name string,
	value any,
	spec Spec,
) (ownedResource[Resource, Spec], error) {
	asynchronous, ok := value.(Resource)
	if !ok {
		return ownedResource[Resource, Spec]{}, fmt.Errorf(
			"%s %q has no asynchronous execution implementation",
			kind,
			name,
		)
	}
	return ownedResource[Resource, Spec]{
		value: asynchronous,
		spec:  spec,
		owner: owner,
	}, nil
}

func (resource ownedResource[Resource, Spec]) resourceIdentity(
	kind sdk.ResourceKind,
	name string,
) sdk.ResourceIdentity {
	return sdk.NewResourceIdentity(
		resource.owner.manifest,
		kind,
		name,
		resource.spec,
	)
}

func (resource ownedResource[Resource, Spec]) resourceRevision(
	kind sdk.ResourceKind,
	name string,
) string {
	return resource.resourceIdentity(kind, name).Revision()
}

type ownedHook struct {
	ownedResource[sdk.Hook, sdk.HookSpec]
	seq uint64
}

type ownedEvent struct {
	owner    *mountState
	contract sdk.EventContract
}

type ownedAgent struct {
	owner *mountState
	spec  sdk.AgentSpec
}

func (agent ownedAgent) resourceIdentity(name string) sdk.ResourceIdentity {
	return sdk.NewResourceIdentity(
		agent.owner.manifest,
		sdk.ResourceKindAgent,
		name,
		agent.spec,
	)
}

func (agent ownedAgent) resourceRevision(name string) string {
	return agent.resourceIdentity(name).Revision()
}

type registrySnapshot struct {
	generation   uint64
	plugins      map[string]*mountState
	providers    map[string]ownedResource[sdk.AsyncProvider, sdk.ProviderSpec]
	tools        map[string]ownedResource[sdk.AsyncTool, sdk.ToolSpec]
	agents       map[string]ownedAgent
	hooks        map[string][]ownedHook
	subscribers  map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec]
	capabilities map[string]ownedResource[sdk.AsyncCapability, sdk.CapabilitySpec]
	events       map[string]ownedEvent
}

func (snapshot *registrySnapshot) includePluginOwner(owner *mountState) {
	if owner != nil {
		snapshot.plugins[owner.manifest.Name] = owner
	}
}

func initialSnapshot() *registrySnapshot {
	snapshot := &registrySnapshot{
		generation:   1,
		plugins:      make(map[string]*mountState),
		providers:    make(map[string]ownedResource[sdk.AsyncProvider, sdk.ProviderSpec]),
		tools:        make(map[string]ownedResource[sdk.AsyncTool, sdk.ToolSpec]),
		agents:       make(map[string]ownedAgent),
		hooks:        make(map[string][]ownedHook),
		subscribers:  make(map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec]),
		capabilities: make(map[string]ownedResource[sdk.AsyncCapability, sdk.CapabilitySpec]),
		events:       make(map[string]ownedEvent),
	}
	for _, builtin := range builtinEventContracts {
		snapshot.events[builtin.Name] = ownedEvent{
			contract: sdk.CloneEventContract(builtin.EventContract),
		}
	}
	return snapshot
}

func (snapshot *registrySnapshot) clone() *registrySnapshot {
	result := &registrySnapshot{
		generation:   snapshot.generation,
		plugins:      maps.Clone(snapshot.plugins),
		providers:    maps.Clone(snapshot.providers),
		tools:        make(map[string]ownedResource[sdk.AsyncTool, sdk.ToolSpec], len(snapshot.tools)),
		agents:       make(map[string]ownedAgent, len(snapshot.agents)),
		hooks:        make(map[string][]ownedHook, len(snapshot.hooks)),
		subscribers:  make(map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec], len(snapshot.subscribers)),
		capabilities: make(map[string]ownedResource[sdk.AsyncCapability, sdk.CapabilitySpec], len(snapshot.capabilities)),
		events:       make(map[string]ownedEvent, len(snapshot.events)),
	}
	for name, tool := range snapshot.tools {
		tool.spec = sdk.CloneToolSpec(tool.spec)
		result.tools[name] = tool
	}
	for name, agent := range snapshot.agents {
		agent.spec = sdk.CloneAgentSpec(agent.spec)
		result.agents[name] = agent
	}
	for event, hooks := range snapshot.hooks {
		result.hooks[event] = append([]ownedHook(nil), hooks...)
	}
	for name, subscriber := range snapshot.subscribers {
		subscriber.spec = sdk.CloneSubscriberSpec(subscriber.spec)
		result.subscribers[name] = subscriber
	}
	for name, capability := range snapshot.capabilities {
		capability.spec = sdk.CloneCapabilitySpec(capability.spec)
		result.capabilities[name] = capability
	}
	for name, event := range snapshot.events {
		event.contract = sdk.CloneEventContract(event.contract)
		result.events[name] = event
	}
	return result
}

func (snapshot *registrySnapshot) resources() map[string]struct{} {
	resources := make(map[string]struct{})
	for name := range snapshot.plugins {
		resources[sdk.PluginResource(name)] = struct{}{}
	}
	for name := range snapshot.providers {
		resources[sdk.ProviderResource(name)] = struct{}{}
	}
	for name := range snapshot.tools {
		resources[sdk.ToolResource(name)] = struct{}{}
	}
	for name := range snapshot.agents {
		resources[sdk.AgentResource(name)] = struct{}{}
	}
	for _, hooks := range snapshot.hooks {
		for _, hook := range hooks {
			resources[sdk.HookResource(hook.spec.Name)] = struct{}{}
		}
	}
	for name := range snapshot.subscribers {
		resources[sdk.SubscriberResource(name)] = struct{}{}
	}
	for name := range snapshot.capabilities {
		resources[sdk.CapabilityResource(name)] = struct{}{}
	}
	for name := range snapshot.events {
		resources[sdk.EventResource(name)] = struct{}{}
	}
	return resources
}

func (snapshot *registrySnapshot) validateComposition() error {
	resources := snapshot.resources()
	for _, plugin := range snapshot.plugins {
		if err := validatePluginResources(plugin.manifest, resources); err != nil {
			return err
		}
	}
	for _, agent := range snapshot.agents {
		if err := validateAgentResources(
			agent.owner.manifest.Name,
			agent.spec,
			resources,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateAgentResources(
	plugin string,
	spec sdk.AgentSpec,
	resources map[string]struct{},
) error {
	if spec.Provider != "" {
		if _, exists := resources[sdk.ProviderResource(spec.Provider)]; !exists {
			return fmt.Errorf(
				"plugin %q agent %q references unavailable provider %q",
				plugin,
				spec.Name,
				spec.Provider,
			)
		}
	}
	for _, tool := range spec.Tools {
		if _, exists := resources[sdk.ToolResource(tool)]; !exists {
			return fmt.Errorf(
				"plugin %q agent %q references unavailable tool %q",
				plugin,
				spec.Name,
				tool,
			)
		}
	}
	return nil
}

func validatePluginResources(
	manifest sdk.Manifest,
	resources map[string]struct{},
) error {
	for _, conflict := range manifest.Conflicts {
		resource := normalizeRequirement(conflict)
		if _, exists := resources[resource]; exists {
			return fmt.Errorf(
				"plugin %q conflicts with resource %q",
				manifest.Name,
				resource,
			)
		}
	}
	for _, required := range manifest.Requires {
		resource := normalizeRequirement(required)
		if _, exists := resources[resource]; !exists {
			return fmt.Errorf(
				"plugin %q requires unavailable resource %q",
				manifest.Name,
				resource,
			)
		}
	}
	return nil
}

func (snapshot *registrySnapshot) add(
	state *mountState,
	staged *plugincontract.AgentRegistrar,
	nextSequence *uint64,
	defaultHookTimeout time.Duration,
) error {
	name := state.manifest.Name
	if _, exists := snapshot.plugins[name]; exists {
		return fmt.Errorf("plugin %q is already mounted", name)
	}

	resources := snapshot.resources()
	resources[sdk.PluginResource(name)] = struct{}{}
	for _, resource := range staged.Resources() {
		if _, exists := resources[resource]; exists {
			return fmt.Errorf(
				"plugin %q cannot register existing resource %q",
				name,
				resource,
			)
		}
		resources[resource] = struct{}{}
	}
	for _, plugin := range snapshot.plugins {
		if err := validatePluginResources(plugin.manifest, resources); err != nil {
			return err
		}
	}
	if err := validatePluginResources(state.manifest, resources); err != nil {
		return err
	}
	for _, agent := range staged.Agents {
		if err := validateAgentResources(
			state.manifest.Name,
			agent,
			resources,
		); err != nil {
			return err
		}
	}

	for _, hookName := range staged.HookOrder {
		hook := staged.Hooks[hookName]
		event := hook.Spec.Event
		if _, exists := snapshot.events[event]; !exists {
			if _, stagedEvent := staged.Events[event]; !stagedEvent {
				return fmt.Errorf(
					"plugin %q hook targets unknown event %q",
					name,
					event,
				)
			}
		}
	}
	for _, subscriber := range staged.Subscribers {
		spec := subscriber.Spec
		for _, event := range spec.Events {
			if _, exists := snapshot.events[event]; !exists {
				if _, stagedEvent := staged.Events[event]; !stagedEvent {
					return fmt.Errorf(
						"plugin %q subscriber %q targets unknown event %q",
						name,
						spec.Name,
						event,
					)
				}
			}
		}
	}

	providers := make(
		map[string]ownedResource[sdk.AsyncProvider, sdk.ProviderSpec],
		len(staged.Providers),
	)
	for providerName, provider := range staged.Providers {
		owned, err := ownAsyncResource[sdk.AsyncProvider](
			state,
			"provider",
			provider.Spec.Name,
			provider.Value,
			provider.Spec,
		)
		if err != nil {
			return err
		}
		providers[providerName] = owned
	}
	tools := make(
		map[string]ownedResource[sdk.AsyncTool, sdk.ToolSpec],
		len(staged.Tools),
	)
	for toolName, tool := range staged.Tools {
		owned, err := ownAsyncResource[sdk.AsyncTool](
			state,
			"tool",
			tool.Spec.Name,
			tool.Value,
			sdk.CloneToolSpec(tool.Spec),
		)
		if err != nil {
			return err
		}
		tools[toolName] = owned
	}
	capabilities := make(
		map[string]ownedResource[sdk.AsyncCapability, sdk.CapabilitySpec],
		len(staged.Capabilities),
	)
	for capabilityName, capability := range staged.Capabilities {
		owned, err := ownAsyncResource[sdk.AsyncCapability](
			state,
			"capability",
			capability.Spec.Name,
			capability.Value,
			sdk.CloneCapabilitySpec(capability.Spec),
		)
		if err != nil {
			return err
		}
		capabilities[capabilityName] = owned
	}

	snapshot.plugins[name] = state
	for providerName, provider := range providers {
		snapshot.providers[providerName] = provider
	}
	for toolName, tool := range tools {
		snapshot.tools[toolName] = tool
	}
	for agentName, agent := range staged.Agents {
		snapshot.agents[agentName] = ownedAgent{
			owner: state,
			spec:  agent,
		}
	}
	for capabilityName, capability := range capabilities {
		snapshot.capabilities[capabilityName] = capability
	}
	for subscriberName, subscriber := range staged.Subscribers {
		snapshot.subscribers[subscriberName] = ownResource(state, subscriber)
	}
	for eventName, contract := range staged.Events {
		snapshot.events[eventName] = ownedEvent{
			owner:    state,
			contract: contract,
		}
	}
	for _, hookName := range staged.HookOrder {
		hook := staged.Hooks[hookName]
		eventName := hook.Spec.Event
		contract := snapshot.events[eventName].contract
		hook.Spec = normalizeHookSpec(
			hook.Spec,
			contract,
			defaultHookTimeout,
		)
		snapshot.hooks[eventName] = append(
			snapshot.hooks[eventName],
			ownedHook{
				ownedResource: ownResource(state, hook),
				seq:           *nextSequence,
			},
		)
		*nextSequence++
		slices.SortFunc(snapshot.hooks[eventName], compareOwnedHooks)
	}
	return nil
}

func (snapshot *registrySnapshot) without(
	state *mountState,
) *registrySnapshot {
	result := snapshot.clone()
	delete(result.plugins, state.manifest.Name)
	for name, provider := range result.providers {
		if provider.owner == state {
			delete(result.providers, name)
		}
	}
	for name, tool := range result.tools {
		if tool.owner == state {
			delete(result.tools, name)
		}
	}
	for name, agent := range result.agents {
		if agent.owner == state {
			delete(result.agents, name)
		}
	}
	for event, hooks := range result.hooks {
		filtered := hooks[:0]
		for _, hook := range hooks {
			if hook.owner != state {
				filtered = append(filtered, hook)
			}
		}
		if len(filtered) == 0 {
			delete(result.hooks, event)
		} else {
			result.hooks[event] = append([]ownedHook(nil), filtered...)
		}
	}
	for name, subscriber := range result.subscribers {
		if subscriber.owner == state {
			delete(result.subscribers, name)
		}
	}
	for name, capability := range result.capabilities {
		if capability.owner == state {
			delete(result.capabilities, name)
		}
	}
	for name, event := range result.events {
		if event.owner == state {
			delete(result.events, name)
		}
	}
	return result
}

func compareOwnedHooks(left, right ownedHook) int {
	if left.spec.Priority < right.spec.Priority {
		return -1
	}
	if left.spec.Priority > right.spec.Priority {
		return 1
	}
	if left.seq < right.seq {
		return -1
	}
	if left.seq > right.seq {
		return 1
	}
	return 0
}

func normalizeHookSpec(
	spec sdk.HookSpec,
	contract sdk.EventContract,
	defaultTimeout time.Duration,
) sdk.HookSpec {
	if spec.Priority == 0 {
		spec.Priority = sdk.PriorityNormal
	}
	if spec.FailurePolicy == "" {
		if contract.AllowsEffect() {
			spec.FailurePolicy = sdk.FailurePolicyFailClosed
		} else {
			spec.FailurePolicy = sdk.FailurePolicyContinue
		}
	}
	if spec.Timeout == 0 {
		spec.Timeout = defaultTimeout
	}
	return spec
}

func normalizeRequirement(reference string) string {
	value := strings.TrimSpace(reference)
	if strings.Contains(value, ":") {
		return value
	}
	return sdk.PluginResource(value)
}
