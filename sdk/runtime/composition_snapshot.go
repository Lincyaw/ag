package runtime

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lincyaw/ag/sdk"
)

// ownedResource ties a published resource to its plugin lifecycle owner.
type ownedResource[Resource, Spec any] struct {
	registeredResource[Resource, Spec]
	owner *mountState
}

func ownResource[Resource, Spec any](
	owner *mountState,
	resource registeredResource[Resource, Spec],
) ownedResource[Resource, Spec] {
	return ownedResource[Resource, Spec]{
		registeredResource: resource,
		owner:              owner,
	}
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

type registrySnapshot struct {
	generation   uint64
	plugins      map[string]*mountState
	providers    map[string]ownedResource[sdk.Provider, sdk.ProviderSpec]
	tools        map[string]ownedResource[sdk.Tool, sdk.ToolSpec]
	agents       map[string]ownedAgent
	hooks        map[string][]ownedHook
	subscribers  map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec]
	capabilities map[string]ownedResource[sdk.Capability, sdk.CapabilitySpec]
	events       map[string]ownedEvent
}

func initialSnapshot() *registrySnapshot {
	snapshot := &registrySnapshot{
		generation:   1,
		plugins:      make(map[string]*mountState),
		providers:    make(map[string]ownedResource[sdk.Provider, sdk.ProviderSpec]),
		tools:        make(map[string]ownedResource[sdk.Tool, sdk.ToolSpec]),
		agents:       make(map[string]ownedAgent),
		hooks:        make(map[string][]ownedHook),
		subscribers:  make(map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec]),
		capabilities: make(map[string]ownedResource[sdk.Capability, sdk.CapabilitySpec]),
		events:       make(map[string]ownedEvent),
	}
	for _, contract := range builtinEventContracts {
		contract.MutableFields = slices.Clone(contract.MutableFields)
		snapshot.events[contract.Name] = ownedEvent{contract: contract}
	}
	return snapshot
}

func (snapshot *registrySnapshot) clone() *registrySnapshot {
	result := &registrySnapshot{
		generation:   snapshot.generation,
		plugins:      maps.Clone(snapshot.plugins),
		providers:    maps.Clone(snapshot.providers),
		tools:        maps.Clone(snapshot.tools),
		agents:       maps.Clone(snapshot.agents),
		hooks:        make(map[string][]ownedHook, len(snapshot.hooks)),
		subscribers:  make(map[string]ownedResource[sdk.Subscriber, sdk.SubscriberSpec], len(snapshot.subscribers)),
		capabilities: maps.Clone(snapshot.capabilities),
		events:       make(map[string]ownedEvent, len(snapshot.events)),
	}
	for event, hooks := range snapshot.hooks {
		result.hooks[event] = append([]ownedHook(nil), hooks...)
	}
	for name, subscriber := range snapshot.subscribers {
		subscriber.spec.Events = append([]string(nil), subscriber.spec.Events...)
		result.subscribers[name] = subscriber
	}
	for name, event := range snapshot.events {
		event.contract.MutableFields = append(
			[]string(nil),
			event.contract.MutableFields...,
		)
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
	for name, event := range snapshot.events {
		if event.owner != nil {
			resources[sdk.EventResource(name)] = struct{}{}
		}
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
	staged *stagingRegistrar,
	nextSequence *uint64,
	defaultHookTimeout time.Duration,
) error {
	name := state.manifest.Name
	if _, exists := snapshot.plugins[name]; exists {
		return fmt.Errorf("plugin %q is already mounted", name)
	}

	resources := snapshot.resources()
	resources[sdk.PluginResource(name)] = struct{}{}
	for _, resource := range staged.resources() {
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
	for _, agent := range staged.agents {
		if err := validateAgentResources(
			state.manifest.Name,
			agent,
			resources,
		); err != nil {
			return err
		}
	}

	for event, hooks := range staged.hooks {
		if _, exists := snapshot.events[event]; !exists {
			if _, stagedEvent := staged.events[event]; !stagedEvent {
				return fmt.Errorf(
					"plugin %q hook targets unknown event %q",
					name,
					event,
				)
			}
		}
		for _, hook := range hooks {
			if hook.spec.Event != event {
				panic("staging registrar indexed a hook under the wrong event")
			}
		}
	}
	for _, subscriber := range staged.subscribers {
		spec := subscriber.spec
		for _, event := range spec.Events {
			if _, exists := snapshot.events[event]; !exists {
				if _, stagedEvent := staged.events[event]; !stagedEvent {
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

	snapshot.plugins[name] = state
	for providerName, provider := range staged.providers {
		snapshot.providers[providerName] = ownResource(state, provider)
	}
	for toolName, tool := range staged.tools {
		snapshot.tools[toolName] = ownResource(state, tool)
	}
	for agentName, agent := range staged.agents {
		snapshot.agents[agentName] = ownedAgent{
			owner: state,
			spec:  agent,
		}
	}
	for capabilityName, capability := range staged.capabilities {
		snapshot.capabilities[capabilityName] = ownResource(state, capability)
	}
	for subscriberName, subscriber := range staged.subscribers {
		snapshot.subscribers[subscriberName] = ownResource(state, subscriber)
	}
	for eventName, contract := range staged.events {
		snapshot.events[eventName] = ownedEvent{
			owner:    state,
			contract: contract,
		}
	}
	for eventName, hooks := range staged.hooks {
		contract := snapshot.events[eventName].contract
		for _, hook := range hooks {
			hook.spec = normalizeHookSpec(
				hook.spec,
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
		}
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
		if len(contract.MutableFields) > 0 ||
			contract.AllowBlock ||
			contract.AllowAction {
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
