package sdk

import (
	"fmt"
	"slices"
	"strings"
)

type ownedProvider struct {
	owner    *mountState
	provider Provider
}

type ownedTool struct {
	owner *mountState
	tool  Tool
}

type ownedHook struct {
	owner *mountState
	hook  Hook
	spec  HookSpec
	seq   uint64
}

type ownedSubscriber struct {
	owner      *mountState
	subscriber Subscriber
	spec       SubscriberSpec
}

type ownedCapability struct {
	owner      *mountState
	capability Capability
}

type ownedEvent struct {
	owner    *mountState
	contract EventContract
}

type registrySnapshot struct {
	generation   uint64
	plugins      map[string]*mountState
	providers    map[string]ownedProvider
	tools        map[string]ownedTool
	hooks        map[string][]ownedHook
	subscribers  map[string]ownedSubscriber
	capabilities map[string]ownedCapability
	events       map[string]ownedEvent
}

func initialSnapshot() *registrySnapshot {
	snapshot := &registrySnapshot{
		generation:   1,
		plugins:      make(map[string]*mountState),
		providers:    make(map[string]ownedProvider),
		tools:        make(map[string]ownedTool),
		hooks:        make(map[string][]ownedHook),
		subscribers:  make(map[string]ownedSubscriber),
		capabilities: make(map[string]ownedCapability),
		events:       make(map[string]ownedEvent),
	}
	for _, contract := range BuiltinEventContracts() {
		snapshot.events[contract.Name] = ownedEvent{contract: contract}
	}
	return snapshot
}

func (snapshot *registrySnapshot) clone() *registrySnapshot {
	result := &registrySnapshot{
		generation:   snapshot.generation,
		plugins:      make(map[string]*mountState, len(snapshot.plugins)),
		providers:    make(map[string]ownedProvider, len(snapshot.providers)),
		tools:        make(map[string]ownedTool, len(snapshot.tools)),
		hooks:        make(map[string][]ownedHook, len(snapshot.hooks)),
		subscribers:  make(map[string]ownedSubscriber, len(snapshot.subscribers)),
		capabilities: make(map[string]ownedCapability, len(snapshot.capabilities)),
		events:       make(map[string]ownedEvent, len(snapshot.events)),
	}
	for name, plugin := range snapshot.plugins {
		result.plugins[name] = plugin
	}
	for name, provider := range snapshot.providers {
		result.providers[name] = provider
	}
	for name, tool := range snapshot.tools {
		result.tools[name] = tool
	}
	for event, hooks := range snapshot.hooks {
		result.hooks[event] = append([]ownedHook(nil), hooks...)
	}
	for name, subscriber := range snapshot.subscribers {
		subscriber.spec.Events = append([]string(nil), subscriber.spec.Events...)
		result.subscribers[name] = subscriber
	}
	for name, capability := range snapshot.capabilities {
		result.capabilities[name] = capability
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
		resources[PluginResource(name)] = struct{}{}
	}
	for name := range snapshot.providers {
		resources[ProviderResource(name)] = struct{}{}
	}
	for name := range snapshot.tools {
		resources[ToolResource(name)] = struct{}{}
	}
	for hooks := range snapshot.hooks {
		for _, hook := range snapshot.hooks[hooks] {
			resources[HookResource(hook.spec.Name)] = struct{}{}
		}
	}
	for name := range snapshot.subscribers {
		resources[SubscriberResource(name)] = struct{}{}
	}
	for name := range snapshot.capabilities {
		resources[CapabilityResource(name)] = struct{}{}
	}
	for name, event := range snapshot.events {
		if event.owner != nil {
			resources[EventResource(name)] = struct{}{}
		}
	}
	return resources
}

func (snapshot *registrySnapshot) validateDependencies() error {
	resources := snapshot.resources()
	for _, plugin := range snapshot.plugins {
		for _, required := range plugin.manifest.Requires {
			resource := normalizeRequirement(required)
			if _, exists := resources[resource]; !exists {
				return fmt.Errorf(
					"plugin %q requires unavailable resource %q",
					plugin.manifest.Name,
					resource,
				)
			}
		}
	}
	return nil
}

func (snapshot *registrySnapshot) add(
	state *mountState,
	staged *stagingRegistrar,
	nextSequence *uint64,
) error {
	name := state.manifest.Name
	if _, exists := snapshot.plugins[name]; exists {
		return fmt.Errorf("plugin %q is already mounted", name)
	}

	resources := snapshot.resources()
	resources[PluginResource(name)] = struct{}{}
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
	for _, conflict := range state.manifest.Conflicts {
		resource := normalizeRequirement(conflict)
		if _, exists := resources[resource]; exists {
			return fmt.Errorf(
				"plugin %q conflicts with resource %q",
				name,
				resource,
			)
		}
	}
	for _, required := range state.manifest.Requires {
		resource := normalizeRequirement(required)
		if _, exists := resources[resource]; !exists {
			return fmt.Errorf(
				"plugin %q requires unavailable resource %q",
				name,
				resource,
			)
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
			if hook.Spec().Event != event {
				return fmt.Errorf(
					"plugin %q hook %q event changed during install",
					name,
					hook.Spec().Name,
				)
			}
		}
	}
	for _, subscriber := range staged.subscribers {
		spec := subscriber.Spec()
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
		snapshot.providers[providerName] = ownedProvider{
			owner:    state,
			provider: provider,
		}
	}
	for toolName, tool := range staged.tools {
		snapshot.tools[toolName] = ownedTool{owner: state, tool: tool}
	}
	for capabilityName, capability := range staged.capabilities {
		snapshot.capabilities[capabilityName] = ownedCapability{
			owner:      state,
			capability: capability,
		}
	}
	for subscriberName, subscriber := range staged.subscribers {
		spec := subscriber.Spec()
		spec.Events = append([]string(nil), spec.Events...)
		snapshot.subscribers[subscriberName] = ownedSubscriber{
			owner:      state,
			subscriber: subscriber,
			spec:       spec,
		}
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
			spec := normalizeHookSpec(hook.Spec(), contract)
			snapshot.hooks[eventName] = append(
				snapshot.hooks[eventName],
				ownedHook{
					owner: state,
					hook:  hook,
					spec:  spec,
					seq:   *nextSequence,
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

func normalizeHookSpec(spec HookSpec, contract EventContract) HookSpec {
	if spec.Priority == 0 {
		spec.Priority = PriorityNormal
	}
	if spec.FailurePolicy == "" {
		if contract.active() {
			spec.FailurePolicy = FailurePolicyFailClosed
		} else {
			spec.FailurePolicy = FailurePolicyContinue
		}
	}
	return spec
}

func normalizeRequirement(reference string) string {
	value := strings.TrimSpace(reference)
	if strings.Contains(value, ":") {
		return value
	}
	return PluginResource(value)
}
