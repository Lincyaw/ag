package runtime

import (
	"slices"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

// MountedPlugin describes one plugin published in the active composition.
type MountedPlugin struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description"`
	Source      string   `json:"source"`
	Registers   []string `json:"registers"`
}

type CatalogSnapshot struct {
	Generation   uint64               `json:"generation"`
	Plugins      []MountedPlugin      `json:"plugins"`
	Providers    []sdk.ProviderSpec   `json:"providers"`
	Tools        []sdk.ToolSpec       `json:"tools"`
	Agents       []sdk.AgentSpec      `json:"agents"`
	Hooks        []sdk.HookSpec       `json:"hooks"`
	Subscribers  []sdk.SubscriberSpec `json:"subscribers"`
	Capabilities []sdk.CapabilitySpec `json:"capabilities"`
	Events       []sdk.EventContract  `json:"events"`
}

var builtinEventContracts = [...]sdk.EventContract{
	{
		Name:          sdk.EventBeforeAgentStart,
		MutableFields: []string{"messages", "system"},
		AllowBlock:    true,
	},
	{Name: sdk.EventAgentStart},
	{Name: sdk.EventTurnStart},
	{
		Name:          sdk.EventBeforeProvider,
		MutableFields: []string{"messages", "provider", "system", "tools"},
	},
	{Name: sdk.EventAfterProvider},
	{
		Name:          sdk.EventBeforeTool,
		MutableFields: []string{"call"},
		AllowBlock:    true,
	},
	{
		Name:          sdk.EventToolError,
		MutableFields: []string{"result"},
	},
	{
		Name:          sdk.EventAfterTool,
		MutableFields: []string{"result"},
	},
	{Name: sdk.EventDecide, AllowAction: true},
	{Name: sdk.EventTurnEnd},
	{Name: sdk.EventAgentEnd},
	{Name: sdk.EventPluginMounted},
	{Name: sdk.EventPluginUnmounted},
	{Name: sdk.EventTrajectoryAppend},
	{Name: sdk.EventTrajectoryRestore},
	{Name: sdk.EventTrajectoryRollback},
}

func (runtime *Runtime) Catalog() CatalogSnapshot {
	if runtime == nil {
		return CatalogSnapshot{}
	}
	return catalogFromSnapshot(runtime.current.Load())
}

func catalogFromSnapshot(snapshot *registrySnapshot) CatalogSnapshot {
	result := CatalogSnapshot{Generation: snapshot.generation}
	for _, state := range snapshot.plugins {
		result.Plugins = append(result.Plugins, MountedPlugin{
			Name:        state.manifest.Name,
			Version:     state.manifest.Version,
			Description: state.manifest.Description,
			Source:      state.source,
			Registers:   append([]string(nil), state.manifest.Registers...),
		})
	}
	for _, provider := range snapshot.providers {
		result.Providers = append(result.Providers, provider.spec)
	}
	for _, tool := range snapshot.tools {
		result.Tools = append(result.Tools, cloneToolSpec(tool.spec))
	}
	for _, agent := range snapshot.agents {
		result.Agents = append(
			result.Agents,
			sdk.CloneAgentSpec(agent.spec),
		)
	}
	for _, hooks := range snapshot.hooks {
		for _, hook := range hooks {
			result.Hooks = append(result.Hooks, hook.spec)
		}
	}
	for _, subscriber := range snapshot.subscribers {
		spec := subscriber.spec
		spec.Events = append([]string(nil), spec.Events...)
		result.Subscribers = append(result.Subscribers, spec)
	}
	for _, capability := range snapshot.capabilities {
		result.Capabilities = append(
			result.Capabilities,
			cloneCapabilitySpec(capability.spec),
		)
	}
	for _, event := range snapshot.events {
		contract := event.contract
		contract.MutableFields = append([]string(nil), contract.MutableFields...)
		result.Events = append(result.Events, contract)
	}
	sortCatalog(&result)
	return result
}

func sortCatalog(catalog *CatalogSnapshot) {
	slices.SortFunc(catalog.Plugins, func(left, right MountedPlugin) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Providers, func(left, right sdk.ProviderSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Tools, func(left, right sdk.ToolSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Agents, func(left, right sdk.AgentSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Hooks, func(left, right sdk.HookSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Subscribers, func(left, right sdk.SubscriberSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Capabilities, func(left, right sdk.CapabilitySpec) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.Events, func(left, right sdk.EventContract) int {
		return strings.Compare(left.Name, right.Name)
	})
}
