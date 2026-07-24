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
	Commands     []sdk.CommandSpec    `json:"commands"`
}

type builtinEventContract struct {
	sdk.EventContract
	// sessionExecutionScoped marks built-in events that can be dispatched while
	// a prompt execution is running.
	sessionExecutionScoped bool
	// trajectoryEnvironmentScoped marks built-in events that must remain part
	// of the durable trajectory environment used for resume and recovery.
	trajectoryEnvironmentScoped bool
}

var builtinEventContracts = [...]builtinEventContract{
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:          sdk.EventBeforeAgentStart,
		MutableFields: []string{"messages", "system"},
		AllowBlock:    true,
	}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventAgentStart}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventTurnStart}),
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:          sdk.EventBeforeProvider,
		MutableFields: []string{"messages", "provider", "system", "tools"},
	}),
	sessionExecutionEvent(sdk.EventContract{Name: sdk.EventProviderOutcome}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventAfterProvider}),
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:          sdk.EventBeforeTool,
		MutableFields: []string{"call"},
		AllowBlock:    true,
	}),
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:          sdk.EventToolError,
		MutableFields: []string{"result"},
	}),
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:          sdk.EventAfterTool,
		MutableFields: []string{"result"},
	}),
	trajectoryEnvironmentEvent(sdk.EventContract{
		Name:        sdk.EventDecide,
		AllowAction: true,
	}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventTurnEnd}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventAgentEnd}),
	{EventContract: sdk.EventContract{Name: sdk.EventPluginMounted}},
	{EventContract: sdk.EventContract{Name: sdk.EventPluginUnmounted}},
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventTrajectoryAppend}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventTrajectoryRestore}),
	trajectoryEnvironmentEvent(sdk.EventContract{Name: sdk.EventTrajectoryRollback}),
}

func trajectoryEnvironmentEvent(contract sdk.EventContract) builtinEventContract {
	return builtinEventContract{
		EventContract:               contract,
		sessionExecutionScoped:      true,
		trajectoryEnvironmentScoped: true,
	}
}

func sessionExecutionEvent(contract sdk.EventContract) builtinEventContract {
	return builtinEventContract{
		EventContract:          contract,
		sessionExecutionScoped: true,
	}
}

func builtinEventInSessionExecution(name string) bool {
	for _, contract := range builtinEventContracts {
		if contract.Name == name {
			return contract.sessionExecutionScoped
		}
	}
	return false
}

func builtinEventInTrajectoryEnvironment(name string) bool {
	for _, contract := range builtinEventContracts {
		if contract.Name == name {
			return contract.trajectoryEnvironmentScoped
		}
	}
	return false
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
		result.Tools = append(
			result.Tools,
			sdk.CloneToolSpec(tool.spec),
		)
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
		result.Subscribers = append(
			result.Subscribers,
			sdk.CloneSubscriberSpec(subscriber.spec),
		)
	}
	for _, capability := range snapshot.capabilities {
		result.Capabilities = append(
			result.Capabilities,
			sdk.CloneCapabilitySpec(capability.spec),
		)
	}
	for _, event := range snapshot.events {
		result.Events = append(
			result.Events,
			sdk.CloneEventContract(event.contract),
		)
	}
	for _, command := range snapshot.commands {
		result.Commands = append(result.Commands, command.spec)
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
	slices.SortFunc(catalog.Commands, func(left, right sdk.CommandSpec) int {
		return strings.Compare(left.Name, right.Name)
	})
}
