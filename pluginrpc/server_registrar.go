package pluginrpc

import (
	"errors"
	"fmt"
	"slices"

	"github.com/lincyaw/ag/sdk"
)

type serverRegistrar struct {
	providers    map[string]sdk.Provider
	tools        map[string]sdk.Tool
	hooks        map[string]sdk.Hook
	subscribers  map[string]sdk.Subscriber
	capabilities map[string]sdk.Capability
	events       map[string]sdk.EventContract
}

func newServerRegistrar() *serverRegistrar {
	return &serverRegistrar{
		providers:    make(map[string]sdk.Provider),
		tools:        make(map[string]sdk.Tool),
		hooks:        make(map[string]sdk.Hook),
		subscribers:  make(map[string]sdk.Subscriber),
		capabilities: make(map[string]sdk.Capability),
		events:       make(map[string]sdk.EventContract),
	}
}

func (registrar *serverRegistrar) RegisterProvider(provider sdk.Provider) error {
	if provider == nil {
		return errors.New("provider is nil")
	}
	name := provider.Spec().Name
	if name == "" {
		return errors.New("provider name is empty")
	}
	if _, exists := registrar.providers[name]; exists {
		return fmt.Errorf("provider %q registered twice", name)
	}
	if _, async := provider.(sdk.AsyncProvider); !async {
		if _, sync := provider.(sdk.SyncProvider); !sync {
			return fmt.Errorf("provider %q has no execution implementation", name)
		}
	}
	registrar.providers[name] = provider
	return nil
}

func (registrar *serverRegistrar) RegisterTool(tool sdk.Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	name := tool.Spec().Name
	if name == "" {
		return errors.New("tool name is empty")
	}
	if _, exists := registrar.tools[name]; exists {
		return fmt.Errorf("tool %q registered twice", name)
	}
	if _, async := tool.(sdk.AsyncTool); !async {
		if _, sync := tool.(sdk.SyncTool); !sync {
			return fmt.Errorf("tool %q has no execution implementation", name)
		}
	}
	registrar.tools[name] = tool
	return nil
}

func (registrar *serverRegistrar) RegisterHook(hook sdk.Hook) error {
	if hook == nil {
		return errors.New("hook is nil")
	}
	name := hook.Spec().Name
	if name == "" {
		return errors.New("hook name is empty")
	}
	if _, exists := registrar.hooks[name]; exists {
		return fmt.Errorf("hook %q registered twice", name)
	}
	registrar.hooks[name] = hook
	return nil
}

func (registrar *serverRegistrar) RegisterSubscriber(
	subscriber sdk.Subscriber,
) error {
	if subscriber == nil {
		return errors.New("subscriber is nil")
	}
	name := subscriber.Spec().Name
	if name == "" {
		return errors.New("subscriber name is empty")
	}
	if _, exists := registrar.subscribers[name]; exists {
		return fmt.Errorf("subscriber %q registered twice", name)
	}
	registrar.subscribers[name] = subscriber
	return nil
}

func (registrar *serverRegistrar) RegisterCapability(
	capability sdk.Capability,
) error {
	if capability == nil {
		return errors.New("capability is nil")
	}
	name := capability.Spec().Name
	if name == "" {
		return errors.New("capability name is empty")
	}
	if _, exists := registrar.capabilities[name]; exists {
		return fmt.Errorf("capability %q registered twice", name)
	}
	registrar.capabilities[name] = capability
	return nil
}

func (registrar *serverRegistrar) RegisterEvent(contract sdk.EventContract) error {
	if contract.Name == "" {
		return errors.New("event name is empty")
	}
	if _, exists := registrar.events[contract.Name]; exists {
		return fmt.Errorf("event %q registered twice", contract.Name)
	}
	contract.MutableFields = append([]string(nil), contract.MutableFields...)
	registrar.events[contract.Name] = contract
	return nil
}

func (registrar *serverRegistrar) validateManifest(manifest sdk.Manifest) error {
	actual := make([]string, 0,
		len(registrar.providers)+len(registrar.tools)+len(registrar.hooks)+
			len(registrar.subscribers)+len(registrar.capabilities)+len(registrar.events),
	)
	for name := range registrar.providers {
		actual = append(actual, sdk.ProviderResource(name))
	}
	for name := range registrar.tools {
		actual = append(actual, sdk.ToolResource(name))
	}
	for name := range registrar.hooks {
		actual = append(actual, sdk.HookResource(name))
	}
	for name := range registrar.subscribers {
		actual = append(actual, sdk.SubscriberResource(name))
	}
	for name := range registrar.capabilities {
		actual = append(actual, sdk.CapabilityResource(name))
	}
	for name := range registrar.events {
		actual = append(actual, sdk.EventResource(name))
	}
	slices.Sort(actual)
	declared := append([]string(nil), manifest.Registers...)
	slices.Sort(declared)
	if !slices.Equal(actual, declared) {
		return fmt.Errorf(
			"plugin %q manifest registers %v, but server collected %v",
			manifest.Name,
			declared,
			actual,
		)
	}
	return nil
}
