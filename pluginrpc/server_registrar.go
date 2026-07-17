package pluginrpc

import (
	"errors"
	"fmt"
	"slices"

	"github.com/lincyaw/ag/sdk"
)

type serverResource[Resource, Spec any] struct {
	value Resource
	spec  Spec
}

func registerServerResource[Resource, Spec any](
	resources map[string]serverResource[Resource, Spec],
	kind string,
	name string,
	value Resource,
	spec Spec,
) error {
	if name == "" {
		return fmt.Errorf("%s name is empty", kind)
	}
	if _, exists := resources[name]; exists {
		return fmt.Errorf("%s %q registered twice", kind, name)
	}
	resources[name] = serverResource[Resource, Spec]{value: value, spec: spec}
	return nil
}

type serverRegistrar struct {
	providers    map[string]serverResource[sdk.Provider, sdk.ProviderSpec]
	tools        map[string]serverResource[sdk.Tool, sdk.ToolSpec]
	hooks        map[string]serverResource[sdk.Hook, sdk.HookSpec]
	subscribers  map[string]serverResource[sdk.Subscriber, sdk.SubscriberSpec]
	capabilities map[string]serverResource[sdk.Capability, sdk.CapabilitySpec]
	events       map[string]sdk.EventContract
}

func newServerRegistrar() *serverRegistrar {
	return &serverRegistrar{
		providers:    make(map[string]serverResource[sdk.Provider, sdk.ProviderSpec]),
		tools:        make(map[string]serverResource[sdk.Tool, sdk.ToolSpec]),
		hooks:        make(map[string]serverResource[sdk.Hook, sdk.HookSpec]),
		subscribers:  make(map[string]serverResource[sdk.Subscriber, sdk.SubscriberSpec]),
		capabilities: make(map[string]serverResource[sdk.Capability, sdk.CapabilitySpec]),
		events:       make(map[string]sdk.EventContract),
	}
}

func (registrar *serverRegistrar) RegisterProvider(provider sdk.Provider) error {
	if provider == nil {
		return errors.New("provider is nil")
	}
	spec := provider.Spec()
	if _, async := provider.(sdk.AsyncProvider); !async {
		if _, sync := provider.(sdk.SyncProvider); !sync {
			return fmt.Errorf("provider %q has no execution implementation", spec.Name)
		}
	}
	return registerServerResource(
		registrar.providers,
		"provider",
		spec.Name,
		provider,
		spec,
	)
}

func (registrar *serverRegistrar) RegisterTool(tool sdk.Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	spec := tool.Spec()
	if _, async := tool.(sdk.AsyncTool); !async {
		if _, sync := tool.(sdk.SyncTool); !sync {
			return fmt.Errorf("tool %q has no execution implementation", spec.Name)
		}
	}
	encoded, err := toProtoToolSpec(spec)
	if err != nil {
		return err
	}
	return registerServerResource(
		registrar.tools,
		"tool",
		spec.Name,
		tool,
		fromProtoToolSpec(encoded),
	)
}

func (registrar *serverRegistrar) RegisterHook(hook sdk.Hook) error {
	if hook == nil {
		return errors.New("hook is nil")
	}
	spec := hook.Spec()
	return registerServerResource(registrar.hooks, "hook", spec.Name, hook, spec)
}

func (registrar *serverRegistrar) RegisterSubscriber(
	subscriber sdk.Subscriber,
) error {
	if subscriber == nil {
		return errors.New("subscriber is nil")
	}
	spec := subscriber.Spec()
	spec.Events = slices.Clone(spec.Events)
	return registerServerResource(
		registrar.subscribers,
		"subscriber",
		spec.Name,
		subscriber,
		spec,
	)
}

func (registrar *serverRegistrar) RegisterCapability(
	capability sdk.Capability,
) error {
	if capability == nil {
		return errors.New("capability is nil")
	}
	spec := capability.Spec()
	_, asynchronous := capability.(sdk.AsyncCapability)
	_, synchronous := capability.(sdk.SyncCapability)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"capability %q implements neither AsyncCapability nor SyncCapability",
			spec.Name,
		)
	}
	encoded, err := toProtoCapabilitySpec(spec)
	if err != nil {
		return err
	}
	return registerServerResource(
		registrar.capabilities,
		"capability",
		spec.Name,
		capability,
		fromProtoCapabilitySpec(encoded),
	)
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
	actual = appendResourceNames(actual, registrar.providers, sdk.ProviderResource)
	actual = appendResourceNames(actual, registrar.tools, sdk.ToolResource)
	actual = appendResourceNames(actual, registrar.hooks, sdk.HookResource)
	actual = appendResourceNames(actual, registrar.subscribers, sdk.SubscriberResource)
	actual = appendResourceNames(actual, registrar.capabilities, sdk.CapabilityResource)
	actual = appendResourceNames(actual, registrar.events, sdk.EventResource)
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

func appendResourceNames[Value any](
	target []string,
	resources map[string]Value,
	resourceName func(string) string,
) []string {
	for name := range resources {
		target = append(target, resourceName(name))
	}
	return target
}
