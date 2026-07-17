package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type registeredResource[Resource, Spec any] struct {
	value Resource
	spec  Spec
}

func registerResource[Resource, Spec any](
	resources map[string]registeredResource[Resource, Spec],
	kind string,
	name string,
	value Resource,
	spec Spec,
) error {
	if _, exists := resources[name]; exists {
		return fmt.Errorf("%s %q registered twice", kind, name)
	}
	resources[name] = registeredResource[Resource, Spec]{value: value, spec: spec}
	return nil
}

type stagingRegistrar struct {
	providers    map[string]registeredResource[sdk.Provider, sdk.ProviderSpec]
	tools        map[string]registeredResource[sdk.Tool, sdk.ToolSpec]
	hooks        map[string][]registeredResource[sdk.Hook, sdk.HookSpec]
	hookNames    map[string]struct{}
	subscribers  map[string]registeredResource[sdk.Subscriber, sdk.SubscriberSpec]
	capabilities map[string]registeredResource[sdk.Capability, sdk.CapabilitySpec]
	events       map[string]sdk.EventContract
}

func newStagingRegistrar() *stagingRegistrar {
	return &stagingRegistrar{
		providers:    make(map[string]registeredResource[sdk.Provider, sdk.ProviderSpec]),
		tools:        make(map[string]registeredResource[sdk.Tool, sdk.ToolSpec]),
		hooks:        make(map[string][]registeredResource[sdk.Hook, sdk.HookSpec]),
		hookNames:    make(map[string]struct{}),
		subscribers:  make(map[string]registeredResource[sdk.Subscriber, sdk.SubscriberSpec]),
		capabilities: make(map[string]registeredResource[sdk.Capability, sdk.CapabilitySpec]),
		events:       make(map[string]sdk.EventContract),
	}
}

func (registrar *stagingRegistrar) RegisterProvider(provider sdk.Provider) error {
	if provider == nil {
		return errors.New("provider is nil")
	}
	spec := provider.Spec()
	if err := validateProviderSpec(spec); err != nil {
		return err
	}
	_, asynchronous := provider.(sdk.AsyncProvider)
	_, synchronous := provider.(sdk.SyncProvider)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"provider %q implements neither AsyncProvider nor SyncProvider",
			spec.Name,
		)
	}
	return registerResource(registrar.providers, "provider", spec.Name, provider, spec)
}

func (registrar *stagingRegistrar) RegisterTool(tool sdk.Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	spec := tool.Spec()
	if err := validateToolSpec(spec); err != nil {
		return err
	}
	_, asynchronous := tool.(sdk.AsyncTool)
	_, synchronous := tool.(sdk.SyncTool)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"tool %q implements neither AsyncTool nor SyncTool",
			spec.Name,
		)
	}
	return registerResource(registrar.tools, "tool", spec.Name, tool, cloneToolSpec(spec))
}

func (registrar *stagingRegistrar) RegisterHook(hook sdk.Hook) error {
	if hook == nil {
		return errors.New("hook is nil")
	}
	spec := hook.Spec()
	if err := validateHookSpec(spec); err != nil {
		return err
	}
	if _, exists := registrar.hookNames[spec.Name]; exists {
		return fmt.Errorf("hook %q registered twice", spec.Name)
	}
	registrar.hookNames[spec.Name] = struct{}{}
	registrar.hooks[spec.Event] = append(registrar.hooks[spec.Event], registeredResource[sdk.Hook, sdk.HookSpec]{
		value: hook,
		spec:  spec,
	})
	return nil
}

func (registrar *stagingRegistrar) RegisterSubscriber(
	subscriber sdk.Subscriber,
) error {
	if subscriber == nil {
		return errors.New("subscriber is nil")
	}
	spec := subscriber.Spec()
	if err := validateSubscriberSpec(spec); err != nil {
		return err
	}
	spec.Events = slices.Clone(spec.Events)
	return registerResource(
		registrar.subscribers,
		"subscriber",
		spec.Name,
		subscriber,
		spec,
	)
}

func (registrar *stagingRegistrar) RegisterCapability(
	capability sdk.Capability,
) error {
	if capability == nil {
		return errors.New("capability is nil")
	}
	spec := capability.Spec()
	if err := validateCapabilitySpec(spec); err != nil {
		return err
	}
	_, asynchronous := capability.(sdk.AsyncCapability)
	_, synchronous := capability.(sdk.SyncCapability)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"capability %q implements neither AsyncCapability nor SyncCapability",
			spec.Name,
		)
	}
	return registerResource(
		registrar.capabilities,
		"capability",
		spec.Name,
		capability,
		cloneCapabilitySpec(spec),
	)
}

func (registrar *stagingRegistrar) RegisterEvent(
	contract sdk.EventContract,
) error {
	if err := validateEventContract(contract); err != nil {
		return err
	}
	if _, exists := registrar.events[contract.Name]; exists {
		return fmt.Errorf("event %q registered twice", contract.Name)
	}
	contract.MutableFields = append([]string(nil), contract.MutableFields...)
	registrar.events[contract.Name] = contract
	return nil
}

func (registrar *stagingRegistrar) resources() []string {
	resources := make([]string, 0,
		len(registrar.providers)+
			len(registrar.tools)+
			len(registrar.hookNames)+
			len(registrar.subscribers)+
			len(registrar.capabilities)+
			len(registrar.events),
	)
	resources = appendResourceNames(resources, registrar.providers, sdk.ProviderResource)
	resources = appendResourceNames(resources, registrar.tools, sdk.ToolResource)
	resources = appendResourceNames(resources, registrar.hookNames, sdk.HookResource)
	resources = appendResourceNames(resources, registrar.subscribers, sdk.SubscriberResource)
	resources = appendResourceNames(resources, registrar.capabilities, sdk.CapabilityResource)
	resources = appendResourceNames(resources, registrar.events, sdk.EventResource)
	slices.Sort(resources)
	return resources
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

func (registrar *stagingRegistrar) validateManifest(manifest sdk.Manifest) error {
	actual := registrar.resources()
	declared := slices.Clone(manifest.Registers)
	slices.Sort(declared)
	declared = slices.Compact(declared)
	if !slices.Equal(actual, declared) {
		return fmt.Errorf(
			"plugin %q manifest registers %v, but install registered %v",
			manifest.Name,
			declared,
			actual,
		)
	}
	return nil
}

func validateProviderSpec(spec sdk.ProviderSpec) error {
	if err := sdk.ValidateResourceName("provider", spec.Name); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Model) == "" {
		return fmt.Errorf("provider %q model is empty", spec.Name)
	}
	return nil
}

func validateToolSpec(spec sdk.ToolSpec) error {
	if err := sdk.ValidateResourceName("tool", spec.Name); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Description) == "" {
		return fmt.Errorf("tool %q description is empty", spec.Name)
	}
	if spec.Parameters == nil {
		return fmt.Errorf("tool %q parameters schema is nil", spec.Name)
	}
	return nil
}

func validateHookSpec(spec sdk.HookSpec) error {
	if err := sdk.ValidateResourceName("hook", spec.Name); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName("event", spec.Event); err != nil {
		return err
	}
	if spec.Priority < 0 {
		return fmt.Errorf("hook %q priority cannot be negative", spec.Name)
	}
	switch spec.FailurePolicy {
	case "", sdk.FailurePolicyFailClosed, sdk.FailurePolicyContinue:
	default:
		return fmt.Errorf(
			"hook %q has invalid failure policy %q",
			spec.Name,
			spec.FailurePolicy,
		)
	}
	if spec.Timeout < 0 {
		return fmt.Errorf("hook %q timeout cannot be negative", spec.Name)
	}
	return nil
}

func validateSubscriberSpec(spec sdk.SubscriberSpec) error {
	if err := sdk.ValidateResourceName("subscriber", spec.Name); err != nil {
		return err
	}
	if len(spec.Events) == 0 {
		return fmt.Errorf("subscriber %q has no events", spec.Name)
	}
	seen := make(map[string]struct{}, len(spec.Events))
	for _, event := range spec.Events {
		if err := sdk.ValidateResourceName("event", event); err != nil {
			return err
		}
		if _, exists := seen[event]; exists {
			return fmt.Errorf("subscriber %q contains duplicate event %q", spec.Name, event)
		}
		seen[event] = struct{}{}
	}
	if spec.Timeout < 0 {
		return fmt.Errorf("subscriber %q timeout cannot be negative", spec.Name)
	}
	return nil
}

func validateCapabilitySpec(spec sdk.CapabilitySpec) error {
	if err := sdk.ValidateResourceName("capability", spec.Name); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Description) == "" {
		return fmt.Errorf("capability %q description is empty", spec.Name)
	}
	if spec.InputSchema == nil || spec.OutputSchema == nil {
		return fmt.Errorf(
			"capability %q input and output schemas are required",
			spec.Name,
		)
	}
	return nil
}

func validateEventContract(contract sdk.EventContract) error {
	if err := sdk.ValidateResourceName("event", contract.Name); err != nil {
		return err
	}
	mutableFields := slices.Clone(contract.MutableFields)
	slices.Sort(mutableFields)
	mutableFields = slices.Compact(mutableFields)
	if len(contract.MutableFields) != len(mutableFields) {
		return fmt.Errorf("event %q has duplicate mutable fields", contract.Name)
	}
	for _, field := range contract.MutableFields {
		if err := sdk.ValidateResourceName("event field", field); err != nil {
			return err
		}
	}
	return nil
}

func cloneToolSpec(spec sdk.ToolSpec) sdk.ToolSpec {
	spec.Parameters = cloneJSONMap(spec.Parameters)
	return spec
}

func cloneCapabilitySpec(spec sdk.CapabilitySpec) sdk.CapabilitySpec {
	spec.InputSchema = cloneJSONMap(spec.InputSchema)
	spec.OutputSchema = cloneJSONMap(spec.OutputSchema)
	return spec
}

func cloneJSONMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	result := make(map[string]any, len(source))
	for name, value := range source {
		result[name] = cloneJSONValue(value)
	}
	return result
}

func cloneJSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneJSONMap(value)
	case map[string]string:
		return maps.Clone(value)
	case []any:
		result := make([]any, len(value))
		for index := range value {
			result[index] = cloneJSONValue(value[index])
		}
		return result
	case []string:
		return slices.Clone(value)
	case json.RawMessage:
		return slices.Clone(value)
	case []byte:
		return slices.Clone(value)
	default:
		return value
	}
}
