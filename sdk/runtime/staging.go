package runtime

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

type stagingRegistrar struct {
	providers    map[string]Provider
	tools        map[string]Tool
	hooks        map[string][]Hook
	hookNames    map[string]struct{}
	subscribers  map[string]Subscriber
	capabilities map[string]Capability
	events       map[string]EventContract
}

func newStagingRegistrar() *stagingRegistrar {
	return &stagingRegistrar{
		providers:    make(map[string]Provider),
		tools:        make(map[string]Tool),
		hooks:        make(map[string][]Hook),
		hookNames:    make(map[string]struct{}),
		subscribers:  make(map[string]Subscriber),
		capabilities: make(map[string]Capability),
		events:       make(map[string]EventContract),
	}
}

func (registrar *stagingRegistrar) RegisterProvider(provider Provider) error {
	if provider == nil {
		return errors.New("provider is nil")
	}
	spec := provider.Spec()
	if err := validateProviderSpec(spec); err != nil {
		return err
	}
	_, asynchronous := provider.(AsyncProvider)
	_, synchronous := provider.(SyncProvider)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"provider %q implements neither AsyncProvider nor SyncProvider",
			spec.Name,
		)
	}
	if _, exists := registrar.providers[spec.Name]; exists {
		return fmt.Errorf("provider %q registered twice", spec.Name)
	}
	registrar.providers[spec.Name] = provider
	return nil
}

func (registrar *stagingRegistrar) RegisterTool(tool Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	spec := tool.Spec()
	if err := validateToolSpec(spec); err != nil {
		return err
	}
	_, asynchronous := tool.(AsyncTool)
	_, synchronous := tool.(SyncTool)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"tool %q implements neither AsyncTool nor SyncTool",
			spec.Name,
		)
	}
	if _, exists := registrar.tools[spec.Name]; exists {
		return fmt.Errorf("tool %q registered twice", spec.Name)
	}
	registrar.tools[spec.Name] = tool
	return nil
}

func (registrar *stagingRegistrar) RegisterHook(hook Hook) error {
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
	registrar.hooks[spec.Event] = append(registrar.hooks[spec.Event], hook)
	return nil
}

func (registrar *stagingRegistrar) RegisterSubscriber(
	subscriber Subscriber,
) error {
	if subscriber == nil {
		return errors.New("subscriber is nil")
	}
	spec := subscriber.Spec()
	if err := validateSubscriberSpec(spec); err != nil {
		return err
	}
	if _, exists := registrar.subscribers[spec.Name]; exists {
		return fmt.Errorf("subscriber %q registered twice", spec.Name)
	}
	registrar.subscribers[spec.Name] = subscriber
	return nil
}

func (registrar *stagingRegistrar) RegisterCapability(
	capability Capability,
) error {
	if capability == nil {
		return errors.New("capability is nil")
	}
	spec := capability.Spec()
	if err := validateCapabilitySpec(spec); err != nil {
		return err
	}
	_, asynchronous := capability.(AsyncCapability)
	_, synchronous := capability.(SyncCapability)
	if !asynchronous && !synchronous {
		return fmt.Errorf(
			"capability %q implements neither AsyncCapability nor SyncCapability",
			spec.Name,
		)
	}
	if _, exists := registrar.capabilities[spec.Name]; exists {
		return fmt.Errorf("capability %q registered twice", spec.Name)
	}
	registrar.capabilities[spec.Name] = capability
	return nil
}

func (registrar *stagingRegistrar) RegisterEvent(
	contract EventContract,
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
	for name := range registrar.providers {
		resources = append(resources, ProviderResource(name))
	}
	for name := range registrar.tools {
		resources = append(resources, ToolResource(name))
	}
	for name := range registrar.hookNames {
		resources = append(resources, HookResource(name))
	}
	for name := range registrar.subscribers {
		resources = append(resources, SubscriberResource(name))
	}
	for name := range registrar.capabilities {
		resources = append(resources, CapabilityResource(name))
	}
	for name := range registrar.events {
		resources = append(resources, EventResource(name))
	}
	slices.Sort(resources)
	return resources
}

func (registrar *stagingRegistrar) validateManifest(manifest Manifest) error {
	actual := registrar.resources()
	declared := normalizeResources(manifest.Registers)
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

func validateProviderSpec(spec ProviderSpec) error {
	if err := validateResourceName("provider", spec.Name); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Model) == "" {
		return fmt.Errorf("provider %q model is empty", spec.Name)
	}
	return nil
}

func validateToolSpec(spec ToolSpec) error {
	if err := validateResourceName("tool", spec.Name); err != nil {
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

func validateHookSpec(spec HookSpec) error {
	if err := validateResourceName("hook", spec.Name); err != nil {
		return err
	}
	if err := validateResourceName("event", spec.Event); err != nil {
		return err
	}
	if spec.Priority < 0 {
		return fmt.Errorf("hook %q priority cannot be negative", spec.Name)
	}
	switch spec.FailurePolicy {
	case "", FailurePolicyFailClosed, FailurePolicyContinue:
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

func validateSubscriberSpec(spec SubscriberSpec) error {
	if err := validateResourceName("subscriber", spec.Name); err != nil {
		return err
	}
	if len(spec.Events) == 0 {
		return fmt.Errorf("subscriber %q has no events", spec.Name)
	}
	seen := make(map[string]struct{}, len(spec.Events))
	for _, event := range spec.Events {
		if err := validateResourceName("event", event); err != nil {
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

func validateCapabilitySpec(spec CapabilitySpec) error {
	if err := validateResourceName("capability", spec.Name); err != nil {
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

func validateEventContract(contract EventContract) error {
	if err := validateResourceName("event", contract.Name); err != nil {
		return err
	}
	if len(contract.MutableFields) != len(
		slices.Compact(append([]string(nil), contract.MutableFields...)),
	) {
		return fmt.Errorf("event %q has duplicate mutable fields", contract.Name)
	}
	for _, field := range contract.MutableFields {
		if err := validateResourceName("event field", field); err != nil {
			return err
		}
	}
	return nil
}

func cloneStringMap[V any](source map[string]V) map[string]V {
	return maps.Clone(source)
}
