// Package plugincontract collects and validates plugin contributions before
// runtime publication or RPC serving.
package plugincontract

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type Contribution[Resource, Spec any] struct {
	Value Resource
	Spec  Spec
}

// Registrar collects the contribution types supported by every plugin
// transport. It deliberately does not implement sdk.AgentRegistrar.
type Registrar struct {
	Providers    map[string]Contribution[sdk.Provider, sdk.ProviderSpec]
	Tools        map[string]Contribution[sdk.Tool, sdk.ToolSpec]
	Agents       map[string]sdk.AgentSpec
	Hooks        map[string]Contribution[sdk.Hook, sdk.HookSpec]
	HookOrder    []string
	Subscribers  map[string]Contribution[sdk.Subscriber, sdk.SubscriberSpec]
	Capabilities map[string]Contribution[sdk.Capability, sdk.CapabilitySpec]
	Events       map[string]sdk.EventContract
}

func NewRegistrar() *Registrar {
	return &Registrar{
		Providers:    make(map[string]Contribution[sdk.Provider, sdk.ProviderSpec]),
		Tools:        make(map[string]Contribution[sdk.Tool, sdk.ToolSpec]),
		Agents:       make(map[string]sdk.AgentSpec),
		Hooks:        make(map[string]Contribution[sdk.Hook, sdk.HookSpec]),
		Subscribers:  make(map[string]Contribution[sdk.Subscriber, sdk.SubscriberSpec]),
		Capabilities: make(map[string]Contribution[sdk.Capability, sdk.CapabilitySpec]),
		Events:       make(map[string]sdk.EventContract),
	}
}

// AgentRegistrar adds the same-process agent contribution understood by the
// runtime but intentionally absent from the RPC plugin protocol.
type AgentRegistrar struct {
	*Registrar
}

func NewAgentRegistrar() *AgentRegistrar {
	return &AgentRegistrar{Registrar: NewRegistrar()}
}

func (registrar *Registrar) RegisterProvider(provider sdk.Provider) error {
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
	return register(
		registrar.Providers,
		"provider",
		spec.Name,
		provider,
		spec,
	)
}

func (registrar *Registrar) RegisterTool(tool sdk.Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	spec := tool.Spec()
	if err := ValidateToolSpec(spec); err != nil {
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
	return register(
		registrar.Tools,
		"tool",
		spec.Name,
		tool,
		sdk.CloneToolSpec(spec),
	)
}

func (registrar *AgentRegistrar) RegisterAgent(spec sdk.AgentSpec) error {
	if err := validateAgentSpec(spec); err != nil {
		return err
	}
	if _, exists := registrar.Agents[spec.Name]; exists {
		return fmt.Errorf("agent %q registered twice", spec.Name)
	}
	registrar.Agents[spec.Name] = sdk.CloneAgentSpec(spec)
	return nil
}

func (registrar *Registrar) RegisterHook(hook sdk.Hook) error {
	if hook == nil {
		return errors.New("hook is nil")
	}
	spec := hook.Spec()
	if err := validateHookSpec(spec); err != nil {
		return err
	}
	if err := register(
		registrar.Hooks,
		"hook",
		spec.Name,
		hook,
		spec,
	); err != nil {
		return err
	}
	registrar.HookOrder = append(registrar.HookOrder, spec.Name)
	return nil
}

func (registrar *Registrar) RegisterSubscriber(
	subscriber sdk.Subscriber,
) error {
	if subscriber == nil {
		return errors.New("subscriber is nil")
	}
	spec := subscriber.Spec()
	if err := validateSubscriberSpec(spec); err != nil {
		return err
	}
	return register(
		registrar.Subscribers,
		"subscriber",
		spec.Name,
		subscriber,
		sdk.CloneSubscriberSpec(spec),
	)
}

func (registrar *Registrar) RegisterCapability(
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
	return register(
		registrar.Capabilities,
		"capability",
		spec.Name,
		capability,
		sdk.CloneCapabilitySpec(spec),
	)
}

func (registrar *Registrar) RegisterEvent(contract sdk.EventContract) error {
	if err := validateEventContract(contract); err != nil {
		return err
	}
	if _, exists := registrar.Events[contract.Name]; exists {
		return fmt.Errorf("event %q registered twice", contract.Name)
	}
	registrar.Events[contract.Name] = sdk.CloneEventContract(contract)
	return nil
}

func register[Resource, Spec any](
	resources map[string]Contribution[Resource, Spec],
	kind string,
	name string,
	value Resource,
	spec Spec,
) error {
	if _, exists := resources[name]; exists {
		return fmt.Errorf("%s %q registered twice", kind, name)
	}
	resources[name] = Contribution[Resource, Spec]{
		Value: value,
		Spec:  spec,
	}
	return nil
}

func (registrar *Registrar) ResourceSpec(
	kind sdk.ResourceKind,
	name string,
) (any, bool) {
	if registrar == nil {
		return nil, false
	}
	switch kind {
	case sdk.ResourceKindProvider:
		resource, exists := registrar.Providers[name]
		return resource.Spec, exists
	case sdk.ResourceKindTool:
		resource, exists := registrar.Tools[name]
		return resource.Spec, exists
	case sdk.ResourceKindHook:
		resource, exists := registrar.Hooks[name]
		return resource.Spec, exists
	case sdk.ResourceKindSubscriber:
		resource, exists := registrar.Subscribers[name]
		return resource.Spec, exists
	case sdk.ResourceKindCapability:
		resource, exists := registrar.Capabilities[name]
		return resource.Spec, exists
	case sdk.ResourceKindEvent:
		resource, exists := registrar.Events[name]
		return resource, exists
	default:
		return nil, false
	}
}

func (registrar *Registrar) ResourceIdentity(
	manifest sdk.Manifest,
	kind sdk.ResourceKind,
	name string,
) sdk.ResourceIdentity {
	spec, _ := registrar.ResourceSpec(kind, name)
	return sdk.NewResourceIdentity(manifest, kind, name, spec)
}

func (registrar *Registrar) ResourceRevision(
	manifest sdk.Manifest,
	kind sdk.ResourceKind,
	name string,
) string {
	return registrar.ResourceIdentity(manifest, kind, name).Revision()
}

func (registrar *Registrar) Resources() []string {
	resources := make(
		[]string,
		0,
		len(registrar.Providers)+
			len(registrar.Tools)+
			len(registrar.Agents)+
			len(registrar.Hooks)+
			len(registrar.Subscribers)+
			len(registrar.Capabilities)+
			len(registrar.Events),
	)
	resources = appendNames(resources, registrar.Providers, sdk.ProviderResource)
	resources = appendNames(resources, registrar.Tools, sdk.ToolResource)
	resources = appendNames(resources, registrar.Agents, sdk.AgentResource)
	resources = appendNames(resources, registrar.Hooks, sdk.HookResource)
	resources = appendNames(resources, registrar.Subscribers, sdk.SubscriberResource)
	resources = appendNames(resources, registrar.Capabilities, sdk.CapabilityResource)
	resources = appendNames(resources, registrar.Events, sdk.EventResource)
	slices.Sort(resources)
	return resources
}

func (registrar *Registrar) ValidateManifest(manifest sdk.Manifest) error {
	actual := registrar.Resources()
	declared := slices.Clone(manifest.Registers)
	slices.Sort(declared)
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

func appendNames[Value any](
	target []string,
	resources map[string]Value,
	name func(string) string,
) []string {
	for resource := range resources {
		target = append(target, name(resource))
	}
	return target
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

func ValidateToolSpec(spec sdk.ToolSpec) error {
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

func validateAgentSpec(spec sdk.AgentSpec) error {
	if err := sdk.ValidateResourceName("agent", spec.Name); err != nil {
		return err
	}
	if strings.TrimSpace(spec.Description) == "" {
		return fmt.Errorf("agent %q description is empty", spec.Name)
	}
	if spec.Provider != "" {
		if err := sdk.ValidateResourceName(
			"agent provider",
			spec.Provider,
		); err != nil {
			return err
		}
	}
	if spec.MaxTurns < 0 {
		return fmt.Errorf("agent %q max turns cannot be negative", spec.Name)
	}
	seen := make(map[string]struct{}, len(spec.Tools))
	for _, tool := range spec.Tools {
		if err := sdk.ValidateResourceName("agent tool", tool); err != nil {
			return err
		}
		if _, duplicate := seen[tool]; duplicate {
			return fmt.Errorf(
				"agent %q contains duplicate tool %q",
				spec.Name,
				tool,
			)
		}
		seen[tool] = struct{}{}
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
			return fmt.Errorf(
				"subscriber %q contains duplicate event %q",
				spec.Name,
				event,
			)
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

var _ sdk.Registrar = (*Registrar)(nil)
var _ sdk.AgentRegistrar = (*AgentRegistrar)(nil)
