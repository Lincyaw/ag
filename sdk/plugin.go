package sdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const APIVersion = 1

var resourceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Manifest struct {
	Name          string   `json:"name"`
	Version       string   `json:"version"`
	Description   string   `json:"description"`
	APIVersion    int      `json:"api_version"`
	MinAPIVersion int      `json:"min_api_version,omitempty"`
	MaxAPIVersion int      `json:"max_api_version,omitempty"`
	Requires      []string `json:"requires,omitempty"`
	Conflicts     []string `json:"conflicts,omitempty"`
	Registers     []string `json:"registers,omitempty"`
}

func CloneManifest(manifest Manifest) Manifest {
	manifest.Requires = slices.Clone(manifest.Requires)
	manifest.Conflicts = slices.Clone(manifest.Conflicts)
	manifest.Registers = slices.Clone(manifest.Registers)
	return manifest
}

func (manifest Manifest) Validate() error {
	if err := ValidateResourceName("plugin", manifest.Name); err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Version) == "" {
		return errors.New("plugin version is empty")
	}
	if strings.TrimSpace(manifest.Description) == "" {
		return errors.New("plugin description is empty")
	}
	minimum, maximum := manifest.APIRange()
	if minimum < 1 || maximum < minimum {
		return fmt.Errorf(
			"plugin %q has invalid API version range %d..%d",
			manifest.Name,
			minimum,
			maximum,
		)
	}
	if APIVersion < minimum || APIVersion > maximum {
		return fmt.Errorf(
			"plugin %q API versions %d..%d are incompatible with SDK API version %d",
			manifest.Name,
			minimum,
			maximum,
			APIVersion,
		)
	}
	return validateUniqueStrings(
		manifest.Name,
		slices.Concat(manifest.Registers, manifest.Requires, manifest.Conflicts),
	)
}

func (manifest Manifest) APIRange() (int, int) {
	minimum := manifest.APIVersion
	maximum := manifest.APIVersion
	if manifest.MinAPIVersion != 0 {
		minimum = manifest.MinAPIVersion
	}
	if manifest.MaxAPIVersion != 0 {
		maximum = manifest.MaxAPIVersion
	}
	return minimum, maximum
}

type Registrar interface {
	RegisterProvider(Provider) error
	RegisterTool(Tool) error
	RegisterHook(Hook) error
	RegisterSubscriber(Subscriber) error
	RegisterCapability(Capability) error
	RegisterEvent(EventContract) error
}

// AgentRegistrar is an optional same-process registrar extension. RPC plugin
// registrars intentionally do not implement it.
type AgentRegistrar interface {
	RegisterAgent(AgentSpec) error
}

func RegisterAgent(registrar Registrar, spec AgentSpec) error {
	agents, ok := registrar.(AgentRegistrar)
	if !ok {
		return errors.New(
			"agent registration requires a same-process runtime registrar",
		)
	}
	return agents.RegisterAgent(spec)
}

type Plugin interface {
	Manifest() Manifest
	Install(context.Context, Registrar) error
}

type PluginFunc struct {
	PluginManifest Manifest
	InstallFunc    func(context.Context, Registrar) error
}

func (plugin PluginFunc) Manifest() Manifest {
	return plugin.PluginManifest
}

func (plugin PluginFunc) Install(
	ctx context.Context,
	registrar Registrar,
) error {
	if plugin.InstallFunc == nil {
		return errors.New("plugin install function is nil")
	}
	return plugin.InstallFunc(ctx, registrar)
}

type Connection interface {
	Plugin
	Close(context.Context) error
}

type Source interface {
	Open(context.Context) (Connection, error)
	String() string
}

type ResourceKind string

const (
	ResourceKindPlugin     ResourceKind = "plugin"
	ResourceKindProvider   ResourceKind = "provider"
	ResourceKindTool       ResourceKind = "tool"
	ResourceKindAgent      ResourceKind = "agent"
	ResourceKindHook       ResourceKind = "hook"
	ResourceKindSubscriber ResourceKind = "subscriber"
	ResourceKindCapability ResourceKind = "capability"
	ResourceKindEvent      ResourceKind = "event"
)

func (kind ResourceKind) ResourceName(name string) string {
	return string(kind) + ":" + name
}

func ProviderResource(name string) string {
	return ResourceKindProvider.ResourceName(name)
}

func ToolResource(name string) string { return ResourceKindTool.ResourceName(name) }

func AgentResource(name string) string {
	return ResourceKindAgent.ResourceName(name)
}

func HookResource(name string) string { return ResourceKindHook.ResourceName(name) }

func SubscriberResource(name string) string {
	return ResourceKindSubscriber.ResourceName(name)
}

func CapabilityResource(name string) string {
	return ResourceKindCapability.ResourceName(name)
}

func EventResource(name string) string { return ResourceKindEvent.ResourceName(name) }

func PluginResource(name string) string { return ResourceKindPlugin.ResourceName(name) }

type ResourceIdentity struct {
	Plugin        string       `json:"plugin"`
	PluginVersion string       `json:"version"`
	Kind          ResourceKind `json:"kind"`
	Name          string       `json:"name"`
	Spec          any          `json:"spec"`
}

func NewResourceIdentity(
	manifest Manifest,
	kind ResourceKind,
	name string,
	spec any,
) ResourceIdentity {
	return ResourceIdentity{
		Plugin:        manifest.Name,
		PluginVersion: manifest.Version,
		Kind:          kind,
		Name:          name,
		Spec:          spec,
	}
}

func (identity ResourceIdentity) Revision() string {
	raw, err := json.Marshal(identity)
	if err != nil {
		raw = []byte(
			identity.Plugin + "\x00" + identity.PluginVersion + "\x00" +
				string(identity.Kind) + "\x00" + identity.Name,
		)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func ResourceRevision(manifest Manifest, kind, name string, spec any) string {
	return NewResourceIdentity(
		manifest,
		ResourceKind(kind),
		name,
		spec,
	).Revision()
}

func ValidateResourceName(kind, name string) error {
	if !resourceNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%s name %q must match %s",
			kind,
			name,
			resourceNamePattern,
		)
	}
	return nil
}

func validateUniqueStrings(owner string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("plugin %q contains an empty resource reference", owner)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf(
				"plugin %q contains duplicate resource reference %q",
				owner,
				value,
			)
		}
		seen[value] = struct{}{}
	}
	return nil
}
