package registry

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/lincyaw/ag/sdk"
)

const (
	DefaultNamespace = "default"
	DefaultPageSize  = 100
	MaxPageSize      = 1000
)

var (
	ErrInstanceNotFound = errors.New("plugin instance not found")
	ErrInstanceConflict = errors.New("plugin instance already registered")
	ErrLeaseNotFound    = errors.New("plugin lease not found")
	ErrLeaseExpired     = errors.New("plugin lease expired")
	ErrLeaseFenced      = errors.New("plugin lease fenced")
	ErrCursorExpired    = errors.New("registry change cursor expired")
	ErrClosed           = errors.New("plugin directory closed")
)

type InstanceKey struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	InstanceID string `json:"instance_id"`
}

func (key InstanceKey) String() string {
	return key.Namespace + "/" + key.Name + "/" + key.InstanceID
}

type PluginRegistration struct {
	Namespace  string            `json:"namespace"`
	Name       string            `json:"name"`
	InstanceID string            `json:"instance_id"`
	URI        string            `json:"uri"`
	Manifest   sdk.Manifest      `json:"manifest"`
	Labels     map[string]string `json:"labels,omitempty"`
}

func (registration PluginRegistration) Key() InstanceKey {
	return InstanceKey{
		Namespace:  registration.Namespace,
		Name:       registration.Name,
		InstanceID: registration.InstanceID,
	}
}

type PluginInstance struct {
	PluginRegistration
	RegisteredAt time.Time `json:"registered_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	Revision     uint64    `json:"revision"`
	Epoch        uint64    `json:"epoch"`
}

type LeaseOptions struct {
	TTL time.Duration
}

type PluginLease struct {
	ID        string      `json:"id"`
	Token     string      `json:"token"`
	Key       InstanceKey `json:"key"`
	ExpiresAt time.Time   `json:"expires_at"`
	Epoch     uint64      `json:"epoch"`
}

type LeaseCredential struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

type DiscoveryQuery struct {
	Namespace string            `json:"namespace,omitempty"`
	Name      string            `json:"name,omitempty"`
	Version   string            `json:"version,omitempty"`
	Resource  string            `json:"resource,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type PageRequest struct {
	After string `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type DiscoveryPage struct {
	Items    []PluginInstance `json:"items"`
	Next     string           `json:"next,omitempty"`
	Revision uint64           `json:"revision"`
}

type ChangeKind string

const (
	ChangeUpsert ChangeKind = "upsert"
	ChangeDelete ChangeKind = "delete"
	ChangeExpire ChangeKind = "expire"
)

type PluginChange struct {
	Revision uint64         `json:"revision"`
	Kind     ChangeKind     `json:"kind"`
	Instance PluginInstance `json:"instance"`
}

type ChangePollRequest struct {
	Query         DiscoveryQuery `json:"query"`
	AfterRevision uint64         `json:"after_revision"`
	Limit         int            `json:"limit,omitempty"`
	Wait          time.Duration  `json:"wait,omitempty"`
}

type ChangePage struct {
	Changes         []PluginChange `json:"changes"`
	NextRevision    uint64         `json:"next_revision"`
	CurrentRevision uint64         `json:"current_revision"`
}

type Capabilities struct {
	Durable          bool `json:"durable"`
	MultiProcessSafe bool `json:"multi_process_safe"`
	Distributed      bool `json:"distributed"`
	Poll             bool `json:"poll"`
	NativeLease      bool `json:"native_lease"`
}

type Directory interface {
	Register(
		context.Context,
		PluginRegistration,
		LeaseOptions,
	) (PluginLease, error)
	Renew(
		context.Context,
		LeaseCredential,
		time.Duration,
	) (PluginLease, error)
	Unregister(context.Context, LeaseCredential) error
	Get(context.Context, InstanceKey) (PluginInstance, error)
	List(context.Context, DiscoveryQuery, PageRequest) (DiscoveryPage, error)
	Poll(context.Context, ChangePollRequest) (ChangePage, error)
	Capabilities() Capabilities
	String() string
	Close(context.Context) error
}

func normalizeRegistration(
	registration PluginRegistration,
) (PluginRegistration, error) {
	registration.Namespace = strings.TrimSpace(registration.Namespace)
	if registration.Namespace == "" {
		registration.Namespace = DefaultNamespace
	}
	registration.Name = strings.TrimSpace(registration.Name)
	registration.InstanceID = strings.TrimSpace(registration.InstanceID)
	registration.URI = strings.TrimSpace(registration.URI)
	if err := validateKey(registration.Key()); err != nil {
		return PluginRegistration{}, err
	}
	if err := registration.Manifest.Validate(); err != nil {
		return PluginRegistration{}, err
	}
	if registration.Manifest.Name != registration.Name {
		return PluginRegistration{}, fmt.Errorf(
			"registration name %q does not match manifest name %q",
			registration.Name,
			registration.Manifest.Name,
		)
	}
	if err := validatePluginURI(registration.URI); err != nil {
		return PluginRegistration{}, err
	}
	for key, value := range registration.Labels {
		if err := validateLabel(key, value); err != nil {
			return PluginRegistration{}, err
		}
	}
	registration.Manifest = cloneManifest(registration.Manifest)
	registration.Labels = maps.Clone(registration.Labels)
	return registration, nil
}

func normalizeQuery(query DiscoveryQuery) (DiscoveryQuery, error) {
	query.Namespace = strings.TrimSpace(query.Namespace)
	query.Name = strings.TrimSpace(query.Name)
	query.Version = strings.TrimSpace(query.Version)
	query.Resource = strings.TrimSpace(query.Resource)
	if query.Namespace != "" {
		if err := sdk.ValidateResourceName("registry namespace", query.Namespace); err != nil {
			return DiscoveryQuery{}, err
		}
	}
	if query.Name != "" {
		if err := sdk.ValidateResourceName("plugin", query.Name); err != nil {
			return DiscoveryQuery{}, err
		}
	}
	for key, value := range query.Labels {
		if err := validateLabel(key, value); err != nil {
			return DiscoveryQuery{}, err
		}
	}
	query.Labels = maps.Clone(query.Labels)
	return query, nil
}

func validateKey(key InstanceKey) error {
	if err := sdk.ValidateResourceName("registry namespace", key.Namespace); err != nil {
		return err
	}
	if err := sdk.ValidateResourceName("plugin", key.Name); err != nil {
		return err
	}
	return sdk.ValidateResourceName("plugin instance", key.InstanceID)
}

func validatePluginURI(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse plugin URI %q: %w", raw, err)
	}
	if parsed.Scheme == "" {
		return fmt.Errorf("plugin URI %q has no scheme", raw)
	}
	if parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "" {
		return fmt.Errorf("plugin URI %q has no target", raw)
	}
	return nil
}

func validateLabel(key, value string) error {
	trimmedKey := strings.TrimSpace(key)
	if key != trimmedKey {
		return fmt.Errorf("plugin label key %q has surrounding whitespace", key)
	}
	if trimmedKey == "" || len(trimmedKey) > 128 {
		return fmt.Errorf("plugin label key %q must contain 1..128 bytes", key)
	}
	if len(value) > 512 {
		return fmt.Errorf("plugin label %q value exceeds 512 bytes", key)
	}
	for _, character := range key + value {
		if unicode.IsControl(character) {
			return fmt.Errorf("plugin label %q contains control characters", key)
		}
	}
	return nil
}

func validatePage(request PageRequest) (PageRequest, string, error) {
	if request.Limit == 0 {
		request.Limit = DefaultPageSize
	}
	if request.Limit < 1 || request.Limit > MaxPageSize {
		return PageRequest{}, "", fmt.Errorf(
			"page limit must be between 1 and %d",
			MaxPageSize,
		)
	}
	if request.After == "" {
		return request, "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(request.After)
	if err != nil {
		return PageRequest{}, "", errors.New("invalid registry page cursor")
	}
	return request, string(raw), nil
}

func encodePageCursor(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func validatePoll(request ChangePollRequest) (ChangePollRequest, error) {
	query, err := normalizeQuery(request.Query)
	if err != nil {
		return ChangePollRequest{}, err
	}
	request.Query = query
	if request.Limit == 0 {
		request.Limit = DefaultPageSize
	}
	if request.Limit < 1 || request.Limit > MaxPageSize {
		return ChangePollRequest{}, fmt.Errorf(
			"poll limit must be between 1 and %d",
			MaxPageSize,
		)
	}
	if request.Wait < 0 {
		return ChangePollRequest{}, errors.New("poll wait cannot be negative")
	}
	return request, nil
}

func matches(instance PluginInstance, query DiscoveryQuery) bool {
	if query.Namespace != "" && instance.Namespace != query.Namespace {
		return false
	}
	if query.Name != "" && instance.Name != query.Name {
		return false
	}
	if query.Version != "" && instance.Manifest.Version != query.Version {
		return false
	}
	if query.Resource != "" &&
		!slices.Contains(instance.Manifest.Registers, query.Resource) {
		return false
	}
	for key, value := range query.Labels {
		if instance.Labels[key] != value {
			return false
		}
	}
	return true
}

func cloneManifest(manifest sdk.Manifest) sdk.Manifest {
	manifest.Requires = slices.Clone(manifest.Requires)
	manifest.Conflicts = slices.Clone(manifest.Conflicts)
	manifest.Registers = slices.Clone(manifest.Registers)
	return manifest
}

func cloneRegistration(registration PluginRegistration) PluginRegistration {
	registration.Manifest = cloneManifest(registration.Manifest)
	registration.Labels = maps.Clone(registration.Labels)
	return registration
}

func cloneInstance(instance PluginInstance) PluginInstance {
	instance.PluginRegistration = cloneRegistration(instance.PluginRegistration)
	return instance
}

func cloneChange(change PluginChange) PluginChange {
	change.Instance = cloneInstance(change.Instance)
	return change
}

func instanceMapKey(key InstanceKey) string {
	return key.Namespace + "\x00" + key.Name + "\x00" + key.InstanceID
}
