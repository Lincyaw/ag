package sdk

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrLeaseNotFound = errors.New("plugin lease not found")
	ErrLeaseExpired  = errors.New("plugin lease expired")
)

type PluginRegistration struct {
	Name     string   `json:"name"`
	URI      string   `json:"uri"`
	Manifest Manifest `json:"manifest"`
}

type PluginLease struct {
	ID        string    `json:"id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type LeaseRegistryConfig struct {
	Registry *PluginRegistry
	Clock    func() time.Time
}

type LeaseRegistry struct {
	mu            sync.Mutex
	registry      *PluginRegistry
	clock         func() time.Time
	registrations map[string]PluginRegistration
	leases        map[string]PluginLease
	names         map[string]string
}

func NewLeaseRegistry(config LeaseRegistryConfig) *LeaseRegistry {
	if config.Registry == nil {
		config.Registry = NewPluginRegistry()
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	return &LeaseRegistry{
		registry:      config.Registry,
		clock:         config.Clock,
		registrations: make(map[string]PluginRegistration),
		leases:        make(map[string]PluginLease),
		names:         make(map[string]string),
	}
}

func (registry *LeaseRegistry) Register(
	ctx context.Context,
	registration PluginRegistration,
	ttl time.Duration,
) (PluginLease, error) {
	if registry == nil {
		return PluginLease{}, errors.New("lease registry is nil")
	}
	if err := ctx.Err(); err != nil {
		return PluginLease{}, err
	}
	if ttl <= 0 {
		return PluginLease{}, errors.New("plugin lease TTL must be positive")
	}
	if strings.TrimSpace(registration.Name) == "" {
		registration.Name = registration.Manifest.Name
	}
	if registration.Name != registration.Manifest.Name {
		return PluginLease{}, errors.New("registration name must match manifest name")
	}
	if err := registration.Manifest.Validate(); err != nil {
		return PluginLease{}, err
	}
	registration.URI = strings.TrimSpace(registration.URI)
	if registration.URI == "" {
		return PluginLease{}, errors.New("registration URI is empty")
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.clock().UTC()
	registry.pruneLocked(now)
	if _, exists := registry.names[registration.Name]; exists {
		return PluginLease{}, fmt.Errorf("plugin registration %q already has an active lease", registration.Name)
	}
	lease := PluginLease{ID: NewID(), ExpiresAt: now.Add(ttl)}
	if err := registry.registry.register(PluginReference{
		Name:        registration.Name,
		URI:         registration.URI,
		Description: registration.Manifest.Description,
	}, lease.ID); err != nil {
		return PluginLease{}, err
	}
	registration.Manifest = cloneManifest(registration.Manifest)
	registry.registrations[lease.ID] = registration
	registry.leases[lease.ID] = lease
	registry.names[registration.Name] = lease.ID
	return lease, nil
}

func (registry *LeaseRegistry) Renew(
	ctx context.Context,
	id string,
	ttl time.Duration,
) (PluginLease, error) {
	if registry == nil {
		return PluginLease{}, errors.New("lease registry is nil")
	}
	if err := ctx.Err(); err != nil {
		return PluginLease{}, err
	}
	if ttl <= 0 {
		return PluginLease{}, errors.New("plugin lease TTL must be positive")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.clock().UTC()
	lease, exists := registry.leases[id]
	if !exists {
		return PluginLease{}, fmt.Errorf("%w: %s", ErrLeaseNotFound, id)
	}
	if !lease.ExpiresAt.After(now) {
		registry.removeLocked(id)
		return PluginLease{}, fmt.Errorf("%w: %s", ErrLeaseExpired, id)
	}
	lease.ExpiresAt = now.Add(ttl)
	registry.leases[id] = lease
	return lease, nil
}

func (registry *LeaseRegistry) Unregister(
	ctx context.Context,
	id string,
) error {
	if registry == nil {
		return errors.New("lease registry is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.leases[id]; !exists {
		return fmt.Errorf("%w: %s", ErrLeaseNotFound, id)
	}
	registry.removeLocked(id)
	return nil
}

func (registry *LeaseRegistry) List(
	ctx context.Context,
) ([]PluginRegistration, error) {
	if registry == nil {
		return nil, errors.New("lease registry is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.pruneLocked(registry.clock().UTC())
	result := make([]PluginRegistration, 0, len(registry.registrations))
	for _, registration := range registry.registrations {
		registration.Manifest = cloneManifest(registration.Manifest)
		result = append(result, registration)
	}
	slices.SortFunc(result, func(left, right PluginRegistration) int {
		return strings.Compare(left.Name, right.Name)
	})
	return result, nil
}

func (registry *LeaseRegistry) Prune(ctx context.Context) error {
	if registry == nil {
		return errors.New("lease registry is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.pruneLocked(registry.clock().UTC())
	return nil
}

func (registry *LeaseRegistry) pruneLocked(now time.Time) {
	for id, lease := range registry.leases {
		if !lease.ExpiresAt.After(now) {
			registry.removeLocked(id)
		}
	}
}

func (registry *LeaseRegistry) removeLocked(id string) {
	registration, exists := registry.registrations[id]
	if !exists {
		delete(registry.leases, id)
		return
	}
	if registry.names[registration.Name] == id {
		delete(registry.names, registration.Name)
		registry.registry.unregisterOwned(registration.Name, id)
	}
	delete(registry.registrations, id)
	delete(registry.leases, id)
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.Requires = append([]string(nil), manifest.Requires...)
	manifest.Conflicts = append([]string(nil), manifest.Conflicts...)
	manifest.Registers = append([]string(nil), manifest.Registers...)
	return manifest
}
