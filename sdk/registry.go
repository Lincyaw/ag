package sdk

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
)

type PluginReference struct {
	Name        string            `json:"name"`
	URI         string            `json:"uri,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Source      Source            `json:"-"`
}

type PluginDescriptor struct {
	Name        string            `json:"name"`
	URI         string            `json:"uri,omitempty"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Scheme      string            `json:"scheme"`
}

type DiscoveryQuery struct {
	Name           string
	Labels         map[string]string
	IncludeDrivers bool
}

type PluginDriver interface {
	Scheme() string
	Resolve(context.Context, PluginReference) (Source, error)
	Discover(context.Context, DiscoveryQuery) ([]PluginReference, error)
}

type registeredPluginDriver struct {
	scheme string
	driver PluginDriver
}

type PluginRegistry struct {
	mu      sync.RWMutex
	entries map[string]PluginReference
	drivers map[string]PluginDriver
}

func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		entries: make(map[string]PluginReference),
		drivers: make(map[string]PluginDriver),
	}
}

func (registry *PluginRegistry) RegisterDrivers(drivers ...PluginDriver) error {
	additions := make(map[string]PluginDriver, len(drivers))
	for _, driver := range drivers {
		if driver == nil {
			return errors.New("plugin driver is nil")
		}
		scheme := normalizeScheme(driver.Scheme())
		if err := ValidateResourceName("plugin driver scheme", scheme); err != nil {
			return err
		}
		if _, exists := additions[scheme]; exists {
			return fmt.Errorf("plugin driver %q is already registered", scheme)
		}
		additions[scheme] = driver
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for scheme := range additions {
		if _, exists := registry.drivers[scheme]; exists {
			return fmt.Errorf("plugin driver %q is already registered", scheme)
		}
	}
	maps.Copy(registry.drivers, additions)
	return nil
}

func (registry *PluginRegistry) Register(reference PluginReference) error {
	reference, err := normalizePluginReference(reference)
	if err != nil {
		return err
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.entries[reference.Name]; exists {
		return fmt.Errorf("plugin registration %q already exists", reference.Name)
	}
	registry.entries[reference.Name] = reference
	return nil
}

func (registry *PluginRegistry) Unregister(name string) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.entries[name]; !exists {
		return fmt.Errorf("plugin registration %q not found", name)
	}
	delete(registry.entries, name)
	return nil
}

func (registry *PluginRegistry) Resolve(
	ctx context.Context,
	nameOrURI string,
) (Source, error) {
	key := strings.TrimSpace(nameOrURI)
	if key == "" {
		return nil, errors.New("plugin name or URI is empty")
	}

	registry.mu.RLock()
	reference, registered := registry.entries[key]
	registry.mu.RUnlock()
	if registered {
		if reference.Source != nil {
			return reference.Source, nil
		}
		return registry.resolveReference(ctx, reference)
	}

	if !strings.Contains(key, "://") {
		return nil, fmt.Errorf("plugin registration %q not found", key)
	}
	return registry.resolveReference(ctx, PluginReference{
		Name: key,
		URI:  key,
	})
}

func (registry *PluginRegistry) Discover(
	ctx context.Context,
	query DiscoveryQuery,
) ([]PluginDescriptor, error) {
	query.Labels = cloneLabels(query.Labels)
	registry.mu.RLock()
	entries := make([]PluginReference, 0, len(registry.entries))
	for _, entry := range registry.entries {
		entries = append(entries, entry)
	}
	drivers := make([]registeredPluginDriver, 0, len(registry.drivers))
	if query.IncludeDrivers {
		for scheme, driver := range registry.drivers {
			drivers = append(drivers, registeredPluginDriver{
				scheme: scheme,
				driver: driver,
			})
		}
	}
	registry.mu.RUnlock()
	slices.SortFunc(drivers, func(left, right registeredPluginDriver) int {
		return strings.Compare(left.scheme, right.scheme)
	})

	for _, registered := range drivers {
		driverQuery := query
		driverQuery.Labels = cloneLabels(query.Labels)
		discovered, err := registered.driver.Discover(ctx, driverQuery)
		if err != nil {
			return nil, fmt.Errorf(
				"discover plugins with %s driver: %w",
				registered.scheme,
				err,
			)
		}
		entries = append(entries, discovered...)
	}

	result := make([]PluginDescriptor, 0, len(entries))
	for _, entry := range entries {
		if !matchesQuery(entry, query) {
			continue
		}
		descriptor, err := descriptorFor(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, descriptor)
	}
	slices.SortFunc(result, func(left, right PluginDescriptor) int {
		return cmp.Or(
			strings.Compare(left.Name, right.Name),
			strings.Compare(left.Scheme, right.Scheme),
			strings.Compare(left.URI, right.URI),
			strings.Compare(left.Description, right.Description),
		)
	})
	return result, nil
}

func (registry *PluginRegistry) resolveReference(
	ctx context.Context,
	reference PluginReference,
) (Source, error) {
	parsed, err := parsePluginURI(reference.URI)
	if err != nil {
		return nil, err
	}
	scheme := normalizeScheme(parsed.Scheme)

	registry.mu.RLock()
	driver := registry.drivers[scheme]
	registry.mu.RUnlock()
	if driver == nil {
		return nil, fmt.Errorf("no plugin driver registered for scheme %q", scheme)
	}
	reference.Labels = cloneLabels(reference.Labels)
	source, err := driver.Resolve(ctx, reference)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve plugin %q through %s driver: %w",
			reference.Name,
			scheme,
			err,
		)
	}
	if source == nil {
		return nil, fmt.Errorf(
			"plugin driver %q returned a nil source for %q",
			scheme,
			reference.Name,
		)
	}
	return source, nil
}

func descriptorFor(reference PluginReference) (PluginDescriptor, error) {
	reference, err := normalizePluginReference(reference)
	if err != nil {
		return PluginDescriptor{}, err
	}
	scheme := "local"
	if reference.Source == nil {
		parsed, err := parsePluginURI(reference.URI)
		if err != nil {
			return PluginDescriptor{}, err
		}
		scheme = normalizeScheme(parsed.Scheme)
	}
	return PluginDescriptor{
		Name:        reference.Name,
		URI:         reference.URI,
		Description: reference.Description,
		Labels:      reference.Labels,
		Scheme:      scheme,
	}, nil
}

func normalizePluginReference(
	reference PluginReference,
) (PluginReference, error) {
	if err := ValidateResourceName("plugin reference", reference.Name); err != nil {
		return PluginReference{}, err
	}
	reference.URI = strings.TrimSpace(reference.URI)
	if (reference.Source == nil) == (reference.URI == "") {
		return PluginReference{}, errors.New(
			"plugin reference must provide exactly one of Source or URI",
		)
	}
	if reference.URI != "" {
		if _, err := parsePluginURI(reference.URI); err != nil {
			return PluginReference{}, err
		}
	}
	reference.Labels = cloneLabels(reference.Labels)
	return reference, nil
}

func parsePluginURI(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parse plugin URI %q: %w", raw, err)
	}
	if parsed.Scheme == "" {
		return nil, fmt.Errorf("plugin URI %q has no scheme", raw)
	}
	if parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "" {
		return nil, fmt.Errorf("plugin URI %q has no target", raw)
	}
	return parsed, nil
}

func normalizeScheme(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func matchesQuery(reference PluginReference, query DiscoveryQuery) bool {
	if query.Name != "" && reference.Name != query.Name {
		return false
	}
	for key, value := range query.Labels {
		if reference.Labels[key] != value {
			return false
		}
	}
	return true
}

func cloneLabels(labels map[string]string) map[string]string { return maps.Clone(labels) }
