package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lincyaw/ag/sdk"
)

type Driver interface {
	Scheme() string
	Open(context.Context, *url.URL) (Directory, error)
}

type BackendRegistry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

func NewBackendRegistry() *BackendRegistry {
	return &BackendRegistry{drivers: make(map[string]Driver)}
}

func NewDefaultBackendRegistry() *BackendRegistry {
	return &BackendRegistry{drivers: map[string]Driver{
		"file":   fileDriver{},
		"memory": memoryDriver{},
	}}
}

func (registry *BackendRegistry) RegisterDrivers(drivers ...Driver) error {
	if registry == nil {
		return errors.New("registry backend driver registry is nil")
	}
	additions := make(map[string]Driver, len(drivers))
	for _, driver := range drivers {
		if driver == nil {
			return errors.New("registry backend driver is nil")
		}
		scheme := strings.ToLower(strings.TrimSpace(driver.Scheme()))
		if err := sdk.ValidateResourceName("registry backend scheme", scheme); err != nil {
			return err
		}
		if _, exists := additions[scheme]; exists {
			return fmt.Errorf("registry backend driver %q is repeated", scheme)
		}
		additions[scheme] = driver
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for scheme := range additions {
		if _, exists := registry.drivers[scheme]; exists {
			return fmt.Errorf(
				"registry backend driver %q is already registered",
				scheme,
			)
		}
	}
	for scheme, driver := range additions {
		registry.drivers[scheme] = driver
	}
	return nil
}

func (registry *BackendRegistry) Open(
	ctx context.Context,
	rawURI string,
) (Directory, error) {
	if registry == nil {
		return nil, errors.New("registry backend driver registry is nil")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return nil, fmt.Errorf("parse registry backend URI: %w", err)
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme == "" {
		return nil, errors.New("registry backend URI has no scheme")
	}
	registry.mu.RLock()
	driver := registry.drivers[scheme]
	registry.mu.RUnlock()
	if driver == nil {
		return nil, fmt.Errorf(
			"no registry backend driver registered for scheme %q",
			scheme,
		)
	}
	directory, err := driver.Open(ctx, parsed)
	if err != nil {
		return nil, fmt.Errorf("open %s registry backend: %w", scheme, err)
	}
	if directory == nil {
		return nil, fmt.Errorf(
			"registry backend driver %q returned a nil directory",
			scheme,
		)
	}
	return directory, nil
}

type memoryDriver struct{}

func (memoryDriver) Scheme() string { return "memory" }

func (memoryDriver) Open(
	ctx context.Context,
	parsed *url.URL,
) (Directory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, errors.New("memory registry URI is nil")
	}
	if err := validateBackendURI(parsed, "memory"); err != nil {
		return nil, err
	}
	if parsed.Host != "" && parsed.Host != "local" {
		return nil, fmt.Errorf(
			"memory registry URI host must be empty or local, got %q",
			parsed.Host,
		)
	}
	if parsed.Path != "" {
		return nil, fmt.Errorf(
			"memory registry URI must not contain path %q",
			parsed.Path,
		)
	}
	return NewMemoryDirectory(MemoryConfig{}), nil
}

type fileDriver struct{}

func (fileDriver) Scheme() string { return "file" }

func (fileDriver) Open(
	ctx context.Context,
	parsed *url.URL,
) (Directory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, errors.New("file registry URI is nil")
	}
	if err := validateBackendURI(parsed, "file"); err != nil {
		return nil, err
	}
	path := parsed.Path
	if parsed.Host != "" && parsed.Host != "localhost" {
		path = filepath.Join(string(filepath.Separator)+parsed.Host, parsed.Path)
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("file registry URI has no path")
	}
	return NewFileDirectory(FileConfig{Directory: path})
}
