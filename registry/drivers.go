package registry

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
		"etcd":   etcdDriver{scheme: "etcd"},
		"etcds":  etcdDriver{scheme: "etcds"},
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

type etcdDriver struct {
	scheme string
}

func (driver etcdDriver) Scheme() string { return driver.scheme }

func (driver etcdDriver) Open(
	ctx context.Context,
	parsed *url.URL,
) (Directory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if parsed == nil {
		return nil, errors.New("etcd registry URI is nil")
	}
	if parsed.Scheme != driver.scheme {
		return nil, fmt.Errorf(
			"%s registry driver cannot open %q",
			driver.scheme,
			parsed.Scheme,
		)
	}
	if parsed.User != nil {
		return nil, errors.New(
			"etcd registry URI must not contain credentials",
		)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return nil, errors.New("etcd registry URI has no endpoint")
	}
	query := parsed.Query()
	for key := range query {
		switch key {
		case "dial_timeout", "endpoint", "server_name":
		default:
			return nil, fmt.Errorf(
				"etcd registry URI query parameter %q is unsupported",
				key,
			)
		}
	}
	dialTimeout := 5 * time.Second
	if raw := strings.TrimSpace(query.Get("dial_timeout")); raw != "" {
		var err error
		dialTimeout, err = time.ParseDuration(raw)
		if err != nil || dialTimeout <= 0 {
			return nil, fmt.Errorf(
				"parse etcd registry dial timeout %q",
				raw,
			)
		}
	}
	transportScheme := "http"
	var tlsConfig *tls.Config
	if driver.scheme == "etcds" {
		transportScheme = "https"
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: strings.TrimSpace(
				query.Get("server_name"),
			),
		}
	} else if query.Get("server_name") != "" {
		return nil, errors.New(
			"etcd server_name requires an etcds:// URI",
		)
	}
	endpoints := []string{
		transportScheme + "://" + parsed.Host,
	}
	for _, endpoint := range query["endpoint"] {
		normalized, err := normalizeEtcdEndpoint(
			endpoint,
			transportScheme,
		)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, normalized)
	}
	display := *parsed
	display.RawQuery = query.Encode()
	return NewEtcdDirectory(EtcdConfig{
		Endpoints:   endpoints,
		Prefix:      parsed.Path,
		DialTimeout: dialTimeout,
		TLS:         tlsConfig,
		DisplayURI:  display.String(),
	})
}

func normalizeEtcdEndpoint(
	raw string,
	scheme string,
) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("etcd registry endpoint is empty")
	}
	if !strings.Contains(raw, "://") {
		return scheme + "://" + raw, nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != scheme ||
		parsed.Host == "" || parsed.Path != "" {
		return "", fmt.Errorf(
			"etcd registry endpoint %q must use %s://host:port",
			raw,
			scheme,
		)
	}
	return parsed.String(), nil
}
