package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/storage/duckdb"
)

type duckDBStateBackend struct {
	backendStores
	trajectories *duckdb.Store
	path         string
	sidecarRoot  string
	namespace    string
	closeOnce    sync.Once
	closeErr     error
}

func NewDuckDBStateBackend(path string) (sdk.StateBackend, error) {
	return newDuckDBStateBackend(path, "default")
}

func newDuckDBTrajectoryStore(
	path string,
	namespace string,
) (*duckdb.Store, error) {
	return duckdb.NewTrajectoryStore(path, namespace)
}

func newDuckDBStateBackend(
	path string,
	namespace string,
) (*duckDBStateBackend, error) {
	absolute, err := prepareDuckDBPath(path)
	if err != nil {
		return nil, err
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}
	trajectories, err := duckdb.NewTrajectoryStore(absolute, namespace)
	if err != nil {
		return nil, err
	}
	sidecarRoot := filepath.Join(
		absolute+".state",
		"namespaces",
		namespace,
	)
	operations, err := NewFileOperationStore(
		filepath.Join(sidecarRoot, "operations"),
	)
	if err != nil {
		_ = trajectories.Close()
		return nil, err
	}
	return &duckDBStateBackend{
		backendStores: newBackendStores(trajectories, operations),
		trajectories:  trajectories,
		path:          absolute,
		sidecarRoot:   sidecarRoot,
		namespace:     namespace,
	}, nil
}

func prepareDuckDBPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("DuckDB state path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve DuckDB state path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create DuckDB state directory: %w", err)
	}
	info, err := os.Stat(absolute)
	if err == nil && info.IsDir() {
		return "", fmt.Errorf("DuckDB state path %q is a directory", absolute)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat DuckDB state path: %w", err)
	}
	return absolute, nil
}

func (backend *duckDBStateBackend) Deliveries(
	name string,
) (sdk.DeliveryStore, error) {
	return backend.delivery(name, func() (sdk.DeliveryStore, error) {
		return NewFileDeliveryStore(
			filepath.Join(backend.sidecarRoot, "deliveries", name),
		)
	})
}

func (*duckDBStateBackend) Capabilities() sdk.StorageCapabilities {
	return sdk.StorageCapabilities{
		Durable:            true,
		MultiProcessSafe:   false,
		AtomicState:        false,
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (backend *duckDBStateBackend) Namespace() string {
	return backend.namespace
}

func (backend *duckDBStateBackend) Health(ctx context.Context) error {
	if backend == nil || backend.trajectories == nil {
		return errors.New("DuckDB state backend is not initialized")
	}
	return backend.trajectories.Ping(ctx)
}

func (backend *duckDBStateBackend) Close(context.Context) error {
	if backend == nil {
		return nil
	}
	backend.closeOnce.Do(func() {
		backend.closeErr = backend.trajectories.Close()
	})
	return backend.closeErr
}

func (backend *duckDBStateBackend) String() string {
	return (&url.URL{
		Scheme:   "duckdb",
		Path:     backend.path,
		RawQuery: url.Values{"namespace": {backend.namespace}}.Encode(),
	}).String()
}

type duckDBStorageDriver struct{}

func (duckDBStorageDriver) Scheme() string { return "duckdb" }

func (duckDBStorageDriver) Open(
	_ context.Context,
	parsed *url.URL,
) (sdk.StateBackend, error) {
	if parsed == nil {
		return nil, errors.New("DuckDB storage URI is nil")
	}
	if parsed.Opaque != "" {
		return nil, fmt.Errorf(
			"DuckDB storage URI %q must use an absolute file path",
			parsed,
		)
	}
	path := parsed.Path
	if parsed.Host != "" && parsed.Host != "localhost" {
		path = filepath.Join(
			string(filepath.Separator)+parsed.Host,
			parsed.Path,
		)
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("DuckDB storage URI has no path")
	}
	namespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	return newDuckDBStateBackend(path, namespace)
}
