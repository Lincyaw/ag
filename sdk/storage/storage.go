package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	. "github.com/lincyaw/ag/sdk"
)

type StorageRegistry struct {
	mu      sync.RWMutex
	drivers map[string]StorageDriver
}

func NewStorageRegistry() *StorageRegistry {
	return &StorageRegistry{drivers: make(map[string]StorageDriver)}
}

func NewDefaultStorageRegistry() *StorageRegistry {
	registry := NewStorageRegistry()
	_ = registry.RegisterDriver(memoryStorageDriver{})
	_ = registry.RegisterDriver(fileStorageDriver{})
	return registry
}

func (registry *StorageRegistry) RegisterDriver(driver StorageDriver) error {
	if registry == nil {
		return errors.New("storage registry is nil")
	}
	if driver == nil {
		return errors.New("storage driver is nil")
	}
	scheme := strings.ToLower(strings.TrimSpace(driver.Scheme()))
	if err := ValidateResourceName("storage driver scheme", scheme); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.drivers[scheme]; exists {
		return fmt.Errorf("storage driver %q is already registered", scheme)
	}
	registry.drivers[scheme] = driver
	return nil
}

func (registry *StorageRegistry) Open(
	ctx context.Context,
	rawURI string,
) (StateBackend, error) {
	if registry == nil {
		return nil, errors.New("storage registry is nil")
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURI))
	if err != nil {
		return nil, fmt.Errorf("parse storage URI: %w", err)
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme == "" {
		return nil, errors.New("storage URI has no scheme")
	}
	registry.mu.RLock()
	driver := registry.drivers[scheme]
	registry.mu.RUnlock()
	if driver == nil {
		return nil, fmt.Errorf("no storage driver registered for scheme %q", scheme)
	}
	backend, err := driver.Open(ctx, parsed)
	if err != nil {
		return nil, fmt.Errorf("open %s state backend: %w", scheme, err)
	}
	if backend == nil {
		return nil, fmt.Errorf("storage driver %q returned a nil backend", scheme)
	}
	if err := backend.Health(ctx); err != nil {
		_ = backend.Close(context.Background())
		return nil, fmt.Errorf("check %s state backend health: %w", scheme, err)
	}
	return backend, nil
}

type memoryStateBackend struct {
	namespace    string
	trajectories TrajectoryStore
	operations   OperationStore
	mu           sync.Mutex
	deliveries   map[string]DeliveryStore
}

func NewMemoryStateBackend() StateBackend {
	return NewMemoryStateBackendWithNamespace("default")
}

func NewMemoryStateBackendWithNamespace(namespace string) StateBackend {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return &memoryStateBackend{
		namespace:    namespace,
		trajectories: NewMemoryTrajectoryStore(),
		operations:   NewMemoryOperationStore(),
		deliveries:   make(map[string]DeliveryStore),
	}
}

func (backend *memoryStateBackend) Trajectories() TrajectoryStore {
	return backend.trajectories
}

func (backend *memoryStateBackend) Operations() OperationStore {
	return backend.operations
}

func (backend *memoryStateBackend) Deliveries(name string) (DeliveryStore, error) {
	if err := ValidateResourceName("delivery queue", name); err != nil {
		return nil, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if existing := backend.deliveries[name]; existing != nil {
		return existing, nil
	}
	store := NewMemoryDeliveryStore()
	backend.deliveries[name] = store
	return store, nil
}

func (*memoryStateBackend) Capabilities() StorageCapabilities {
	return StorageCapabilities{
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (backend *memoryStateBackend) Namespace() string { return backend.namespace }

func (backend *memoryStateBackend) Prune(
	ctx context.Context,
	policy RetentionPolicy,
) (PruneResult, error) {
	var result PruneResult
	var err error
	if !policy.OperationsBefore.IsZero() {
		result.Operations, err = backend.operations.PurgeTerminal(
			ctx,
			policy.OperationsBefore,
		)
		if err != nil {
			return result, err
		}
	}
	if !policy.DeliveriesBefore.IsZero() {
		backend.mu.Lock()
		queues := make([]DeliveryStore, 0, len(backend.deliveries))
		for _, queue := range backend.deliveries {
			queues = append(queues, queue)
		}
		backend.mu.Unlock()
		for _, queue := range queues {
			removed, purgeErr := queue.PurgeTerminal(
				ctx,
				policy.DeliveriesBefore,
			)
			result.Deliveries += removed
			if purgeErr != nil {
				return result, purgeErr
			}
		}
	}
	if !policy.TrajectoriesBefore.IsZero() {
		items, listErr := backend.trajectories.List(ctx)
		if listErr != nil {
			return result, listErr
		}
		for _, item := range items {
			if item.UpdatedAt.Before(policy.TrajectoriesBefore) {
				if deleteErr := backend.trajectories.Delete(
					ctx,
					item.ID,
				); deleteErr != nil {
					return result, deleteErr
				}
				result.Trajectories++
			}
		}
	}
	return result, nil
}

func (*memoryStateBackend) Health(ctx context.Context) error { return ctx.Err() }
func (*memoryStateBackend) Close(context.Context) error      { return nil }
func (backend *memoryStateBackend) String() string {
	return "memory://local?namespace=" + url.QueryEscape(backend.namespace)
}

type memoryStorageDriver struct{}

func (memoryStorageDriver) Scheme() string { return "memory" }

func (memoryStorageDriver) Open(
	_ context.Context,
	parsed *url.URL,
) (StateBackend, error) {
	namespace := "default"
	if parsed != nil && strings.TrimSpace(parsed.Query().Get("namespace")) != "" {
		namespace = parsed.Query().Get("namespace")
	}
	if err := ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}
	return NewMemoryStateBackendWithNamespace(namespace), nil
}

type fileStateBackend struct {
	root         string
	namespace    string
	trajectories TrajectoryStore
	operations   OperationStore
	mu           sync.Mutex
	deliveries   map[string]DeliveryStore
}

func NewFileStateBackend(root string) (StateBackend, error) {
	return newFileStateBackend(root, "default", false)
}

func NewFileStateBackendWithNamespace(
	root string,
	namespace string,
) (StateBackend, error) {
	return newFileStateBackend(root, namespace, true)
}

func newFileStateBackend(
	root string,
	namespace string,
	partition bool,
) (StateBackend, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return nil, fmt.Errorf("resolve file state root: %w", err)
	}
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	if err := ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}
	if partition {
		absolute = filepath.Join(absolute, "namespaces", namespace)
	}
	trajectories, err := NewFileTrajectoryStore(filepath.Join(absolute, "trajectories"))
	if err != nil {
		return nil, err
	}
	operations, err := NewFileOperationStore(filepath.Join(absolute, "operations"))
	if err != nil {
		return nil, err
	}
	return &fileStateBackend{
		root:         absolute,
		namespace:    namespace,
		trajectories: trajectories,
		operations:   operations,
		deliveries:   make(map[string]DeliveryStore),
	}, nil
}

func (backend *fileStateBackend) Trajectories() TrajectoryStore {
	return backend.trajectories
}

func (backend *fileStateBackend) Operations() OperationStore {
	return backend.operations
}

func (backend *fileStateBackend) Deliveries(name string) (DeliveryStore, error) {
	if err := ValidateResourceName("delivery queue", name); err != nil {
		return nil, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if existing := backend.deliveries[name]; existing != nil {
		return existing, nil
	}
	store, err := NewFileDeliveryStore(filepath.Join(backend.root, "deliveries", name))
	if err != nil {
		return nil, err
	}
	backend.deliveries[name] = store
	return store, nil
}

func (*fileStateBackend) Capabilities() StorageCapabilities {
	return StorageCapabilities{
		Durable:            true,
		MultiProcessSafe:   FileLocksAreMultiProcessSafe(),
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (backend *fileStateBackend) Namespace() string { return backend.namespace }

func (backend *fileStateBackend) Prune(
	ctx context.Context,
	policy RetentionPolicy,
) (PruneResult, error) {
	var result PruneResult
	var err error
	if !policy.OperationsBefore.IsZero() {
		result.Operations, err = backend.operations.PurgeTerminal(
			ctx,
			policy.OperationsBefore,
		)
		if err != nil {
			return result, err
		}
	}
	if !policy.DeliveriesBefore.IsZero() {
		backend.mu.Lock()
		queues := make([]DeliveryStore, 0, len(backend.deliveries))
		for _, queue := range backend.deliveries {
			queues = append(queues, queue)
		}
		backend.mu.Unlock()
		for _, queue := range queues {
			removed, purgeErr := queue.PurgeTerminal(
				ctx,
				policy.DeliveriesBefore,
			)
			result.Deliveries += removed
			if purgeErr != nil {
				return result, purgeErr
			}
		}
	}
	if !policy.TrajectoriesBefore.IsZero() {
		items, listErr := backend.trajectories.List(ctx)
		if listErr != nil {
			return result, listErr
		}
		for _, item := range items {
			if item.UpdatedAt.Before(policy.TrajectoriesBefore) {
				if deleteErr := backend.trajectories.Delete(
					ctx,
					item.ID,
				); deleteErr != nil {
					return result, deleteErr
				}
				result.Trajectories++
			}
		}
	}
	return result, nil
}

func (backend *fileStateBackend) Health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if backend == nil || backend.root == "" {
		return errors.New("file state backend is not initialized")
	}
	return nil
}

func (*fileStateBackend) Close(context.Context) error { return nil }
func (backend *fileStateBackend) String() string {
	return "file://" + backend.root + "?namespace=" +
		url.QueryEscape(backend.namespace)
}

type fileStorageDriver struct{}

func (fileStorageDriver) Scheme() string { return "file" }

func (fileStorageDriver) Open(
	_ context.Context,
	parsed *url.URL,
) (StateBackend, error) {
	if parsed == nil {
		return nil, errors.New("file storage URI is nil")
	}
	path := parsed.Path
	if parsed.Host != "" && parsed.Host != "localhost" {
		path = filepath.Join(string(filepath.Separator)+parsed.Host, parsed.Path)
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("file storage URI has no path")
	}
	namespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	if namespace == "" {
		return NewFileStateBackend(path)
	}
	return NewFileStateBackendWithNamespace(path, namespace)
}
