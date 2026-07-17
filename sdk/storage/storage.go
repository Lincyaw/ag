// Package storage provides reference infrastructure adapters for the durable
// state ports defined by sdk. Agent execution policy remains in sdk/runtime.
package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	sdk "github.com/lincyaw/ag/sdk"
)

type StorageRegistry struct {
	mu      sync.RWMutex
	drivers map[string]sdk.StorageDriver
}

func NewStorageRegistry() *StorageRegistry {
	return &StorageRegistry{drivers: make(map[string]sdk.StorageDriver)}
}

func NewDefaultStorageRegistry() *StorageRegistry {
	return &StorageRegistry{drivers: map[string]sdk.StorageDriver{
		"duckdb":     duckDBStorageDriver{},
		"file":       fileStorageDriver{},
		"memory":     memoryStorageDriver{},
		"postgres":   postgresStorageDriver{scheme: "postgres"},
		"postgresql": postgresStorageDriver{scheme: "postgresql"},
	}}
}

func (registry *StorageRegistry) RegisterDriver(driver sdk.StorageDriver) error {
	if registry == nil {
		return errors.New("storage registry is nil")
	}
	if driver == nil {
		return errors.New("storage driver is nil")
	}
	scheme := strings.ToLower(strings.TrimSpace(driver.Scheme()))
	if err := sdk.ValidateResourceName("storage driver scheme", scheme); err != nil {
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
) (sdk.StateBackend, error) {
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
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		closeErr := backend.Close(closeCtx)
		cancel()
		return nil, fmt.Errorf(
			"check %s state backend health: %w",
			scheme,
			errors.Join(err, closeErr),
		)
	}
	return backend, nil
}

type backendStores struct {
	trajectories sdk.TrajectoryStore
	operations   sdk.OperationStore
	mu           sync.Mutex
	deliveries   map[string]sdk.DeliveryStore
}

func newBackendStores(
	trajectories sdk.TrajectoryStore,
	operations sdk.OperationStore,
) backendStores {
	return backendStores{
		trajectories: trajectories,
		operations:   operations,
		deliveries:   make(map[string]sdk.DeliveryStore),
	}
}

func (stores *backendStores) Trajectories() sdk.TrajectoryStore {
	return stores.trajectories
}

func (stores *backendStores) Operations() sdk.OperationStore {
	return stores.operations
}

func (stores *backendStores) delivery(
	name string,
	open func() (sdk.DeliveryStore, error),
) (sdk.DeliveryStore, error) {
	if err := sdk.ValidateResourceName("delivery queue", name); err != nil {
		return nil, err
	}
	stores.mu.Lock()
	defer stores.mu.Unlock()
	if existing := stores.deliveries[name]; existing != nil {
		return existing, nil
	}
	store, err := open()
	if err != nil {
		return nil, err
	}
	stores.deliveries[name] = store
	return store, nil
}

func (stores *backendStores) Prune(
	ctx context.Context,
	policy sdk.RetentionPolicy,
) (sdk.PruneResult, error) {
	stores.mu.Lock()
	deliveries := make([]sdk.DeliveryStore, 0, len(stores.deliveries))
	for _, store := range stores.deliveries {
		deliveries = append(deliveries, store)
	}
	stores.mu.Unlock()
	return pruneState(
		ctx,
		policy,
		stores.trajectories,
		stores.operations,
		deliveries,
	)
}

func pruneState(
	ctx context.Context,
	policy sdk.RetentionPolicy,
	trajectories sdk.TrajectoryStore,
	operations sdk.OperationStore,
	deliveries []sdk.DeliveryStore,
) (sdk.PruneResult, error) {
	var result sdk.PruneResult
	var err error
	if !policy.OperationsBefore.IsZero() {
		items, listErr := trajectories.List(ctx)
		if listErr != nil {
			return result, listErr
		}
		for _, item := range items {
			if item.ExecutionState == sdk.TrajectoryExecutionPending ||
				item.ExecutionState == sdk.TrajectoryExecutionRunning {
				return result, fmt.Errorf(
					"%w: cannot prune operations while trajectory %s execution %s is active",
					sdk.ErrTrajectoryExecution,
					item.ID,
					item.ExecutionID,
				)
			}
		}
		result.Operations, err = operations.PurgeTerminal(
			ctx,
			policy.OperationsBefore,
		)
		if err != nil {
			return result, err
		}
	}
	if !policy.DeliveriesBefore.IsZero() {
		for _, queue := range deliveries {
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
		items, listErr := trajectories.List(ctx)
		if listErr != nil {
			return result, listErr
		}
		for index := len(items) - 1; index >= 0; index-- {
			item := items[index]
			if item.UpdatedAt.Before(policy.TrajectoriesBefore) {
				if deleteErr := trajectories.Delete(
					ctx,
					item.ID,
				); deleteErr != nil {
					if errors.Is(deleteErr, sdk.ErrTrajectoryReferenced) ||
						errors.Is(deleteErr, sdk.ErrTrajectoryExecution) {
						continue
					}
					return result, deleteErr
				}
				result.Trajectories++
			}
		}
	}
	return result, nil
}

func pageWindow[T any](
	items []T,
	request sdk.PageRequest,
	id func(T) string,
) ([]T, string, error) {
	if request.Limit == 0 {
		request.Limit = sdk.DefaultPageSize
	}
	if request.Limit < 0 {
		return nil, "", errors.New("page limit cannot be negative")
	}
	if request.Limit > sdk.MaxPageSize {
		return nil, "", fmt.Errorf(
			"page limit %d exceeds maximum %d",
			request.Limit,
			sdk.MaxPageSize,
		)
	}
	start := 0
	if request.After != "" {
		start = -1
		for index, item := range items {
			if id(item) == request.After {
				start = index + 1
				break
			}
		}
		if start < 0 {
			return nil, "", fmt.Errorf(
				"pagination cursor %q was not found",
				request.After,
			)
		}
	}
	end := min(start+request.Limit, len(items))
	page := append([]T(nil), items[start:end]...)
	next := ""
	if end < len(items) && len(page) > 0 {
		next = id(page[len(page)-1])
	}
	return page, next, nil
}
