package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type memoryStateBackend struct {
	backendStores
	contextInjections sdk.ContextInjectionStore
	namespace         string
}

func NewMemoryStateBackend() sdk.StateBackend {
	return newMemoryStateBackend("default")
}

func newMemoryStateBackend(namespace string) sdk.StateBackend {
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	return &memoryStateBackend{
		backendStores: newBackendStores(
			NewMemoryTrajectoryStore(),
			NewMemoryOperationStore(),
		),
		contextInjections: NewMemoryContextInjectionStore(),
		namespace:         namespace,
	}
}

func (backend *memoryStateBackend) ContextInjections() sdk.ContextInjectionStore {
	return backend.contextInjections
}

func (backend *memoryStateBackend) Deliveries(name string) (sdk.DeliveryStore, error) {
	return backend.delivery(name, func() (sdk.DeliveryStore, error) {
		return NewMemoryDeliveryStore(), nil
	})
}

func (*memoryStateBackend) Capabilities() sdk.StorageCapabilities {
	return sdk.StorageCapabilities{
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (backend *memoryStateBackend) Namespace() string { return backend.namespace }

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
) (sdk.StateBackend, error) {
	if parsed == nil {
		return nil, errors.New("memory storage URI is nil")
	}
	if (parsed.Host != "" && parsed.Host != "local") ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.Opaque != "" {
		return nil, fmt.Errorf("memory storage URI %q must target local without a path", parsed)
	}
	namespace := "default"
	if strings.TrimSpace(parsed.Query().Get("namespace")) != "" {
		namespace = parsed.Query().Get("namespace")
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
		return nil, err
	}
	return newMemoryStateBackend(namespace), nil
}
