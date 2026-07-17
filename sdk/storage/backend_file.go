package storage

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

type fileStateBackend struct {
	backendStores
	root      string
	namespace string
}

func NewFileStateBackend(root string) (sdk.StateBackend, error) {
	return newFileStateBackend(root, "default", false)
}

func newFileStateBackend(
	root string,
	namespace string,
	partition bool,
) (sdk.StateBackend, error) {
	absolute, err := prepareDirectory("state", root)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(namespace) == "" {
		namespace = "default"
	}
	if err := sdk.ValidateResourceName("storage namespace", namespace); err != nil {
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
		backendStores: newBackendStores(trajectories, operations),
		root:          absolute,
		namespace:     namespace,
	}, nil
}

func (backend *fileStateBackend) Deliveries(name string) (sdk.DeliveryStore, error) {
	return backend.delivery(name, func() (sdk.DeliveryStore, error) {
		return NewFileDeliveryStore(filepath.Join(backend.root, "deliveries", name))
	})
}

func (*fileStateBackend) Capabilities() sdk.StorageCapabilities {
	return sdk.StorageCapabilities{
		Durable:            true,
		MultiProcessSafe:   fileLocksAreMultiProcessSafe,
		OperationFencing:   true,
		NamedQueues:        true,
		Pagination:         true,
		Maintenance:        true,
		NamespaceIsolation: true,
	}
}

func (backend *fileStateBackend) Namespace() string { return backend.namespace }

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
) (sdk.StateBackend, error) {
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
	return newFileStateBackend(path, namespace, true)
}
