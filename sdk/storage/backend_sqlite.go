package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/storage/gormstore"
)

type sqliteStateBackend struct {
	store *gormstore.Store
	path  string
}

func NewSQLiteStateBackend(path string) (sdk.StateBackend, error) {
	return newSQLiteStateBackend(path, "default")
}

func newSQLiteStateBackend(path string, namespace string) (*sqliteStateBackend, error) {
	absolute, err := prepareSQLitePath(path)
	if err != nil {
		return nil, err
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = "default"
	}
	store, err := gormstore.Open(absolute, namespace)
	if err != nil {
		return nil, fmt.Errorf("open SQLite state backend: %w", err)
	}
	return &sqliteStateBackend{store: store, path: absolute}, nil
}

func prepareSQLitePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("SQLite state path is empty")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve SQLite state path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return "", fmt.Errorf("create SQLite state directory: %w", err)
	}
	info, err := os.Stat(absolute)
	if err == nil && info.IsDir() {
		return "", fmt.Errorf("SQLite state path %q is a directory", absolute)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat SQLite state path: %w", err)
	}
	return absolute, nil
}

func (backend *sqliteStateBackend) Trajectories() sdk.TrajectoryStore {
	return backend.store.Trajectories()
}

func (backend *sqliteStateBackend) Operations() sdk.OperationStore {
	return backend.store.Operations()
}

func (backend *sqliteStateBackend) ContextInjections() sdk.ContextInjectionStore {
	return backend.store.ContextInjections()
}

func (backend *sqliteStateBackend) Deliveries(name string) (sdk.DeliveryStore, error) {
	return backend.store.Deliveries(name)
}

func (backend *sqliteStateBackend) Capabilities() sdk.StorageCapabilities {
	return backend.store.Capabilities()
}

func (backend *sqliteStateBackend) Namespace() string {
	return backend.store.Namespace()
}

func (backend *sqliteStateBackend) Prune(
	ctx context.Context,
	policy sdk.RetentionPolicy,
) (sdk.PruneResult, error) {
	return backend.store.Prune(ctx, policy)
}

func (backend *sqliteStateBackend) Health(ctx context.Context) error {
	return backend.store.Health(ctx)
}

func (backend *sqliteStateBackend) Close(ctx context.Context) error {
	return backend.store.Close(ctx)
}

func (backend *sqliteStateBackend) String() string {
	return (&url.URL{
		Scheme:   "sqlite",
		Path:     backend.path,
		RawQuery: url.Values{"namespace": {backend.store.Namespace()}}.Encode(),
	}).String()
}

func (backend *sqliteStateBackend) AppendTrajectory(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	return backend.store.AppendTrajectory(ctx, commit)
}

func (backend *sqliteStateBackend) StartExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
) (sdk.ExecutionMutationResult, error) {
	return backend.store.StartExecution(ctx, commit)
}

func (backend *sqliteStateBackend) CommitExecution(
	ctx context.Context,
	commit sdk.ExecutionMutationCommit,
) (sdk.ExecutionMutationResult, error) {
	return backend.store.CommitExecution(ctx, commit)
}

func (backend *sqliteStateBackend) CancelExecution(
	ctx context.Context,
	commit sdk.ExecutionCancelCommit,
) (sdk.ExecutionCancelResult, error) {
	return backend.store.CancelExecution(ctx, commit)
}

var _ sdk.AtomicStateBackend = (*sqliteStateBackend)(nil)

type sqliteStorageDriver struct{}

func (sqliteStorageDriver) Scheme() string { return "sqlite" }

func (sqliteStorageDriver) Open(
	_ context.Context,
	parsed *url.URL,
) (sdk.StateBackend, error) {
	if parsed == nil {
		return nil, errors.New("SQLite storage URI is nil")
	}
	if parsed.Opaque != "" {
		return nil, fmt.Errorf(
			"SQLite storage URI %q must use an absolute file path",
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
		return nil, errors.New("SQLite storage URI has no path")
	}
	namespace := strings.TrimSpace(parsed.Query().Get("namespace"))
	return newSQLiteStateBackend(path, namespace)
}
