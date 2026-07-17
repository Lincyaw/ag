package storage

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	. "github.com/lincyaw/ag/sdk"
)

type testStorageDriver struct {
	opened  *url.URL
	backend StateBackend
}

func (*testStorageDriver) Scheme() string { return "teststore" }

func (driver *testStorageDriver) Open(
	_ context.Context,
	uri *url.URL,
) (StateBackend, error) {
	driver.opened = uri
	return driver.backend, nil
}

func TestStorageRegistryAcceptsApplicationDriver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registry := NewStorageRegistry()
	driver := &testStorageDriver{
		backend: NewMemoryStateBackendWithNamespace("tenant-a"),
	}
	if err := registry.RegisterDriver(driver); err != nil {
		t.Fatal(err)
	}
	backend, err := registry.Open(
		ctx,
		"teststore://cluster/agent-state?region=eu",
	)
	if err != nil {
		t.Fatal(err)
	}
	if driver.opened == nil || driver.opened.Host != "cluster" {
		t.Fatalf("driver received URI %#v", driver.opened)
	}
	if backend.Namespace() != "tenant-a" {
		t.Fatalf("namespace = %q, want tenant-a", backend.Namespace())
	}
}

func TestDefaultStorageRegistryExposesNamedQueuesAndNamespaces(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registry := NewDefaultStorageRegistry()
	backend, err := registry.Open(
		ctx,
		"memory://local?namespace=tenant-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close backend: %v", err)
		}
	}()
	if backend.Namespace() != "tenant-a" {
		t.Fatalf("namespace = %q, want tenant-a", backend.Namespace())
	}
	capabilities := backend.Capabilities()
	if !capabilities.NamedQueues || !capabilities.OperationFencing ||
		!capabilities.NamespaceIsolation {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	hostOutbox, err := backend.Deliveries(HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	pluginInbox, err := backend.Deliveries(PluginInboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	if hostOutbox == pluginInbox {
		t.Fatal("host outbox and plugin inbox resolved to the same queue")
	}
}

func TestFileStorageNamespaceHasItsOwnPartition(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	backend, err := NewFileStateBackendWithNamespace(root, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close backend: %v", err)
		}
	}()
	store, ok := backend.Trajectories().(*FileTrajectoryStore)
	if !ok {
		t.Fatalf("trajectory store type = %T", backend.Trajectories())
	}
	want := filepath.Join(root, "namespaces", "tenant-a", "trajectories")
	if store.Directory() != want {
		t.Fatalf("trajectory directory = %q, want %q", store.Directory(), want)
	}
}
