package storage

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type testStorageDriver struct {
	opened  *url.URL
	backend sdk.StateBackend
}

func (*testStorageDriver) Scheme() string { return "teststore" }

func (driver *testStorageDriver) Open(
	_ context.Context,
	uri *url.URL,
) (sdk.StateBackend, error) {
	driver.opened = uri
	return driver.backend, nil
}

type unhealthyStateBackend struct {
	sdk.StateBackend
	healthErr        error
	closeErr         error
	closed           bool
	closeHasDeadline bool
}

func (backend *unhealthyStateBackend) Health(context.Context) error {
	return backend.healthErr
}

func (backend *unhealthyStateBackend) Close(ctx context.Context) error {
	backend.closed = true
	_, backend.closeHasDeadline = ctx.Deadline()
	return backend.closeErr
}

func TestStorageRegistryAcceptsApplicationDriver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registry := NewStorageRegistry()
	driver := &testStorageDriver{
		backend: newMemoryStateBackend("tenant-a"),
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

func TestStorageRegistryClosesUnhealthyBackend(t *testing.T) {
	t.Parallel()
	healthErr := errors.New("unhealthy")
	closeErr := errors.New("close failed")
	backend := &unhealthyStateBackend{
		healthErr: healthErr,
		closeErr:  closeErr,
	}
	registry := NewStorageRegistry()
	if err := registry.RegisterDriver(&testStorageDriver{backend: backend}); err != nil {
		t.Fatal(err)
	}

	_, err := registry.Open(t.Context(), "teststore://state")
	if !errors.Is(err, healthErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Open() error = %v, want health and close errors", err)
	}
	if !backend.closed || !backend.closeHasDeadline {
		t.Fatalf(
			"unhealthy backend cleanup: closed=%t deadline=%t",
			backend.closed,
			backend.closeHasDeadline,
		)
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
	hostOutbox, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	pluginInbox, err := backend.Deliveries(sdk.PluginInboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	if hostOutbox == pluginInbox {
		t.Fatal("host outbox and plugin inbox resolved to the same queue")
	}
}

func TestMemoryStorageURIRejectsNonLocalTargets(t *testing.T) {
	t.Parallel()
	registry := NewDefaultStorageRegistry()
	for _, uri := range []string{
		"memory://remote",
		"memory://local/state",
		"memory:opaque",
	} {
		t.Run(uri, func(t *testing.T) {
			t.Parallel()
			if _, err := registry.Open(t.Context(), uri); err == nil {
				t.Fatalf("Open(%q) unexpectedly succeeded", uri)
			}
		})
	}
}

func TestPrunePreservesOperationsWhileTrajectoryExecutionIsActive(
	t *testing.T,
) {
	t.Parallel()
	ctx := t.Context()
	backend := NewMemoryStateBackend()
	trajectories := backend.Trajectories()
	if err := trajectories.Create(
		ctx,
		sdk.Trajectory{ID: "active-prune"},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := trajectories.BeginExecution(
		ctx,
		"active-prune",
		"",
		sdk.TrajectoryExecutionStart{
			ID: "active-execution", MaxTurns: 2,
		},
		trajectoryTestEntry(
			"active-input",
			"",
			sdk.TrajectoryKindUserMessage,
			`{"role":"user","content":"keep operation"}`,
		),
	); err != nil {
		t.Fatal(err)
	}
	operation, _, err := backend.Operations().Submit(
		ctx,
		sdk.OperationRecord{
			Operation: sdk.Operation{IdempotencyKey: "stable-operation"},
			Kind:      sdk.OperationKindTool,
			Resource:  "test-tool",
			Input:     []byte(`{}`),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Operations().Transition(
		ctx,
		operation.Operation.ID,
		operation.Operation.Revision,
		sdk.OperationFailed,
		nil,
		"finished failure",
	); err != nil {
		t.Fatal(err)
	}

	_, err = backend.Prune(ctx, sdk.RetentionPolicy{
		OperationsBefore: time.Now().UTC().Add(time.Hour),
	})
	if !errors.Is(err, sdk.ErrTrajectoryExecution) {
		t.Fatalf("Prune() error = %v", err)
	}
	if _, err := backend.Operations().Get(
		ctx,
		operation.Operation.ID,
	); err != nil {
		t.Fatalf("active execution operation was pruned: %v", err)
	}
}

func TestFileStorageNamespaceHasItsOwnPartition(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	uri := (&url.URL{
		Scheme:   "file",
		Path:     root,
		RawQuery: url.Values{"namespace": {"tenant-a"}}.Encode(),
	}).String()
	backend, err := NewDefaultStorageRegistry().Open(t.Context(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close backend: %v", err)
		}
	}()
	if err := backend.Trajectories().Create(
		t.Context(),
		sdk.Trajectory{ID: "partition-probe"},
	); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(
		root, "namespaces", "tenant-a", "trajectories", "partition-probe.json",
	)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat namespaced trajectory: %v", err)
	}
}
