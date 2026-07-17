package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

func TestManagerDiscoversAndFencesSessionPlugins(t *testing.T) {
	ctx := t.Context()
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	defer directory.Close(context.Background())
	leases := make(map[string]registry.PluginLease)
	for _, instanceID := range []string{"node-b", "node-a"} {
		lease, err := directory.Register(
			ctx,
			testRegistration("file", instanceID),
			registry.LeaseOptions{TTL: time.Minute},
		)
		if err != nil {
			t.Fatal(err)
		}
		leases[instanceID] = lease
	}
	store := NewMemorySessionStore()
	defer store.Close(context.Background())
	created, err := store.Create(ctx, Session{
		ID: "managed", UserID: "user-a", Provider: "openai", MaxTurns: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	guardCalls := 0
	manager, err := NewManager(ManagerConfig{
		Store:     store,
		Directory: directory,
		RequireIdle: func(context.Context, Session) error {
			guardCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := manager.Discover(
		ctx,
		registry.DiscoveryQuery{Name: "file"},
		registry.PageRequest{},
	)
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("discover page=%#v err=%v", page, err)
	}
	if _, err := manager.AttachPlugin(
		ctx,
		"user-a",
		created.ID,
		"file",
		created.Revision,
	); !errors.Is(err, ErrPluginAmbiguous) {
		t.Fatalf("ambiguous attach error = %v", err)
	}
	attached, err := manager.AttachPlugin(
		ctx,
		"user-a",
		created.ID,
		"file@node-a",
		created.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if guardCalls != 2 || attached.Revision != 2 ||
		len(attached.Plugins) != 1 ||
		attached.Plugins[0].InstanceID != "node-a" {
		t.Fatalf(
			"guard=%d attached=%#v",
			guardCalls,
			attached,
		)
	}
	if _, err := manager.AttachPlugin(
		ctx,
		"user-b",
		created.ID,
		"file@node-a",
		attached.Revision,
	); !errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign attach error = %v", err)
	}
	if _, err := manager.DetachPlugin(
		ctx,
		"user-a",
		attached.ID,
		"file",
		0,
	); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero revision detach error = %v", err)
	}
	references, err := manager.ResolvePlugins(ctx, attached)
	if err != nil || len(references) != 1 ||
		references[0].URI != "grpc://127.0.0.1:9001" {
		t.Fatalf("references=%#v err=%v", references, err)
	}
	lease := leases["node-a"]
	if err := directory.Unregister(ctx, registry.LeaseCredential{
		ID: lease.ID, Token: lease.Token,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ResolvePlugins(
		ctx,
		attached,
	); !errors.Is(err, ErrBindingStale) {
		t.Fatalf("stale binding error = %v", err)
	}
	if _, err := directory.Register(
		ctx,
		testRegistration("file", "node-a"),
		registry.LeaseOptions{TTL: time.Minute},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ResolvePlugins(ctx, attached); !errors.Is(
		err,
		ErrBindingStale,
	) || strings.Contains(err.Error(), "grpc://") {
		t.Fatalf("replaced binding error = %v", err)
	}
	detached, err := manager.DetachPlugin(
		ctx,
		"user-a",
		attached.ID,
		"file",
		attached.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if detached.Revision != 3 || len(detached.Plugins) != 0 {
		t.Fatalf("detached = %#v", detached)
	}
}

func TestManagerRejectsCompositionChangeWhileBusy(t *testing.T) {
	ctx := t.Context()
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	defer directory.Close(context.Background())
	if _, err := directory.Register(
		ctx,
		testRegistration("file", "node-a"),
		registry.LeaseOptions{TTL: time.Minute},
	); err != nil {
		t.Fatal(err)
	}
	store := NewMemorySessionStore()
	defer store.Close(context.Background())
	session, err := store.Create(ctx, Session{
		ID: "busy", UserID: "user-a", Provider: "openai", MaxTurns: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	busy := errors.New("session execution is active")
	manager, err := NewManager(ManagerConfig{
		Store:     store,
		Directory: directory,
		RequireIdle: func(context.Context, Session) error {
			return busy
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AttachPlugin(
		ctx,
		"user-a",
		session.ID,
		"file@node-a",
		0,
	); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero revision attach error = %v", err)
	}
	if _, err := manager.AttachPlugin(
		ctx,
		"user-a",
		session.ID,
		"file@node-a",
		session.Revision,
	); !errors.Is(err, busy) {
		t.Fatalf("busy attach error = %v", err)
	}
}

func testRegistration(name, instanceID string) registry.PluginRegistration {
	port := "9001"
	if instanceID == "node-b" {
		port = "9002"
	}
	return registry.PluginRegistration{
		Namespace:  registry.DefaultNamespace,
		Name:       name,
		InstanceID: instanceID,
		URI:        "grpc://127.0.0.1:" + port,
		Manifest: sdk.Manifest{
			Name: name, Version: "1.0.0",
			Description: "test " + name + " plugin",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("read_file")},
		},
		Labels: map[string]string{"zone": "local"},
	}
}
