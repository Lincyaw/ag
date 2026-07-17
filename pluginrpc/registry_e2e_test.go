package pluginrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistryServiceDirectoryContract(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 16, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(duration time.Duration) {
		clockMu.Lock()
		now = now.Add(duration)
		clockMu.Unlock()
	}
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{Clock: clock})
	registryAdapter, err := NewRegistryServer(directory)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pluginv1.RegisterRegistryServiceServer(grpcServer, registryAdapter)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = listener.Close()
		select {
		case serveErr := <-serveErrors:
			if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
				t.Errorf("serve: %v", serveErr)
			}
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
		if err := directory.Close(context.Background()); err != nil {
			t.Errorf("close directory: %v", err)
		}
	})

	registryURI := "grpc://" + listener.Addr().String()
	client, err := NewRegistryClient(ctx, registryURI, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	registration := func(instanceID, zone string) registry.PluginRegistration {
		return registry.PluginRegistration{
			Namespace:  registry.DefaultNamespace,
			Name:       "discoverable",
			InstanceID: instanceID,
			URI:        registryURI,
			Labels:     map[string]string{"zone": zone},
			Manifest: sdk.Manifest{
				Name:        "discoverable",
				Version:     "1.0.0",
				Description: "discoverable remote plugin",
				APIVersion:  sdk.APIVersion,
				Registers:   []string{sdk.ToolResource("echo")},
			},
		}
	}
	firstLease, err := client.Register(
		ctx,
		registration("node-a", "north"),
		registry.LeaseOptions{TTL: time.Minute},
	)
	if err != nil {
		t.Fatalf("register first instance: %v", err)
	}
	secondLease, err := client.Register(
		ctx,
		registration("node-b", "south"),
		registry.LeaseOptions{TTL: time.Minute},
	)
	if err != nil {
		t.Fatalf("register second instance: %v", err)
	}
	if firstLease.ID == "" || firstLease.Token == "" ||
		firstLease.Key.InstanceID != "node-a" ||
		!firstLease.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("first lease = %#v", firstLease)
	}
	if secondLease.Key.Name != firstLease.Key.Name {
		t.Fatalf("same-name leases = %#v %#v", firstLease, secondLease)
	}

	firstPage, err := client.List(
		ctx,
		registry.DiscoveryQuery{Name: "discoverable"},
		registry.PageRequest{Limit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 1 || firstPage.Next == "" ||
		firstPage.Revision != 2 {
		t.Fatalf("first page = %#v", firstPage)
	}
	secondPage, err := client.List(
		ctx,
		registry.DiscoveryQuery{Name: "discoverable"},
		registry.PageRequest{Limit: 1, After: firstPage.Next},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Items) != 1 || secondPage.Next != "" ||
		secondPage.Items[0].InstanceID == firstPage.Items[0].InstanceID {
		t.Fatalf("second page = %#v", secondPage)
	}
	instance, err := client.Get(ctx, firstLease.Key)
	if err != nil {
		t.Fatal(err)
	}
	if instance.InstanceID != "node-a" || instance.Labels["zone"] != "north" {
		t.Fatalf("instance = %#v", instance)
	}
	changes, err := client.Poll(ctx, registry.ChangePollRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(changes.Changes) != 2 || changes.NextRevision != 2 {
		t.Fatalf("initial changes = %#v", changes)
	}

	discoveryRegistry := sdk.NewPluginRegistry()
	if err := RegisterDrivers(
		discoveryRegistry,
		ClientConfig{RegistryURI: registryURI},
	); err != nil {
		t.Fatal(err)
	}
	discovered, err := discoveryRegistry.Discover(
		ctx,
		sdk.DiscoveryQuery{
			Name: "discoverable", IncludeDrivers: true,
		},
	)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(discovered) != 2 {
		t.Fatalf("discovered = %#v", discovered)
	}

	if _, err := client.Renew(
		ctx,
		registry.LeaseCredential{ID: firstLease.ID, Token: "wrong"},
		time.Minute,
	); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("fenced renew status = %s, error = %v", status.Code(err), err)
	}
	advance(30 * time.Second)
	firstLease, err = client.Renew(
		ctx,
		registry.LeaseCredential{
			ID: firstLease.ID, Token: firstLease.Token,
		},
		2*time.Minute,
	)
	if err != nil {
		t.Fatalf("renew first instance: %v", err)
	}
	pollResult := make(chan registry.ChangePage, 1)
	pollErrors := make(chan error, 1)
	go func() {
		page, pollErr := client.Poll(ctx, registry.ChangePollRequest{
			AfterRevision: 2,
			Wait:          time.Second,
		})
		if pollErr != nil {
			pollErrors <- pollErr
			return
		}
		pollResult <- page
	}()
	time.Sleep(20 * time.Millisecond)
	if err := client.Unregister(ctx, registry.LeaseCredential{
		ID: firstLease.ID, Token: firstLease.Token,
	}); err != nil {
		t.Fatalf("unregister first instance: %v", err)
	}
	select {
	case pollErr := <-pollErrors:
		t.Fatal(pollErr)
	case page := <-pollResult:
		if len(page.Changes) != 1 ||
			page.Changes[0].Kind != registry.ChangeDelete ||
			page.NextRevision != 3 {
			t.Fatalf("long poll delete = %#v", page)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("registry long poll did not wake for unregister")
	}
	advance(30 * time.Second)
	active, err := client.List(
		ctx,
		registry.DiscoveryQuery{},
		registry.PageRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(active.Items) != 0 || active.Revision != 4 {
		t.Fatalf("active after delete and expiry = %#v", active)
	}
	finalChanges, err := client.Poll(ctx, registry.ChangePollRequest{
		AfterRevision: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(finalChanges.Changes) != 1 ||
		finalChanges.Changes[0].Kind != registry.ChangeExpire {
		t.Fatalf("final changes = %#v", finalChanges)
	}
	if _, err := client.Renew(
		ctx,
		registry.LeaseCredential{
			ID: secondLease.ID, Token: secondLease.Token,
		},
		time.Minute,
	); status.Code(err) != codes.NotFound {
		t.Fatalf("stale renew status = %s, error = %v", status.Code(err), err)
	}
}

func TestRegistryClientRejectsUnrepresentableRequests(t *testing.T) {
	t.Parallel()
	client := &RegistryClient{}
	if _, err := client.Register(
		context.Background(),
		registry.PluginRegistration{},
		registry.LeaseOptions{},
	); err == nil {
		t.Fatal("zero TTL registration succeeded")
	}
	if _, err := client.Renew(
		context.Background(),
		registry.LeaseCredential{},
		-time.Nanosecond,
	); err == nil {
		t.Fatal("negative TTL renewal succeeded")
	}
	if _, err := client.List(
		context.Background(),
		registry.DiscoveryQuery{},
		registry.PageRequest{Limit: registry.MaxPageSize + 1},
	); err == nil {
		t.Fatal("oversized registry page succeeded")
	}
	if _, err := client.Poll(
		context.Background(),
		registry.ChangePollRequest{Wait: -time.Nanosecond},
	); err == nil {
		t.Fatal("negative registry poll wait succeeded")
	}
	if value, err := durationMillis(
		time.Nanosecond,
		false,
		"test duration",
	); err != nil || value != 1 {
		t.Fatalf("sub-millisecond duration = %d, %v", value, err)
	}
}

func eventuallyRPC(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
