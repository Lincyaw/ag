package pluginrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistryServiceDiscoveryAndExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pluginAdapter, err := NewServer(ctx, ServerConfig{
		Plugin:     newE2EPlugin(),
		Operations: sdkstorage.NewMemoryOperationStore(),
		Inbox:      sdkstorage.NewMemoryDeliveryStore(),
	})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer, err := NewGRPCServer(pluginAdapter, 0)
	if err != nil {
		t.Fatal(err)
	}
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
	leaseRegistry := sdk.NewLeaseRegistry(sdk.LeaseRegistryConfig{Clock: clock})
	registryAdapter, err := NewRegistryServer(leaseRegistry)
	if err != nil {
		t.Fatal(err)
	}
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
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := pluginAdapter.Close(closeCtx); err != nil {
			t.Errorf("close plugin adapter: %v", err)
		}
	})

	registryURI := "grpc://" + listener.Addr().String()
	client, err := NewRegistryClient(ctx, registryURI, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration := sdk.PluginRegistration{
		Name: "discoverable",
		URI:  registryURI,
		Manifest: sdk.Manifest{
			Name:        "discoverable",
			Version:     "1.0.0",
			Description: "discoverable remote plugin",
			APIVersion:  sdk.APIVersion,
		},
	}
	lease, err := client.Register(ctx, registration, time.Minute)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if lease.ID == "" || !lease.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("lease = %#v", lease)
	}
	discoveryRegistry := sdk.NewPluginRegistry()
	if err := RegisterDrivers(discoveryRegistry, ClientConfig{RegistryURI: registryURI}); err != nil {
		t.Fatal(err)
	}
	discovered, err := discoveryRegistry.Discover(ctx, sdk.DiscoveryQuery{IncludeDrivers: true})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(discovered) != 1 || discovered[0].Name != registration.Name ||
		discovered[0].URI != registration.URI {
		t.Fatalf("discovered = %#v", discovered)
	}
	advance(time.Minute)
	discovered, err = discoveryRegistry.Discover(ctx, sdk.DiscoveryQuery{IncludeDrivers: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(discovered) != 0 {
		t.Fatalf("expired discovery = %#v", discovered)
	}
	if _, err := client.Renew(ctx, lease.ID, time.Minute); status.Code(err) != codes.NotFound {
		t.Fatalf("stale renew status = %s, error = %v", status.Code(err), err)
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
