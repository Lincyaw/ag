package sdk

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseRegistryConcurrentRenewExpiryAndReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 15, 0, 0, 0, time.UTC)
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
	base := NewPluginRegistry()
	registry := NewLeaseRegistry(LeaseRegistryConfig{Registry: base, Clock: clock})
	registration := PluginRegistration{
		Name: "leased-plugin",
		URI:  "grpc://127.0.0.1:9000",
		Manifest: Manifest{
			Name:        "leased-plugin",
			Version:     "1.0.0",
			Description: "leased plugin",
			APIVersion:  APIVersion,
		},
	}
	lease, err := registry.Register(ctx, registration, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	const renewers = 64
	var wait sync.WaitGroup
	var renewErrors atomic.Int64
	for range renewers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if _, err := registry.Renew(ctx, lease.ID, 2*time.Minute); err != nil {
				renewErrors.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := renewErrors.Load(); got != 0 {
		t.Fatalf("renew errors = %d", got)
	}
	advance(2 * time.Minute)
	listed, err := registry.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Fatalf("expired registrations = %v", listed)
	}
	if _, err := base.Resolve(ctx, registration.Name); err == nil {
		t.Fatal("expired registration remained in PluginRegistry")
	}
	if _, err := registry.Renew(ctx, lease.ID, time.Minute); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("stale renew = %v, want ErrLeaseNotFound after prune", err)
	}

	replacement := registration
	replacement.URI = "grpc://127.0.0.1:9001"
	replacementLease, err := registry.Register(ctx, replacement, time.Minute)
	if err != nil {
		t.Fatalf("register replacement: %v", err)
	}
	if replacementLease.ID == lease.ID {
		t.Fatal("replacement reused old lease ID")
	}
	if err := registry.Unregister(ctx, lease.ID); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("old lease unregister = %v", err)
	}
	discovered, err := base.Discover(ctx, DiscoveryQuery{Name: replacement.Name})
	if err != nil || len(discovered) != 1 || discovered[0].URI != replacement.URI {
		t.Fatalf("replacement discovery = %v, %v", discovered, err)
	}
}

func TestLeaseRegistryConcurrentSameNameRegistrationHasOneWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	registry := NewLeaseRegistry(LeaseRegistryConfig{})
	const workers = 32
	var created atomic.Int64
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := registry.Register(ctx, PluginRegistration{
				Name: "one-winner",
				URI:  "grpc://127.0.0.1:9010",
				Manifest: Manifest{
					Name:        "one-winner",
					Version:     "1.0.0",
					Description: "one winner",
					APIVersion:  APIVersion,
				},
			}, time.Minute)
			if err == nil {
				created.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := created.Load(); got != 1 {
		t.Fatalf("successful registrations = %d, want 1", got)
	}
}
