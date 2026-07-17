package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{
		now: time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC),
	}
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

type directoryFactory struct {
	name string
	open func(*testing.T, *testClock, int) Directory
}

func directoryFactories() []directoryFactory {
	return []directoryFactory{
		{
			name: "memory",
			open: func(
				_ *testing.T,
				clock *testClock,
				maxChanges int,
			) Directory {
				return NewMemoryDirectory(MemoryConfig{
					Clock: clock.Now, MaxChanges: maxChanges,
				})
			},
		},
		{
			name: "file",
			open: func(
				t *testing.T,
				clock *testClock,
				maxChanges int,
			) Directory {
				t.Helper()
				directory, err := NewFileDirectory(FileConfig{
					Directory: t.TempDir(), Clock: clock.Now,
					PollInterval: 5 * time.Millisecond,
					MaxChanges:   maxChanges,
				})
				if err != nil {
					t.Fatal(err)
				}
				return directory
			},
		},
	}
}

func TestDirectoryContract(t *testing.T) {
	t.Parallel()
	for _, factory := range directoryFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()
			clock := newTestClock()
			directory := factory.open(t, clock, 32)
			t.Cleanup(func() {
				if err := directory.Close(context.Background()); err != nil {
					t.Errorf("close: %v", err)
				}
			})
			ctx := context.Background()

			first := testRegistration("file", "node-a", "grpc://127.0.0.1:9001")
			first.Labels = map[string]string{"zone": "local", "tier": "tools"}
			firstLease, err := directory.Register(
				ctx,
				first,
				LeaseOptions{TTL: time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}
			if firstLease.ID == "" || firstLease.Token == "" ||
				firstLease.Key != first.Key() || firstLease.Epoch != 1 {
				t.Fatalf("first lease = %#v", firstLease)
			}

			second := testRegistration("file", "node-b", "grpc://127.0.0.1:9002")
			second.Labels = map[string]string{"zone": "remote", "tier": "tools"}
			secondLease, err := directory.Register(
				ctx,
				second,
				LeaseOptions{TTL: 2 * time.Minute},
			)
			if err != nil {
				t.Fatal(err)
			}
			if secondLease.Key.Name != firstLease.Key.Name {
				t.Fatalf("same plugin replicas have different names: %#v %#v", firstLease, secondLease)
			}
			if _, err := directory.Register(
				ctx,
				first,
				LeaseOptions{TTL: time.Minute},
			); !errors.Is(err, ErrInstanceConflict) {
				t.Fatalf("duplicate registration error = %v", err)
			}

			filtered, err := directory.List(
				ctx,
				DiscoveryQuery{
					Name: "file", Resource: sdk.ToolResource("read_file"),
					Labels: map[string]string{"zone": "local"},
				},
				PageRequest{},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(filtered.Items) != 1 ||
				filtered.Items[0].InstanceID != first.InstanceID ||
				filtered.Revision != 2 {
				t.Fatalf("filtered page = %#v", filtered)
			}

			pageOne, err := directory.List(
				ctx,
				DiscoveryQuery{Name: "file"},
				PageRequest{Limit: 1},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(pageOne.Items) != 1 || pageOne.Next == "" {
				t.Fatalf("first page = %#v", pageOne)
			}
			pageTwo, err := directory.List(
				ctx,
				DiscoveryQuery{Name: "file"},
				PageRequest{Limit: 1, After: pageOne.Next},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(pageTwo.Items) != 1 ||
				pageTwo.Items[0].InstanceID == pageOne.Items[0].InstanceID ||
				pageTwo.Next != "" {
				t.Fatalf("second page = %#v", pageTwo)
			}

			instance, err := directory.Get(ctx, first.Key())
			if err != nil {
				t.Fatal(err)
			}
			instance.Labels["zone"] = "mutated"
			reloaded, err := directory.Get(ctx, first.Key())
			if err != nil {
				t.Fatal(err)
			}
			if reloaded.Labels["zone"] != "local" {
				t.Fatalf("directory leaked mutable labels: %#v", reloaded.Labels)
			}

			if _, err := directory.Renew(
				ctx,
				LeaseCredential{ID: firstLease.ID, Token: "wrong"},
				time.Minute,
			); !errors.Is(err, ErrLeaseFenced) {
				t.Fatalf("wrong-token renewal error = %v", err)
			}
			clock.Advance(30 * time.Second)
			renewed, err := directory.Renew(
				ctx,
				LeaseCredential{ID: firstLease.ID, Token: firstLease.Token},
				2*time.Minute,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !renewed.ExpiresAt.Equal(clock.Now().Add(2 * time.Minute)) {
				t.Fatalf("renewed lease = %#v", renewed)
			}
			renewedInstance, err := directory.Get(ctx, first.Key())
			if err != nil {
				t.Fatal(err)
			}
			if !renewedInstance.UpdatedAt.Equal(clock.Now()) {
				t.Fatalf("renewed instance update time = %s", renewedInstance.UpdatedAt)
			}
			afterRenew, err := directory.List(ctx, DiscoveryQuery{}, PageRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if afterRenew.Revision != 2 {
				t.Fatalf("renewal changed topology revision to %d", afterRenew.Revision)
			}

			initialChanges, err := directory.Poll(ctx, ChangePollRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(initialChanges.Changes) != 2 ||
				initialChanges.Changes[0].Kind != ChangeUpsert ||
				initialChanges.NextRevision != 2 {
				t.Fatalf("initial changes = %#v", initialChanges)
			}

			if err := directory.Unregister(ctx, LeaseCredential{
				ID: firstLease.ID, Token: firstLease.Token,
			}); err != nil {
				t.Fatal(err)
			}
			deleted, err := directory.Poll(ctx, ChangePollRequest{
				AfterRevision: 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(deleted.Changes) != 1 ||
				deleted.Changes[0].Kind != ChangeDelete ||
				deleted.Changes[0].Instance.InstanceID != first.InstanceID {
				t.Fatalf("delete changes = %#v", deleted)
			}

			clock.Advance(2 * time.Minute)
			active, err := directory.List(ctx, DiscoveryQuery{}, PageRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(active.Items) != 0 || active.Revision != 4 {
				t.Fatalf("active after expiry = %#v", active)
			}
			expired, err := directory.Poll(ctx, ChangePollRequest{
				AfterRevision: 3,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(expired.Changes) != 1 ||
				expired.Changes[0].Kind != ChangeExpire ||
				expired.Changes[0].Instance.InstanceID != second.InstanceID {
				t.Fatalf("expiry changes = %#v", expired)
			}
			if _, err := directory.Get(ctx, second.Key()); !errors.Is(err, ErrInstanceNotFound) {
				t.Fatalf("expired instance lookup error = %v", err)
			}
		})
	}
}

func TestDirectoryPollWaitsForTopologyChange(t *testing.T) {
	t.Parallel()
	for _, factory := range directoryFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()
			clock := newTestClock()
			directory := factory.open(t, clock, 32)
			t.Cleanup(func() { _ = directory.Close(context.Background()) })

			result := make(chan ChangePage, 1)
			failures := make(chan error, 1)
			go func() {
				page, err := directory.Poll(context.Background(), ChangePollRequest{
					Wait: time.Second,
				})
				if err != nil {
					failures <- err
					return
				}
				result <- page
			}()
			time.Sleep(20 * time.Millisecond)
			if _, err := directory.Register(
				context.Background(),
				testRegistration("observer", "node-a", "grpc://127.0.0.1:9010"),
				LeaseOptions{TTL: time.Minute},
			); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-failures:
				t.Fatal(err)
			case page := <-result:
				if len(page.Changes) != 1 || page.Changes[0].Kind != ChangeUpsert {
					t.Fatalf("long poll page = %#v", page)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("long poll did not observe registration")
			}
		})
	}
}

func TestDirectoryPollObservesLeaseExpiry(t *testing.T) {
	t.Parallel()
	for _, factory := range directoryFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()
			clock := newTestClock()
			directory := factory.open(t, clock, 32)
			t.Cleanup(func() { _ = directory.Close(context.Background()) })
			lease, err := directory.Register(
				context.Background(),
				testRegistration("ephemeral", "node-a", "grpc://127.0.0.1:9020"),
				LeaseOptions{TTL: 50 * time.Millisecond},
			)
			if err != nil {
				t.Fatal(err)
			}
			page, err := directory.List(
				context.Background(),
				DiscoveryQuery{},
				PageRequest{},
			)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				time.Sleep(25 * time.Millisecond)
				clock.Advance(time.Second)
			}()
			changes, err := directory.Poll(
				context.Background(),
				ChangePollRequest{
					AfterRevision: page.Revision,
					Wait:          time.Second,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes.Changes) != 1 ||
				changes.Changes[0].Kind != ChangeExpire ||
				changes.Changes[0].Instance.InstanceID != lease.Key.InstanceID {
				t.Fatalf("expiry poll = %#v", changes)
			}
		})
	}
}

func TestDirectoryRejectsCompactedCursor(t *testing.T) {
	t.Parallel()
	for _, factory := range directoryFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()
			clock := newTestClock()
			directory := factory.open(t, clock, 2)
			t.Cleanup(func() { _ = directory.Close(context.Background()) })
			for index := range 4 {
				registration := testRegistration(
					"worker",
					fmt.Sprintf("node-%d", index),
					fmt.Sprintf("grpc://127.0.0.1:%d", 9100+index),
				)
				if _, err := directory.Register(
					context.Background(),
					registration,
					LeaseOptions{TTL: time.Minute},
				); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := directory.Poll(context.Background(), ChangePollRequest{
				AfterRevision: 1,
			}); !errors.Is(err, ErrCursorExpired) {
				t.Fatalf("compacted cursor error = %v", err)
			}
		})
	}
}

func TestDirectoryRejectsAmbiguousLabelKeys(t *testing.T) {
	t.Parallel()
	for _, factory := range directoryFactories() {
		factory := factory
		t.Run(factory.name, func(t *testing.T) {
			t.Parallel()
			directory := factory.open(t, newTestClock(), 32)
			t.Cleanup(func() { _ = directory.Close(context.Background()) })
			registration := testRegistration(
				"worker",
				"node-a",
				"grpc://127.0.0.1:9150",
			)
			registration.Labels = map[string]string{" zone": "local"}
			if _, err := directory.Register(
				context.Background(),
				registration,
				LeaseOptions{TTL: time.Minute},
			); err == nil {
				t.Fatal("registration with whitespace label key succeeded")
			}
		})
	}
}

func TestFileDirectoryPersistsAndCoordinatesInstances(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	clock := newTestClock()
	open := func() Directory {
		directory, err := NewFileDirectory(FileConfig{
			Directory: root, Clock: clock.Now,
			PollInterval: time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return directory
	}
	first := open()
	second := open()
	t.Cleanup(func() {
		_ = first.Close(context.Background())
		_ = second.Close(context.Background())
	})

	lease, err := first.Register(
		context.Background(),
		testRegistration("file", "node-a", "grpc://127.0.0.1:9200"),
		LeaseOptions{TTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := second.List(context.Background(), DiscoveryQuery{}, PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].InstanceID != "node-a" {
		t.Fatalf("second file directory page = %#v", page)
	}
	if _, err := second.Renew(
		context.Background(),
		LeaseCredential{ID: lease.ID, Token: lease.Token},
		2*time.Minute,
	); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(root, "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("registry file mode = %o", info.Mode().Perm())
	}

	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopened := open()
	defer reopened.Close(context.Background())
	persisted, err := reopened.Get(context.Background(), InstanceKey{
		Namespace: DefaultNamespace, Name: "file", InstanceID: "node-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.ExpiresAt.Equal(clock.Now().Add(2 * time.Minute)) {
		t.Fatalf("persisted instance = %#v", persisted)
	}
}

func TestDefaultBackendRegistryOpensMemoryAndFile(t *testing.T) {
	t.Parallel()
	backends := NewDefaultBackendRegistry()
	memory, err := backends.Open(context.Background(), "memory://local")
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close(context.Background())
	if memory.String() != "memory://local" || memory.Capabilities().Durable {
		t.Fatalf("memory backend = %s %#v", memory.String(), memory.Capabilities())
	}

	root := filepath.Join(t.TempDir(), "registry")
	fileURI := (&urlForTest{path: root}).String()
	file, err := backends.Open(context.Background(), fileURI)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close(context.Background())
	if !file.Capabilities().Durable {
		t.Fatalf("file backend capabilities = %#v", file.Capabilities())
	}
	if _, err := backends.Open(context.Background(), "unknown://registry"); err == nil {
		t.Fatal("unknown backend scheme succeeded")
	}
}

type urlForTest struct{ path string }

func (value *urlForTest) String() string {
	return "file://" + value.path
}

func testRegistration(name, instanceID, uri string) PluginRegistration {
	return PluginRegistration{
		Namespace:  DefaultNamespace,
		Name:       name,
		InstanceID: instanceID,
		URI:        uri,
		Manifest: sdk.Manifest{
			Name:        name,
			Version:     "1.0.0",
			Description: name + " plugin",
			APIVersion:  sdk.APIVersion,
			Registers:   []string{sdk.ToolResource("read_file")},
		},
	}
}
