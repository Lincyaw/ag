package registry

import (
	"context"
	"errors"
	"math"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestEtcdDriverValidatesAndRedactsConfiguration(t *testing.T) {
	t.Parallel()
	backends := NewDefaultBackendRegistry()
	directory, err := backends.Open(
		context.Background(),
		"etcd://127.0.0.1:2379/test/registry?"+
			"endpoint=127.0.0.2%3A2379&dial_timeout=2s",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !directory.Capabilities().Distributed ||
		!directory.Capabilities().Durable ||
		directory.String() == "" {
		t.Fatalf(
			"etcd directory = %s %#v",
			directory.String(),
			directory.Capabilities(),
		)
	}
	if err := directory.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"etcd://user:secret@127.0.0.1:2379/registry",
		"etcd://127.0.0.1:2379/registry?",
		"etcd://127.0.0.1:2379/registry#ignored",
		"etcd://127.0.0.1:2379/registry?unknown=true",
		"etcd://127.0.0.1:2379/registry?dial_timeout=1s;ignored=true",
		"etcd://127.0.0.1:2379/registry?dial_timeout=",
		"etcd://127.0.0.1:2379/registry?dial_timeout=1s&dial_timeout=2s",
		"etcd://127.0.0.1:2379/registry?server_name=etcd.local",
		"etcd://127.0.0.1:2379/registry?endpoint=other%3A2379%2Fnested",
		"etcd://127.0.0.1:2379/registry?endpoint=http%3A%2F%2Fother%3A2379%3Fignored%3Dtrue",
		"etcd://127.0.0.1:2379/registry?endpoint=http%3A%2F%2Fother%3A2379%23ignored",
		"etcd://127.0.0.1:2379/registry?endpoint=http%3A%2F%2Fuser%3Asecret%40other%3A2379",
		"etcds://127.0.0.1:2379/registry?endpoint=http%3A%2F%2Fother%3A2379",
		"etcds://127.0.0.1:2379/registry?server_name=one&server_name=two",
	} {
		if _, err := backends.Open(
			context.Background(),
			raw,
		); err == nil || strings.Contains(
			err.Error(),
			"secret",
		) {
			t.Fatalf("invalid etcd URI %q error = %v", raw, err)
		}
	}
}

func TestEtcdDirectoryClassifiesInvalidRequests(t *testing.T) {
	t.Parallel()
	directory, err := NewEtcdDirectory(EtcdConfig{
		Endpoints: []string{"http://127.0.0.1:1"},
		Prefix:    "/test/invalid-requests",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := directory.Close(context.Background()); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	registration := testRegistration(
		"file",
		"node-a",
		"grpc://127.0.0.1:9001",
	)
	tests := map[string]func() error{
		"registration": func() error {
			registration := registration
			registration.Name = "invalid name"
			_, err := directory.Register(
				context.Background(),
				registration,
				LeaseOptions{TTL: time.Minute},
			)
			return err
		},
		"lease TTL": func() error {
			_, err := directory.Renew(
				context.Background(),
				LeaseCredential{ID: "1", Token: "token"},
				0,
			)
			return err
		},
		"instance key": func() error {
			_, err := directory.Get(context.Background(), InstanceKey{
				Name: "invalid name", InstanceID: "node-a",
			})
			return err
		},
		"discovery query": func() error {
			_, err := directory.List(
				context.Background(),
				DiscoveryQuery{Name: "invalid name"},
				PageRequest{},
			)
			return err
		},
		"page cursor": func() error {
			_, err := directory.List(
				context.Background(),
				DiscoveryQuery{},
				PageRequest{After: "not-base64!"},
			)
			return err
		},
		"poll request": func() error {
			_, err := directory.Poll(
				context.Background(),
				ChangePollRequest{Wait: -time.Second},
			)
			return err
		},
		"poll revision": func() error {
			_, err := directory.Poll(
				context.Background(),
				ChangePollRequest{AfterRevision: math.MaxUint64},
			)
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestEtcdDirectoryRealServer(t *testing.T) {
	rawURI := strings.TrimSpace(os.Getenv("AG_TEST_ETCD_URI"))
	if rawURI == "" {
		t.Skip("AG_TEST_ETCD_URI is not configured")
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = path.Join(
		parsed.Path,
		"ag-test-"+sdk.NewID(),
	)
	directory, err := NewDefaultBackendRegistry().Open(
		context.Background(),
		parsed.String(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = directory.Close(context.Background()) })
	ctx := context.Background()
	registration := func(instanceID string) PluginRegistration {
		return PluginRegistration{
			Namespace:  DefaultNamespace,
			Name:       "etcd-e2e",
			InstanceID: instanceID,
			URI:        "grpc://127.0.0.1:9999",
			Labels:     map[string]string{"backend": "etcd"},
			Manifest: sdk.Manifest{
				Name:        "etcd-e2e",
				Version:     "1.0.0",
				Description: "etcd directory integration test",
				APIVersion:  sdk.APIVersion,
				Registers:   []string{sdk.ToolResource("echo")},
			},
		}
	}
	first, err := directory.Register(
		ctx,
		registration("node-a"),
		LeaseOptions{TTL: 2 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := directory.Register(
		ctx,
		registration("node-b"),
		LeaseOptions{TTL: 2 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch != 1 || second.Epoch != 1 ||
		first.Token == "" || first.ID == second.ID {
		t.Fatalf("initial etcd leases = %#v %#v", first, second)
	}
	page, err := directory.List(
		ctx,
		DiscoveryQuery{
			Name:   "etcd-e2e",
			Labels: map[string]string{"backend": "etcd"},
		},
		PageRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Revision == 0 {
		t.Fatalf("initial etcd page = %#v", page)
	}
	firstPage, err := directory.List(
		ctx,
		DiscoveryQuery{Name: "etcd-e2e"},
		PageRequest{Limit: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Items) != 1 || firstPage.Next == "" {
		t.Fatalf("first etcd page = %#v", firstPage)
	}
	secondPage, err := directory.List(
		ctx,
		DiscoveryQuery{Name: "etcd-e2e"},
		PageRequest{Limit: 1, After: firstPage.Next},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Items) != 1 ||
		secondPage.Next != "" ||
		secondPage.Items[0].InstanceID ==
			firstPage.Items[0].InstanceID {
		t.Fatalf("second etcd page = %#v", secondPage)
	}
	if _, err := directory.Renew(
		ctx,
		LeaseCredential{ID: first.ID, Token: "wrong"},
		2*time.Second,
	); !errors.Is(err, ErrLeaseFenced) {
		t.Fatalf("wrong token renewal = %v", err)
	}
	replaced, err := directory.Renew(
		ctx,
		LeaseCredential{ID: first.ID, Token: first.Token},
		4*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.ID == first.ID || replaced.Epoch != first.Epoch {
		t.Fatalf("replacement lease = %#v, first = %#v", replaced, first)
	}
	afterRenew, err := directory.List(
		ctx,
		DiscoveryQuery{},
		PageRequest{},
	)
	if err != nil {
		t.Fatal(err)
	}

	deleteResult := make(chan ChangePage, 1)
	deleteErrors := make(chan error, 1)
	go func() {
		changes, pollErr := pollEtcdChange(
			ctx,
			directory,
			afterRenew.Revision,
			3*time.Second,
		)
		if pollErr != nil {
			deleteErrors <- pollErr
			return
		}
		deleteResult <- changes
	}()
	time.Sleep(100 * time.Millisecond)
	if err := directory.Unregister(ctx, LeaseCredential{
		ID: replaced.ID, Token: replaced.Token,
	}); err != nil {
		t.Fatal(err)
	}
	var deleted ChangePage
	select {
	case err := <-deleteErrors:
		t.Fatal(err)
	case deleted = <-deleteResult:
	case <-time.After(5 * time.Second):
		t.Fatal("etcd delete poll did not return")
	}
	if len(deleted.Changes) != 1 ||
		deleted.Changes[0].Kind != ChangeDelete ||
		deleted.Changes[0].Instance.InstanceID != "node-a" {
		t.Fatalf("etcd delete changes = %#v", deleted)
	}

	expired, err := pollEtcdChange(
		ctx,
		directory,
		deleted.NextRevision,
		5*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired.Changes) != 1 ||
		expired.Changes[0].Kind != ChangeExpire ||
		expired.Changes[0].Instance.InstanceID != "node-b" {
		t.Fatalf("etcd expiry changes = %#v", expired)
	}
	replacement, err := directory.Register(
		ctx,
		registration("node-a"),
		LeaseOptions{TTL: 5 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Epoch != 2 {
		t.Fatalf("replacement epoch = %d", replacement.Epoch)
	}
	if err := directory.Unregister(ctx, LeaseCredential{
		ID: replacement.ID, Token: replacement.Token,
	}); err != nil {
		t.Fatal(err)
	}

	concrete, ok := directory.(*etcdDirectory)
	if !ok {
		t.Fatalf("etcd backend type = %T", directory)
	}
	revision, err := concrete.client.Get(
		ctx,
		concrete.instancePrefix,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := concrete.client.Compact(
		ctx,
		revision.Header.Revision,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Poll(ctx, ChangePollRequest{
		AfterRevision: 1,
	}); !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("compacted etcd cursor = %v", err)
	}
}

func pollEtcdChange(
	ctx context.Context,
	directory Directory,
	afterRevision uint64,
	wait time.Duration,
) (ChangePage, error) {
	deadline := time.Now().Add(wait)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ChangePage{
				NextRevision: afterRevision,
			}, nil
		}
		page, err := directory.Poll(ctx, ChangePollRequest{
			AfterRevision: afterRevision,
			Wait:          remaining,
		})
		if err != nil || len(page.Changes) != 0 {
			return page, err
		}
		if page.NextRevision == afterRevision {
			return page, nil
		}
		afterRevision = page.NextRevision
	}
}
