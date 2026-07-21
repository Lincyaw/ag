package gateway

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestSessionStoreContract(t *testing.T) {
	factories := map[string]func(*testing.T) SessionStore{
		"memory": func(*testing.T) SessionStore {
			return NewMemorySessionStore()
		},
		"file": func(t *testing.T) SessionStore {
			store, err := NewFileSessionStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"gorm-sqlite": func(t *testing.T) SessionStore {
			store, err := NewGORMSessionStore(
				t.Context(), gormEventStoreTestURI(t, "sessions"),
			)
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			testSessionStoreContract(t, factory(t))
		})
	}
}

func TestFileSessionStorePersistsPrivateRuntimeConfig(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileSessionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	session := testSession("runtime-profile")
	session.RuntimeConfig = []byte(`{"openai":{"enabled":false}}`)
	if _, err := store.Create(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileSessionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	loaded, err := reopened.Get(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.RuntimeConfig) != string(session.RuntimeConfig) {
		t.Fatalf("runtime config = %s", loaded.RuntimeConfig)
	}
}

func testSessionStoreContract(t *testing.T, store SessionStore) {
	t.Helper()
	ctx := t.Context()
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	created, err := store.Create(ctx, testSession("session-b"))
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != 1 || created.CreatedAt.IsZero() {
		t.Fatalf("created session = %#v", created)
	}
	created.Plugins[0].Labels["zone"] = "mutated"
	loaded, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Plugins[0].Labels["zone"] != "local" {
		t.Fatalf("stored labels were mutated: %#v", loaded.Plugins)
	}
	if _, err := store.Create(ctx, testSession(created.ID)); !errors.Is(
		err,
		ErrSessionExists,
	) {
		t.Fatalf("duplicate create error = %v", err)
	}
	loaded.System = "updated"
	updated, err := store.Save(ctx, loaded, loaded.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision != 2 || updated.System != "updated" {
		t.Fatalf("updated session = %#v", updated)
	}
	if _, err := store.Save(ctx, loaded, 1); !errors.Is(
		err,
		ErrSessionConflict,
	) {
		t.Fatalf("stale save error = %v", err)
	}
	if _, err := store.Create(ctx, testSession("session-a")); err != nil {
		t.Fatal(err)
	}
	page, err := store.List(ctx, sdk.PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "session-a" ||
		page.Next != "session-a" {
		t.Fatalf("first page = %#v", page)
	}
	page, err = store.List(ctx, sdk.PageRequest{
		After: page.Next,
		Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "session-b" ||
		page.Next != "" {
		t.Fatalf("second page = %#v", page)
	}
	otherUser := testSession("session-c")
	otherUser.UserID = "user-b"
	if _, err := store.Create(ctx, otherUser); err != nil {
		t.Fatal(err)
	}
	page, err = store.ListByUser(ctx, "user-a", sdk.PageRequest{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "session-a" ||
		page.Next != "session-a" {
		t.Fatalf("first user page = %#v", page)
	}
	page, err = store.ListByUser(ctx, "user-a", sdk.PageRequest{
		After: page.Next,
		Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "session-b" ||
		page.Next != "" {
		t.Fatalf("second user page = %#v", page)
	}
	if err := store.Delete(ctx, updated.ID, updated.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, updated.ID); !errors.Is(
		err,
		ErrSessionNotFound,
	) {
		t.Fatalf("deleted get error = %v", err)
	}
}

func TestFileSessionStorePersistsPrivateState(t *testing.T) {
	ctx := t.Context()
	directory := t.TempDir()
	store, err := NewFileSessionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, testSession("persistent")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileSessionStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if _, err := reopened.Get(ctx, "persistent"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("session state mode = %o", info.Mode().Perm())
	}
}

func TestSessionRevisionDoesNotWrap(t *testing.T) {
	current := testSession("revision-exhausted")
	current.Revision = math.MaxUint64

	_, err := prepareSessionUpdate(
		current,
		current,
		math.MaxUint64,
		time.Now(),
	)
	if !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("revision overflow error = %v", err)
	}
}

func testSession(id string) Session {
	return Session{
		ID:       id,
		UserID:   "user-a",
		Provider: "openai",
		MaxTurns: 8,
		Plugins: []PluginBinding{{
			Namespace:  "default",
			Name:       "file",
			InstanceID: "file-a",
			URI:        "grpc://127.0.0.1:9000",
			Manifest: sdk.Manifest{
				Name:        "file",
				Version:     "1.0.0",
				Description: "test file plugin",
				APIVersion:  sdk.APIVersion,
				Registers:   []string{sdk.ToolResource("read_file")},
			},
			Labels: map[string]string{"zone": "local"},
			Epoch:  1,
		}},
	}
}
