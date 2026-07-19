package gateway

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
)

func TestDuckDBSessionStateFactoryUsesSessionScopedDatabase(t *testing.T) {
	factory, err := NewDuckDBSessionStateFactory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend, err := factory.Open(context.Background(), Session{ID: "session-one"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})
	parsed, err := url.Parse(backend.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "duckdb" ||
		filepath.Base(parsed.Path) != "session-one.duckdb" ||
		parsed.Query().Get("namespace") != "session-one" {
		t.Fatalf("backend URI = %s", backend.String())
	}
	if capabilities := backend.Capabilities(); !capabilities.AtomicState {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}
