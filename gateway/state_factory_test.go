package gateway

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
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
	if capabilities := sdk.InspectStorageCapabilities(backend); !capabilities.AtomicState {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestStorageSessionStateFactoryScopesConfiguredBackend(t *testing.T) {
	root := t.TempDir()
	configured := (&url.URL{
		Scheme: "file",
		Path:   root,
		RawQuery: url.Values{
			"namespace": {"replica"},
		}.Encode(),
	}).String()
	factory, err := NewStorageSessionStateFactory(configured)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := factory.Open(t.Context(), Session{ID: "session-one"})
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
	if parsed.Scheme != "file" ||
		filepath.Clean(filepath.Dir(filepath.Dir(parsed.Path))) != filepath.Clean(root) ||
		parsed.Query().Get("namespace") != "replica-session-one" {
		t.Fatalf("backend URI = %s", backend.String())
	}
}

func TestSessionStateBackendURIPreservesPostgresConnectionOptions(t *testing.T) {
	configured := "postgresql://agent:secret@db.internal/agentm" +
		"?sslmode=require&namespace=manager"
	resolved, err := sessionStateBackendURI(configured, "trajectory-one")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "postgresql" ||
		parsed.Host != "db.internal" ||
		parsed.Path != "/agentm" ||
		parsed.Query().Get("sslmode") != "require" ||
		parsed.Query().Get("namespace") != "manager-trajectory-one" {
		t.Fatalf("resolved URI = %s", parsed.Redacted())
	}
}

func TestStorageSessionStateFactoryPersistsPostgresTrajectoryNamespace(
	t *testing.T,
) {
	rawURI := strings.TrimSpace(os.Getenv("AG_TEST_POSTGRES_DSN"))
	if rawURI == "" {
		t.Skip("set AG_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	baseNamespace := "gateway-smoke-" + sdk.NewID()
	query := parsed.Query()
	query.Set("namespace", baseNamespace)
	parsed.RawQuery = query.Encode()
	factory, err := NewStorageSessionStateFactory(parsed.String())
	if err != nil {
		t.Fatal(err)
	}

	first, err := factory.Open(t.Context(), Session{ID: "trajectory-one"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Namespace() != baseNamespace+"-trajectory-one" {
		t.Fatalf("namespace = %q", first.Namespace())
	}
	if err := first.Trajectories().Create(
		t.Context(),
		sdk.Trajectory{ID: "durable-trajectory"},
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := factory.Open(t.Context(), Session{ID: "trajectory-one"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL backend: %v", err)
		}
	})
	if _, err := second.Trajectories().LoadMetadata(
		t.Context(),
		"durable-trajectory",
	); err != nil {
		t.Fatal(err)
	}
	if parsed.User != nil {
		password, hasPassword := parsed.User.Password()
		displayURI, parseErr := url.Parse(second.String())
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if displayURI.User != nil {
			displayPassword, displayed := displayURI.User.Password()
			if hasPassword && displayed && password != "" &&
				displayPassword == password {
				t.Fatal("backend display URI exposed PostgreSQL password")
			}
		}
	}
}
