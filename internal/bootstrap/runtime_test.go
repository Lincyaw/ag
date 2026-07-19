package bootstrap

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	appconfig "github.com/lincyaw/ag/internal/config"
)

func TestOpenStateBackendDefaultsStateDirectoryToDuckDB(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	resolution, err := ResolveStateBackend(appconfig.Config{
		State: appconfig.State{Directory: directory},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Source != StateBackendDefaultDuckDB ||
		resolution.LegacyFileFallback() {
		t.Fatalf("default resolution = %#v", resolution)
	}
	backend, err := OpenStateBackend(
		context.Background(),
		appconfig.Config{
			State: appconfig.State{Directory: directory},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close DuckDB default backend: %v", err)
		}
	})
	parsed, err := url.Parse(backend.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "duckdb" ||
		parsed.Path != filepath.Join(directory, defaultDuckDBStateFile) {
		t.Fatalf("default backend = %s", backend.String())
	}
	if capabilities := backend.Capabilities(); !capabilities.AtomicState {
		t.Fatalf("default capabilities = %#v", capabilities)
	}
}

func TestOpenStateBackendPreservesLegacyFileStateDirectory(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	trajectoryDirectory := filepath.Join(directory, "trajectories")
	if err := os.MkdirAll(trajectoryDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(trajectoryDirectory, "legacy.json"),
		[]byte(`{}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	resolution, err := ResolveStateBackend(appconfig.Config{
		State: appconfig.State{Directory: directory},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Source != StateBackendLegacyFileFallback ||
		!resolution.LegacyFileFallback() {
		t.Fatalf("legacy resolution = %#v", resolution)
	}
	backend, err := OpenStateBackend(
		context.Background(),
		appconfig.Config{
			State: appconfig.State{Directory: directory},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close legacy file backend: %v", err)
		}
	})
	parsed, err := url.Parse(backend.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "file" || parsed.Path != directory {
		t.Fatalf("legacy backend = %s", backend.String())
	}
}
