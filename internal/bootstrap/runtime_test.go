package bootstrap

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	appconfig "github.com/lincyaw/ag/internal/config"
)

func TestOpenStateBackendDefaultsStateDirectoryToSQLite(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	resolution, err := ResolveStateBackend(appconfig.Config{
		State: appconfig.State{Directory: directory},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Source != StateBackendDefaultSQLite ||
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
			t.Errorf("close SQLite default backend: %v", err)
		}
	})
	parsed, err := url.Parse(backend.String())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "sqlite" ||
		parsed.Path != filepath.Join(directory, defaultSQLiteStateFile) {
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

func TestOpenStateBackendPrefersExistingSQLiteOverLegacyFileStateDirectory(
	t *testing.T,
) {
	t.Parallel()
	directory := t.TempDir()
	backend, err := OpenStateBackend(
		context.Background(),
		appconfig.Config{
			State: appconfig.State{Directory: directory},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(context.Background()); err != nil {
		t.Fatalf("close initial SQLite backend: %v", err)
	}
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
	if resolution.Source != StateBackendDefaultSQLite ||
		resolution.LegacyFileFallback() {
		t.Fatalf("mixed state resolution = %#v", resolution)
	}
	parsed, err := url.Parse(resolution.URI)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "sqlite" ||
		parsed.Path != filepath.Join(directory, defaultSQLiteStateFile) {
		t.Fatalf("mixed state backend = %s", resolution.URI)
	}
}

func TestOpenStateBackendPrefersExistingDuckDBOverSQLite(
	t *testing.T,
) {
	t.Parallel()
	directory := t.TempDir()
	// Create a DuckDB file to simulate pre-existing DuckDB state.
	duckDBPath := filepath.Join(directory, defaultDuckDBStateFile)
	if err := os.WriteFile(duckDBPath, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	// Also create a SQLite file.
	sqlitePath := filepath.Join(directory, defaultSQLiteStateFile)
	if err := os.WriteFile(sqlitePath, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}

	resolution, err := ResolveStateBackend(appconfig.Config{
		State: appconfig.State{Directory: directory},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Source != StateBackendDefaultDuckDB {
		t.Fatalf("expected DuckDB to take priority, got %#v", resolution)
	}
	parsed, err := url.Parse(resolution.URI)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Scheme != "duckdb" ||
		parsed.Path != duckDBPath {
		t.Fatalf("duckdb priority backend = %s", resolution.URI)
	}
}
