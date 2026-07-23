package manager

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestManagerStartsOnceAndReusesHealthyProcessConcurrently(t *testing.T) {
	directory := t.TempDir()
	const target = "grpc://127.0.0.1:19001"
	var launches atomic.Int32
	manager, err := New(Config{
		Directory: directory, RuntimeConfig: []byte(`{"agent":{}}`),
		Probe: func(_ context.Context, got string) error {
			if got != target {
				return errors.New("wrong target")
			}
			return nil
		},
		Launcher: func(_ string, readyPath string, _ string) (<-chan error, error) {
			launches.Add(1)
			if err := writeTestReady(readyPath, target); err != nil {
				return nil, err
			}
			return make(chan error), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	results := make(chan Ready, callers)
	errorsFound := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			ready, err := manager.Ensure(t.Context())
			results <- ready
			errorsFound <- err
		}()
	}
	group.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	for ready := range results {
		if ready.Target != target {
			t.Fatalf("target = %q", ready.Target)
		}
	}
	if launches.Load() != 1 {
		t.Fatalf("launches = %d, want 1", launches.Load())
	}
	info, err := os.Stat(filepath.Join(directory, DirectoryName, ConfigName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("runtime config permissions = %o", info.Mode().Perm())
	}
}

func TestManagerReplacesStaleReadiness(t *testing.T) {
	directory := t.TempDir()
	managed := filepath.Join(directory, DirectoryName)
	if err := os.MkdirAll(managed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeTestReady(
		filepath.Join(managed, ReadyName), "grpc://127.0.0.1:1",
	); err != nil {
		t.Fatal(err)
	}
	const healthy = "grpc://127.0.0.1:19002"
	var launches atomic.Int32
	manager, err := New(Config{
		Directory: directory, RuntimeConfig: []byte(`{}`),
		Probe: func(_ context.Context, target string) error {
			if target == healthy {
				return nil
			}
			return errors.New("stale")
		},
		Launcher: func(_ string, readyPath string, _ string) (<-chan error, error) {
			launches.Add(1)
			if err := writeTestReady(readyPath, healthy); err != nil {
				return nil, err
			}
			return make(chan error), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := manager.Ensure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Target != healthy || launches.Load() != 1 {
		t.Fatalf("ready=%#v launches=%d", ready, launches.Load())
	}
}

func TestManagerReplacesHealthyGatewayFromDifferentExecutable(t *testing.T) {
	directory := t.TempDir()
	managed := filepath.Join(directory, DirectoryName)
	if err := os.MkdirAll(managed, 0o700); err != nil {
		t.Fatal(err)
	}
	const (
		staleTarget = "grpc://127.0.0.1:19020"
		freshTarget = "grpc://127.0.0.1:19021"
	)
	readyPath := filepath.Join(managed, ReadyName)
	raw, err := json.Marshal(Ready{
		Target: staleTarget, Directory: directory, PID: 4242,
		Executable: "/tmp/old-ag", ExecutableSHA256: strings.Repeat("0", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	manager, err := New(Config{
		Directory: directory, RuntimeConfig: []byte(`{}`),
		Probe: func(context.Context, string) error {
			return nil
		},
		Stopper: func(_ context.Context, ready Ready) error {
			if ready.PID != 4242 || ready.Executable != "/tmp/old-ag" {
				t.Fatalf("stopped readiness = %#v", ready)
			}
			stopped.Store(true)
			return nil
		},
		Launcher: func(_ string, readyPath string, _ string) (<-chan error, error) {
			if !stopped.Load() {
				t.Fatal("replacement launched before stale executable stopped")
			}
			if err := writeTestReady(readyPath, freshTarget); err != nil {
				return nil, err
			}
			return make(chan error), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := manager.Ensure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Target != freshTarget || !stopped.Load() {
		t.Fatalf("ready=%#v stopped=%v", ready, stopped.Load())
	}
}

func TestManagerStopsRecordedInstanceBeforeReplacement(t *testing.T) {
	directory := t.TempDir()
	managed := filepath.Join(directory, DirectoryName)
	if err := os.MkdirAll(managed, 0o700); err != nil {
		t.Fatal(err)
	}
	const (
		staleTarget = "grpc://127.0.0.1:19010"
		freshTarget = "grpc://127.0.0.1:19011"
	)
	readyPath := filepath.Join(managed, ReadyName)
	raw, err := json.Marshal(Ready{
		Target: staleTarget, Directory: directory, PID: 4242,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Bool
	manager, err := New(Config{
		Directory: directory, RuntimeConfig: []byte(`{}`),
		Probe: func(_ context.Context, target string) error {
			if target == freshTarget {
				return nil
			}
			return errors.New("stale")
		},
		Stopper: func(_ context.Context, ready Ready) error {
			if ready.PID != 4242 || ready.Directory != directory {
				t.Fatalf("stopped readiness = %#v", ready)
			}
			stopped.Store(true)
			return nil
		},
		Launcher: func(_ string, readyPath string, _ string) (<-chan error, error) {
			if !stopped.Load() {
				t.Fatal("replacement launched before stale process stopped")
			}
			if err := writeTestReady(readyPath, freshTarget); err != nil {
				return nil, err
			}
			return make(chan error), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := manager.Ensure(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Target != freshTarget || !stopped.Load() {
		t.Fatalf("ready=%#v stopped=%v", ready, stopped.Load())
	}
}

func TestManagerIncludesChildDiagnostics(t *testing.T) {
	manager, err := New(Config{
		Directory: t.TempDir(), RuntimeConfig: []byte(`{}`),
		Probe: func(context.Context, string) error { return errors.New("down") },
		Launcher: func(_ string, _ string, logPath string) (<-chan error, error) {
			if err := os.WriteFile(logPath, []byte("bind failed\n"), 0o600); err != nil {
				return nil, err
			}
			done := make(chan error, 1)
			done <- errors.New("exit status 1")
			close(done)
			return done, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Ensure(t.Context())
	if err == nil || !strings.Contains(err.Error(), "bind failed") {
		t.Fatalf("startup error = %v", err)
	}
}

func TestChildRequestUsesPrivateEnvironmentProtocol(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv(ChildModeEnvironment, "1")
	t.Setenv(ChildConfigEnvironment, configPath)
	got, active, err := ChildRequestFromEnvironment()
	if err != nil || !active || got != configPath {
		t.Fatalf("path=%q active=%v error=%v", got, active, err)
	}
}

func writeTestReady(path string, target string) error {
	executable, executableSHA256, err := CurrentExecutableIdentity()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(Ready{
		Target: target, PID: os.Getpid(),
		Executable: executable, ExecutableSHA256: executableSHA256,
	})
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o600)
}
