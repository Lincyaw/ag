package bash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

func TestBashToolExecutesInRootWithExplicitEnvironment(t *testing.T) {
	root := t.TempDir()
	t.Setenv("AGENTM_UNEXPOSED_SECRET", "do-not-leak")
	runner := mustRunner(t, Config{
		Root:        root,
		Environment: []string{"EXPLICIT_VALUE=visible"},
	})
	result := call(t, runner, map[string]any{
		"command": `printf 'cwd=%s\nsecret=%s\nexplicit=%s\n' "$PWD" "${AGENTM_UNEXPOSED_SECRET-unset}" "$EXPLICIT_VALUE"`,
	})
	if result.IsError {
		t.Fatalf("command unexpectedly failed: %s", result.Content)
	}
	for _, expected := range []string{
		"cwd=" + runner.root,
		"secret=unset",
		"explicit=visible",
		"exit_code: 0",
	} {
		if !strings.Contains(result.Content, expected) {
			t.Fatalf("result %q does not contain %q", result.Content, expected)
		}
	}
}

func TestBashToolReportsStderrAndExitStatus(t *testing.T) {
	runner := mustRunner(t, Config{Root: t.TempDir()})
	result := call(t, runner, map[string]any{
		"command": `printf 'before\n'; printf 'broken\n' >&2; exit 17`,
	})
	if !result.IsError {
		t.Fatalf("non-zero command should be a tool error: %s", result.Content)
	}
	for _, expected := range []string{"stdout:\nbefore", "stderr:\nbroken", "exit_code: 17"} {
		if !strings.Contains(result.Content, expected) {
			t.Fatalf("result %q does not contain %q", result.Content, expected)
		}
	}
}

func TestBashToolTimeoutKillsProcessGroup(t *testing.T) {
	runner := mustRunner(t, Config{
		Root:           t.TempDir(),
		DefaultTimeout: 100 * time.Millisecond,
		MaxTimeout:     time.Second,
	})
	started := time.Now()
	result := call(t, runner, map[string]any{
		"command": `sleep 30 & child=$!; printf '%s\n' "$child"; wait`,
	})
	if !result.IsError || !strings.Contains(result.Content, "timed out") {
		t.Fatalf("expected timeout result, got %+v", result)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
	lines := strings.Split(result.Content, "\n")
	if len(lines) < 2 {
		t.Fatalf("missing child pid in %q", result.Content)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(lines[1]))
	if err != nil {
		t.Fatalf("parse child pid from %q: %v", result.Content, err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child process %d still exists after timeout: %v", pid, err)
	}
}

func TestBashToolDrainsButBoundsLargeOutput(t *testing.T) {
	runner := mustRunner(t, Config{
		Root:           t.TempDir(),
		MaxOutputBytes: 128,
	})
	result := call(t, runner, map[string]any{
		"command": `printf '%050000d' 0; printf '%050000d' 0 >&2`,
	})
	if result.IsError {
		t.Fatalf("large-output command failed: %s", result.Content)
	}
	if count := strings.Count(result.Content, "[output truncated]"); count != 2 {
		t.Fatalf("expected both streams truncated, got %d in %q", count, result.Content)
	}
	if len(result.Content) > 512 {
		t.Fatalf("bounded result unexpectedly large: %d", len(result.Content))
	}
}

func TestBashToolValidationAndCallerCancellation(t *testing.T) {
	runner := mustRunner(t, Config{
		Root:           t.TempDir(),
		DefaultTimeout: 100 * time.Millisecond,
		MaxTimeout:     time.Second,
	})
	testCases := []struct {
		name string
		raw  string
	}{
		{name: "unknown field", raw: `{"command":"true","extra":1}`},
		{name: "empty command", raw: `{"command":" "}`},
		{name: "negative timeout", raw: `{"command":"true","timeout_ms":-1}`},
		{name: "excessive timeout", raw: `{"command":"true","timeout_ms":1001}`},
		{name: "multiple values", raw: `{"command":"true"} {}`},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := (tool{runner: runner}).Call(context.Background(), json.RawMessage(testCase.raw)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (tool{runner: runner}).Call(ctx, json.RawMessage(`{"command":"sleep 30"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller cancellation, got %v", err)
	}
}

func TestBashToolConcurrentCallsAndRuntimeMount(t *testing.T) {
	runner := mustRunner(t, Config{Root: t.TempDir()})
	const workers = 32
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			raw := json.RawMessage(fmt.Sprintf(`{"command":"printf worker-%d"}`, index))
			result, err := (tool{runner: runner}).Call(context.Background(), raw)
			if err != nil {
				errors <- err
				return
			}
			if !strings.Contains(result.Content, fmt.Sprintf("worker-%d", index)) {
				errors <- fmt.Errorf("worker %d got %q", index, result.Content)
			}
		}(index)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}

	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	connection, err := runtime.Mount(context.Background(), sdk.Local(New(Config{Root: t.TempDir()})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Unmount(context.Background())
	catalog := runtime.Catalog()
	if len(catalog.Tools) != 1 || catalog.Tools[0].Name != "bash" {
		t.Fatalf("unexpected catalog: %+v", catalog.Tools)
	}
}

func TestRunnerRejectsUnsafeConfiguration(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, config := range []Config{
		{Root: file},
		{Root: t.TempDir(), Shell: "sh"},
		{Root: t.TempDir(), Shell: file},
		{Root: t.TempDir(), DefaultTimeout: time.Second, MaxTimeout: time.Millisecond},
		{Root: t.TempDir(), MaxOutputBytes: -1},
		{Root: t.TempDir(), Environment: []string{"INVALID"}},
	} {
		if _, err := newRunner(config); err == nil {
			t.Fatalf("expected config rejection: %+v", config)
		}
	}
}

func mustRunner(t *testing.T, config Config) *runner {
	t.Helper()
	runner, err := newRunner(config)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func call(t *testing.T, runner *runner, arguments map[string]any) sdk.ToolResult {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (tool{runner: runner}).Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
