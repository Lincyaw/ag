package bash

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultTimeout        = 30 * time.Second
	defaultMaxTimeout     = 5 * time.Minute
	defaultMaxOutputBytes = 1 << 20
)

type Config struct {
	Root           string
	Shell          string
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
	MaxOutputBytes int64
	Environment    []string
}

type Plugin struct {
	config Config
}

func New(config Config) *Plugin { return &Plugin{config: config} }

func (Plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "bash",
		Version:     "1.0.0",
		Description: "bounded shell command execution with explicit environment and cancellation",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.ToolResource("bash")},
	}
}

func (plugin *Plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	runner, err := newRunner(plugin.config)
	if err != nil {
		return err
	}
	return registrar.RegisterTool(tool{runner: runner})
}

type runner struct {
	root           string
	shell          string
	defaultTimeout time.Duration
	maxTimeout     time.Duration
	maxOutputBytes int64
	environment    []string
}

func newRunner(config Config) (*runner, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		root = "."
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve bash root: %w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve bash root symlinks: %w", err)
	}
	info, err := os.Stat(resolvedRoot)
	if err != nil {
		return nil, fmt.Errorf("stat bash root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("bash root %q is not a directory", resolvedRoot)
	}

	shell := strings.TrimSpace(config.Shell)
	if shell == "" {
		shell = "/bin/sh"
	}
	if !filepath.IsAbs(shell) {
		return nil, errors.New("bash shell must be an absolute path")
	}
	resolvedShell, err := filepath.EvalSymlinks(shell)
	if err != nil {
		return nil, fmt.Errorf("resolve bash shell: %w", err)
	}
	shellInfo, err := os.Stat(resolvedShell)
	if err != nil {
		return nil, fmt.Errorf("stat bash shell: %w", err)
	}
	if !shellInfo.Mode().IsRegular() || shellInfo.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("bash shell %q is not executable", resolvedShell)
	}

	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = defaultTimeout
	}
	if config.MaxTimeout == 0 {
		config.MaxTimeout = defaultMaxTimeout
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	if config.DefaultTimeout < time.Millisecond || config.MaxTimeout < config.DefaultTimeout {
		return nil, errors.New("bash timeouts must be positive and max must cover default")
	}
	if config.MaxOutputBytes < 1 {
		return nil, errors.New("bash output limit must be positive")
	}
	environment, err := buildEnvironment(resolvedRoot, config.Environment)
	if err != nil {
		return nil, err
	}
	return &runner{
		root:           resolvedRoot,
		shell:          resolvedShell,
		defaultTimeout: config.DefaultTimeout,
		maxTimeout:     config.MaxTimeout,
		maxOutputBytes: config.MaxOutputBytes,
		environment:    environment,
	}, nil
}

func buildEnvironment(root string, configured []string) ([]string, error) {
	values := map[string]string{
		"HOME": root,
		"LANG": "C.UTF-8",
		"PATH": "/usr/local/bin:/usr/bin:/bin",
	}
	for _, entry := range configured {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" || strings.ContainsRune(name, '\x00') ||
			strings.ContainsRune(name, '=') || strings.ContainsRune(value, '\x00') {
			return nil, fmt.Errorf("invalid bash environment entry %q", entry)
		}
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+values[name])
	}
	return result, nil
}

type tool struct{ runner *runner }

func (tool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "bash",
		Description: "Run a shell command in the configured root with an explicit environment and bounded output.",
		Parameters: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"command": map[string]any{
					"type":        "string",
					"description": "Shell command to execute.",
				},
				"timeout_ms": map[string]any{
					"type":        "integer",
					"description": "Optional timeout in milliseconds, bounded by plugin configuration.",
					"minimum":     1,
				},
			},
			"required": []string{"command"},
		},
	}
}

type arguments struct {
	Command   string `json:"command"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

func (tool tool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	arguments, err := decodeArguments(raw)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	timeout := tool.runner.defaultTimeout
	if arguments.TimeoutMS != 0 {
		if arguments.TimeoutMS < 1 {
			return sdk.ToolResult{}, errors.New("timeout_ms must be positive")
		}
		timeout = time.Duration(arguments.TimeoutMS) * time.Millisecond
	}
	if timeout > tool.runner.maxTimeout {
		return sdk.ToolResult{}, fmt.Errorf("timeout %s exceeds maximum %s", timeout, tool.runner.maxTimeout)
	}
	return tool.runner.run(ctx, arguments.Command, timeout)
}

func decodeArguments(raw json.RawMessage) (arguments, error) {
	var value arguments
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("decode bash arguments: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return value, err
	}
	if strings.TrimSpace(value.Command) == "" {
		return value, errors.New("command is empty")
	}
	return value, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var trailing any
	err := decoder.Decode(&trailing)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing bash arguments: %w", err)
	}
	return errors.New("bash arguments contain multiple JSON values")
}

func (runner *runner) run(
	ctx context.Context,
	command string,
	timeout time.Duration,
) (sdk.ToolResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	runContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stdout := newBoundedBuffer(runner.maxOutputBytes)
	stderr := newBoundedBuffer(runner.maxOutputBytes)
	process := exec.CommandContext(runContext, runner.shell, "-lc", command)
	process.Dir = runner.root
	process.Env = append([]string(nil), runner.environment...)
	process.Stdout = stdout
	process.Stderr = stderr
	process.WaitDelay = 2 * time.Second
	configureProcess(process)

	err := process.Run()
	if runContext.Err() != nil {
		if waitErr := waitProcessGroup(process, time.Second); waitErr != nil {
			return sdk.ToolResult{}, fmt.Errorf("stop bash command: %w", waitErr)
		}
	}
	if parentErr := ctx.Err(); parentErr != nil {
		return sdk.ToolResult{}, parentErr
	}
	if errors.Is(runContext.Err(), context.DeadlineExceeded) {
		return sdk.ToolResult{
			Content: formatResult(stdout, stderr, -1, fmt.Sprintf("command timed out after %s", timeout)),
			IsError: true,
		}, nil
	}
	if err == nil {
		return sdk.ToolResult{Content: formatResult(stdout, stderr, 0, "")}, nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return sdk.ToolResult{
			Content: formatResult(stdout, stderr, exitError.ExitCode(), ""),
			IsError: true,
		}, nil
	}
	return sdk.ToolResult{}, fmt.Errorf("run bash command: %w", err)
}

func formatResult(stdout, stderr *boundedBuffer, exitCode int, failure string) string {
	var output strings.Builder
	writeStream := func(name string, stream *boundedBuffer) {
		content, truncated := stream.snapshot()
		if content == "" && !truncated {
			return
		}
		fmt.Fprintf(&output, "%s:\n%s", name, content)
		if content != "" && !strings.HasSuffix(content, "\n") {
			output.WriteByte('\n')
		}
		if truncated {
			output.WriteString("[output truncated]\n")
		}
	}
	writeStream("stdout", stdout)
	writeStream("stderr", stderr)
	if failure != "" {
		fmt.Fprintf(&output, "error: %s\n", failure)
	}
	fmt.Fprintf(&output, "exit_code: %d", exitCode)
	return output.String()
}

type boundedBuffer struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	limit     int64
	written   int64
	truncated bool
}

func newBoundedBuffer(limit int64) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	originalLength := len(data)
	remaining := buffer.limit - buffer.written
	if remaining > 0 {
		keep := int64(len(data))
		if keep > remaining {
			keep = remaining
		}
		_, _ = buffer.buffer.Write(data[:keep])
		buffer.written += keep
	}
	if int64(originalLength) > remaining {
		buffer.truncated = true
	}
	return originalLength, nil
}

func (buffer *boundedBuffer) snapshot() (string, bool) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	content := buffer.buffer.String()
	if !utf8.ValidString(content) {
		content = strings.ToValidUTF8(content, "�")
	}
	return content, buffer.truncated
}
