package git

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

const defaultMaxOutputBytes int64 = 64 << 10

type Config struct {
	Root           string
	MaxOutputBytes int64
}

type plugin struct{ config Config }

func New(config Config) sdk.Plugin { return plugin{config: config} }

func (plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "git",
		Version:     "0.1.0",
		Description: "read-only git repository context tools for status and diff-aware workflows",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.ToolResource("git_status")},
	}
}

func (plugin plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	repository, err := newRepository(plugin.config)
	if err != nil {
		return err
	}
	return registrar.RegisterTool(statusTool{repository: repository})
}

type repository struct {
	root           string
	maxOutputBytes int64
}

func newRepository(config Config) (*repository, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve git root: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return nil, fmt.Errorf("stat git root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("git root %q is not a directory", absolute)
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	if config.MaxOutputBytes < 1 {
		return nil, errors.New("git max output bytes must be positive")
	}
	return &repository{root: absolute, maxOutputBytes: config.MaxOutputBytes}, nil
}

type statusTool struct{ repository *repository }

func (statusTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "git_status",
		Description: "Return concise read-only Git repository context: root, branch, HEAD, upstream, and porcelain status.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}
}

func (tool statusTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct{}
	if len(bytes.TrimSpace(raw)) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&arguments); err != nil {
			return toolFailure(err), nil
		}
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	root, err := tool.repository.git(ctx, "rev-parse", "--show-toplevel")
	if err != nil {
		return toolFailure(err), nil
	}
	branch, _ := tool.repository.git(ctx, "branch", "--show-current")
	head, _ := tool.repository.git(ctx, "rev-parse", "--short", "HEAD")
	upstream, _ := tool.repository.git(ctx, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
	porcelain, err := tool.repository.git(ctx, "status", "--porcelain=v1", "--branch")
	if err != nil {
		return toolFailure(err), nil
	}

	var output strings.Builder
	fmt.Fprintf(&output, "root: %s\n", strings.TrimSpace(root))
	if strings.TrimSpace(branch) != "" {
		fmt.Fprintf(&output, "branch: %s\n", strings.TrimSpace(branch))
	}
	if strings.TrimSpace(head) != "" {
		fmt.Fprintf(&output, "head: %s\n", strings.TrimSpace(head))
	}
	if strings.TrimSpace(upstream) != "" {
		fmt.Fprintf(&output, "upstream: %s\n", strings.TrimSpace(upstream))
	}
	output.WriteString("---\n")
	if strings.TrimSpace(porcelain) == "" {
		output.WriteString("clean")
	} else {
		output.WriteString(strings.TrimRight(porcelain, "\n"))
	}
	return sdk.ToolResult{Content: output.String()}, nil
}

func (repository *repository) git(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", append([]string{"-C", repository.root}, args...)...)
	var stdout, stderr limitedBuffer
	stdout.limit = repository.maxOutputBytes
	stderr.limit = repository.maxOutputBytes
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", errors.New(message)
	}
	if stdout.truncated {
		return stdout.String(), fmt.Errorf("git output exceeded %d byte limit", repository.maxOutputBytes)
	}
	return stdout.String(), nil
}

type limitedBuffer struct {
	buffer    bytes.Buffer
	limit     int64
	truncated bool
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	if buffer.limit <= 0 {
		buffer.truncated = true
		return len(data), nil
	}
	remaining := buffer.limit - int64(buffer.buffer.Len())
	if remaining <= 0 {
		buffer.truncated = true
		return len(data), nil
	}
	if int64(len(data)) > remaining {
		_, _ = buffer.buffer.Write(data[:remaining])
		buffer.truncated = true
		return len(data), nil
	}
	_, _ = buffer.buffer.Write(data)
	return len(data), nil
}

func (buffer *limitedBuffer) String() string { return buffer.buffer.String() }

func toolFailure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: err.Error(), IsError: true}
}
