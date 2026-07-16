package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/agent"
)

const defaultMaxReadBytes int64 = 64 << 10

type Config struct {
	Root         string
	MaxReadBytes int64
}

type plugin struct {
	config Config
}

func New(config Config) agent.Plugin {
	return plugin{config: config}
}

func (plugin) Name() string {
	return "workspace"
}

func (p plugin) Install(host agent.Host) error {
	root, err := filepath.Abs(p.config.Root)
	if err != nil {
		return fmt.Errorf("resolve workspace root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve workspace root symlinks: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("stat workspace root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("workspace root %q is not a directory", root)
	}
	if p.config.MaxReadBytes == 0 {
		p.config.MaxReadBytes = defaultMaxReadBytes
	}
	if p.config.MaxReadBytes < 1 {
		return errors.New("workspace max read bytes must be positive")
	}

	workspace := &rootedWorkspace{
		root:         root,
		maxReadBytes: p.config.MaxReadBytes,
	}
	if err := host.RegisterTool(readFileTool{workspace: workspace}); err != nil {
		return err
	}
	return host.RegisterTool(listFilesTool{workspace: workspace})
}

type rootedWorkspace struct {
	root         string
	maxReadBytes int64
}

func (w *rootedWorkspace) resolve(relative string) (string, error) {
	if filepath.IsAbs(relative) {
		return "", errors.New("path must be relative to the workspace")
	}
	candidate := filepath.Join(w.root, filepath.Clean(relative))
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(w.root, resolved)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes the workspace")
	}
	return resolved, nil
}

type readFileTool struct {
	workspace *rootedWorkspace
}

func (readFileTool) Spec() agent.ToolSpec {
	return agent.ToolSpec{
		Name:        "read_file",
		Description: "Read a UTF-8 text file relative to the workspace root.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative path to the file.",
				},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

func (tool readFileTool) Call(
	_ context.Context,
	raw json.RawMessage,
) (string, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return "", err
	}
	if strings.TrimSpace(args.Path) == "" {
		return "", errors.New("path is empty")
	}
	path, err := tool.workspace.resolve(args.Path)
	if err != nil {
		return "", err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, tool.workspace.maxReadBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > tool.workspace.maxReadBytes {
		return "", fmt.Errorf(
			"file exceeds %d byte read limit",
			tool.workspace.maxReadBytes,
		)
	}
	if !utf8.Valid(data) {
		return "", errors.New("file is not valid UTF-8 text")
	}
	return string(data), nil
}

type listFilesTool struct {
	workspace *rootedWorkspace
}

func (listFilesTool) Spec() agent.ToolSpec {
	return agent.ToolSpec{
		Name:        "list_files",
		Description: "List direct children of a directory relative to the workspace root.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative directory path; use . for the root.",
				},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

func (tool listFilesTool) Call(
	_ context.Context,
	raw json.RawMessage,
) (string, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return "", err
	}
	if strings.TrimSpace(args.Path) == "" {
		args.Path = "."
	}
	path, err := tool.workspace.resolve(args.Path)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	var output strings.Builder
	for _, entry := range entries {
		kind := "file"
		name := entry.Name()
		if entry.IsDir() {
			kind = "dir"
			name += "/"
		}
		fmt.Fprintf(&output, "%s\t%s\n", kind, name)
	}
	return strings.TrimSuffix(output.String(), "\n"), nil
}

func decodeArgs(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode tool arguments: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("tool arguments contain trailing JSON")
	}
	return nil
}
