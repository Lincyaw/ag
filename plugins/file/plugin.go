package file

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

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxReadBytes  int64 = 1 << 20
	defaultMaxWriteBytes int64 = 1 << 20
	defaultMaxEntries          = 1000
)

type Config struct {
	Root          string
	MaxReadBytes  int64
	MaxWriteBytes int64
	MaxEntries    int
	EnableWrite   bool
}

type Plugin struct {
	config Config
}

func New(config Config) *Plugin { return &Plugin{config: config} }

func (plugin *Plugin) Manifest() sdk.Manifest {
	registers := []string{
		sdk.ToolResource("read_file"),
		sdk.ToolResource("list_files"),
	}
	if plugin.config.EnableWrite {
		registers = append(registers, sdk.ToolResource("write_file"))
	}
	return sdk.Manifest{
		Name:        "file",
		Version:     "1.0.0",
		Description: "root-confined file read, list, and optional atomic write tools",
		APIVersion:  sdk.APIVersion,
		Registers:   registers,
	}
}

func (plugin *Plugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	filesystem, err := newRootedFS(plugin.config)
	if err != nil {
		return err
	}
	if err := registrar.RegisterTool(readTool{filesystem: filesystem}); err != nil {
		return err
	}
	if err := registrar.RegisterTool(listTool{filesystem: filesystem}); err != nil {
		return err
	}
	if plugin.config.EnableWrite {
		return registrar.RegisterTool(writeTool{filesystem: filesystem})
	}
	return nil
}

type rootedFS struct {
	root          string
	maxReadBytes  int64
	maxWriteBytes int64
	maxEntries    int
}

func newRootedFS(config Config) (*rootedFS, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve file root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve file root symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("stat file root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("file root %q is not a directory", resolved)
	}
	if config.MaxReadBytes == 0 {
		config.MaxReadBytes = defaultMaxReadBytes
	}
	if config.MaxWriteBytes == 0 {
		config.MaxWriteBytes = defaultMaxWriteBytes
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultMaxEntries
	}
	if config.MaxReadBytes < 1 || config.MaxWriteBytes < 1 || config.MaxEntries < 1 {
		return nil, errors.New("file limits must be positive")
	}
	return &rootedFS{
		root:          resolved,
		maxReadBytes:  config.MaxReadBytes,
		maxWriteBytes: config.MaxWriteBytes,
		maxEntries:    config.MaxEntries,
	}, nil
}

func (filesystem *rootedFS) existing(relative string) (string, error) {
	candidate, err := filesystem.candidate(relative)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	if err := filesystem.confined(resolved); err != nil {
		return "", err
	}
	return resolved, nil
}

func (filesystem *rootedFS) writable(relative string) (string, error) {
	candidate, err := filesystem.candidate(relative)
	if err != nil {
		return "", err
	}
	base := filepath.Base(candidate)
	if base == "." || base == string(filepath.Separator) {
		return "", errors.New("file path has no basename")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(candidate))
	if err != nil {
		return "", fmt.Errorf("resolve destination parent: %w", err)
	}
	if err := filesystem.confined(parent); err != nil {
		return "", err
	}
	info, err := os.Stat(parent)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("destination parent is not a directory")
	}
	target := filepath.Join(parent, base)
	if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("refusing to replace a symbolic link")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return target, nil
}

func (filesystem *rootedFS) candidate(relative string) (string, error) {
	value := strings.TrimSpace(relative)
	if value == "" {
		return "", errors.New("path is empty")
	}
	if filepath.IsAbs(value) {
		return "", errors.New("path must be relative to the file root")
	}
	clean := filepath.Clean(value)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes the file root")
	}
	candidate := filepath.Join(filesystem.root, clean)
	if err := filesystem.confined(candidate); err != nil {
		return "", err
	}
	return candidate, nil
}

func (filesystem *rootedFS) confined(path string) error {
	relative, err := filepath.Rel(filesystem.root, path)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("path escapes the file root")
	}
	return nil
}

type readTool struct{ filesystem *rootedFS }

func (readTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "read_file",
		Description: "Read one UTF-8 text file relative to the configured root.",
		Parameters:  pathSchema("Relative path to the file."),
	}
}

func (tool readTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path string `json:"path"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	path, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	if !info.Mode().IsRegular() {
		return sdk.ToolResult{}, errors.New("path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, tool.filesystem.maxReadBytes+1))
	if err != nil {
		return sdk.ToolResult{}, err
	}
	if int64(len(data)) > tool.filesystem.maxReadBytes {
		return sdk.ToolResult{}, fmt.Errorf("file exceeds %d byte read limit", tool.filesystem.maxReadBytes)
	}
	if !utf8.Valid(data) {
		return sdk.ToolResult{}, errors.New("file is not valid UTF-8 text")
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{Content: string(data)}, nil
}

type listTool struct{ filesystem *rootedFS }

func (listTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "list_files",
		Description: "List direct children of a directory relative to the configured root.",
		Parameters:  pathSchema("Relative directory path; use . for the root."),
	}
}

func (tool listTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path string `json:"path"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	if strings.TrimSpace(arguments.Path) == "" {
		arguments.Path = "."
	}
	path, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	if len(entries) > tool.filesystem.maxEntries {
		return sdk.ToolResult{}, fmt.Errorf("directory exceeds %d entry limit", tool.filesystem.maxEntries)
	}
	var output strings.Builder
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return sdk.ToolResult{}, err
		}
		kind := "file"
		name := entry.Name()
		if entry.IsDir() {
			kind = "dir"
			name += "/"
		} else if entry.Type()&os.ModeSymlink != 0 {
			kind = "symlink"
		}
		fmt.Fprintf(&output, "%s\t%s\n", kind, name)
	}
	return sdk.ToolResult{Content: strings.TrimSuffix(output.String(), "\n")}, nil
}

type writeTool struct{ filesystem *rootedFS }

func (writeTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "write_file",
		Description: "Atomically write one UTF-8 text file relative to the configured root.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
			},
			"required":             []string{"path", "content"},
			"additionalProperties": false,
		},
	}
}

func (tool writeTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return sdk.ToolResult{}, err
	}
	if int64(len(arguments.Content)) > tool.filesystem.maxWriteBytes {
		return sdk.ToolResult{}, fmt.Errorf("content exceeds %d byte write limit", tool.filesystem.maxWriteBytes)
	}
	if !utf8.ValidString(arguments.Content) {
		return sdk.ToolResult{}, errors.New("content is not valid UTF-8 text")
	}
	target, err := tool.filesystem.writable(arguments.Path)
	if err != nil {
		return sdk.ToolResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".agentm-file-*.tmp")
	if err != nil {
		return sdk.ToolResult{}, err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return sdk.ToolResult{}, err
	}
	if _, err := io.WriteString(temporary, arguments.Content); err != nil {
		_ = temporary.Close()
		return sdk.ToolResult{}, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return sdk.ToolResult{}, err
	}
	if err := temporary.Close(); err != nil {
		return sdk.ToolResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	if err := os.Rename(temporaryPath, target); err != nil {
		return sdk.ToolResult{}, err
	}
	removeTemporary = false
	directory, err := os.Open(filepath.Dir(target))
	if err != nil {
		return sdk.ToolResult{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return sdk.ToolResult{}, err
	}
	return sdk.ToolResult{Content: fmt.Sprintf("wrote %d bytes to %s", len(arguments.Content), arguments.Path)}, nil
}

func pathSchema(description string) map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{"type": "string", "description": description},
		},
		"required":             []string{"path"},
		"additionalProperties": false,
	}
}

func decodeArguments(raw json.RawMessage, target any) error {
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
