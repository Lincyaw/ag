package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxReadBytes  int64 = 1 << 20
	defaultMaxWriteBytes int64 = 1 << 20
	defaultMaxEntries          = 1000
	defaultReadLineLimit       = 250
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
		sdk.ToolResource("search_files"),
	}
	if plugin.config.EnableWrite {
		registers = append(
			registers,
			sdk.ToolResource("write_file"),
			sdk.ToolResource("edit_file"),
		)
	}
	return sdk.Manifest{
		Name:        "file",
		Version:     "1.1.0",
		Description: "root-confined, revision-aware file tools for agent-native read, search, and edit workflows",
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
	if err := registrar.RegisterTool(searchTool{filesystem: filesystem}); err != nil {
		return err
	}
	if plugin.config.EnableWrite {
		if err := registrar.RegisterTool(writeTool{filesystem: filesystem}); err != nil {
			return err
		}
		return registrar.RegisterTool(editTool{filesystem: filesystem})
	}
	return nil
}

type rootedFS struct {
	root          string
	maxReadBytes  int64
	maxWriteBytes int64
	maxEntries    int
	pathLocks     sync.Map
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
		Description: "Read a numbered range from one UTF-8 text file. The result includes a SHA-256 revision for conflict-safe edits.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type": "string", "description": "Relative path to the file.",
				},
				"offset": map[string]any{
					"type": "integer", "minimum": 1,
					"description": "First 1-based line to return; defaults to 1.",
				},
				"limit": map[string]any{
					"type": "integer", "minimum": 1,
					"description": fmt.Sprintf(
						"Maximum lines to return; defaults to %d.",
						defaultReadLineLimit,
					),
				},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

func (tool readTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path   string `json:"path"`
		Offset *int   `json:"offset"`
		Limit  *int   `json:"limit"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	offset := 1
	if arguments.Offset != nil {
		offset = *arguments.Offset
	}
	limit := defaultReadLineLimit
	if arguments.Limit != nil {
		limit = *arguments.Limit
	} else if limit > tool.filesystem.maxEntries {
		limit = tool.filesystem.maxEntries
	}
	if offset < 1 {
		return toolFailure(errors.New("offset must be at least 1")), nil
	}
	if limit < 1 || limit > tool.filesystem.maxEntries {
		return toolFailure(fmt.Errorf(
			"limit must be between 1 and %d", tool.filesystem.maxEntries,
		)), nil
	}
	path, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	data, _, err := tool.filesystem.readText(path)
	if err != nil {
		return toolFailure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	lines := splitTextLines(string(data))
	if len(lines) > 0 && offset > len(lines) {
		return toolFailure(fmt.Errorf(
			"offset %d is past end of file (%d lines)", offset, len(lines),
		)), nil
	}
	if len(lines) == 0 && offset != 1 {
		return toolFailure(errors.New("offset is past end of empty file")), nil
	}
	end := len(lines)
	if candidate := offset - 1 + limit; candidate < end {
		end = candidate
	}
	startIndex := offset - 1
	if len(lines) == 0 {
		startIndex = 0
		end = 0
	}
	return sdk.ToolResult{Content: formatFileRange(
		arguments.Path,
		data,
		lines,
		startIndex,
		end,
	)}, nil
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
		return toolFailure(err), nil
	}
	if strings.TrimSpace(arguments.Path) == "" {
		arguments.Path = "."
	}
	path, err := tool.filesystem.existing(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return toolFailure(err), nil
	}
	if len(entries) > tool.filesystem.maxEntries {
		return toolFailure(fmt.Errorf(
			"directory exceeds %d entry limit", tool.filesystem.maxEntries,
		)), nil
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
		Description: "Atomically create or replace a UTF-8 file. Replacing an existing file requires the SHA-256 revision returned by read_file.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path":    map[string]any{"type": "string"},
				"content": map[string]any{"type": "string"},
				"expected_sha256": map[string]any{
					"type":        "string",
					"description": "Required when replacing an existing file; omit when creating a new file.",
				},
			},
			"required":             []string{"path", "content"},
			"additionalProperties": false,
		},
	}
}

func (tool writeTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path           string  `json:"path"`
		Content        *string `json:"content"`
		ExpectedSHA256 string  `json:"expected_sha256"`
	}
	if err := decodeArguments(raw, &arguments); err != nil {
		return toolFailure(err), nil
	}
	if arguments.Content == nil {
		return toolFailure(errors.New("content is required")), nil
	}
	if int64(len(*arguments.Content)) > tool.filesystem.maxWriteBytes {
		return toolFailure(fmt.Errorf(
			"content exceeds %d byte write limit", tool.filesystem.maxWriteBytes,
		)), nil
	}
	if !utf8.ValidString(*arguments.Content) {
		return toolFailure(errors.New("content is not valid UTF-8 text")), nil
	}
	target, err := tool.filesystem.writable(arguments.Path)
	if err != nil {
		return toolFailure(err), nil
	}
	expectedRevision := strings.TrimSpace(arguments.ExpectedSHA256)
	if expectedRevision != "" && !isSHA256Revision(expectedRevision) {
		return toolFailure(errors.New(
			"expected_sha256 must be a 64-character hexadecimal SHA-256 revision",
		)), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	unlock := tool.filesystem.lockPath(target)
	defer unlock()

	mode := os.FileMode(0o600)
	existing, info, readErr := tool.filesystem.readText(target)
	switch {
	case readErr == nil:
		mode = info.Mode().Perm()
		actualRevision := fileRevision(existing)
		if expectedRevision == "" {
			return toolFailure(errors.New(
				"expected_sha256 is required when replacing an existing file; call read_file first",
			)), nil
		}
		if !strings.EqualFold(expectedRevision, actualRevision) {
			return toolFailure(errors.New(
				"stale file revision; call read_file again before overwriting",
			)), nil
		}
	case errors.Is(readErr, os.ErrNotExist):
		if expectedRevision != "" {
			return toolFailure(errors.New(
				"file no longer exists; omit expected_sha256 to create it",
			)), nil
		}
	default:
		return toolFailure(readErr), nil
	}
	if err := tool.filesystem.atomicWrite(ctx, target, []byte(*arguments.Content), mode); err != nil {
		return sdk.ToolResult{}, err
	}
	revision := fileRevision([]byte(*arguments.Content))
	return sdk.ToolResult{Content: fmt.Sprintf(
		"wrote: %q\nbytes: %d\nsha256: %s",
		cleanDisplayPath(arguments.Path),
		len(*arguments.Content),
		revision,
	)}, nil
}

func (filesystem *rootedFS) readText(path string) ([]byte, os.FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, filesystem.maxReadBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if int64(len(data)) > filesystem.maxReadBytes {
		return nil, nil, fmt.Errorf("file exceeds %d byte read limit", filesystem.maxReadBytes)
	}
	if !utf8.Valid(data) {
		return nil, nil, errors.New("file is not valid UTF-8 text")
	}
	return data, info, nil
}

func (filesystem *rootedFS) lockPath(path string) func() {
	value, _ := filesystem.pathLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func (filesystem *rootedFS) atomicWrite(
	ctx context.Context,
	target string,
	data []byte,
	mode os.FileMode,
) error {
	temporary, err := os.CreateTemp(filepath.Dir(target), ".agentm-file-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, target); err != nil {
		return err
	}
	removeTemporary = false
	directory, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return err
	}
	written, err := os.ReadFile(target)
	if err != nil {
		return fmt.Errorf("verify write: %w", err)
	}
	if !bytes.Equal(written, data) {
		return errors.New("verify write: on-disk content differs")
	}
	return nil
}

func formatFileRange(
	path string,
	data []byte,
	lines []string,
	start int,
	end int,
) string {
	var output strings.Builder
	fmt.Fprintf(&output, "file: %q\n", cleanDisplayPath(path))
	fmt.Fprintf(&output, "bytes: %d\n", len(data))
	if len(lines) == 0 {
		output.WriteString("lines: 0-0 of 0\n")
	} else {
		fmt.Fprintf(&output, "lines: %d-%d of %d\n", start+1, end, len(lines))
	}
	fmt.Fprintf(&output, "sha256: %s\n---", fileRevision(data))
	for index := start; index < end; index++ {
		fmt.Fprintf(&output, "\n%d\t%s", index+1, lines[index])
	}
	return output.String()
}

func splitTextLines(content string) []string {
	if content == "" {
		return nil
	}
	lines := strings.Split(content, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for index := range lines {
		lines[index] = strings.TrimSuffix(lines[index], "\r")
	}
	return lines
}

func fileRevision(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

func isSHA256Revision(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') &&
			!(character >= 'a' && character <= 'f') &&
			!(character >= 'A' && character <= 'F') {
			return false
		}
	}
	return true
}

func cleanDisplayPath(value string) string {
	return filepath.ToSlash(filepath.Clean(strings.TrimSpace(value)))
}

func toolFailure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: err.Error(), IsError: true}
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
