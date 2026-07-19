package hostfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxReadBytes int64 = 1 << 20
	defaultMaxEntries         = 500
	defaultMaxDepth           = 3
	defaultReadLines          = 250
)

type Config struct {
	Roots        []string
	MaxReadBytes int64
	MaxEntries   int
	MaxDepth     int
}

type plugin struct{ config Config }

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "hostfs",
		Version:     "1.0.0",
		Description: "read-only bounded access to configured host filesystem roots outside the workspace",
		APIVersion:  sdk.APIVersion,
		Registers: []string{
			sdk.ToolResource("hostfs_read_file"),
			sdk.ToolResource("hostfs_tree"),
		},
	}
}

func (plugin *plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	filesystem, err := newFS(plugin.config)
	if err != nil {
		return err
	}
	if err := registrar.RegisterTool(readTool{filesystem: filesystem}); err != nil {
		return err
	}
	return registrar.RegisterTool(treeTool{filesystem: filesystem})
}

type hostFS struct {
	roots        []string
	maxReadBytes int64
	maxEntries   int
	maxDepth     int
}

func newFS(config Config) (*hostFS, error) {
	if len(config.Roots) == 0 {
		config.Roots = []string{filesystemRoot()}
	}
	if config.MaxReadBytes == 0 {
		config.MaxReadBytes = defaultMaxReadBytes
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultMaxEntries
	}
	if config.MaxDepth == 0 {
		config.MaxDepth = defaultMaxDepth
	}
	if config.MaxReadBytes < 1 || config.MaxEntries < 1 || config.MaxDepth < 0 {
		return nil, errors.New("hostfs limits are invalid")
	}
	roots := make([]string, 0, len(config.Roots))
	for _, root := range config.Roots {
		resolved, err := resolveRoot(root)
		if err != nil {
			return nil, err
		}
		roots = append(roots, resolved)
	}
	return &hostFS{
		roots:        roots,
		maxReadBytes: config.MaxReadBytes,
		maxEntries:   config.MaxEntries,
		maxDepth:     config.MaxDepth,
	}, nil
}

type readTool struct{ filesystem *hostFS }

func (readTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "hostfs_read_file",
		Description: "Read a numbered range from one UTF-8 text file using an absolute path under a configured hostfs root.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Absolute file path, or a path beginning with ~, under a configured hostfs root.",
				},
				"offset": map[string]any{
					"type":        "integer",
					"minimum":     1,
					"description": "First 1-based line to return; defaults to 1.",
				},
				"limit": map[string]any{
					"type":        "integer",
					"minimum":     1,
					"description": fmt.Sprintf("Maximum lines to return; defaults to %d.", defaultReadLines),
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
	if err := decode(raw, &arguments); err != nil {
		return failure(err), nil
	}
	path, err := tool.filesystem.resolve(arguments.Path)
	if err != nil {
		return failure(err), nil
	}
	offset := 1
	if arguments.Offset != nil {
		offset = *arguments.Offset
	}
	limit := defaultReadLines
	if arguments.Limit != nil {
		limit = *arguments.Limit
	} else if limit > tool.filesystem.maxEntries {
		limit = tool.filesystem.maxEntries
	}
	if offset < 1 {
		return failure(errors.New("offset must be at least 1")), nil
	}
	if limit < 1 || limit > tool.filesystem.maxEntries {
		return failure(fmt.Errorf("limit must be between 1 and %d", tool.filesystem.maxEntries)), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	data, err := tool.filesystem.readText(path)
	if err != nil {
		return failure(err), nil
	}
	lines := splitLines(string(data))
	if len(lines) > 0 && offset > len(lines) {
		return failure(fmt.Errorf("offset %d is past end of file (%d lines)", offset, len(lines))), nil
	}
	if len(lines) == 0 && offset != 1 {
		return failure(errors.New("offset is past end of empty file")), nil
	}
	start := offset - 1
	end := len(lines)
	if candidate := start + limit; candidate < end {
		end = candidate
	}
	if len(lines) == 0 {
		start, end = 0, 0
	}
	return sdk.ToolResult{Content: formatFile(path, data, lines, start, end)}, nil
}

type treeTool struct{ filesystem *hostFS }

func (treeTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "hostfs_tree",
		Description: "Show a recursive file tree for an absolute path under a configured hostfs root with bounded depth/output.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Absolute directory or file path, or a path beginning with ~, under a configured hostfs root.",
				},
				"max_depth": map[string]any{"type": "integer", "minimum": 0},
				"limit":     map[string]any{"type": "integer", "minimum": 1},
				"pattern": map[string]any{
					"type":        "string",
					"description": "Optional filepath glob matched against each absolute slash path or basename.",
				},
				"include_hidden": map[string]any{"type": "boolean"},
			},
			"required":             []string{"path"},
			"additionalProperties": false,
		},
	}
}

func (tool treeTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path          string `json:"path"`
		MaxDepth      *int   `json:"max_depth"`
		Limit         *int   `json:"limit"`
		Pattern       string `json:"pattern"`
		IncludeHidden bool   `json:"include_hidden"`
	}
	if err := decode(raw, &arguments); err != nil {
		return failure(err), nil
	}
	depth := tool.filesystem.maxDepth
	if arguments.MaxDepth != nil {
		depth = *arguments.MaxDepth
	}
	limit := tool.filesystem.maxEntries
	if arguments.Limit != nil {
		limit = *arguments.Limit
	}
	if depth < 0 {
		return failure(errors.New("max_depth cannot be negative")), nil
	}
	if limit < 1 || limit > tool.filesystem.maxEntries {
		return failure(fmt.Errorf("limit must be between 1 and %d", tool.filesystem.maxEntries)), nil
	}
	if strings.TrimSpace(arguments.Pattern) != "" {
		if _, err := filepath.Match(arguments.Pattern, "check"); err != nil {
			return failure(fmt.Errorf("invalid pattern: %w", err)), nil
		}
	}
	start, err := tool.filesystem.resolve(arguments.Path)
	if err != nil {
		return failure(err), nil
	}
	entries, truncated, err := tool.filesystem.list(ctx, start, depth, limit, arguments.Pattern, arguments.IncludeHidden)
	if err != nil {
		return failure(err), nil
	}
	return sdk.ToolResult{Content: formatTree(start, entries, truncated)}, nil
}

func (filesystem *hostFS) resolve(path string) (string, error) {
	absolute, err := absolutePath(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	for _, root := range filesystem.roots {
		if contains(root, resolved) {
			return resolved, nil
		}
	}
	return "", fmt.Errorf("path %q is outside configured hostfs roots", absolute)
}

func (filesystem *hostFS) readText(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("path is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, filesystem.maxReadBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > filesystem.maxReadBytes {
		return nil, fmt.Errorf("file exceeds %d byte read limit", filesystem.maxReadBytes)
	}
	if !utf8.Valid(data) {
		return nil, errors.New("file is not valid UTF-8 text")
	}
	return data, nil
}

func (filesystem *hostFS) list(ctx context.Context, start string, maxDepth, limit int, pattern string, includeHidden bool) ([]string, bool, error) {
	var entries []string
	truncated := false
	rootDepth := depth(start)
	err := filepath.WalkDir(start, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if path != start && !includeHidden && hidden(path) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if depth(path)-rootDepth > maxDepth {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		name := filepath.ToSlash(path)
		if entry.IsDir() && !strings.HasSuffix(name, "/") {
			name += "/"
		}
		if entry.IsDir() || matches(pattern, path) {
			if len(entries) >= limit {
				truncated = true
				return fs.SkipAll
			}
			entries = append(entries, name)
		}
		return nil
	})
	sort.Strings(entries)
	return entries, truncated, err
}

func resolveRoot(value string) (string, error) {
	path, err := absolutePath(value)
	if err != nil {
		return "", fmt.Errorf("resolve hostfs root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve hostfs root symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("stat hostfs root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("hostfs root %q is not a directory", resolved)
	}
	return resolved, nil
}

func absolutePath(value string) (string, error) {
	path := strings.TrimSpace(value)
	if path == "" {
		return "", errors.New("path is empty")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, path[2:])
		}
	}
	if !filepath.IsAbs(path) {
		return "", errors.New("path must be absolute or begin with ~")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return absolute, nil
}

func contains(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func filesystemRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return string(filepath.Separator)
	}
	volume := filepath.VolumeName(cwd)
	return volume + string(filepath.Separator)
}

func splitLines(content string) []string {
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

func formatFile(path string, data []byte, lines []string, start, end int) string {
	var output strings.Builder
	fmt.Fprintf(&output, "file: %q\n", filepath.ToSlash(path))
	fmt.Fprintf(&output, "bytes: %d\n", len(data))
	if len(lines) == 0 {
		output.WriteString("lines: 0-0 of 0\n")
	} else {
		fmt.Fprintf(&output, "lines: %d-%d of %d\n", start+1, end, len(lines))
	}
	fmt.Fprintf(&output, "sha256: %x\n---", sha256.Sum256(data))
	for index := start; index < end; index++ {
		fmt.Fprintf(&output, "\n%d\t%s", index+1, lines[index])
	}
	return output.String()
}

func formatTree(root string, entries []string, truncated bool) string {
	var output strings.Builder
	fmt.Fprintf(&output, "root: %q\nentries: %d\n", filepath.ToSlash(root), len(entries))
	for _, entry := range entries {
		output.WriteString(entry)
		output.WriteByte('\n')
	}
	if truncated {
		output.WriteString("truncated: true\n")
	}
	return output.String()
}

func depth(path string) int {
	clean := strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/")
	if clean == "" || clean == "." {
		return 0
	}
	return strings.Count(clean, "/") + 1
}

func hidden(path string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if strings.HasPrefix(part, ".") && part != "." && part != ".." {
			return true
		}
	}
	return false
}

func matches(pattern, path string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return true
	}
	slash := filepath.ToSlash(path)
	base := filepath.Base(path)
	return glob(pattern, slash) || glob(pattern, base)
}

func glob(pattern, name string) bool {
	ok, err := filepath.Match(pattern, name)
	return err == nil && ok
}

func decode(raw json.RawMessage, target any) error {
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

func failure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: err.Error(), IsError: true}
}
