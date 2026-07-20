package tree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxEntries = 500
	defaultMaxDepth   = 4
)

type Config struct {
	Root       string
	MaxEntries int
	MaxDepth   int
}

type plugin struct{ config Config }

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "tree",
		Version:     "1.0.0",
		Description: "bounded deterministic workspace file tree listing",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.ToolResource("workspace_tree")},
	}
}

func (plugin *plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	lister, err := newLister(plugin.config)
	if err != nil {
		return err
	}
	return registrar.RegisterTool(listTool{lister: lister})
}

type lister struct {
	root       string
	maxEntries int
	maxDepth   int
}

func newLister(config Config) (*lister, error) {
	root := strings.TrimSpace(config.Root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve tree root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve tree root symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, fmt.Errorf("stat tree root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("tree root %q is not a directory", resolved)
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultMaxEntries
	}
	if config.MaxDepth == 0 {
		config.MaxDepth = defaultMaxDepth
	}
	if config.MaxEntries < 1 || config.MaxDepth < 0 {
		return nil, errors.New("tree limits are invalid")
	}
	return &lister{root: resolved, maxEntries: config.MaxEntries, maxDepth: config.MaxDepth}, nil
}

type listTool struct{ lister *lister }

func (listTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "workspace_tree",
		Description: "Show a recursive file tree under the workspace root with deterministic ordering and bounded depth/output.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative directory or file path to list; defaults to the workspace root.",
				},
				"max_depth": map[string]any{
					"type":        "integer",
					"minimum":     0,
					"description": "Maximum directory depth below path; defaults to plugin configuration.",
				},
				"limit": map[string]any{
					"type":        "integer",
					"minimum":     1,
					"description": "Maximum entries to return; capped by plugin configuration.",
				},
				"pattern": map[string]any{
					"type":        "string",
					"description": "Optional filepath glob matched against each slash-relative path or basename.",
				},
				"include_hidden": map[string]any{
					"type":        "boolean",
					"description": "Include dotfiles and hidden directories.",
				},
			},
			"additionalProperties": false,
		},
	}
}

func (tool listTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var arguments struct {
		Path          string `json:"path"`
		MaxDepth      *int   `json:"max_depth"`
		Limit         *int   `json:"limit"`
		Pattern       string `json:"pattern"`
		IncludeHidden bool   `json:"include_hidden"`
	}
	if err := json.Unmarshal(raw, &arguments); err != nil {
		return failure(fmt.Errorf("decode arguments: %w", err)), nil
	}
	depth := tool.lister.maxDepth
	if arguments.MaxDepth != nil {
		depth = *arguments.MaxDepth
	}
	limit := tool.lister.maxEntries
	if arguments.Limit != nil {
		limit = *arguments.Limit
	}
	if depth < 0 {
		return failure(errors.New("max_depth cannot be negative")), nil
	}
	if limit < 1 || limit > tool.lister.maxEntries {
		return failure(fmt.Errorf("limit must be between 1 and %d", tool.lister.maxEntries)), nil
	}
	if strings.TrimSpace(arguments.Pattern) != "" {
		if _, err := filepath.Match(arguments.Pattern, "check"); err != nil {
			return failure(fmt.Errorf("invalid pattern: %w", err)), nil
		}
	}
	start, display, err := tool.lister.resolve(arguments.Path)
	if err != nil {
		return failure(err), nil
	}
	entries, truncated, err := tool.lister.list(ctx, start, display, depth, limit, arguments.Pattern, arguments.IncludeHidden)
	if err != nil {
		return failure(err), nil
	}
	return sdk.ToolResult{Content: format(display, entries, truncated)}, nil
}

func (lister *lister) resolve(relative string) (string, string, error) {
	clean := filepath.Clean(strings.TrimSpace(relative))
	if clean == "." || clean == "" {
		return lister.root, ".", nil
	}
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", "", errors.New("path must stay within the workspace root")
	}
	full := filepath.Join(lister.root, clean)
	resolved, err := filepath.EvalSymlinks(full)
	if err != nil {
		return "", "", err
	}
	if resolved != lister.root && !strings.HasPrefix(resolved, lister.root+string(os.PathSeparator)) {
		return "", "", errors.New("path escapes the workspace root")
	}
	return resolved, filepath.ToSlash(clean), nil
}

func (lister *lister) list(ctx context.Context, start, display string, maxDepth, limit int, pattern string, includeHidden bool) ([]string, bool, error) {
	var entries []string
	truncated := false
	rootDepth := depth(display)
	err := filepath.WalkDir(start, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(lister.root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			rel = display
		}
		if rel != "." && !includeHidden && hidden(rel) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if depth(rel)-rootDepth > maxDepth {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		name := rel
		if entry.IsDir() && name != "." && !strings.HasSuffix(name, "/") {
			name += "/"
		}
		if entry.IsDir() || matches(pattern, rel) {
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

func format(root string, entries []string, truncated bool) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "root: %q\nentries: %d\n", root, len(entries))
	for _, entry := range entries {
		builder.WriteString(entry)
		builder.WriteByte('\n')
	}
	if truncated {
		builder.WriteString("truncated: true\n")
	}
	return builder.String()
}

func matches(pattern, rel string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return true
	}
	base := filepath.Base(rel)
	return glob(pattern, rel) || glob(pattern, base)
}

func glob(pattern, name string) bool {
	ok, err := filepath.Match(pattern, name)
	return err == nil && ok
}

func hidden(rel string) bool {
	for _, part := range strings.Split(rel, "/") {
		if strings.HasPrefix(part, ".") && part != "." && part != ".." {
			return true
		}
	}
	return false
}

func depth(rel string) int {
	rel = strings.Trim(rel, "/")
	if rel == "" || rel == "." {
		return 0
	}
	return strings.Count(rel, "/") + 1
}

func failure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: err.Error(), IsError: true}
}
