// Package memory provides project-local, persistent agent memory.
package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/goccy/go-yaml"
	"github.com/lincyaw/ag/sdk"
)

const (
	defaultPath         = ".ag/memory"
	defaultMaxReadBytes = int64(1 << 20)
	defaultMaxIndex     = 200
)

var memoryNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type Config struct {
	WorkspaceRoot       string
	Path                string
	EnableWrite         bool
	IndexInSystemPrompt bool
	MaxReadBytes        int64
	MaxIndexEntries     int
}

type plugin struct{ config Config }

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (plugin *plugin) Manifest() sdk.Manifest {
	registers := []string{
		sdk.ToolResource("memory_read"),
		sdk.ToolResource("memory_search"),
	}
	if plugin.config.IndexInSystemPrompt {
		registers = append(registers, sdk.HookResource("memory-index"))
	}
	if plugin.config.EnableWrite {
		registers = append(registers,
			sdk.ToolResource("memory_save"),
			sdk.ToolResource("memory_delete"),
		)
	}
	return sdk.Manifest{
		Name:        "memory",
		Version:     "1.0.0",
		Description: "project-local persistent memory with an on-demand indexed store",
		APIVersion:  sdk.APIVersion,
		Registers:   registers,
	}
}

func (plugin *plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	store, err := newStore(plugin.config)
	if err != nil {
		return err
	}
	if err := registrar.RegisterTool(readTool{store}); err != nil {
		return err
	}
	if err := registrar.RegisterTool(searchTool{store}); err != nil {
		return err
	}
	if plugin.config.EnableWrite {
		if err := registrar.RegisterTool(saveTool{store}); err != nil {
			return err
		}
		if err := registrar.RegisterTool(deleteTool{store}); err != nil {
			return err
		}
	}
	if !plugin.config.IndexInSystemPrompt {
		return nil
	}
	return registrar.RegisterHook(sdk.TypedHook[sdk.BeforeAgentStartPayload](
		sdk.HookSpec{
			Name:          "memory-index",
			Event:         sdk.EventBeforeAgentStart,
			Priority:      sdk.PriorityPost,
			FailurePolicy: sdk.FailurePolicyContinue,
		},
		func(_ context.Context, payload sdk.BeforeAgentStartPayload) (sdk.Effect, error) {
			index, indexErr := store.indexBlock()
			if indexErr != nil || index == "" {
				return sdk.Effect{}, indexErr
			}
			system := strings.TrimSpace(payload.System)
			if system != "" {
				system += "\n\n"
			}
			return sdk.Patch(map[string]any{"system": system + index})
		},
	))
}

type store struct {
	root       string
	maxRead    int64
	maxEntries int
	mu         sync.RWMutex
}

type metadata struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Type        string `yaml:"type"`
}

type entry struct {
	metadata
	path string
}

func newStore(config Config) (*store, error) {
	workspace := strings.TrimSpace(config.WorkspaceRoot)
	if workspace == "" {
		workspace = "."
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve memory workspace: %w", err)
	}
	path := strings.TrimSpace(config.Path)
	if path == "" {
		path = defaultPath
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(workspace, path)
	}
	maxRead := config.MaxReadBytes
	if maxRead == 0 {
		maxRead = defaultMaxReadBytes
	}
	maxEntries := config.MaxIndexEntries
	if maxEntries == 0 {
		maxEntries = defaultMaxIndex
	}
	if maxRead < 1 || maxEntries < 1 {
		return nil, errors.New("memory limits must be positive")
	}
	return &store{
		root: filepath.Clean(path), maxRead: maxRead, maxEntries: maxEntries,
	}, nil
}

func (s *store) entries() ([]entry, error) {
	items, err := os.ReadDir(s.root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read memory directory: %w", err)
	}
	result := make([]entry, 0, len(items))
	for _, item := range items {
		if item.IsDir() || item.Type()&os.ModeSymlink != 0 ||
			item.Name() == "MEMORY.md" ||
			!strings.HasSuffix(item.Name(), ".md") {
			continue
		}
		path := filepath.Join(s.root, item.Name())
		content, readErr := readLimited(path, s.maxRead)
		if readErr != nil {
			continue
		}
		meta, _, parseErr := parseMemory(content)
		if parseErr != nil || meta.Name == "" {
			continue
		}
		result = append(result, entry{metadata: meta, path: path})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Type == result[j].Type {
			return result[i].Name < result[j].Name
		}
		return result[i].Type < result[j].Type
	})
	return result, nil
}

func (s *store) find(name string) (entry, error) {
	parts := strings.Split(name, "/")
	if len(parts) > 2 || !memoryNamePattern.MatchString(parts[len(parts)-1]) {
		return entry{}, fmt.Errorf("invalid memory name %q", name)
	}
	if len(parts) == 2 && !validMemoryType(parts[0]) {
		return entry{}, fmt.Errorf("invalid memory type %q", parts[0])
	}
	entries, err := s.entries()
	if err != nil {
		return entry{}, err
	}
	var matches []entry
	for _, candidate := range entries {
		if candidate.Name == name || candidate.Type+"/"+candidate.Name == name {
			matches = append(matches, candidate)
		}
	}
	if len(matches) == 0 {
		return entry{}, fmt.Errorf("memory %q not found", name)
	}
	if len(matches) > 1 {
		return entry{}, fmt.Errorf(
			"memory name %q is ambiguous; use type/name", name,
		)
	}
	return matches[0], nil
}

func (s *store) indexBlock() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := s.entries()
	if err != nil || len(entries) == 0 {
		return "", err
	}
	var builder strings.Builder
	builder.WriteString("<memory>\n")
	builder.WriteString("Persistent project memories are listed below. Use `memory_read` to load a relevant body.\n")
	for index, item := range entries {
		if index >= s.maxEntries {
			fmt.Fprintf(&builder, "- ... %d more memories omitted\n", len(entries)-index)
			break
		}
		fmt.Fprintf(&builder, "- %s/%s: %s\n",
			item.Type, item.Name, strings.TrimSpace(item.Description))
	}
	builder.WriteString("</memory>")
	return builder.String(), nil
}

func (s *store) rewriteIndex() error {
	entries, err := s.entries()
	if err != nil {
		return err
	}
	var builder strings.Builder
	builder.WriteString("# Project Memory\n\n")
	for _, item := range entries {
		fmt.Fprintf(&builder, "- `%s/%s` — %s\n",
			item.Type, item.Name, strings.TrimSpace(item.Description))
	}
	return atomicWrite(filepath.Join(s.root, "MEMORY.md"), []byte(builder.String()))
}

func parseMemory(content string) (metadata, string, error) {
	if !strings.HasPrefix(content, "---\n") {
		return metadata{}, "", errors.New("memory has no YAML frontmatter")
	}
	rest := content[4:]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return metadata{}, "", errors.New("memory frontmatter is not terminated")
	}
	var meta metadata
	if err := yaml.Unmarshal([]byte(rest[:end]), &meta); err != nil {
		return metadata{}, "", fmt.Errorf("decode memory frontmatter: %w", err)
	}
	body := strings.TrimPrefix(rest[end+4:], "\n")
	return meta, body, nil
}

func serializeMemory(meta metadata, body string) (string, error) {
	frontmatter, err := yaml.Marshal(meta)
	if err != nil {
		return "", err
	}
	return "---\n" + string(frontmatter) + "---\n" + body, nil
}

func readLimited(path string, limit int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	if info.Size() > limit {
		return "", fmt.Errorf("memory exceeds %d bytes", limit)
	}
	content, err := os.ReadFile(path)
	return string(content), err
}

func atomicWrite(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".memory-*")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(content); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}

func failure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: "error: " + err.Error(), IsError: true}
}

type readTool struct{ store *store }

func (readTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "memory_read", Description: "Read a persistent memory by name or type/name.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: objectSchema(map[string]any{
			"name": map[string]any{"type": "string"},
		}, []string{"name"}),
	}
}

func (tool readTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := decode(raw, &args); err != nil {
		return failure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	tool.store.mu.RLock()
	defer tool.store.mu.RUnlock()
	item, err := tool.store.find(strings.TrimSpace(args.Name))
	if err != nil {
		return failure(err), nil
	}
	content, err := readLimited(item.path, tool.store.maxRead)
	if err != nil {
		return failure(err), nil
	}
	return sdk.ToolResult{Content: content}, nil
}

type searchTool struct{ store *store }

func (searchTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "memory_search", Description: "Search persistent memory names and descriptions.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: objectSchema(map[string]any{
			"query": map[string]any{"type": "string"},
			"limit": map[string]any{"type": "integer", "minimum": 1, "maximum": 100},
		}, []string{"query"}),
	}
}

func (tool searchTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var args struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}
	if err := decode(raw, &args); err != nil {
		return failure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	query := strings.ToLower(strings.TrimSpace(args.Query))
	if query == "" {
		return failure(errors.New("query is required")), nil
	}
	if args.Limit == 0 {
		args.Limit = 10
	}
	if args.Limit < 1 || args.Limit > 100 {
		return failure(errors.New("limit must be between 1 and 100")), nil
	}
	tool.store.mu.RLock()
	defer tool.store.mu.RUnlock()
	entries, err := tool.store.entries()
	if err != nil {
		return failure(err), nil
	}
	var lines []string
	for _, item := range entries {
		haystack := strings.ToLower(item.Type + " " + item.Name + " " + item.Description)
		if strings.Contains(haystack, query) {
			lines = append(lines, fmt.Sprintf("- %s/%s: %s",
				item.Type, item.Name, item.Description))
			if len(lines) == args.Limit {
				break
			}
		}
	}
	if len(lines) == 0 {
		return sdk.ToolResult{Content: "no memories matched " + fmt.Sprintf("%q", query)}, nil
	}
	return sdk.ToolResult{Content: strings.Join(lines, "\n")}, nil
}

type saveTool struct{ store *store }

func (saveTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "memory_save", Description: "Save a durable project, user, feedback, or reference memory.",
		Parameters: objectSchema(map[string]any{
			"type": map[string]any{
				"type": "string", "enum": []string{"feedback", "project", "user", "reference"},
			},
			"name":        map[string]any{"type": "string"},
			"description": map[string]any{"type": "string"},
			"content":     map[string]any{"type": "string"},
		}, []string{"type", "name", "description", "content"}),
	}
}

func (tool saveTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var args struct {
		Type, Name, Description, Content string
	}
	if err := decode(raw, &args); err != nil {
		return failure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	if !memoryNamePattern.MatchString(args.Name) {
		return failure(fmt.Errorf("invalid memory name %q", args.Name)), nil
	}
	if !validMemoryType(args.Type) {
		return failure(fmt.Errorf("invalid memory type %q", args.Type)), nil
	}
	args.Description = strings.TrimSpace(args.Description)
	if args.Description == "" || strings.Contains(args.Description, "\n") {
		return failure(errors.New("description must be one non-empty line")), nil
	}
	content, err := serializeMemory(metadata{
		Name: args.Name, Description: args.Description, Type: args.Type,
	}, args.Content)
	if err != nil {
		return failure(err), nil
	}
	if int64(len(content)) > tool.store.maxRead {
		return failure(fmt.Errorf(
			"memory exceeds %d bytes", tool.store.maxRead,
		)), nil
	}
	tool.store.mu.Lock()
	defer tool.store.mu.Unlock()
	path := filepath.Join(tool.store.root, args.Type+"_"+args.Name+".md")
	if err := atomicWrite(path, []byte(content)); err != nil {
		return failure(err), nil
	}
	if err := tool.store.rewriteIndex(); err != nil {
		return failure(err), nil
	}
	return sdk.ToolResult{Content: "saved memory " + args.Type + "/" + args.Name}, nil
}

type deleteTool struct{ store *store }

func (deleteTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "memory_delete", Description: "Delete a persistent memory by name or type/name.",
		Parameters: objectSchema(map[string]any{
			"name": map[string]any{"type": "string"},
		}, []string{"name"}),
	}
}

func (tool deleteTool) Call(ctx context.Context, raw json.RawMessage) (sdk.ToolResult, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := decode(raw, &args); err != nil {
		return failure(err), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	tool.store.mu.Lock()
	defer tool.store.mu.Unlock()
	item, err := tool.store.find(strings.TrimSpace(args.Name))
	if err != nil {
		return failure(err), nil
	}
	if err := os.Remove(item.path); err != nil {
		return failure(err), nil
	}
	if err := tool.store.rewriteIndex(); err != nil {
		return failure(err), nil
	}
	return sdk.ToolResult{Content: "deleted memory " + item.Type + "/" + item.Name}, nil
}

func decode(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode arguments: %w", err)
	}
	return nil
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	return map[string]any{
		"type": "object", "properties": properties, "required": required,
		"additionalProperties": false,
	}
}

func validMemoryType(value string) bool {
	switch value {
	case "feedback", "project", "user", "reference":
		return true
	default:
		return false
	}
}
