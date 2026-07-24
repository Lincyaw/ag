// Package skills discovers SKILL.md resources, advertises them in the system
// prompt, and exposes a load_skill tool for reading full instructions.
package skills

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/goccy/go-yaml"
	"github.com/lincyaw/ag/sdk"
)

const (
	defaultMaxReadBytes         int64 = 1 << 20
	defaultMaxNameLength              = 64
	defaultMaxDescriptionLength       = 1024
)

var skillNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)

type Config struct {
	WorkspaceRoot   string
	Paths           []string
	IncludeDefaults bool
	MaxReadBytes    int64
	Logger          *slog.Logger
}

type record struct {
	Name                   string
	Description            string
	Path                   string
	DisableModelInvocation bool
}

type plugin struct {
	config Config
}

func New(config Config) sdk.Plugin {
	config.Paths = slices.Clone(config.Paths)
	return &plugin{config: config}
}

func (*plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "skills",
		Version:     "1.0.0",
		Description: "discover SKILL.md instructions and expose them through load_skill",
		APIVersion:  sdk.APIVersion,
		Registers: []string{
			sdk.ToolResource("load_skill"),
			sdk.HookResource("skills-index"),
		},
	}
}

func (plugin *plugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	catalog, err := discover(plugin.config)
	if err != nil {
		return err
	}
	if err := registrar.RegisterTool(loadSkillTool{
		skills:       catalog,
		maxReadBytes: normalizedMaxReadBytes(plugin.config.MaxReadBytes),
	}); err != nil {
		return err
	}
	index := formatIndex(catalog)
	return registrar.RegisterHook(sdk.TypedHook[sdk.BeforeAgentStartPayload](
		sdk.HookSpec{
			Name:          "skills-index",
			Event:         sdk.EventBeforeAgentStart,
			Priority:      sdk.PriorityPost,
			FailurePolicy: sdk.FailurePolicyContinue,
		},
		func(
			_ context.Context,
			payload sdk.BeforeAgentStartPayload,
		) (sdk.Effect, error) {
			if index == "" {
				return sdk.Effect{}, nil
			}
			system := strings.TrimSpace(payload.System)
			if system != "" {
				system += "\n\n"
			}
			system += index
			return sdk.Patch(map[string]any{"system": system})
		},
	))
}

type loadSkillTool struct {
	skills       map[string]record
	maxReadBytes int64
}

func (loadSkillTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "load_skill",
		Description: "Load the full SKILL.md instructions for a skill listed in <available_skills>.",
		Concurrency: sdk.ToolConcurrencyParallel,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Skill name from <available_skills>.",
				},
			},
			"required":             []string{"name"},
			"additionalProperties": false,
		},
	}
}

func (tool loadSkillTool) Call(
	ctx context.Context,
	raw json.RawMessage,
) (sdk.ToolResult, error) {
	var arguments struct {
		Name string `json:"name"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&arguments); err != nil {
		return skillFailure(fmt.Errorf("decode arguments: %w", err)), nil
	}
	if err := ctx.Err(); err != nil {
		return sdk.ToolResult{}, err
	}
	name := strings.TrimSpace(arguments.Name)
	if name == "" {
		return skillFailure(errors.New("name is required")), nil
	}
	skill, exists := tool.skills[name]
	if !exists {
		available := sortedKeys(tool.skills)
		value := strings.Join(available, ", ")
		if value == "" {
			value = "(none)"
		}
		return skillFailure(fmt.Errorf(
			"skill %q not found; available: %s",
			name,
			value,
		)), nil
	}
	content, err := readSkill(skill.Path, tool.maxReadBytes)
	if err != nil {
		return skillFailure(fmt.Errorf("read skill %q: %w", name, err)), nil
	}
	return sdk.ToolResult{Content: content}, nil
}

func skillFailure(err error) sdk.ToolResult {
	return sdk.ToolResult{Content: "error: " + err.Error(), IsError: true}
}

func discover(config Config) (map[string]record, error) {
	root, err := workspaceRoot(config.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	if config.MaxReadBytes < 0 {
		return nil, errors.New("skill max read bytes must be positive")
	}
	sources := configuredPaths(root, config.Paths)
	if config.IncludeDefaults {
		sources = append(sources, defaultPaths(root)...)
	}
	catalog := make(map[string]record)
	seenFiles := make(map[string]struct{})
	for _, source := range sources {
		if err := discoverSource(
			source,
			normalizedMaxReadBytes(config.MaxReadBytes),
			catalog,
			seenFiles,
			config.Logger,
		); err != nil {
			return nil, err
		}
	}
	return catalog, nil
}

func normalizedMaxReadBytes(value int64) int64 {
	if value == 0 {
		return defaultMaxReadBytes
	}
	return value
}

func workspaceRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve skill workspace root: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("stat skill workspace root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("skill workspace root %q is not a directory", absolute)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve skill workspace root symlinks: %w", err)
	}
	return filepath.Clean(resolved), nil
}

func configuredPaths(root string, paths []string) []string {
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		path = os.ExpandEnv(strings.TrimSpace(path))
		if path == "" {
			continue
		}
		if strings.HasPrefix(path, "~"+string(filepath.Separator)) {
			if home, err := os.UserHomeDir(); err == nil {
				path = filepath.Join(home, strings.TrimPrefix(
					path,
					"~"+string(filepath.Separator),
				))
			}
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		result = append(result, filepath.Clean(path))
	}
	return result
}

func defaultPaths(root string) []string {
	result := []string{
		filepath.Join(root, ".ag", "skills"),
		filepath.Join(root, ".agentm", "skills"),
		filepath.Join(root, ".agents", "skills"),
		filepath.Join(root, ".claude", "skills"),
		filepath.Join(root, ".codex", "skills"),
	}
	if home, err := os.UserHomeDir(); err == nil {
		result = append(result,
			filepath.Join(home, ".ag", "skills"),
			filepath.Join(home, ".agentm", "skills"),
			filepath.Join(home, ".claude", "skills"),
		)
	}
	return result
}

func discoverSource(
	source string,
	maxReadBytes int64,
	catalog map[string]record,
	seenFiles map[string]struct{},
	logger *slog.Logger,
) error {
	info, err := os.Stat(source)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat skill path %q: %w", source, err)
	}
	if !info.IsDir() {
		return addSkill(source, maxReadBytes, catalog, seenFiles, logger)
	}
	visited := make(map[string]struct{})
	return filepath.WalkDir(source, func(
		path string,
		entry fs.DirEntry,
		walkErr error,
	) error {
		if walkErr != nil {
			if logger != nil {
				logger.Warn("skip unreadable skill path", "path", path, "error", walkErr)
			}
			return nil
		}
		if entry.IsDir() {
			real, err := filepath.EvalSymlinks(path)
			if err != nil {
				return fs.SkipDir
			}
			if _, exists := visited[real]; exists {
				return fs.SkipDir
			}
			visited[real] = struct{}{}
			return nil
		}
		if entry.Name() != "SKILL.md" {
			return nil
		}
		if err := addSkill(path, maxReadBytes, catalog, seenFiles, logger); err != nil {
			return err
		}
		return fs.SkipDir
	})
}

func addSkill(
	path string,
	maxReadBytes int64,
	catalog map[string]record,
	seenFiles map[string]struct{},
	logger *slog.Logger,
) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil
	}
	if _, exists := seenFiles[real]; exists {
		return nil
	}
	seenFiles[real] = struct{}{}
	content, err := readSkill(real, maxReadBytes)
	if err != nil {
		if logger != nil {
			logger.Warn("skip invalid skill", "path", real, "error", err)
		}
		return nil
	}
	skill, err := parseSkill(real, content)
	if err != nil {
		if logger != nil {
			logger.Warn("skip invalid skill", "path", real, "error", err)
		}
		return nil
	}
	if existing, exists := catalog[skill.Name]; exists {
		if logger != nil {
			logger.Warn(
				"skip colliding skill",
				"name", skill.Name,
				"path", real,
				"existing_path", existing.Path,
			)
		}
		return nil
	}
	catalog[skill.Name] = skill
	return nil
}

func readSkill(path string, maxBytes int64) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("path is not a regular file")
	}
	if info.Size() > maxBytes {
		return "", fmt.Errorf("file exceeds %d byte limit", maxBytes)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if int64(len(content)) > maxBytes {
		return "", fmt.Errorf("file exceeds %d byte limit", maxBytes)
	}
	if !utf8.Valid(content) {
		return "", errors.New("file is not valid UTF-8 text")
	}
	return string(content), nil
}

func parseSkill(path, content string) (record, error) {
	metadata, _, ok, err := splitFrontmatter(content)
	if err != nil {
		return record{}, fmt.Errorf("decode SKILL.md frontmatter: %w", err)
	}
	if !ok {
		return record{}, errors.New("SKILL.md has no YAML frontmatter")
	}
	name := strings.TrimSpace(metadata.Name)
	if name == "" {
		name = filepath.Base(filepath.Dir(path))
	}
	if len(name) > defaultMaxNameLength || !skillNamePattern.MatchString(name) ||
		strings.Contains(name, "--") {
		return record{}, fmt.Errorf("invalid skill name %q", name)
	}
	if name != filepath.Base(filepath.Dir(path)) {
		return record{}, fmt.Errorf(
			"skill name %q does not match parent directory %q",
			name,
			filepath.Base(filepath.Dir(path)),
		)
	}
	description := strings.TrimSpace(metadata.Description)
	if description == "" {
		return record{}, errors.New("skill description is required")
	}
	if len(description) > defaultMaxDescriptionLength {
		return record{}, fmt.Errorf(
			"skill description exceeds %d characters",
			defaultMaxDescriptionLength,
		)
	}
	return record{
		Name:                   name,
		Description:            description,
		Path:                   path,
		DisableModelInvocation: metadata.DisableModelInvocation,
	}, nil
}

type skillFrontmatter struct {
	Name                   string `yaml:"name"`
	Description            string `yaml:"description"`
	DisableModelInvocation bool   `yaml:"disable-model-invocation"`
}

func splitFrontmatter(
	content string,
) (skillFrontmatter, string, bool, error) {
	content = strings.TrimPrefix(content, "\ufeff")
	content = strings.ReplaceAll(content, "\r\n", "\n")
	if !strings.HasPrefix(content, "---\n") {
		return skillFrontmatter{}, content, false, nil
	}
	remainder := content[4:]
	end := strings.Index(remainder, "\n---\n")
	if end < 0 {
		if strings.HasSuffix(remainder, "\n---") {
			end = len(remainder) - len("\n---")
		} else {
			return skillFrontmatter{}, content, false, nil
		}
	}
	head := remainder[:end]
	body := strings.TrimPrefix(remainder[end+len("\n---"):], "\n")
	var metadata skillFrontmatter
	if err := yaml.Unmarshal([]byte(head), &metadata); err != nil {
		return skillFrontmatter{}, content, true, err
	}
	return metadata, body, true, nil
}

func formatIndex(catalog map[string]record) string {
	names := sortedKeys(catalog)
	var visible []record
	for _, name := range names {
		if !catalog[name].DisableModelInvocation {
			visible = append(visible, catalog[name])
		}
	}
	if len(visible) == 0 {
		return ""
	}
	var output strings.Builder
	output.WriteString("# Skills\n\n")
	output.WriteString(
		"Use `load_skill` before responding whenever a task matches one of " +
			"the skills below. The loaded instructions may contain current, " +
			"project-specific requirements. If no skill matches, proceed normally.\n\n",
	)
	output.WriteString("<available_skills>\n")
	for _, skill := range visible {
		output.WriteString("  <skill>\n")
		fmt.Fprintf(&output, "    <name>%s</name>\n", html.EscapeString(skill.Name))
		fmt.Fprintf(
			&output,
			"    <description>%s</description>\n",
			html.EscapeString(skill.Description),
		)
		fmt.Fprintf(
			&output,
			"    <location>%s</location>\n",
			html.EscapeString(skill.Path),
		)
		output.WriteString("  </skill>\n")
	}
	output.WriteString("</available_skills>")
	return output.String()
}

func sortedKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}
