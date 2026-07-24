// Package systemprompt contributes workspace context to the agent system
// prompt without coupling filesystem policy to the runtime.
package systemprompt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

const defaultMaxFileBytes int64 = 1 << 20

var contextFilenames = [...]string{"AGENTS.md", "CLAUDE.md"}

// Config controls the system-prompt source. PromptFile wins over Prompt. When
// neither is set, AGENTS.md and CLAUDE.md are collected from the filesystem
// root down to WorkspaceRoot.
type Config struct {
	WorkspaceRoot string
	Prompt        string
	PromptFile    string
	MaxFileBytes  int64
	Logger        *slog.Logger
}

type plugin struct {
	config Config
}

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (*plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "system-prompt",
		Version:     "1.0.0",
		Description: "assemble configured and workspace context into the agent system prompt",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.HookResource("system-prompt")},
	}
}

func (plugin *plugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	prompt, err := resolvePrompt(plugin.config)
	if err != nil {
		return err
	}
	return registrar.RegisterHook(sdk.TypedHook[sdk.BeforeAgentStartPayload](
		sdk.HookSpec{
			Name:          "system-prompt",
			Event:         sdk.EventBeforeAgentStart,
			Priority:      sdk.PriorityPre,
			FailurePolicy: sdk.FailurePolicyFailClosed,
		},
		func(
			_ context.Context,
			payload sdk.BeforeAgentStartPayload,
		) (sdk.Effect, error) {
			if prompt == "" {
				return sdk.Effect{}, nil
			}
			system := prompt
			if current := strings.TrimSpace(payload.System); current != "" {
				system += "\n\n" + current
			}
			return sdk.Patch(map[string]any{"system": system})
		},
	))
}

func resolvePrompt(config Config) (string, error) {
	maxBytes := config.MaxFileBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxFileBytes
	}
	if maxBytes < 1 {
		return "", errors.New("system prompt max file bytes must be positive")
	}
	root, err := absoluteRoot(config.WorkspaceRoot)
	if err != nil {
		return "", err
	}
	if path := strings.TrimSpace(config.PromptFile); path != "" {
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		content, err := readBoundedText(path, maxBytes)
		if err != nil {
			return "", fmt.Errorf("read system prompt file %q: %w", path, err)
		}
		return strings.TrimSpace(content), nil
	}
	if config.Prompt != "" {
		return strings.TrimSpace(config.Prompt), nil
	}
	return discoverContext(root, maxBytes, config.Logger), nil
}

func absoluteRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf("stat workspace root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workspace root %q is not a directory", absolute)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root symlinks: %w", err)
	}
	return filepath.Clean(resolved), nil
}

func discoverContext(root string, maxBytes int64, logger *slog.Logger) string {
	directories := []string{root}
	for parent := filepath.Dir(root); parent != root; parent = filepath.Dir(parent) {
		directories = append(directories, parent)
		root = parent
	}
	for left, right := 0, len(directories)-1; left < right; left, right = left+1, right-1 {
		directories[left], directories[right] = directories[right], directories[left]
	}

	var parts []string
	for _, directory := range directories {
		for _, filename := range contextFilenames {
			path := filepath.Join(directory, filename)
			content, err := readBoundedText(path, maxBytes)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				if logger != nil {
					logger.Warn(
						"skip unreadable system prompt context",
						"path", path,
						"error", err,
					)
				}
				continue
			}
			if content = strings.TrimSpace(content); content != "" {
				parts = append(parts, content)
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func readBoundedText(path string, maxBytes int64) (string, error) {
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
