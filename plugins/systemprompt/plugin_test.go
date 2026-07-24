package systemprompt

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

func TestPluginAssemblesHierarchicalContextBeforeBasePrompt(t *testing.T) {
	root := t.TempDir()
	project := filepath.Join(root, "project")
	workspace := filepath.Join(project, "service")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	writeText(t, filepath.Join(root, "AGENTS.md"), "root instructions")
	writeText(t, filepath.Join(project, "CLAUDE.md"), "project instructions")
	writeText(t, filepath.Join(workspace, "AGENTS.md"), "service instructions")

	registrar := plugincontract.NewRegistrar()
	installed := New(Config{WorkspaceRoot: workspace})
	if err := installed.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	hook := registrar.Hooks["system-prompt"].Value
	payload, err := json.Marshal(sdk.BeforeAgentStartPayload{
		System: "base prompt",
	})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := hook.Handle(t.Context(), sdk.Event{
		Name: sdk.EventBeforeAgentStart, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var system string
	if err := json.Unmarshal(effect.Patch["system"], &system); err != nil {
		t.Fatal(err)
	}
	want := strings.Join([]string{
		"root instructions",
		"project instructions",
		"service instructions",
		"base prompt",
	}, "\n\n")
	if system != want {
		t.Fatalf("assembled system prompt = %q, want %q", system, want)
	}
}

func TestPromptFileWinsOverInlineAndContext(t *testing.T) {
	root := t.TempDir()
	writeText(t, filepath.Join(root, "AGENTS.md"), "context")
	writeText(t, filepath.Join(root, "prompt.md"), "file prompt")
	prompt, err := resolvePrompt(Config{
		WorkspaceRoot: root,
		Prompt:        "inline prompt",
		PromptFile:    "prompt.md",
	})
	if err != nil {
		t.Fatal(err)
	}
	if prompt != "file prompt" {
		t.Fatalf("resolved prompt = %q", prompt)
	}
}

func writeText(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
