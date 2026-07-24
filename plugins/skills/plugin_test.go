package skills

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

func TestPluginDiscoversIndexesAndLoadsSkill(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", filepath.Join(root, "home"))
	skillDir := filepath.Join(root, ".ag", "skills", "review")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "---\n" +
		"name: review\n" +
		"description: >\n" +
		"  Review code with project-specific\n" +
		"  release criteria.\n" +
		"---\n" +
		"# Review\n\nRead every changed file.\n"
	if err := os.WriteFile(
		filepath.Join(skillDir, "SKILL.md"),
		[]byte(content),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	registrar := plugincontract.NewRegistrar()
	installed := New(Config{
		WorkspaceRoot:   root,
		IncludeDefaults: true,
	})
	if err := installed.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}

	tool := registrar.Tools["load_skill"].Value.(sdk.SyncTool)
	result, err := tool.Call(
		t.Context(),
		json.RawMessage(`{"name":"review"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || result.Content != content {
		t.Fatalf("load_skill result = %#v", result)
	}

	hook := registrar.Hooks["skills-index"].Value
	payload, _ := json.Marshal(sdk.BeforeAgentStartPayload{
		System: "base prompt",
	})
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
	for _, expected := range []string{
		"base prompt",
		"<available_skills>",
		"<name>review</name>",
		"Review code with project-specific release criteria.",
		"`load_skill`",
	} {
		if !strings.Contains(system, expected) {
			t.Fatalf("system prompt %q does not contain %q", system, expected)
		}
	}
}

func TestExplicitSkillPathWinsNameCollision(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", filepath.Join(root, "home"))
	explicit := filepath.Join(root, "explicit", "review")
	fallback := filepath.Join(root, ".ag", "skills", "review")
	writeSkill(t, explicit, "explicit")
	writeSkill(t, fallback, "fallback")

	catalog, err := discover(Config{
		WorkspaceRoot:   root,
		Paths:           []string{filepath.Dir(explicit)},
		IncludeDefaults: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := catalog["review"].Description; got != "explicit" {
		t.Fatalf("collision winner description = %q", got)
	}
}

func writeSkill(t *testing.T, directory, description string) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "---\nname: review\ndescription: " + description +
		"\n---\nbody\n"
	if err := os.WriteFile(
		filepath.Join(directory, "SKILL.md"),
		[]byte(content),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
}
