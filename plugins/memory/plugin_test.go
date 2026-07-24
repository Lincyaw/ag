package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

func TestMemoryLifecycleAndPromptIndex(t *testing.T) {
	root := t.TempDir()
	registrar := plugincontract.NewRegistrar()
	installed := New(Config{
		WorkspaceRoot:       root,
		EnableWrite:         true,
		IndexInSystemPrompt: true,
	})
	if err := installed.Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}

	save := registrar.Tools["memory_save"].Value.(sdk.SyncTool)
	result, err := save.Call(t.Context(), json.RawMessage(`{
		"type":"project",
		"name":"test_policy",
		"description":"Tests use real runtime boundaries.",
		"content":"Run the integration suite before shipping."
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("memory_save = %#v", result)
	}

	index, err := os.ReadFile(filepath.Join(root, ".ag", "memory", "MEMORY.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(index), "`project/test_policy`") {
		t.Fatalf("MEMORY.md = %q", index)
	}

	read := registrar.Tools["memory_read"].Value.(sdk.SyncTool)
	result, err = read.Call(
		t.Context(), json.RawMessage(`{"name":"project/test_policy"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError ||
		!strings.Contains(result.Content, "Run the integration suite") {
		t.Fatalf("memory_read = %#v", result)
	}

	search := registrar.Tools["memory_search"].Value.(sdk.SyncTool)
	result, err = search.Call(
		t.Context(), json.RawMessage(`{"query":"runtime"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError ||
		!strings.Contains(result.Content, "project/test_policy") {
		t.Fatalf("memory_search = %#v", result)
	}

	hook := registrar.Hooks["memory-index"].Value
	payload, _ := json.Marshal(sdk.BeforeAgentStartPayload{System: "base"})
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
		"base", "<memory>", "project/test_policy", "`memory_read`",
	} {
		if !strings.Contains(system, expected) {
			t.Fatalf("system %q does not contain %q", system, expected)
		}
	}

	deleteMemory := registrar.Tools["memory_delete"].Value.(sdk.SyncTool)
	result, err = deleteMemory.Call(
		t.Context(), json.RawMessage(`{"name":"test_policy"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("memory_delete = %#v", result)
	}
	result, err = read.Call(
		t.Context(), json.RawMessage(`{"name":"test_policy"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("deleted memory_read = %#v", result)
	}
}

func TestReadOnlyMemoryDoesNotRegisterMutationTools(t *testing.T) {
	registrar := plugincontract.NewRegistrar()
	if err := New(Config{
		WorkspaceRoot: t.TempDir(),
	}).Install(t.Context(), registrar); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"memory_save", "memory_delete"} {
		if _, exists := registrar.Tools[name]; exists {
			t.Fatalf("read-only memory registered %q", name)
		}
	}
}
