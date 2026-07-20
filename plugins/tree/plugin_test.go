package tree

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

func TestListFilesToolListsDeterministicallyWithinBounds(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "b.txt"), "b")
	write(t, filepath.Join(root, "a", "one.go"), "a")
	write(t, filepath.Join(root, "a", "nested", "two.go"), "nested")
	write(t, filepath.Join(root, ".secret"), "hidden")

	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Root: root, MaxEntries: 10, MaxDepth: 3}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["workspace_tree"].Value.(sdk.SyncTool)
	result := call(t, tool, map[string]any{"pattern": "*.go"})
	if result.IsError {
		t.Fatalf("workspace_tree failed: %s", result.Content)
	}
	for _, want := range []string{"a/", "a/nested/", "a/nested/two.go", "a/one.go"} {
		if !strings.Contains(result.Content, want+"\n") {
			t.Fatalf("result missing %q:\n%s", want, result.Content)
		}
	}
	if strings.Contains(result.Content, ".secret") {
		t.Fatalf("hidden file leaked:\n%s", result.Content)
	}
	if strings.Index(result.Content, "a/\n") > strings.Index(result.Content, "a/one.go\n") {
		t.Fatalf("entries are not sorted:\n%s", result.Content)
	}
}

func TestListFilesRejectsEscapingPath(t *testing.T) {
	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Root: t.TempDir()}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["workspace_tree"].Value.(sdk.SyncTool)
	result := call(t, tool, map[string]any{"path": "../outside"})
	if !result.IsError || !strings.Contains(result.Content, "workspace root") {
		t.Fatalf("result = %#v", result)
	}
}

func call(t *testing.T, tool sdk.SyncTool, args map[string]any) sdk.ToolResult {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
