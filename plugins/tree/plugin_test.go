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

func TestListFilesAllowsPathsOutsideWorkspace(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "workspace")
	adjacent := filepath.Join(parent, "AgentM")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(adjacent, "README.md"), "adjacent")

	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Root: root}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["workspace_tree"].Value.(sdk.SyncTool)

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "parent relative", path: "../AgentM", want: "../AgentM/README.md"},
		{
			name: "absolute",
			path: adjacent,
			want: filepath.ToSlash(filepath.Join(adjacent, "README.md")),
		},
	}
	if err := os.Symlink(adjacent, filepath.Join(root, "agentm-link")); err == nil {
		tests = append(tests, struct {
			name string
			path string
			want string
		}{name: "symlink", path: "agentm-link", want: "agentm-link/README.md"})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := call(t, tool, map[string]any{"path": test.path})
			if result.IsError || !strings.Contains(result.Content, test.want+"\n") {
				t.Fatalf("result = %#v, want %q", result, test.want)
			}
		})
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
