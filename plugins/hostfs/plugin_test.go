package hostfs

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

func TestReadFileAllowsConfiguredAbsolutePath(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "note.txt")
	write(t, path, "one\ntwo\nthree\n")

	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Roots: []string{root}, MaxEntries: 10}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["hostfs_read_file"].Value.(sdk.SyncTool)
	result := call(t, tool, map[string]any{"path": path, "offset": 2, "limit": 1})
	if result.IsError {
		t.Fatalf("hostfs_read_file failed: %s", result.Content)
	}
	if !strings.Contains(result.Content, "2\ttwo") || strings.Contains(result.Content, "1\tone") {
		t.Fatalf("unexpected content:\n%s", result.Content)
	}
}

func TestReadFileRejectsOutsideRoot(t *testing.T) {
	allowed := t.TempDir()
	outside := t.TempDir()
	path := filepath.Join(outside, "secret.txt")
	write(t, path, "secret")

	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Roots: []string{allowed}}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["hostfs_read_file"].Value.(sdk.SyncTool)
	result := call(t, tool, map[string]any{"path": path})
	if !result.IsError || !strings.Contains(result.Content, "outside configured hostfs roots") {
		t.Fatalf("result = %#v", result)
	}
}

func TestTreeListsConfiguredAbsolutePath(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "b.txt"), "b")
	write(t, filepath.Join(root, "a", "one.go"), "a")
	write(t, filepath.Join(root, ".hidden"), "hidden")

	registrar := plugincontract.NewRegistrar()
	if err := New(Config{Roots: []string{root}, MaxEntries: 10, MaxDepth: 2}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	tool := registrar.Tools["hostfs_tree"].Value.(sdk.SyncTool)
	result := call(t, tool, map[string]any{"path": root, "pattern": "*.go"})
	if result.IsError {
		t.Fatalf("hostfs_tree failed: %s", result.Content)
	}
	if !strings.Contains(result.Content, filepath.ToSlash(filepath.Join(root, "a"))+"/\n") ||
		!strings.Contains(result.Content, filepath.ToSlash(filepath.Join(root, "a", "one.go"))+"\n") {
		t.Fatalf("result missing expected entries:\n%s", result.Content)
	}
	if strings.Contains(result.Content, ".hidden") {
		t.Fatalf("hidden file leaked:\n%s", result.Content)
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
