package file

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestReadEditAndWriteUseExplicitFileRevisions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "notes.txt")
	source := "alpha\nshared value\nomega\nshared value\n"
	if err := os.WriteFile(path, []byte(source), 0o640); err != nil {
		t.Fatal(err)
	}
	filesystem, err := newRootedFS(Config{
		Root: root, MaxReadBytes: 4096, MaxWriteBytes: 4096, MaxEntries: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	read := readTool{filesystem: filesystem}
	edit := editTool{filesystem: filesystem}
	write := writeTool{filesystem: filesystem}

	missingContent := callToolForTest(t, write, map[string]any{"path": "empty.txt"})
	if !missingContent.IsError || !strings.Contains(missingContent.Content, "content is required") {
		t.Fatalf("missing write content = %#v", missingContent)
	}
	readResult, err := read.Call(ctx, []byte(`{"path":"notes.txt","offset":2,"limit":2}`))
	if err != nil || readResult.IsError {
		t.Fatalf("read = %#v, %v", readResult, err)
	}
	for _, expected := range []string{
		`file: "notes.txt"`,
		"bytes: 38",
		"lines: 2-3 of 4",
		"2\tshared value",
		"3\tomega",
	} {
		if !strings.Contains(readResult.Content, expected) {
			t.Fatalf("read result %q missing %q", readResult.Content, expected)
		}
	}
	revision := revisionFromResult(t, readResult.Content)

	ambiguous := callToolForTest(t, edit, map[string]any{
		"path":            "notes.txt",
		"expected_sha256": revision,
		"old_text":        "shared value",
		"new_text":        "changed",
	})
	if !ambiguous.IsError || !strings.Contains(ambiguous.Content, "matched 2 locations") {
		t.Fatalf("ambiguous edit = %#v", ambiguous)
	}
	missingNewText := callToolForTest(t, edit, map[string]any{
		"path":            "notes.txt",
		"expected_sha256": revision,
		"old_text":        "alpha",
	})
	if !missingNewText.IsError ||
		!strings.Contains(missingNewText.Content, "new_text is required") {
		t.Fatalf("missing edit replacement = %#v", missingNewText)
	}

	edited := callToolForTest(t, edit, map[string]any{
		"path":            "notes.txt",
		"expected_sha256": revision,
		"old_text":        "alpha\nshared value",
		"new_text":        "alpha\nfirst value",
	})
	if edited.IsError ||
		!strings.Contains(edited.Content, "replacements: 1") ||
		!strings.Contains(edited.Content, "2\tfirst value") {
		t.Fatalf("exact edit = %#v", edited)
	}
	updatedRevision := revisionFromResult(t, edited.Content)
	if updatedRevision == revision {
		t.Fatal("edit did not advance revision")
	}

	stale := callToolForTest(t, edit, map[string]any{
		"path":            "notes.txt",
		"expected_sha256": revision,
		"start_line":      3,
		"end_line":        3,
		"new_text":        "new omega",
	})
	if !stale.IsError || !strings.Contains(stale.Content, "stale file revision") {
		t.Fatalf("stale edit = %#v", stale)
	}

	lineEdit := callToolForTest(t, edit, map[string]any{
		"path":            "notes.txt",
		"expected_sha256": updatedRevision,
		"start_line":      3,
		"end_line":        4,
		"new_text":        "new omega",
	})
	if lineEdit.IsError || !strings.Contains(lineEdit.Content, "3\tnew omega") {
		t.Fatalf("line edit = %#v", lineEdit)
	}
	lineRevision := revisionFromResult(t, lineEdit.Content)

	missingRevision := callToolForTest(t, write, map[string]any{
		"path": "notes.txt", "content": "replace everything\n",
	})
	if !missingRevision.IsError ||
		!strings.Contains(missingRevision.Content, "expected_sha256 is required") {
		t.Fatalf("unguarded overwrite = %#v", missingRevision)
	}
	overwritten := callToolForTest(t, write, map[string]any{
		"path":            "notes.txt",
		"content":         "replace everything\n",
		"expected_sha256": lineRevision,
	})
	if overwritten.IsError || !strings.Contains(overwritten.Content, "bytes: 19") {
		t.Fatalf("guarded overwrite = %#v", overwritten)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("overwrite mode = %o, want 640", info.Mode().Perm())
	}
}

func TestLineEditPreservesCRLF(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	path := filepath.Join(root, "windows.txt")
	source := "first\r\nsecond\r\nthird\r\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	filesystem, err := newRootedFS(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	result := callToolForTest(t, editTool{filesystem: filesystem}, map[string]any{
		"path":            "windows.txt",
		"expected_sha256": fileRevision([]byte(source)),
		"start_line":      2,
		"end_line":        2,
		"new_text":        "changed",
	})
	if result.IsError {
		t.Fatalf("CRLF edit = %#v", result)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(written), "first\r\nchanged\r\nthird\r\n"; got != want {
		t.Fatalf("CRLF edit = %q, want %q", got, want)
	}
}

func TestSearchFilesSupportsLiteralRegexGlobAndLimits(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for relative, content := range map[string]string{
		"main.go":          "package main\n// Needle here\n",
		"nested/worker.go": "package nested\nvar needle = 1\nvar Needle = 2\n",
		"nested/worker.py": "needle = 'python'\n",
		".hidden.go":       "var needle = true\n",
	} {
		path := filepath.Join(root, relative)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	filesystem, err := newRootedFS(Config{
		Root: root, MaxReadBytes: 4096, MaxWriteBytes: 4096, MaxEntries: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	search := searchTool{filesystem: filesystem}

	caseInsensitive := false
	result := callToolForTest(t, search, map[string]any{
		"query":          "needle",
		"glob":           "**/*.go",
		"case_sensitive": caseInsensitive,
	})
	if result.IsError {
		t.Fatalf("literal search = %#v", result)
	}
	for _, expected := range []string{
		"matches: 3",
		"main.go:2:4: // Needle here",
		"nested/worker.go:2:5: var needle = 1",
		"nested/worker.go:3:5: var Needle = 2",
	} {
		if !strings.Contains(result.Content, expected) {
			t.Fatalf("search result %q missing %q", result.Content, expected)
		}
	}
	if strings.Contains(result.Content, ".hidden.go") ||
		strings.Contains(result.Content, "worker.py") {
		t.Fatalf("search ignored filters: %q", result.Content)
	}

	regexResult := callToolForTest(t, search, map[string]any{
		"path":        "nested",
		"query":       `needle\s*=\s*\d`,
		"regex":       true,
		"glob":        "*.go",
		"max_results": 1,
	})
	if regexResult.IsError ||
		!strings.Contains(regexResult.Content, "matches: 1") ||
		!strings.Contains(regexResult.Content, "truncated: true") {
		t.Fatalf("regex search = %#v", regexResult)
	}

	invalid := callToolForTest(t, search, map[string]any{
		"query": "[", "regex": true,
	})
	if !invalid.IsError || !strings.Contains(invalid.Content, "compile query") {
		t.Fatalf("invalid regex = %#v", invalid)
	}
}

func callToolForTest(
	t *testing.T,
	tool interface {
		Call(context.Context, json.RawMessage) (result sdk.ToolResult, err error)
	},
	arguments map[string]any,
) sdk.ToolResult {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Call(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func revisionFromResult(t *testing.T, content string) string {
	t.Helper()
	match := regexp.MustCompile(`(?m)^sha256: ([0-9a-f]{64})$`).FindStringSubmatch(content)
	if match == nil {
		t.Fatalf("result has no sha256 revision: %q", content)
	}
	return match[1]
}
