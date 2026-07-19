package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitStatusReportsRepositoryContext(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	ctx := context.Background()
	root := t.TempDir()
	runGit(t, root, "init")
	runGit(t, root, "checkout", "-b", "main")
	runGit(t, root, "config", "user.name", "Test User")
	runGit(t, root, "config", "user.email", "test@example.invalid")
	if err := os.WriteFile(filepath.Join(root, "tracked.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "tracked.txt")
	runGit(t, root, "commit", "-m", "initial")
	if err := os.WriteFile(filepath.Join(root, "tracked.txt"), []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "new.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	repository, err := newRepository(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	result, err := statusTool{repository: repository}.Call(ctx, []byte(`{}`))
	if err != nil || result.IsError {
		t.Fatalf("git_status = %#v, %v", result, err)
	}
	for _, want := range []string{
		"root: " + root,
		"branch: main",
		"## main",
		" M tracked.txt",
		"?? new.txt",
	} {
		if !strings.Contains(result.Content, want) {
			t.Fatalf("git_status missing %q in:\n%s", want, result.Content)
		}
	}
}

func TestGitStatusRejectsUnknownArguments(t *testing.T) {
	t.Parallel()
	repository, err := newRepository(Config{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := statusTool{repository: repository}.Call(context.Background(), []byte(`{"path":"."}`))
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatalf("expected argument error, got %#v", result)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
