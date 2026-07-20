package file

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestFileToolsAcceptRelativeAndAbsolutePathsAndEnforceBounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "too-large.txt"), []byte("123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "binary"), []byte{0xff, 0xfe}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "secret-link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "outside-link")); err != nil {
		t.Fatal(err)
	}
	filesystem, err := newRootedFS(Config{
		Root: root, MaxReadBytes: 8, MaxWriteBytes: 8, MaxEntries: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	read := readTool{filesystem: filesystem}
	list := listTool{filesystem: filesystem}
	search := searchTool{filesystem: filesystem}
	write := writeTool{filesystem: filesystem}

	result, err := read.Call(ctx, []byte(`{"path":"hello.txt"}`))
	if err != nil || result.IsError ||
		!strings.Contains(result.Content, "lines: 1-1 of 1") ||
		!strings.Contains(result.Content, "1\thello") {
		t.Fatalf("read = %#v, %v", result, err)
	}
	absoluteResult, err := read.Call(ctx, json.RawMessage(`{"path":"`+filepath.ToSlash(filepath.Join(outside, "secret.txt"))+`"}`))
	if err != nil || absoluteResult.IsError || !strings.Contains(absoluteResult.Content, "1	secret") {
		t.Fatalf("absolute read = %#v, %v", absoluteResult, err)
	}
	traversalResult, err := read.Call(ctx, json.RawMessage(`{"path":"../outside/secret.txt"}`))
	if err != nil || traversalResult.IsError || !strings.Contains(traversalResult.Content, "1	secret") {
		t.Fatalf("parent traversal read = %#v, %v", traversalResult, err)
	}
	for name, arguments := range map[string]string{
		"symlink escape": `{"path":"secret-link"}`,
		"oversized":      `{"path":"too-large.txt"}`,
		"binary":         `{"path":"binary"}`,
		"unknown field":  `{"path":"hello.txt","extra":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result, err := read.Call(ctx, json.RawMessage(arguments))
			if err != nil || !result.IsError {
				t.Fatalf("unsafe/invalid read = %#v, %v", result, err)
			}
		})
	}
	absoluteList, err := list.Call(ctx, json.RawMessage(`{"path":"`+filepath.ToSlash(outside)+`"}`))
	if err != nil || absoluteList.IsError || !strings.Contains(absoluteList.Content, "file	secret.txt") {
		t.Fatalf("absolute list = %#v, %v", absoluteList, err)
	}
	searchResult, err := search.Call(ctx, json.RawMessage(`{"path":"`+filepath.ToSlash(outside)+`","query":"secret"}`))
	if err != nil || searchResult.IsError || !strings.Contains(searchResult.Content, "secret.txt:1:1: secret") {
		t.Fatalf("absolute search = %#v, %v", searchResult, err)
	}
	if result, err := list.Call(ctx, []byte(`{"path":"outside-link"}`)); err != nil || !result.IsError {
		t.Fatalf("listing a dangling workspace-relative symlink = %#v, %v", result, err)
	}
	absoluteWrite := callToolForTest(t, write, map[string]any{
		"path": filepath.Join(outside, "created.txt"), "content": "made",
	})
	if absoluteWrite.IsError {
		t.Fatalf("absolute write = %#v", absoluteWrite)
	}
	created, err := os.ReadFile(filepath.Join(outside, "created.txt"))
	if err != nil || string(created) != "made" {
		t.Fatalf("absolute write content = %q, %v", created, err)
	}
	if result, err := write.Call(
		ctx,
		[]byte(`{"path":"outside-link/new.txt","content":"bad"}`),
	); err != nil || !result.IsError {
		t.Fatalf("writing through a dangling workspace-relative parent symlink = %#v, %v", result, err)
	}
	if result, err := write.Call(
		ctx,
		[]byte(`{"path":"secret-link","content":"bad"}`),
	); err != nil || !result.IsError {
		t.Fatalf("replacing a symlink = %#v, %v", result, err)
	}
	if result, err := write.Call(
		ctx,
		[]byte(`{"path":"sub/new.txt","content":"123456789"}`),
	); err != nil || !result.IsError {
		t.Fatalf("oversized write = %#v, %v", result, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := write.Call(cancelled, []byte(`{"path":"sub/new.txt","content":"ok"}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write = %v", err)
	}
}

func TestConcurrentWritesAreAtomicAndPluginManifestMatchesMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	filesystem, err := newRootedFS(Config{
		Root: root, MaxWriteBytes: 32 << 10, MaxReadBytes: 32 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	write := writeTool{filesystem: filesystem}
	read := readTool{filesystem: filesystem}
	const writers = 32
	contents := make(map[string]struct{}, writers)
	var contentsMu sync.Mutex
	var wait sync.WaitGroup
	errorsChannel := make(chan error, writers)
	resultsChannel := make(chan sdk.ToolResult, writers)
	for index := range writers {
		content := strings.Repeat(string(rune('a'+index%26)), 4096) + string(rune('0'+index%10))
		contentsMu.Lock()
		contents[content] = struct{}{}
		contentsMu.Unlock()
		wait.Add(1)
		go func(content string) {
			defer wait.Done()
			raw, _ := json.Marshal(map[string]string{"path": "shared.txt", "content": content})
			result, err := write.Call(ctx, raw)
			if err != nil {
				errorsChannel <- err
				return
			}
			resultsChannel <- result
		}(content)
	}
	wait.Wait()
	close(errorsChannel)
	close(resultsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent write: %v", err)
	}
	successes := 0
	conflicts := 0
	for result := range resultsChannel {
		if result.IsError {
			conflicts++
		} else {
			successes++
		}
	}
	if successes != 1 || conflicts != writers-1 {
		t.Fatalf("concurrent writes: successes=%d conflicts=%d", successes, conflicts)
	}
	result, err := read.Call(ctx, []byte(`{"path":"shared.txt"}`))
	if err != nil || result.IsError {
		t.Fatalf("read final file = %#v, %v", result, err)
	}
	onDisk, err := os.ReadFile(filepath.Join(root, "shared.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := contents[string(onDisk)]; !exists {
		t.Fatalf("final file is a torn write: length=%d", len(onDisk))
	}
	temporary, err := filepath.Glob(filepath.Join(root, ".agentm-file-*.tmp"))
	if err != nil || len(temporary) != 0 {
		t.Fatalf("temporary files = %v, %v", temporary, err)
	}

	for _, test := range []struct {
		name        string
		enableWrite bool
		toolCount   int
	}{
		{name: "read-only", toolCount: 3},
		{name: "writable", enableWrite: true, toolCount: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
				Storage: sdkstorage.NewMemoryStateBackend(),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				closeCtx, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				if err := runtime.Close(closeCtx); err != nil {
					t.Errorf("close runtime: %v", err)
				}
			}()
			if _, err := runtime.Mount(ctx, sdk.Local(New(Config{
				Root: root, EnableWrite: test.enableWrite,
			}))); err != nil {
				t.Fatal(err)
			}
			if got := len(runtime.Catalog().Tools); got != test.toolCount {
				t.Fatalf("tool count = %d, want %d", got, test.toolCount)
			}
		})
	}
}

func TestWorkspaceRelativeOperationsRemainConfinedAfterParentSwap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	outside := t.TempDir()
	parent := filepath.Join(root, "parent")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "secret.txt"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	filesystem, err := newRootedFS(Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	readPath, err := filesystem.existing("parent/secret.txt")
	if err != nil {
		t.Fatal(err)
	}
	writePath, err := filesystem.writable("parent/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(parent, filepath.Join(root, "original-parent")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, parent); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	if _, _, err := filesystem.readText(readPath); err == nil {
		t.Fatal("read followed a swapped parent outside the root")
	}
	filesystem.writeMu.Lock()
	err = filesystem.atomicWrite(ctx, writePath, []byte("escaped"), 0o600)
	filesystem.writeMu.Unlock()
	if err == nil {
		t.Fatal("write followed a swapped parent outside the root")
	}
	if _, err := os.Stat(filepath.Join(outside, "new.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside write result = %v, want file not found", err)
	}
}
