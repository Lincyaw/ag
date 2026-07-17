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
)

func TestFileToolsConfinePathsAndEnforceBounds(t *testing.T) {
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
	write := writeTool{filesystem: filesystem}

	result, err := read.Call(ctx, []byte(`{"path":"hello.txt"}`))
	if err != nil || result.IsError ||
		!strings.Contains(result.Content, "lines: 1-1 of 1") ||
		!strings.Contains(result.Content, "1\thello") {
		t.Fatalf("read = %#v, %v", result, err)
	}
	for name, arguments := range map[string]string{
		"parent traversal": `{"path":"../outside/secret.txt"}`,
		"absolute path":    `{"path":"` + filepath.ToSlash(filepath.Join(outside, "secret.txt")) + `"}`,
		"symlink escape":   `{"path":"secret-link"}`,
		"oversized":        `{"path":"too-large.txt"}`,
		"binary":           `{"path":"binary"}`,
		"unknown field":    `{"path":"hello.txt","extra":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result, err := read.Call(ctx, json.RawMessage(arguments))
			if err != nil || !result.IsError {
				t.Fatalf("unsafe/invalid read = %#v, %v", result, err)
			}
		})
	}
	if result, err := list.Call(ctx, []byte(`{"path":"outside-link"}`)); err != nil || !result.IsError {
		t.Fatalf("listing an escaping directory symlink = %#v, %v", result, err)
	}
	if result, err := write.Call(
		ctx,
		[]byte(`{"path":"outside-link/new.txt","content":"bad"}`),
	); err != nil || !result.IsError {
		t.Fatalf("writing through an escaping parent symlink = %#v, %v", result, err)
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
			runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{})
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
