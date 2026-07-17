package filestate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrepareDirectoryCreatesPrivateAbsoluteDirectory(t *testing.T) {
	directory, err := PrepareDirectory(
		"test state",
		filepath.Join(t.TempDir(), "nested"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(directory) {
		t.Fatalf("directory %q is not absolute", directory)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v", info.Mode())
	}
	_, err = PrepareDirectory("test state", " ")
	if err == nil || !strings.Contains(err.Error(), "directory is empty") {
		t.Fatalf("empty directory error = %v", err)
	}
}

func TestWriteJSONPublishesPrivateFileAndCleansTemporary(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	if err := WriteJSON(
		t.Context(),
		directory,
		path,
		"test state",
		map[string]int{"revision": 2},
	); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"revision":2}` {
		t.Fatalf("state = %s", raw)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %v", info.Mode())
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary files = %v", temporary)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = WriteJSON(
		ctx,
		directory,
		path,
		"test state",
		map[string]int{"revision": 3},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled write error = %v", err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"revision":2}` {
		t.Fatalf("state after cancelled write = %s", raw)
	}
}

func TestExclusiveLockSerializesActions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.lock")
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- WithExclusiveLock(path, func() error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	secondStarted := make(chan struct{})
	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		close(secondStarted)
		secondDone <- WithExclusiveLock(path, func() error {
			close(secondEntered)
			return nil
		})
	}()
	<-secondStarted
	enteredBeforeRelease := false
	select {
	case <-secondEntered:
		enteredBeforeRelease = true
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if !enteredBeforeRelease {
		select {
		case <-secondEntered:
		case <-time.After(time.Second):
			t.Fatal("second action did not enter after exclusive lock release")
		}
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if enteredBeforeRelease {
		t.Fatal("second action entered while exclusive lock was held")
	}
}
