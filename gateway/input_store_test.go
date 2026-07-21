package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInputStoreContract(t *testing.T) {
	factories := map[string]func(*testing.T) InputStore{
		"memory": func(*testing.T) InputStore { return NewMemoryInputStore() },
		"file": func(t *testing.T) InputStore {
			store, err := NewFileInputStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
		"gorm-sqlite": func(t *testing.T) InputStore {
			store, err := NewGORMInputStore(
				t.Context(), gormEventStoreTestURI(t, "inputs"),
			)
			if err != nil {
				t.Fatal(err)
			}
			return store
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			testInputStoreContract(t, factory(t))
		})
	}
}

func testInputStoreContract(t *testing.T, store InputStore) {
	t.Helper()
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	first, err := store.Enqueue(t.Context(), AgentInput{
		ID: "input-a", SessionID: "session-a", Content: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.Enqueue(t.Context(), AgentInput{
		ID: "input-a", SessionID: "session-a", Content: "first",
	})
	if err != nil || duplicate.Sequence != first.Sequence {
		t.Fatalf("duplicate = %#v, %v", duplicate, err)
	}
	if _, err := store.Enqueue(t.Context(), AgentInput{
		ID: "input-a", SessionID: "session-a", Content: "changed",
	}); !errors.Is(err, ErrInputConflict) {
		t.Fatalf("reused ID error = %v", err)
	}
	second, err := store.Enqueue(t.Context(), AgentInput{
		ID: "input-b", SessionID: "session-a", Content: "second",
	})
	if err != nil || second.Sequence != first.Sequence+1 {
		t.Fatalf("second = %#v, %v", second, err)
	}
	page, err := store.List(t.Context(), "session-a", InputQuery{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.Next != first.Sequence {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	acquired, ok, err := store.AcquireNext(t.Context(), "session-a")
	if err != nil || !ok || acquired.Resumed || acquired.Input.ID != first.ID ||
		acquired.Input.State != AgentInputDispatching {
		t.Fatalf("acquired = %#v, %v, %v", acquired, ok, err)
	}
	resumed, ok, err := store.AcquireNext(t.Context(), "session-a")
	if err != nil || !ok || !resumed.Resumed || resumed.Input.ID != first.ID {
		t.Fatalf("resumed = %#v, %v, %v", resumed, ok, err)
	}
	bound, err := store.BindExecution(
		t.Context(), "session-a", first.ID, "execution-a",
	)
	if err != nil || bound.ExecutionID != "execution-a" {
		t.Fatalf("bound = %#v, %v", bound, err)
	}
	completed, err := store.Complete(
		t.Context(), "session-a", first.ID, AgentInputSucceeded, "",
	)
	if err != nil || completed.State != AgentInputSucceeded {
		t.Fatalf("completed = %#v, %v", completed, err)
	}
	next, ok, err := store.AcquireNext(t.Context(), "session-a")
	if err != nil || !ok || next.Input.ID != second.ID {
		t.Fatalf("next = %#v, %v, %v", next, ok, err)
	}
	if _, err := store.Complete(
		t.Context(), "session-a", second.ID, AgentInputSucceeded, "",
	); err != nil {
		t.Fatal(err)
	}
	third, err := store.Enqueue(t.Context(), AgentInput{
		ID: "input-c", SessionID: "session-a", Content: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelQueued(
		t.Context(), "session-a", third.ID, third.Revision,
	)
	if err != nil || cancelled.State != AgentInputCancelled {
		t.Fatalf("cancelled = %#v, %v", cancelled, err)
	}
}

func TestFileInputStorePersistsPrivateState(t *testing.T) {
	directory := t.TempDir()
	store, err := NewFileInputStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(t.Context(), AgentInput{
		ID: "persistent", SessionID: "session-a", Content: "remember",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileInputStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if _, err := reopened.Get(t.Context(), "session-a", "persistent"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "inputs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("input state mode = %o", info.Mode().Perm())
	}
}
