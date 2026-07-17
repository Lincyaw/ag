package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestFileOperationStorePreservesIdempotencyAndCASAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	first, err := NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "durable-operation",
			IdempotencyKey: "trajectory-entry",
		},
		Kind:     sdk.OperationKindTool,
		Resource: "file-write",
		Input:    []byte(`{"path":"result.txt","content":"hello"}`),
	}
	created, wasCreated, err := first.Submit(ctx, record)
	if err != nil || !wasCreated {
		t.Fatalf("submit: record=%#v created=%v err=%v", created, wasCreated, err)
	}

	second, err := NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	replayed := record
	replayed.Operation.ID = "different-proposed-id"
	existing, wasCreated, err := second.Submit(ctx, replayed)
	if err != nil || wasCreated || existing.Operation.ID != created.Operation.ID {
		t.Fatalf("idempotent replay: record=%#v created=%v err=%v", existing, wasCreated, err)
	}
	running, err := second.Transition(
		ctx,
		created.Operation.ID,
		1,
		sdk.OperationRunning,
		nil,
		"",
	)
	if err != nil || running.Operation.Revision != 2 {
		t.Fatalf("transition running: %#v, %v", running, err)
	}

	third, err := NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := third.Transition(
		ctx,
		created.Operation.ID,
		2,
		sdk.OperationSucceeded,
		[]byte(`{"ok":true}`),
		"",
	)
	if err != nil || completed.Operation.Revision != 3 {
		t.Fatalf("complete after restart: %#v, %v", completed, err)
	}
	if _, err := first.Transition(
		ctx,
		created.Operation.ID,
		2,
		sdk.OperationFailed,
		nil,
		"stale worker",
	); !errors.Is(err, sdk.ErrOperationConflict) {
		t.Fatalf("stale transition = %v, want ErrOperationConflict", err)
	}
	loaded, err := first.Get(ctx, created.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Operation.State != sdk.OperationSucceeded || string(loaded.Operation.Output) != `{"ok":true}` {
		t.Fatalf("loaded operation = %#v", loaded)
	}
}
