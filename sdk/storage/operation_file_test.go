package storage

import (
	"context"
	"errors"
	"testing"
	"time"

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
	running, err := second.Claim(
		ctx,
		created.Operation.ID,
		"file-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil || running.Operation.Revision != 2 {
		t.Fatalf("claim running: %#v, %v", running, err)
	}

	third, err := NewFileOperationStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := third.Complete(
		ctx,
		created.Operation.ID,
		running.Execution.Token,
		sdk.OperationSucceeded,
		[]byte(`{"ok":true}`),
		"",
	)
	if err != nil || completed.Operation.Revision != 3 {
		t.Fatalf("complete after restart: %#v, %v", completed, err)
	}
	if _, err := first.Fail(
		ctx,
		created.Operation.ID,
		2,
		"stale worker",
	); !errors.Is(err, sdk.ErrOperationConflict) {
		t.Fatalf("stale failure = %v, want ErrOperationConflict", err)
	}
	loaded, err := first.Get(ctx, created.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Operation.State != sdk.OperationSucceeded || string(loaded.Operation.Output) != `{"ok":true}` {
		t.Fatalf("loaded operation = %#v", loaded)
	}
}
