package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/lincyaw/ag/sdk"
)

func TestMemoryOperationStoreConcurrentIdempotentSubmitAndCAS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	const workers = 64
	records := make(chan OperationRecord, workers)
	var created atomic.Int64
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			record, wasCreated, err := store.Submit(ctx, OperationRecord{
				Operation: Operation{
					ID:             NewID(),
					IdempotencyKey: "trajectory-entry-1",
				},
				Kind:     OperationKindTool,
				Resource: "bash",
				Input:    []byte(`{"command":"printf ok"}`),
			})
			if err != nil {
				t.Errorf("submit %d: %v", index, err)
				return
			}
			if wasCreated {
				created.Add(1)
			}
			records <- record
		}(index)
	}
	wait.Wait()
	close(records)
	if got := created.Load(); got != 1 {
		t.Fatalf("created operations = %d, want 1", got)
	}
	var operationID string
	for record := range records {
		if operationID == "" {
			operationID = record.Operation.ID
		}
		if record.Operation.ID != operationID || record.Operation.Revision != 1 {
			t.Fatalf("idempotent submit returned %#v, canonical ID %q", record, operationID)
		}
	}

	var transitioned atomic.Int64
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.Transition(
				ctx,
				operationID,
				1,
				OperationRunning,
				nil,
				"",
			)
			switch {
			case err == nil:
				transitioned.Add(1)
			case errors.Is(err, ErrOperationConflict):
			default:
				t.Errorf("transition: %v", err)
			}
		}()
	}
	wait.Wait()
	if got := transitioned.Load(); got != 1 {
		t.Fatalf("successful CAS transitions = %d, want 1", got)
	}
	completed, err := store.Transition(
		ctx,
		operationID,
		2,
		OperationSucceeded,
		[]byte(`{"content":"ok"}`),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Operation.Revision != 3 || completed.Operation.State != OperationSucceeded {
		t.Fatalf("completed operation = %#v", completed)
	}
	if _, err := store.Transition(
		ctx,
		operationID,
		3,
		OperationRunning,
		nil,
		"",
	); err == nil {
		t.Fatal("terminal operation transitioned back to running")
	}
}

func TestOperationLeaseFencesExpiredWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	submitted, _, err := store.Submit(ctx, OperationRecord{
		Operation: Operation{IdempotencyKey: "fenced-request"},
		Kind:      OperationKindTool,
		Resource:  "writer",
		Input:     []byte(`{"value":"one"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	first, err := store.Claim(
		ctx,
		submitted.Operation.ID,
		"worker-a",
		now,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(
		ctx,
		submitted.Operation.ID,
		"worker-b",
		now.Add(30*time.Minute),
		time.Hour,
	); !errors.Is(err, ErrOperationClaimed) {
		t.Fatalf("live lease claim = %v, want ErrOperationClaimed", err)
	}
	second, err := store.Claim(
		ctx,
		submitted.Operation.ID,
		"worker-b",
		now.Add(2*time.Hour),
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Execution.Token == second.Execution.Token {
		t.Fatal("takeover reused the prior fencing token")
	}
	if _, err := store.Complete(
		ctx,
		submitted.Operation.ID,
		first.Execution.Token,
		OperationSucceeded,
		[]byte(`{"winner":"a"}`),
		"",
	); !errors.Is(err, ErrOperationFence) {
		t.Fatalf("stale completion = %v, want ErrOperationFence", err)
	}
	completed, err := store.Complete(
		ctx,
		submitted.Operation.ID,
		second.Execution.Token,
		OperationSucceeded,
		[]byte(`{"winner":"b"}`),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(completed.Operation.Output) != `{"winner":"b"}` {
		t.Fatalf("completed output = %s", completed.Operation.Output)
	}
}

func TestMemoryOperationStoreRejectsIdempotencyKeyInputCollision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	base := OperationRecord{
		Operation: Operation{IdempotencyKey: "same-key"},
		Kind:      OperationKindProvider,
		Resource:  "model",
		Input:     []byte(`{"prompt":"one"}`),
	}
	if _, created, err := store.Submit(ctx, base); err != nil || !created {
		t.Fatalf("first submit: created=%v err=%v", created, err)
	}
	base.Input = []byte(`{"prompt":"different"}`)
	if _, _, err := store.Submit(ctx, base); err == nil {
		t.Fatal("same idempotency key with different input was accepted")
	}
	listed, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("operation count = %d, want 1", len(listed))
	}
}
