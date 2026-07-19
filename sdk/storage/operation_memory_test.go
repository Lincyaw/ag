package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestMemoryOperationStoreConcurrentIdempotentSubmitAndCancel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	const workers = 64
	records := make(chan sdk.OperationRecord, workers)
	var created atomic.Int64
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			record, wasCreated, err := store.Submit(ctx, sdk.OperationRecord{
				Operation: sdk.Operation{
					ID:             sdk.NewID(),
					IdempotencyKey: "trajectory-entry-1",
				},
				Kind:     sdk.OperationKindTool,
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

	var cancelled atomic.Int64
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.Cancel(
				ctx,
				operationID,
				1,
			)
			switch {
			case err == nil:
				cancelled.Add(1)
			case errors.Is(err, sdk.ErrOperationConflict):
			default:
				t.Errorf("cancel: %v", err)
			}
		}()
	}
	wait.Wait()
	if got := cancelled.Load(); got != 1 {
		t.Fatalf("successful CAS cancellations = %d, want 1", got)
	}
	cancelledRecord, err := store.Get(ctx, operationID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelledRecord.Operation.Revision != 2 ||
		cancelledRecord.Operation.State != sdk.OperationCancelled {
		t.Fatalf("cancelled operation = %#v", cancelledRecord)
	}
	if _, err := store.Fail(
		ctx,
		operationID,
		2,
		"terminal operation should not fail again",
	); err == nil {
		t.Fatal("terminal operation accepted another failure")
	}
}

func TestOperationLeaseFencesExpiredWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	submitted, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "fenced-request"},
		Kind:      sdk.OperationKindTool,
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
	); !errors.Is(err, sdk.ErrOperationClaimed) {
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
		sdk.OperationSucceeded,
		[]byte(`{"winner":"a"}`),
		"",
	); !errors.Is(err, sdk.ErrOperationFence) {
		t.Fatalf("stale completion = %v, want ErrOperationFence", err)
	}
	completed, err := store.Complete(
		ctx,
		submitted.Operation.ID,
		second.Execution.Token,
		sdk.OperationSucceeded,
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

func TestMemoryOperationStoreListsRecoverableOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	now := time.Now().UTC()
	pending, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "pending"},
		Kind:      sdk.OperationKindTool,
		Resource:  "writer",
		Input:     []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	active, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "active"},
		Kind:      sdk.OperationKindTool,
		Resource:  "writer",
		Input:     []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(
		ctx,
		active.Operation.ID,
		"worker",
		now,
		time.Hour,
	); err != nil {
		t.Fatal(err)
	}
	expired, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "expired"},
		Kind:      sdk.OperationKindTool,
		Resource:  "writer",
		Input:     []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(
		ctx,
		expired.Operation.ID,
		"worker",
		now.Add(-2*time.Hour),
		time.Hour,
	); err != nil {
		t.Fatal(err)
	}
	done, _, err := store.Submit(ctx, sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "done"},
		Kind:      sdk.OperationKindTool,
		Resource:  "writer",
		Input:     []byte(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	done, err = store.Claim(
		ctx,
		done.Operation.ID,
		"worker",
		now,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(
		ctx,
		done.Operation.ID,
		done.Execution.Token,
		sdk.OperationSucceeded,
		[]byte(`{}`),
		"",
	); err != nil {
		t.Fatal(err)
	}
	nonTerminal, err := store.ListNonTerminal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := make(map[string]bool, len(nonTerminal))
	for _, record := range nonTerminal {
		open[record.Operation.ID] = true
	}
	if !open[pending.Operation.ID] ||
		!open[active.Operation.ID] ||
		!open[expired.Operation.ID] {
		t.Fatalf("non-terminal operations = %#v", open)
	}
	if open[done.Operation.ID] {
		t.Fatalf("terminal operation was non-terminal: %#v", open)
	}
	recoverable, err := store.ListRecoverable(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(recoverable))
	for _, record := range recoverable {
		got[record.Operation.ID] = true
	}
	if !got[pending.Operation.ID] || !got[expired.Operation.ID] {
		t.Fatalf("recoverable operations = %#v", got)
	}
	if got[active.Operation.ID] || got[done.Operation.ID] {
		t.Fatalf("active or terminal operation was recoverable: %#v", got)
	}
}

func TestMemoryOperationStoreRejectsIdempotencyKeyInputCollision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryOperationStore()
	base := sdk.OperationRecord{
		Operation: sdk.Operation{IdempotencyKey: "same-key"},
		Kind:      sdk.OperationKindProvider,
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
