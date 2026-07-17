package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestOperationStoreValidatesAndClonesInvocationMetadata(
	t *testing.T,
) {
	t.Parallel()
	store := NewMemoryOperationStore()
	record := sdk.OperationRecord{
		Operation: sdk.Operation{
			IdempotencyKey: "invocation-key",
		},
		Kind:     sdk.OperationKindTool,
		Resource: "tool",
		Input:    json.RawMessage(`{}`),
		Invocation: sdk.Invocation{
			ID:           "tool-call",
			RootID:       "root",
			ParentID:     "root",
			SessionID:    "session",
			ExecutionID:  "execution",
			Dependencies: []string{"provider-call"},
		},
	}
	submitted, created, err := store.Submit(
		context.Background(),
		record,
	)
	if err != nil || !created {
		t.Fatalf("submit operation: created=%v err=%v", created, err)
	}
	submitted.Invocation.Dependencies[0] = "mutated"
	loaded, err := store.Get(
		context.Background(),
		submitted.Operation.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Invocation.Dependencies[0] != "provider-call" {
		t.Fatal("operation store aliased invocation dependencies")
	}
	invalid := record
	invalid.Operation.IdempotencyKey = "invalid-invocation"
	invalid.Invocation.RootID = ""
	if _, _, err := store.Submit(
		context.Background(),
		invalid,
	); err == nil || !strings.Contains(
		err.Error(),
		"invocation root is empty",
	) {
		t.Fatalf("invalid invocation error = %v", err)
	}
}
