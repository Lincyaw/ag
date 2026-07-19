package operationworker

import (
	"context"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestRecoveryCandidateCarriesLeaseDelay(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	candidate, ok := RecoveryCandidateFromRecord(sdk.OperationRecord{
		Operation: sdk.Operation{
			ID:             "delayed-operation",
			IdempotencyKey: "delayed-operation",
			State:          sdk.OperationRunning,
			Revision:       2,
		},
		Kind:     sdk.OperationKindTool,
		Resource: "writer",
		Input:    []byte(`{}`),
		Execution: &sdk.OperationLease{
			Owner:     "worker",
			Token:     "lease",
			ExpiresAt: now.Add(time.Minute),
		},
	}, now)
	if !ok {
		t.Fatal("running operation was not a recovery candidate")
	}
	if candidate.OperationID != "delayed-operation" ||
		candidate.Delay != time.Minute {
		t.Fatalf("candidate = %#v", candidate)
	}

	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := candidate.Wait(waitCtx); err != context.Canceled {
		t.Fatalf("cancelled wait = %v", err)
	}
}
