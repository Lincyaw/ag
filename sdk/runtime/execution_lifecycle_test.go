package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func TestExecutionViewOmitsCancelledCheckpointResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryTrajectoryStore()
	const trajectoryID = "cancelled-view"
	const executionID = "cancelled-view-execution"
	if err := store.Create(ctx, sdk.Trajectory{ID: trajectoryID}); err != nil {
		t.Fatal(err)
	}
	inputPayload, err := json.Marshal(sdk.Message{
		Role:    sdk.RoleUser,
		Content: "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.BeginExecution(
		ctx,
		trajectoryID,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       executionID,
			Provider: "test",
			MaxTurns: 1,
		},
		sdk.TrajectoryEntry{
			ID:        "cancelled-view-input",
			Kind:      sdk.TrajectoryKindUserMessage,
			Timestamp: time.Now().UTC(),
			Payload:   inputPayload,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpointPayload, err := json.Marshal(durability.Checkpoint{
		Messages: []sdk.Message{{
			Role:    sdk.RoleAssistant,
			Content: "partial output",
		}},
		Output:   "partial output",
		Provider: "test",
		Turns:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		trajectoryID,
		"test-owner",
		time.Now().UTC(),
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err = store.CommitExecution(
		ctx,
		sdk.TrajectoryExecutionCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  executionID,
			LeaseToken:   execution.LeaseToken,
			ExpectedHead: metadata.Head,
			Entries: []sdk.TrajectoryEntry{{
				ID:        "cancelled-view-checkpoint",
				ParentID:  metadata.Head,
				Kind:      sdk.TrajectoryKindCheckpoint,
				Timestamp: time.Now().UTC(),
				Fields: sdk.TrajectoryEntryFields{
					ExecutionID: executionID,
				},
				Payload: checkpointPayload,
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.CancelExecution(
		ctx,
		sdk.TrajectoryExecutionCancelCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  executionID,
			Reason:       "user cancelled",
			At:           time.Now().UTC(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := LoadExecutionViewFromMetadata(ctx, store, cancelled.Trajectory)
	if err != nil {
		t.Fatal(err)
	}
	if view.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("execution state = %q", view.Execution.State)
	}
	if view.Result != nil {
		t.Fatalf("cancelled execution result = %#v, want nil", view.Result)
	}
}

func TestExecutionLifecycleClassifiesExecutionLookupStates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryTrajectoryStore()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)

	if err := store.Create(ctx, sdk.Trajectory{ID: "missing-execution"}); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadExecutionView(ctx, store, "missing-execution"); !errors.Is(err, ErrExecutionNotFound) ||
		!errors.Is(err, sdk.ErrTrajectoryExecution) {
		t.Fatalf("missing execution view error = %v", err)
	}
	if _, err := LoadExecutionRecoveryCandidate(ctx, store, "missing-execution", now); !errors.Is(err, ErrExecutionNotFound) ||
		!errors.Is(err, sdk.ErrTrajectoryExecution) {
		t.Fatalf("missing recovery candidate error = %v", err)
	}

	terminal := beginLifecycleTestExecution(
		t,
		ctx,
		store,
		"terminal-execution",
	)
	if _, err := store.CancelExecution(
		ctx,
		sdk.TrajectoryExecutionCancelCommit{
			TrajectoryID: terminal.ID,
			ExecutionID:  terminal.Execution.ID,
			Reason:       "done",
			At:           now,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadExecutionRecoveryCandidate(ctx, store, terminal.ID, now); !errors.Is(err, ErrExecutionNotRecoverable) ||
		!errors.Is(err, sdk.ErrTrajectoryExecution) {
		t.Fatalf("terminal recovery candidate error = %v", err)
	}
}

func TestExecutionLifecycleListsRecoveryCandidates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryTrajectoryStore()
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)

	pending := beginLifecycleTestExecution(
		t,
		ctx,
		store,
		"pending-candidate",
	)
	running := beginLifecycleTestExecution(
		t,
		ctx,
		store,
		"expired-running-candidate",
	)
	claimed, err := store.ClaimExecution(
		ctx,
		running.ID,
		"stale-worker",
		now.Add(-2*time.Minute),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := beginLifecycleTestExecution(
		t,
		ctx,
		store,
		"cancelled-candidate",
	)
	if _, err := store.CancelExecution(
		ctx,
		sdk.TrajectoryExecutionCancelCommit{
			TrajectoryID: cancelled.ID,
			ExecutionID:  cancelled.Execution.ID,
			Reason:       "test complete",
			At:           now,
		},
	); err != nil {
		t.Fatal(err)
	}

	lifecycle := NewExecutionLifecycle(store)
	lifecycle.now = func() time.Time { return now }
	candidates, err := lifecycle.ListRecoveryCandidates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("recovery candidates = %#v", candidates)
	}
	byTrajectory := map[string]ExecutionRecoveryCandidate{}
	for _, candidate := range candidates {
		byTrajectory[candidate.TrajectoryID] = candidate
	}
	if candidate := byTrajectory[pending.ID]; candidate.Execution.ID !=
		pending.Execution.ID || candidate.Delay != 0 {
		t.Fatalf("pending candidate = %#v", candidate)
	}
	if candidate := byTrajectory[running.ID]; candidate.Execution.ID !=
		claimed.ID || candidate.Delay != 0 {
		t.Fatalf("expired running candidate = %#v", candidate)
	}
	if _, exists := byTrajectory[cancelled.ID]; exists {
		t.Fatalf("cancelled trajectory was recoverable: %#v", candidates)
	}
}

func beginLifecycleTestExecution(
	t *testing.T,
	ctx context.Context,
	store sdk.TrajectoryStore,
	id string,
) sdk.TrajectoryMetadata {
	t.Helper()
	if err := store.Create(ctx, sdk.Trajectory{ID: id}); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(sdk.Message{
		Role:    sdk.RoleUser,
		Content: id,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := store.BeginExecution(
		ctx,
		id,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       id + "-execution",
			Provider: "test",
			MaxTurns: 1,
		},
		sdk.TrajectoryEntry{
			ID:        id + "-input",
			Kind:      sdk.TrajectoryKindUserMessage,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}
