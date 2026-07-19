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

func TestExecutionViewProjectsTerminalResultOffActiveBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryTrajectoryStore()
	const trajectoryID = "terminal-view"
	const executionID = "terminal-view-execution"
	const terminalID = "terminal-view-end"
	if err := store.Create(ctx, sdk.Trajectory{ID: trajectoryID}); err != nil {
		t.Fatal(err)
	}
	inputPayload, err := json.Marshal(sdk.Message{
		Role:    sdk.RoleUser,
		Content: "stop after this",
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
			ID:        "terminal-view-input",
			Kind:      sdk.TrajectoryKindUserMessage,
			Timestamp: time.Now().UTC(),
			Payload:   inputPayload,
		},
	)
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
	terminalPayload, err := json.Marshal(sdk.AgentEndPayload{
		Messages: []sdk.Message{
			{Role: sdk.RoleUser, Content: "stop after this"},
			{Role: sdk.RoleAssistant, Content: "message fallback"},
		},
		Output:    "stopped cleanly",
		Turns:     2,
		ToolCalls: 3,
		Cause: sdk.Cause{
			Code:   sdk.CauseCancelled,
			Detail: "user cancelled",
			Final:  true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	restorePayload, err := json.Marshal(map[string]string{
		"from": terminalID,
		"to":   metadata.Execution.BaseHead,
	})
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
			State:        sdk.TrajectoryExecutionCancelled,
			Error:        "user cancelled",
			Entries: []sdk.TrajectoryEntry{
				{
					ID:         terminalID,
					ParentID:   metadata.Head,
					Kind:       sdk.TrajectoryKindTerminal,
					Generation: 7,
					Timestamp:  time.Now().UTC(),
					Payload:    terminalPayload,
				},
				{
					ID:        "terminal-view-restore",
					ParentID:  metadata.Execution.BaseHead,
					Kind:      sdk.TrajectoryKindRestore,
					Timestamp: time.Now().UTC(),
					Payload:   restorePayload,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := store.LoadBranch(ctx, trajectoryID, metadata.Head)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range branch {
		if entry.ID == terminalID {
			t.Fatalf("terminal entry %q remained on active branch", terminalID)
		}
	}
	view, err := LoadExecutionViewFromMetadata(ctx, store, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if view.Result == nil {
		t.Fatal("cancelled execution result is nil")
	}
	if view.Result.Output != "stopped cleanly" ||
		view.Result.Turns != 2 ||
		view.Result.ToolCalls != 3 ||
		view.Result.Cause.Code != sdk.CauseCancelled ||
		view.Result.Cause.Detail != "user cancelled" ||
		!view.Result.Cause.Final ||
		view.Result.Generation != 7 {
		t.Fatalf("cancelled execution result = %#v", view.Result)
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

func TestExecutionControlReadsBorrowedRuntimeLifecycleAfterClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newTestStateBackend()
	metadata := beginLifecycleTestExecution(
		t,
		ctx,
		backend.Trajectories(),
		"borrowed-runtime-read",
	)
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}

	view, err := NewRuntimeExecutionControl(runtime).LoadView(ctx, metadata.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Execution.ID != metadata.Execution.ID {
		t.Fatalf("borrowed runtime closed view = %#v", view)
	}
}

func TestExecutionControlRejectsOwnedRuntimeReadAfterClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newTestStateBackend()
	metadata := beginLifecycleTestExecution(
		t,
		ctx,
		backend.Trajectories(),
		"owned-runtime-read",
	)
	runtime, err := NewRuntime(RuntimeConfig{Storage: backend})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := NewRuntimeExecutionControl(runtime).LoadView(
		ctx,
		metadata.ID,
	); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("owned runtime closed read error = %v", err)
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
