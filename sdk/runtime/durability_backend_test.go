package runtime

// Durability tests cover state-backend execution boundaries.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

func newTestStateBackend() sdk.StateBackend {
	return sdkstorage.NewMemoryStateBackend()
}

func testStateBackendWithStores(
	trajectories sdk.TrajectoryStore,
	operations sdk.OperationStore,
) sdk.StateBackend {
	backend := &testStateBackend{StateBackend: newTestStateBackend()}
	if trajectories != nil {
		backend.trajectories = trajectories
	}
	if operations != nil {
		backend.operations = operations
	}
	return backend
}

type testStateBackend struct {
	sdk.StateBackend
	trajectories sdk.TrajectoryStore
	operations   sdk.OperationStore
}

func (backend *testStateBackend) Trajectories() sdk.TrajectoryStore {
	if backend.trajectories != nil {
		return backend.trajectories
	}
	return backend.StateBackend.Trajectories()
}

func (backend *testStateBackend) Operations() sdk.OperationStore {
	if backend.operations != nil {
		return backend.operations
	}
	return backend.StateBackend.Operations()
}

type atomicTestBackend struct {
	sdk.StateBackend
	commits int
}

func (backend *atomicTestBackend) Capabilities() sdk.StorageCapabilities {
	capabilities := backend.StateBackend.Capabilities()
	capabilities.AtomicState = true
	return capabilities
}

func (backend *atomicTestBackend) AppendTrajectory(
	ctx context.Context,
	commit sdk.TrajectoryAppendCommit,
) (sdk.TrajectoryAppendResult, error) {
	head, err := backend.Trajectories().Append(
		ctx,
		commit.TrajectoryID,
		commit.ExpectedHead,
		commit.Entries...,
	)
	if err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		commit.TrajectoryID,
	)
	if err != nil {
		return sdk.TrajectoryAppendResult{}, err
	}
	if metadata.Head != head {
		return sdk.TrajectoryAppendResult{}, sdk.ErrTrajectoryConflict
	}
	return sdk.TrajectoryAppendResult{Trajectory: metadata}, nil
}

func (backend *atomicTestBackend) StartExecution(
	ctx context.Context,
	commit sdk.ExecutionStartCommit,
) (sdk.ExecutionMutationResult, error) {
	metadata, err := backend.Trajectories().BeginExecution(
		ctx,
		commit.TrajectoryID,
		commit.ExpectedHead,
		commit.Start,
		commit.Input,
	)
	return sdk.ExecutionMutationResult{Trajectory: metadata}, err
}

func (backend *atomicTestBackend) CommitExecution(
	ctx context.Context,
	commit sdk.ExecutionMutationCommit,
) (sdk.ExecutionMutationResult, error) {
	backend.commits++
	metadata, err := backend.Trajectories().CommitExecution(
		ctx,
		commit.Trajectory,
	)
	return sdk.ExecutionMutationResult{Trajectory: metadata}, err
}

func (backend *atomicTestBackend) CancelExecution(
	ctx context.Context,
	commit sdk.ExecutionCancelCommit,
) (sdk.ExecutionCancelResult, error) {
	result, err := backend.Trajectories().CancelExecution(
		ctx,
		commit.TrajectoryCommit(),
	)
	return sdk.ExecutionCancelResult{
		Trajectory: result.Trajectory,
		Changed:    result.Changed,
	}, err
}

func TestRuntimeRoutesExecutionCommitThroughAtomicBackend(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	if err := store.Create(
		ctx,
		sdk.Trajectory{ID: "atomic-runtime"},
	); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "atomic-runtime-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"atomic-runtime",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "atomic-runtime-execution",
			Provider: "test-provider",
			MaxTurns: 2,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	execution, err := store.ClaimExecution(
		ctx,
		"atomic-runtime",
		"atomic-runtime-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := runtime.commitTrajectoryExecution(
		ctx,
		sdk.TrajectoryExecutionCommit{
			TrajectoryID: "atomic-runtime",
			ExecutionID:  execution.ID,
			LeaseToken:   execution.LeaseToken,
			ExpectedHead: input.ID,
			Entries: []sdk.TrajectoryEntry{{
				ID:        "atomic-runtime-checkpoint",
				ParentID:  input.ID,
				Kind:      sdk.TrajectoryKindCheckpoint,
				Timestamp: time.Now().UTC(),
				Payload:   json.RawMessage(`{"checkpoint":true}`),
			}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if backend.commits != 1 ||
		metadata.Head != "atomic-runtime-checkpoint" {
		t.Fatalf(
			"atomic commits=%d metadata=%#v",
			backend.commits,
			metadata,
		)
	}
}
