package runtime

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

func (backend *atomicTestBackend) CommitExecutionStep(
	ctx context.Context,
	commit sdk.ExecutionStepCommit,
) (sdk.ExecutionStepResult, error) {
	backend.commits++
	metadata, err := backend.Trajectories().CommitExecution(
		ctx,
		commit.Trajectory,
	)
	return sdk.ExecutionStepResult{Trajectory: metadata}, err
}

func TestRuntimeRoutesExecutionCommitThroughAtomicBackend(t *testing.T) {
	base := newTestStateBackend()
	backend := &atomicTestBackend{StateBackend: base}
	store := backend.Trajectories()
	ctx := t.Context()
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
	runtime := &Runtime{
		storage:      backend,
		trajectories: store,
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
