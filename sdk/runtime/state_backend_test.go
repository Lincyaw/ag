package runtime

import (
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
