package storage

import "github.com/lincyaw/ag/sdk"

// Keep the storage package honest: concrete persistence implementations must
// satisfy the SDK ports without the runtime depending on their concrete types.
var (
	_ sdk.TrajectoryStore = (*MemoryTrajectoryStore)(nil)
	_ sdk.TrajectoryStore = (*FileTrajectoryStore)(nil)

	_ sdk.OperationStore = (*MemoryOperationStore)(nil)
	_ sdk.OperationStore = (*FileOperationStore)(nil)

	_ sdk.DeliveryStore = (*MemoryDeliveryStore)(nil)
	_ sdk.DeliveryStore = (*FileDeliveryStore)(nil)

	_ sdk.StateBackend = (*memoryStateBackend)(nil)
	_ sdk.StateBackend = (*fileStateBackend)(nil)
)
