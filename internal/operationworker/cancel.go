package operationworker

import (
	"context"

	"github.com/lincyaw/ag/sdk"
)

// requestCancel records a non-lease cancellation request. Host.Cancel binds this
// durable request to host-local in-flight cancellation.
func requestCancel(
	ctx context.Context,
	store sdk.OperationStore,
	id string,
	validate func(sdk.OperationRecord) error,
) (record sdk.OperationRecord, requested bool, err error) {
	return mutateNonTerminalOperation(
		ctx,
		store,
		id,
		"cancel",
		func(record sdk.OperationRecord) (
			sdk.OperationRecord,
			bool,
			error,
		) {
			if validate != nil {
				if err := validate(record); err != nil {
					return sdk.OperationRecord{}, false, err
				}
			}
			cancelled, err := store.Cancel(
				ctx,
				id,
				record.Operation.Revision,
			)
			return cancelled, true, err
		},
	)
}
