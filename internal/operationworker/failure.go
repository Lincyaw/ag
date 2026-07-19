package operationworker

import (
	"context"

	"github.com/lincyaw/ag/sdk"
)

// FailInvalid records a non-lease failure when an operation can no longer be
// executed by the current resource definition.
func FailInvalid(
	ctx context.Context,
	store sdk.OperationStore,
	id string,
	validate func(sdk.OperationRecord) error,
) (record sdk.OperationRecord, failed bool, err error) {
	return mutateNonTerminalOperation(
		ctx,
		store,
		id,
		"fail invalid",
		func(record sdk.OperationRecord) (
			sdk.OperationRecord,
			bool,
			error,
		) {
			if validate == nil {
				return record, false, nil
			}
			invalidErr := validate(record)
			if invalidErr == nil {
				return record, false, nil
			}
			failedRecord, err := store.Fail(
				ctx,
				id,
				record.Operation.Revision,
				invalidErr.Error(),
			)
			return failedRecord, true, err
		},
	)
}
