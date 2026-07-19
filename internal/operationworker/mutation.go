package operationworker

import (
	"context"
	"errors"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

type operationRecordMutation func(sdk.OperationRecord) (
	sdk.OperationRecord,
	bool,
	error,
)

func mutateNonTerminalOperation(
	ctx context.Context,
	store sdk.OperationStore,
	id string,
	label string,
	mutate operationRecordMutation,
) (record sdk.OperationRecord, changed bool, err error) {
	if store == nil {
		return sdk.OperationRecord{}, false, fmt.Errorf(
			"%s operation %q: operation store is nil",
			label,
			id,
		)
	}
	for {
		record, err = store.Get(ctx, id)
		if err != nil {
			return sdk.OperationRecord{}, false, err
		}
		if record.Operation.Terminal() {
			return record, false, nil
		}
		updated, changed, err := mutate(record)
		if errors.Is(err, sdk.ErrOperationConflict) {
			continue
		}
		if err != nil {
			return sdk.OperationRecord{}, false, err
		}
		return updated, changed, nil
	}
}
