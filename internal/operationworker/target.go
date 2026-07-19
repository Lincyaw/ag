package operationworker

import (
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

// Target is the stable resource address for a durable operation. The revision
// fences execution against a changed resource definition, while cancellation can
// still address the operation by kind and resource alone.
type Target struct {
	Kind             sdk.OperationKind
	Resource         string
	ResourceRevision string
}

func (target Target) Record(request sdk.OperationRequest) sdk.OperationRecord {
	request = sdk.CloneOperationRequest(request)
	return sdk.OperationRecord{
		Operation:        sdk.Operation{IdempotencyKey: request.IdempotencyKey},
		Kind:             target.Kind,
		Resource:         target.Resource,
		ResourceRevision: target.ResourceRevision,
		Input:            request.Input,
		Invocation:       request.Invocation,
	}
}

func (target Target) Validate(record sdk.OperationRecord) error {
	if err := target.ValidateTarget(record); err != nil {
		return err
	}
	if record.ResourceRevision != "" &&
		target.ResourceRevision != "" &&
		record.ResourceRevision != target.ResourceRevision {
		return fmt.Errorf(
			"operation %q resource revision %s does not match current revision %s",
			record.Operation.ID,
			record.ResourceRevision,
			target.ResourceRevision,
		)
	}
	return nil
}

func (target Target) ValidateTarget(record sdk.OperationRecord) error {
	if record.Kind != target.Kind || record.Resource != target.Resource {
		return fmt.Errorf(
			"operation %q belongs to %s %q, not %s %q",
			record.Operation.ID,
			record.Kind,
			record.Resource,
			target.Kind,
			target.Resource,
		)
	}
	return nil
}
