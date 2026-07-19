package operationworker

import (
	"context"
	"errors"

	"github.com/lincyaw/ag/sdk"
)

// Host combines host-local in-flight cancellation with the durable operation
// runner. Callers still own lifecycle reservation, recovery candidate lookup,
// and resource routing.
type Host struct {
	Inflight *Inflight
	Runner   Runner
}

// ExecutionSlot is the host-local right to execute one operation ID in this
// process. It is separate from the durable store lease claimed by Runner.
type ExecutionSlot struct {
	Context context.Context
	finish  func()
}

func (slot ExecutionSlot) Acquired() bool {
	return slot.finish != nil
}

func (slot ExecutionSlot) Finish() {
	if slot.finish != nil {
		slot.finish()
	}
}

func (host Host) Start(
	parent context.Context,
	id string,
) ExecutionSlot {
	if host.Inflight == nil {
		return ExecutionSlot{}
	}
	return host.Inflight.Start(parent, id)
}

func (host Host) Run(
	parent context.Context,
	id string,
	validate Validator,
	execute Executor,
) bool {
	slot := host.Start(parent, id)
	if !slot.Acquired() {
		return false
	}
	defer slot.Finish()
	host.Runner.Run(slot.Context, id, validate, execute)
	return true
}

// StartReservedAsync consumes one already-reserved lifecycle slot. It starts a
// host goroutine when it can acquire the execution slot; otherwise it releases
// onDone before returning.
func (host Host) StartReservedAsync(
	parent context.Context,
	id string,
	validate Validator,
	execute Executor,
	onDone func(),
) {
	if onDone == nil {
		onDone = func() {}
	}
	slot := host.Start(parent, id)
	if !slot.Acquired() {
		onDone()
		return
	}
	go func() {
		defer onDone()
		defer slot.Finish()
		host.Runner.Run(slot.Context, id, validate, execute)
	}()
}

// SubmitReserved persists one target-bound request and starts a local worker
// when the accepted operation is still non-terminal. The caller passes the
// lifecycle reservation release as onDone; Host calls it exactly once, either
// after the started worker finishes or before returning when no worker starts.
func (host Host) SubmitReserved(
	ctx context.Context,
	target Target,
	request sdk.OperationRequest,
	execute Executor,
	onDone func(),
) (sdk.Operation, error) {
	if onDone == nil {
		onDone = func() {}
	}
	transferred := false
	defer func() {
		if !transferred {
			onDone()
		}
	}()
	if host.Inflight == nil {
		return sdk.Operation{}, errors.New("operation inflight registry is nil")
	}
	if host.Runner.Store == nil {
		return sdk.Operation{}, errors.New("operation store is nil")
	}
	record, _, err := host.Runner.Store.Submit(ctx, target.Record(request))
	if err != nil {
		return sdk.Operation{}, err
	}
	if !record.Operation.Terminal() {
		transferred = true
		host.StartReservedAsync(
			ctx,
			record.Operation.ID,
			target.Validate,
			execute,
			onDone,
		)
	}
	return record.Operation, nil
}

func (host Host) Cancel(
	ctx context.Context,
	id string,
	validate Validator,
) (sdk.OperationRecord, error) {
	cancelled, requested, err := requestCancel(
		ctx,
		host.Runner.Store,
		id,
		validate,
	)
	if err != nil {
		return sdk.OperationRecord{}, err
	}
	if requested && host.Inflight != nil {
		host.Inflight.Cancel(id)
	}
	return cancelled, nil
}
