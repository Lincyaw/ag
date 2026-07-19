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

func (host Host) Start(
	parent context.Context,
	id string,
) (context.Context, func(), bool) {
	if host.Inflight == nil {
		return nil, nil, false
	}
	return host.Inflight.Start(parent, id)
}

func (host Host) Run(
	parent context.Context,
	id string,
	validate Validator,
	execute Executor,
) bool {
	ctx, finish, running := host.Start(parent, id)
	if !running {
		return false
	}
	defer finish()
	host.Runner.Run(ctx, id, validate, execute)
	return true
}

// StartAsync starts one already-reserved operation host goroutine. The caller
// still owns the lifecycle reservation and must release it through onDone.
func (host Host) StartAsync(
	parent context.Context,
	id string,
	validate Validator,
	execute Executor,
	onDone func(),
) bool {
	ctx, finish, running := host.Start(parent, id)
	if !running {
		return false
	}
	if onDone == nil {
		onDone = func() {}
	}
	go func() {
		defer onDone()
		defer finish()
		host.Runner.Run(ctx, id, validate, execute)
	}()
	return true
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
		transferred = host.StartAsync(
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
