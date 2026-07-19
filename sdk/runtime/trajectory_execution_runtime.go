package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/lincyaw/ag/sdk"
)

// trajectoryExecutionRuntime owns the live execution boundary for durable
// trajectories. The store remains the state port; this type owns host
// lifecycle, leases, cancellation propagation, and shutdown participation.
type trajectoryExecutionRuntime struct {
	context  context.Context
	cancel   context.CancelFunc
	work     runtimeWorkGroup
	hosts    *hostedExecutionRegistry
	lease    time.Duration
	workerID string
}

func (trajectory *trajectoryExecutionRuntime) stop() {
	if trajectory.cancel != nil {
		trajectory.cancel()
	}
}

// waitDurableStopped waits for live trajectory execution work to either finish
// or restore recoverable durable state before runtime-owned cleanup continues.
func (trajectory *trajectoryExecutionRuntime) waitDurableStopped() {
	trajectory.work.waitStopped()
}

func (trajectory *trajectoryExecutionRuntime) beginWork(
	runtime *Runtime,
) (func(), bool) {
	return trajectory.work.begin(runtime)
}

func (trajectory *trajectoryExecutionRuntime) stopped() bool {
	return trajectory.context != nil && trajectory.context.Err() != nil
}

func (trajectory *trajectoryExecutionRuntime) cancelAfter(
	cancel context.CancelCauseFunc,
) func() {
	if trajectory.context == nil {
		return func() {}
	}
	stop := context.AfterFunc(
		trajectory.context,
		func() { cancel(trajectory.context.Err()) },
	)
	return func() { stop() }
}

func (trajectory *trajectoryExecutionRuntime) claim(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
) (sdk.TrajectoryExecution, error) {
	if store == nil {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution store is nil",
		)
	}
	return store.ClaimExecution(
		ctx,
		trajectoryID,
		trajectory.workerID,
		time.Now().UTC(),
		trajectory.lease,
	)
}

func (trajectory *trajectoryExecutionRuntime) registerHosted(
	trajectoryID string,
	executionID string,
	cancel context.CancelCauseFunc,
) func() {
	if trajectory.hosts == nil {
		return func() {}
	}
	return trajectory.hosts.register(trajectoryID, executionID, cancel)
}

func (trajectory *trajectoryExecutionRuntime) cancelHostedAndWait(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	cause error,
) (bool, error) {
	if trajectory.hosts == nil {
		return false, nil
	}
	return trajectory.hosts.cancelAndWait(ctx, trajectoryID, executionID, cause)
}

func (trajectory *trajectoryExecutionRuntime) cancelHosted(
	trajectoryID string,
	executionID string,
	cause error,
) {
	if trajectory.hosts == nil {
		return
	}
	trajectory.hosts.cancel(trajectoryID, executionID, cause)
}

func (trajectory *trajectoryExecutionRuntime) heartbeatInterval() time.Duration {
	interval := trajectory.lease / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return interval
}

func (trajectory *trajectoryExecutionRuntime) renew(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	executionID string,
	token string,
	now time.Time,
) (sdk.TrajectoryExecution, error) {
	if store == nil {
		return sdk.TrajectoryExecution{}, errors.New(
			"trajectory execution store is nil",
		)
	}
	return store.RenewExecution(
		ctx,
		trajectoryID,
		executionID,
		token,
		now.UTC(),
		trajectory.lease,
	)
}
