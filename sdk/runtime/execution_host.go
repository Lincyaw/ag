package runtime

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
)

const defaultExecutionHostCloseTimeout = 5 * time.Second

// ExecutionHost groups a runtime with the state backend borrowed to run one
// execution host. Runtime owns shutdown ordering; presenters own host creation.
type ExecutionHost struct {
	Runtime      *Runtime
	State        sdk.StateBackend
	CloseTimeout time.Duration
}

// Control exposes the non-owning execution read/control facade for this host.
// It does not close Runtime or State; callers that borrow a host for one
// command should use the host command methods instead.
func (host ExecutionHost) Control() ExecutionControl {
	var lifecycle ExecutionLifecycle
	if host.State != nil {
		lifecycle = NewStateExecutionLifecycle(host.State)
	}
	return ExecutionControl{
		runtime:   host.Runtime,
		lifecycle: lifecycle,
	}
}

// LoadExecutionView runs one execution read through this host and then closes
// the borrowed runtime/state host with the detached cleanup boundary.
func (host ExecutionHost) LoadExecutionView(
	ctx context.Context,
	trajectoryID string,
) (ExecutionView, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (ExecutionView, error) {
			return host.Control().LoadView(ctx, trajectoryID)
		},
	)
}

// LoadRecoveryCandidate runs one execution recovery lookup through this host
// and then closes the borrowed runtime/state host with the detached cleanup
// boundary.
func (host ExecutionHost) LoadRecoveryCandidate(
	ctx context.Context,
	trajectoryID string,
) (ExecutionRecoveryCandidate, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (ExecutionRecoveryCandidate, error) {
			return host.Control().LoadRecoveryCandidate(ctx, trajectoryID)
		},
	)
}

// RunPromptSubmission hosts one accepted prompt submission and then closes the
// borrowed runtime/state host with the detached cleanup boundary.
func (host ExecutionHost) RunPromptSubmission(
	ctx context.Context,
	submission *PromptSubmission,
) (Result, error) {
	return runExecutionHostCommand(ctx, host, submission.Run)
}

// RecoverExecution hosts recovery for one trajectory execution and then closes
// the borrowed runtime/state host with the detached cleanup boundary.
func (host ExecutionHost) RecoverExecution(
	ctx context.Context,
	trajectoryID string,
) (Result, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (Result, error) {
			if host.Runtime == nil {
				return Result{}, errors.New("execution host runtime is nil")
			}
			return host.Runtime.RecoverExecution(ctx, trajectoryID)
		},
	)
}

// CancelExecution runs one execution cancellation command through this host and
// then closes the borrowed runtime/state host with the detached cleanup boundary.
// Runtime-backed hosts own the full terminal/restore completion; state-only
// callers must use FenceCancellation for an explicit weaker boundary, or
// CancelWithAvailableBoundary when strongest-available semantics are intended.
func (host ExecutionHost) CancelExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (ExecutionView, error) {
			if host.Runtime == nil {
				return ExecutionView{}, errors.New("execution host runtime is nil")
			}
			return host.Runtime.CancelExecution(ctx, trajectoryID, executionID, reason)
		},
	)
}

// CancelWithAvailableBoundary runs the strongest cancellation command available
// through this host and then closes the borrowed runtime/state host.
// Runtime-backed hosts perform full cancellation unwind; state-only hosts
// durably fence.
func (host ExecutionHost) CancelWithAvailableBoundary(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (ExecutionView, error) {
			return host.Control().CancelWithAvailableBoundary(
				ctx,
				trajectoryID,
				executionID,
				reason,
			)
		},
	)
}

// FenceCancellation durably marks an execution cancelled through a state-only
// lifecycle. It does not write terminal/restore trajectory entries or publish
// post-commit events; a later runtime resume projects from the execution base.
func (host ExecutionHost) FenceCancellation(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	return runExecutionHostCommand(
		ctx,
		host,
		func(ctx context.Context) (ExecutionView, error) {
			return host.Control().FenceCancellation(
				ctx,
				trajectoryID,
				executionID,
				reason,
			)
		},
	)
}

func runExecutionHostCommand[T any](
	ctx context.Context,
	host ExecutionHost,
	command func(context.Context) (T, error),
) (result T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"execution host command panic: %v\n%s",
				recovered,
				debug.Stack(),
			)
		}
		err = errors.Join(err, host.CloseDetached(ctx))
	}()
	result, err = command(ctx)
	return result, err
}

func (host ExecutionHost) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := host.CloseTimeout
	if timeout <= 0 {
		timeout = defaultExecutionHostCloseTimeout
	}
	var result error
	collect := func(err error) {
		result = errors.Join(result, err)
	}
	if host.Runtime != nil {
		collect(
			closeExecutionHostStep(
				ctx,
				timeout,
				"drain runtime deliveries",
				host.Runtime.DrainDeliveries,
			),
		)
		collect(
			closeExecutionHostStep(
				ctx,
				timeout,
				"close runtime",
				host.Runtime.Close,
			),
		)
	}
	if host.State != nil {
		collect(
			closeExecutionHostStep(
				ctx,
				timeout,
				"close state backend",
				host.State.Close,
			),
		)
	}
	return result
}

// CloseDetached closes a host after a request or execution context may already
// be cancelled. Context values are preserved, but cancellation is ignored; the
// host close timeout remains the boundedness guard for each cleanup step.
func (host ExecutionHost) CloseDetached(ctx context.Context) error {
	return host.Close(lifecycle.Detached(ctx))
}

func closeExecutionHostStep(
	ctx context.Context,
	timeout time.Duration,
	name string,
	close func(context.Context) error,
) (err error) {
	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%s panic: %v\n%s",
				name,
				recovered,
				debug.Stack(),
			)
		}
	}()
	return close(stepCtx)
}
