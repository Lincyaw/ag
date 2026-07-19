// Package operationworker executes durable operations under a fenced lease.
package operationworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
)

// Runner owns the storage-level execution protocol. Submission, recovery, and
// resource routing remain the responsibility of its caller.
type Runner struct {
	Store  sdk.OperationStore
	Logger *slog.Logger
	Owner  string
	Lease  time.Duration
}

type Validator func(sdk.OperationRecord) error

type Executor func(context.Context, sdk.OperationRecord) (json.RawMessage, error)

// Run claims and executes one operation. A lost lease fences the result; a
// cancelled worker releases its claim so another process can recover it.
func (runner Runner) Run(
	ctx context.Context,
	id string,
	validate Validator,
	execute Executor,
) {
	logger := runner.logger()
	defer recoverRunPanic(ctx, logger, id)
	if runner.Store == nil {
		logger.ErrorContext(ctx, "operation store is nil", "operation_id", id)
		return
	}
	record, err := runner.Store.Claim(
		ctx,
		id,
		runner.Owner,
		time.Now().UTC(),
		runner.Lease,
	)
	if err != nil {
		if !errors.Is(err, sdk.ErrOperationClaimed) {
			logger.Debug("claim operation", "operation_id", id, "error", err)
		}
		return
	}
	if record.Execution == nil {
		logger.ErrorContext(
			ctx,
			"claimed operation has no execution lease",
			"operation_id",
			id,
		)
		return
	}
	if validate != nil {
		if err := validate(record); err != nil {
			if ctx.Err() != nil {
				runner.releaseClaimed(ctx, logger, id, record)
				return
			}
			logger.Warn(
				"claimed operation is no longer executable",
				"operation_id",
				id,
				"error",
				err,
			)
			runner.failClaimedInvalid(ctx, logger, id, record, err)
			return
		}
	}

	executionCtx, cancelExecution := context.WithCancel(
		sdk.WithInvocation(ctx, record.Invocation),
	)
	defer cancelExecution()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	heartbeatDone := make(chan struct{})
	leaseLost := make(chan error, 1)
	go runner.renewLease(
		heartbeatCtx,
		id,
		record.Execution.Token,
		cancelExecution,
		heartbeatDone,
		leaseLost,
	)

	output, executeErr := pluginpolicy.InvokeOperation(
		executionCtx,
		record,
		execute,
	)
	stopHeartbeat()
	<-heartbeatDone
	select {
	case lostErr := <-leaseLost:
		logger.Warn(
			"operation lease lost",
			"operation_id",
			id,
			"error",
			lostErr,
		)
		return
	default:
	}

	token := record.Execution.Token
	if ctx.Err() != nil {
		runner.releaseClaimed(ctx, logger, id, record)
		return
	}

	finalizationCtx, cancelFinalization := runner.finalizationContext(executionCtx)
	defer cancelFinalization()
	state := sdk.OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = sdk.OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = runner.Store.Complete(
		finalizationCtx,
		id,
		token,
		state,
		output,
		errorText,
	)
	if err != nil && !errors.Is(err, sdk.ErrOperationFence) {
		logger.Error("complete operation", "operation_id", id, "error", err)
	}
}

func recoverRunPanic(ctx context.Context, logger *slog.Logger, id string) {
	if recovered := recover(); recovered != nil {
		logger.ErrorContext(
			ctx,
			"operation worker panic",
			"operation_id",
			id,
			"panic",
			recovered,
			"stack",
			string(debug.Stack()),
		)
	}
}

func (runner Runner) releaseClaimed(
	ctx context.Context,
	logger *slog.Logger,
	id string,
	record sdk.OperationRecord,
) {
	if record.Execution == nil {
		return
	}
	finalizationCtx, cancel := runner.finalizationContext(ctx)
	defer cancel()
	_, releaseErr := runner.Store.Release(
		finalizationCtx,
		id,
		record.Execution.Token,
	)
	if releaseErr != nil && !errors.Is(releaseErr, sdk.ErrOperationFence) {
		logger.Error(
			"release operation",
			"operation_id",
			id,
			"error",
			releaseErr,
		)
	}
}

func (runner Runner) failClaimedInvalid(
	ctx context.Context,
	logger *slog.Logger,
	id string,
	record sdk.OperationRecord,
	invalidErr error,
) {
	// Unlike FailInvalid, Runner already owns a lease token and can fence the
	// failure against an expired worker.
	token := record.Execution.Token
	finalizationCtx, cancel := runner.finalizationContext(ctx)
	defer cancel()
	_, completeErr := runner.Store.Complete(
		finalizationCtx,
		id,
		token,
		sdk.OperationFailed,
		nil,
		invalidErr.Error(),
	)
	if completeErr != nil && !errors.Is(completeErr, sdk.ErrOperationFence) {
		logger.Error(
			"fail invalid operation",
			"operation_id",
			id,
			"error",
			completeErr,
		)
	}
}

func (runner Runner) finalizationContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	return lifecycle.WithDetachedFinalization(ctx)
}

func (runner Runner) renewLease(
	ctx context.Context,
	id string,
	token string,
	cancelExecution context.CancelFunc,
	done chan<- struct{},
	lost chan<- error,
) {
	defer close(done)
	defer recoverRenewLeasePanic(ctx, runner.logger(), id, cancelExecution, lost)
	interval := runner.Lease / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_, err := runner.Store.Renew(
				ctx,
				id,
				token,
				now.UTC(),
				runner.Lease,
			)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case lost <- err:
				default:
				}
				cancelExecution()
				return
			}
		}
	}
}

func (runner Runner) logger() *slog.Logger {
	if runner.Logger != nil {
		return runner.Logger
	}
	return slog.Default()
}

func recoverRenewLeasePanic(
	ctx context.Context,
	logger *slog.Logger,
	id string,
	cancelExecution context.CancelFunc,
	lost chan<- error,
) {
	if recovered := recover(); recovered != nil {
		stack := string(debug.Stack())
		err := fmt.Errorf(
			"renew operation lease panic: %v\n%s",
			recovered,
			stack,
		)
		logger.ErrorContext(
			ctx,
			"operation lease renew panic",
			"operation_id",
			id,
			"panic",
			recovered,
			"stack",
			stack,
		)
		select {
		case lost <- err:
		default:
		}
		cancelExecution()
	}
}
