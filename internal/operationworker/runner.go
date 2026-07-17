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

// Run claims and executes one operation. A lost lease fences the result; a
// cancelled worker releases its claim so another process can recover it.
func (runner Runner) Run(
	ctx context.Context,
	id string,
	validate func(sdk.OperationRecord) error,
	execute func(context.Context, sdk.OperationRecord) (json.RawMessage, error),
) {
	logger := runner.Logger
	if logger == nil {
		logger = slog.Default()
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
	if validate != nil {
		if err := validate(record); err != nil {
			logger.Warn(
				"claimed operation is no longer executable",
				"operation_id",
				id,
				"error",
				err,
			)
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

	output, executeErr := invoke(executionCtx, record, execute)
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
		_, releaseErr := runner.Store.Release(context.Background(), id, token)
		if releaseErr != nil && !errors.Is(releaseErr, sdk.ErrOperationFence) {
			logger.Error(
				"release operation",
				"operation_id",
				id,
				"error",
				releaseErr,
			)
		}
		return
	}

	state := sdk.OperationSucceeded
	errorText := ""
	if executeErr != nil {
		state = sdk.OperationFailed
		output = nil
		errorText = executeErr.Error()
	}
	_, err = runner.Store.Complete(
		context.Background(),
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

func (runner Runner) renewLease(
	ctx context.Context,
	id string,
	token string,
	cancelExecution context.CancelFunc,
	done chan<- struct{},
	lost chan<- error,
) {
	defer close(done)
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

func invoke(
	ctx context.Context,
	record sdk.OperationRecord,
	execute func(context.Context, sdk.OperationRecord) (json.RawMessage, error),
) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"plugin operation panic: %v\n%s",
				recovered,
				debug.Stack(),
			)
		}
	}()
	return execute(ctx, record)
}
