// Package deliveryworker executes durable deliveries under a fenced lease.
package deliveryworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
)

const (
	maxRetryDelay = 30 * time.Second
)

// Handler executes one leased delivery. Returning Permanent(cause) moves the
// delivery to dead-letter; any other error follows the retry policy.
type Handler func(context.Context, sdk.Delivery) error

// Runner owns the storage-level delivery execution protocol. Target lookup and
// subscriber invocation remain the responsibility of its caller.
type Runner struct {
	Store       sdk.DeliveryStore
	Logger      *slog.Logger
	Context     context.Context
	Queue       string
	Lease       time.Duration
	Poll        time.Duration
	MaxAttempts int
}

type permanentError struct {
	cause error
}

func (err permanentError) Error() string {
	return err.cause.Error()
}

func (err permanentError) Unwrap() error {
	return err.cause
}

// Permanent marks a delivery failure as non-retryable.
func Permanent(cause error) error {
	if cause == nil {
		return nil
	}
	return permanentError{cause: cause}
}

// Run leases and executes deliveries until the runner context is cancelled.
func (runner Runner) Run(worker int, handle Handler) {
	logger := runner.logger()
	ctx := runner.context()
	if runner.Store == nil {
		logger.ErrorContext(ctx, "delivery store is nil")
		return
	}
	for {
		if !runner.runOnce(ctx, logger, worker, handle) {
			return
		}
	}
}

func (runner Runner) runOnce(
	ctx context.Context,
	logger *slog.Logger,
	worker int,
	handle Handler,
) (again bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.ErrorContext(
				ctx,
				"delivery worker panic",
				"queue",
				runner.Queue,
				"worker",
				worker,
				"panic",
				recovered,
				"stack",
				string(debug.Stack()),
			)
			again = wait(ctx, runner.Poll)
		}
	}()
	if ctx.Err() != nil {
		return false
	}
	delivery, err := runner.Store.Lease(
		ctx,
		time.Now().UTC(),
		runner.Lease,
	)
	if errors.Is(err, sdk.ErrNoDelivery) {
		return wait(ctx, runner.Poll)
	}
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		logger.WarnContext(
			ctx,
			"lease delivery",
			"queue",
			runner.Queue,
			"worker",
			worker,
			"error",
			err,
		)
		return wait(ctx, runner.Poll)
	}
	runner.deliver(ctx, logger, delivery, handle)
	return true
}

func (runner Runner) deliver(
	ctx context.Context,
	logger *slog.Logger,
	delivery sdk.Delivery,
	handle Handler,
) {
	err := invokeHandler(ctx, delivery, handle)
	if err == nil {
		finalizationCtx, cancel := runner.finalizationContext(ctx)
		defer cancel()
		if ackErr := runner.Store.Ack(
			finalizationCtx,
			delivery.ID,
			delivery.LeaseToken,
			time.Now().UTC(),
		); ackErr != nil && !errors.Is(ackErr, context.Canceled) {
			logger.WarnContext(
				finalizationCtx,
				"ack delivery",
				"queue",
				runner.Queue,
				"delivery_id",
				delivery.ID,
				"error",
				ackErr,
			)
		}
		return
	}
	// Worker cancellation is a lifecycle release, not subscriber failure.
	if runner.releaseCancelled(ctx, logger, delivery, err) {
		return
	}
	if cause, ok := permanentCause(err); ok {
		runner.deadLetter(ctx, logger, delivery, cause)
		return
	}
	runner.retry(ctx, logger, delivery, err)
}

func invokeHandler(
	ctx context.Context,
	delivery sdk.Delivery,
	handle Handler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"delivery handler panic: %v\n%s",
				recovered,
				debug.Stack(),
			)
		}
	}()
	return handle(ctx, delivery)
}

func (runner Runner) deadLetter(
	ctx context.Context,
	logger *slog.Logger,
	delivery sdk.Delivery,
	cause error,
) {
	finalizationCtx, cancel := runner.finalizationContext(ctx)
	defer cancel()
	err := runner.Store.DeadLetter(
		finalizationCtx,
		delivery.ID,
		delivery.LeaseToken,
		time.Now().UTC(),
		cause.Error(),
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.WarnContext(
			finalizationCtx,
			"dead-letter delivery",
			"queue",
			runner.Queue,
			"delivery_id",
			delivery.ID,
			"error",
			err,
		)
	}
}

func (runner Runner) retry(
	ctx context.Context,
	logger *slog.Logger,
	delivery sdk.Delivery,
	cause error,
) {
	now := time.Now().UTC()
	finalizationCtx, cancel := runner.finalizationContext(ctx)
	defer cancel()
	var err error
	if delivery.Attempt >= runner.MaxAttempts {
		err = runner.Store.DeadLetter(
			finalizationCtx,
			delivery.ID,
			delivery.LeaseToken,
			now,
			cause.Error(),
		)
	} else {
		err = runner.Store.Retry(
			finalizationCtx,
			delivery.ID,
			delivery.LeaseToken,
			now.Add(retryDelay(runner.Poll, delivery.Attempt)),
			cause.Error(),
		)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.WarnContext(
			finalizationCtx,
			"reschedule delivery",
			"queue",
			runner.Queue,
			"delivery_id",
			delivery.ID,
			"attempt",
			delivery.Attempt,
			"error",
			err,
		)
	}
}

func (runner Runner) releaseCancelled(
	ctx context.Context,
	logger *slog.Logger,
	delivery sdk.Delivery,
	cause error,
) bool {
	if ctx.Err() == nil {
		return false
	}
	finalizationCtx, cancel := runner.finalizationContext(ctx)
	defer cancel()
	err := runner.Store.Retry(
		finalizationCtx,
		delivery.ID,
		delivery.LeaseToken,
		time.Now().UTC(),
		cancelledDeliveryError(ctx, cause),
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.WarnContext(
			finalizationCtx,
			"release cancelled delivery",
			"queue",
			runner.Queue,
			"delivery_id",
			delivery.ID,
			"error",
			err,
		)
	}
	return true
}

func (runner Runner) finalizationContext(
	ctx context.Context,
) (context.Context, context.CancelFunc) {
	return lifecycle.WithDetachedFinalization(ctx)
}

func (runner Runner) logger() *slog.Logger {
	if runner.Logger != nil {
		return runner.Logger
	}
	return slog.Default()
}

func (runner Runner) context() context.Context {
	if runner.Context != nil {
		return runner.Context
	}
	return context.Background()
}

func permanentCause(err error) (error, bool) {
	var permanent permanentError
	if errors.As(err, &permanent) {
		return permanent.cause, true
	}
	return nil, false
}

func cancelledDeliveryError(ctx context.Context, cause error) string {
	if cause != nil {
		return cause.Error()
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err().Error()
	}
	return "delivery worker cancelled"
}

func retryDelay(poll time.Duration, attempt int) time.Duration {
	shift := min(max(attempt-1, 0), 10)
	delay := poll * time.Duration(1<<shift)
	return min(delay, maxRetryDelay)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
