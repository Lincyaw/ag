package lifecycle

import (
	"context"
	"errors"
	"time"
)

// DefaultFinalizationTimeout is the shared boundedness guard for durable
// cleanup/finalization after the caller's request context may be cancelled.
const DefaultFinalizationTimeout = 5 * time.Second

// Detached preserves context values while removing cancellation. Use it only
// for post-cancel cleanup or durable finalization that is bounded elsewhere.
func Detached(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

// WithDetachedTimeout preserves context values, removes cancellation, and adds
// an explicit timeout for cleanup/finalization work.
func WithDetachedTimeout(
	ctx context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(Detached(ctx), timeout)
}

// WithDetachedFinalization applies the shared bounded context for durable
// cleanup/finalization after the caller may already be cancelled.
func WithDetachedFinalization(ctx context.Context) (context.Context, context.CancelFunc) {
	return WithDetachedTimeout(ctx, DefaultFinalizationTimeout)
}

// ExpectedCancellation reports whether err is entirely explained by a context
// that has already been cancelled. It accepts joined and wrapped cancellation
// errors, but rejects mixed joins that contain non-cancellation cleanup failures.
func ExpectedCancellation(ctx context.Context, err error) bool {
	if ctx == nil || ctx.Err() == nil || err == nil {
		return false
	}
	return onlyCancellationError(ctx, err)
}

func onlyCancellationError(ctx context.Context, err error) bool {
	if err == nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyCancellationError(ctx, child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyCancellationError(ctx, wrapped.Unwrap())
	}
	return errors.Is(err, ctx.Err()) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

type valuesContext struct {
	context.Context
	values context.Context
}

func (ctx valuesContext) Value(key any) any {
	if value := ctx.values.Value(key); value != nil {
		return value
	}
	return ctx.Context.Value(key)
}

// WithValues returns a context that uses parent for cancellation and deadline
// while reading values from values. It is useful for runtime-owned asynchronous
// work that must outlive a request cancellation but still carry request-scoped
// logging or tracing values.
func WithValues(parent context.Context, values context.Context) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	if values == nil {
		return parent
	}
	return valuesContext{Context: parent, values: values}
}
