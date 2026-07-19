package lifecycle

import (
	"context"
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

type valuesContext struct {
	context.Context
	values context.Context
}

func (ctx valuesContext) Value(key any) any {
	return ctx.values.Value(key)
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
