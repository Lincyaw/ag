package operationworker

import (
	"context"
	"sync"

	"github.com/lincyaw/ag/internal/lifecycle"
)

// Inflight tracks host-local cancellation handles for operations currently
// executing in this process. Durable state remains in OperationStore.
type Inflight struct {
	context context.Context
	mu      sync.Mutex
	cancel  map[string]context.CancelFunc
}

func NewInflight(ctx context.Context) Inflight {
	if ctx == nil {
		ctx = context.Background()
	}
	return Inflight{
		context: ctx,
		cancel:  make(map[string]context.CancelFunc),
	}
}

func (inflight *Inflight) Start(
	parent context.Context,
	id string,
) (context.Context, func(), bool) {
	if parent == nil {
		parent = context.Background()
	}
	inflight.ensure()
	ctx, cancel := context.WithCancel(lifecycle.Detached(parent))
	stopRootCancel := context.AfterFunc(inflight.context, cancel)

	inflight.mu.Lock()
	if _, running := inflight.cancel[id]; running {
		inflight.mu.Unlock()
		stopRootCancel()
		cancel()
		return nil, nil, false
	}
	inflight.cancel[id] = cancel
	inflight.mu.Unlock()

	finish := func() {
		stopRootCancel()
		cancel()
		inflight.mu.Lock()
		delete(inflight.cancel, id)
		inflight.mu.Unlock()
	}
	return ctx, finish, true
}

func (inflight *Inflight) Cancel(id string) bool {
	inflight.ensure()
	inflight.mu.Lock()
	cancel := inflight.cancel[id]
	inflight.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (inflight *Inflight) ensure() {
	inflight.mu.Lock()
	defer inflight.mu.Unlock()
	if inflight.context == nil {
		inflight.context = context.Background()
	}
	if inflight.cancel == nil {
		inflight.cancel = make(map[string]context.CancelFunc)
	}
}
