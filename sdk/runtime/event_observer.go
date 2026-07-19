package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/sdk"
)

func (runtime *Runtime) observeEvent(ctx context.Context, event sdk.Event) {
	runtime.observer.dispatch(runtime, ctx, event)
}

type eventObserverRuntime struct {
	observe     func(context.Context, sdk.Event)
	context     context.Context
	cancel      context.CancelFunc
	work        runtimeWorkGroup
	stoppedOnce sync.Once
	stopped     chan struct{}
}

func (observer *eventObserverRuntime) dispatch(
	runtime *Runtime,
	ctx context.Context,
	event sdk.Event,
) {
	if observer.observe == nil {
		return
	}
	observe := observer.observe
	observed := sdk.CloneEvent(event)
	observerCtx := lifecycle.WithValues(
		observer.context,
		afterDispatchEventContext(ctx),
	)
	releaseObserver, ok := observer.begin(runtime)
	if !ok {
		return
	}
	go func() {
		defer releaseObserver()
		defer func() {
			if recovered := recover(); recovered != nil {
				runtime.logger.WarnContext(
					observerCtx,
					"runtime event observer panicked",
					"event",
					observed.Name,
					"panic",
					recovered,
				)
			}
		}()
		observe(observerCtx, observed)
	}()
}

func (observer *eventObserverRuntime) begin(runtime *Runtime) (func(), bool) {
	return observer.work.begin(runtime)
}

func (observer *eventObserverRuntime) stop() {
	if observer.cancel != nil {
		observer.cancel()
	}
}

func (observer *eventObserverRuntime) waitBestEffortStopped(
	ctx context.Context,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		timeout = lifecycle.DefaultFinalizationTimeout
	}
	done := observer.stoppedSignal()
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-done:
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf(
			"runtime event observers did not stop: %w",
			waitCtx.Err(),
		)
	}
}

func (observer *eventObserverRuntime) stoppedSignal() <-chan struct{} {
	observer.stoppedOnce.Do(func() {
		if observer.stopped == nil {
			observer.stopped = make(chan struct{})
		}
		go func() {
			observer.work.waitStopped()
			close(observer.stopped)
		}()
	})
	return observer.stopped
}
