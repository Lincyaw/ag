package runtime

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
)

type parallelIndexedOptions struct {
	Limit         int
	CancelOnError bool
}

func runParallelIndexed(
	ctx context.Context,
	count int,
	options parallelIndexedOptions,
	run func(context.Context, int) error,
) []error {
	if count <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx := ctx
	cancel := func() {}
	if options.CancelOnError {
		runCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	var slots chan struct{}
	if options.Limit > 0 {
		slots = make(chan struct{}, options.Limit)
	}
	errs := make([]error, count)
	var wait sync.WaitGroup
	var cancelOnce sync.Once
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			var taskErr error
			defer func() {
				if recovered := recover(); recovered != nil {
					taskErr = fmt.Errorf(
						"parallel task panic: %v\n%s",
						recovered,
						debug.Stack(),
					)
				}
				errs[index] = taskErr
				if taskErr != nil && options.CancelOnError {
					cancelOnce.Do(cancel)
				}
			}()
			releaseSlot, err := acquireParallelSlot(runCtx, slots)
			if err != nil {
				taskErr = err
				return
			}
			defer releaseSlot()
			taskErr = run(runCtx, index)
		}(index)
	}
	wait.Wait()
	return errs
}

func acquireParallelSlot(
	ctx context.Context,
	slots chan struct{},
) (func(), error) {
	if slots == nil {
		return func() {}, nil
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
