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

	errs := make([]error, count)
	var wait sync.WaitGroup
	var cancelOnce sync.Once
	workers := count
	limited := options.Limit > 0
	if limited && options.Limit < workers {
		workers = options.Limit
	}
	jobs := make(chan int)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				taskErr := runParallelIndexedTask(
					runCtx,
					index,
					limited,
					run,
				)
				errs[index] = taskErr
				if taskErr != nil && options.CancelOnError {
					cancelOnce.Do(cancel)
				}
			}
		}()
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	wait.Wait()
	return errs
}

func runParallelIndexedTask(
	ctx context.Context,
	index int,
	limited bool,
	run func(context.Context, int) error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"parallel task panic: %v\n%s",
				recovered,
				debug.Stack(),
			)
		}
	}()
	if limited {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return run(ctx, index)
}
