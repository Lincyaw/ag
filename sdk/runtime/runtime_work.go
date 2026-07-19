package runtime

import (
	"errors"
	"sync"
)

// beginRuntimeWork enters the runtime-wide close gate and registers work that
// Close must wait for before plugin/storage cleanup is allowed to finish.
func (runtime *Runtime) beginRuntimeWork(wait *sync.WaitGroup) (func(), bool) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, false
	}
	wait.Add(1)
	return wait.Done, true
}

// beginTrajectoryWork joins durable trajectory work with runtime shutdown and
// returns the release function for that shutdown lease. Session creation,
// prompt execution, recovery, cancellation, and rollback all mutate or project
// trajectory state, so they share one boundary.
func (runtime *Runtime) beginTrajectoryWork() (func(), error) {
	release, ok := runtime.trajectoryExecution.beginWork(runtime)
	if !ok {
		return nil, errors.New("runtime is closed")
	}
	return release, nil
}

func (runtime *Runtime) shouldLeaveExecutionRecoverable() bool {
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	return closed || runtime.trajectoryExecution.stopped()
}
