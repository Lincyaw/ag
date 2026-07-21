package runtime

import (
	"errors"
	"sync"
)

// ErrRuntimeClosed means a runtime lifecycle gate has already started closing
// and cannot accept new work.
var ErrRuntimeClosed = errors.New("runtime is closed")

// ErrRuntimeDraining is returned by an active prompt after it commits the
// current turn boundary and hands the non-terminal execution to recovery.
var ErrRuntimeDraining = errors.New("runtime is draining")

// runtimeWorkGroup tracks short-lived runtime-owned work behind the runtime
// close gate. It only owns admission and accounting; each subsystem chooses
// whether close waits for the group as a durable boundary or best-effort cleanup.
type runtimeWorkGroup struct {
	wait sync.WaitGroup
}

func (group *runtimeWorkGroup) begin(runtime *Runtime) (func(), bool) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, false
	}
	group.wait.Add(1)
	return group.wait.Done, true
}

func (group *runtimeWorkGroup) waitStopped() {
	group.wait.Wait()
}

// beginTrajectoryWork joins durable trajectory work with runtime shutdown and
// returns the release function for that shutdown lease. Session creation,
// prompt execution, recovery, cancellation, and rollback all mutate or project
// trajectory state, so they share one boundary.
func (runtime *Runtime) beginTrajectoryWork() (func(), error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return nil, ErrRuntimeClosed
	}
	if runtime.draining {
		return nil, ErrRuntimeDraining
	}
	runtime.trajectoryExecution.work.wait.Add(1)
	return runtime.trajectoryExecution.work.wait.Done, nil
}

func (runtime *Runtime) beginOperationWork() (func(), error) {
	release, ok := runtime.operation.beginWork(runtime)
	if !ok {
		return nil, ErrRuntimeClosed
	}
	return release, nil
}

// executionRecoveryHandoffActive reports whether active execution unwind should
// return durable ownership to recovery instead of writing a terminal outcome.
// Runtime close cancels live prompt contexts to release local host resources;
// the trajectory execution itself remains pending for another runtime to claim.
func (runtime *Runtime) executionRecoveryHandoffActive() bool {
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	return closed || runtime.draining || runtime.trajectoryExecution.stopped()
}

// gracefulExecutionDrainActive distinguishes a committed turn-boundary handoff
// from forced runtime close. The former preserves the new checkpoint head so
// recovery continues with the next model turn instead of replaying this one.
func (runtime *Runtime) gracefulExecutionDrainActive() bool {
	runtime.mu.Lock()
	draining := runtime.draining && !runtime.closed
	runtime.mu.Unlock()
	return draining && !runtime.trajectoryExecution.stopped()
}

// operationRecoveryHandoffActive reports whether an interrupted operation await
// should leave the durable operation non-terminal for recovery instead of
// recording a durable cancellation. Runtime close cancels local operation worker
// contexts to release host resources; the operation lease and idempotency record
// remain the recovery boundary.
func (runtime *Runtime) operationRecoveryHandoffActive() bool {
	runtime.mu.Lock()
	closed := runtime.closed
	runtime.mu.Unlock()
	return closed || runtime.operation.stopped()
}
