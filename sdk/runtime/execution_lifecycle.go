package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

var (
	// ErrExecutionNotFound means the trajectory exists but has no current
	// execution record to project.
	ErrExecutionNotFound = errors.New("runtime execution not found")
	// ErrExecutionNotRecoverable means the trajectory has an execution record, but
	// it is terminal and therefore not a recovery candidate.
	ErrExecutionNotRecoverable = errors.New(
		"runtime execution is not recoverable",
	)
)

// ExecutionView is the runtime-facing read model for one trajectory execution.
// It keeps external presenters from reimplementing result extraction from
// checkpoint entries.
type ExecutionView struct {
	TrajectoryID string                  `json:"trajectory_id"`
	Execution    sdk.TrajectoryExecution `json:"execution"`
	Result       *Result                 `json:"result,omitempty"`
}

// ExecutionRecoveryCandidate is the runtime-facing recovery read model for
// one non-terminal trajectory execution.
type ExecutionRecoveryCandidate struct {
	TrajectoryID string                  `json:"trajectory_id"`
	Execution    sdk.TrajectoryExecution `json:"execution"`
	Delay        time.Duration           `json:"delay"`
}

// Wait blocks until this recovery candidate may be claimed. A zero or negative
// delay means the execution is immediately recoverable.
func (candidate ExecutionRecoveryCandidate) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if candidate.Delay <= 0 {
		return nil
	}
	if !waitContext(ctx, candidate.Delay) {
		return ctx.Err()
	}
	return nil
}

func (candidate ExecutionRecoveryCandidate) ExecutionView() ExecutionView {
	return ExecutionView{
		TrajectoryID: candidate.TrajectoryID,
		Execution:    candidate.Execution,
	}
}

// ExecutionLifecycle exposes the trajectory-backed execution read and control
// model used by hosts and presenters outside a Runtime instance.
type ExecutionLifecycle struct {
	store sdk.TrajectoryStore
	now   func() time.Time
}

// ExecutionControl is a read/control facade over the strongest available
// trajectory execution boundary. A live runtime host can expose current views
// and unwind cancellation with runtime-owned terminal entries and events.
// A lifecycle-only controller can read and durably fence execution state, but it
// does not own terminal/restore entries.
type ExecutionControl struct {
	runtime   *Runtime
	lifecycle ExecutionLifecycle
}

func NewExecutionLifecycle(store sdk.TrajectoryStore) ExecutionLifecycle {
	return ExecutionLifecycle{store: store}
}

func NewStateExecutionLifecycle(
	state sdk.StateBackend,
) ExecutionLifecycle {
	if state == nil {
		return ExecutionLifecycle{}
	}
	return NewExecutionLifecycle(state.Trajectories())
}

func NewRuntimeExecutionControl(runtime *Runtime) ExecutionControl {
	return ExecutionControl{runtime: runtime}
}

func (control ExecutionControl) LoadView(
	ctx context.Context,
	trajectoryID string,
) (ExecutionView, error) {
	release, err := control.beginRuntimeRead()
	if err != nil {
		return ExecutionView{}, err
	}
	defer release()
	return control.resolvedLifecycle().LoadView(ctx, trajectoryID)
}

func (control ExecutionControl) LoadRecoveryCandidate(
	ctx context.Context,
	trajectoryID string,
) (ExecutionRecoveryCandidate, error) {
	release, err := control.beginRuntimeRead()
	if err != nil {
		return ExecutionRecoveryCandidate{}, err
	}
	defer release()
	return control.resolvedLifecycle().LoadRecoveryCandidate(ctx, trajectoryID)
}

func (control ExecutionControl) ListRecoveryCandidates(
	ctx context.Context,
) ([]ExecutionRecoveryCandidate, error) {
	release, err := control.beginRuntimeRead()
	if err != nil {
		return nil, err
	}
	defer release()
	return control.resolvedLifecycle().ListRecoveryCandidates(ctx)
}

func (lifecycle ExecutionLifecycle) LoadView(
	ctx context.Context,
	trajectoryID string,
) (ExecutionView, error) {
	if lifecycle.store == nil {
		return ExecutionView{}, errors.New("execution lifecycle store is nil")
	}
	metadata, err := lifecycle.store.LoadMetadata(ctx, trajectoryID)
	if err != nil {
		return ExecutionView{}, err
	}
	return LoadExecutionViewFromMetadata(ctx, lifecycle.store, metadata)
}

func (lifecycle ExecutionLifecycle) LoadRecoveryCandidate(
	ctx context.Context,
	trajectoryID string,
) (ExecutionRecoveryCandidate, error) {
	if lifecycle.store == nil {
		return ExecutionRecoveryCandidate{}, errors.New("execution lifecycle store is nil")
	}
	metadata, err := lifecycle.store.LoadMetadata(ctx, trajectoryID)
	if err != nil {
		return ExecutionRecoveryCandidate{}, err
	}
	candidate, err := executionRecoveryCandidateFromMetadata(
		metadata,
		lifecycle.currentTime(),
	)
	if err != nil {
		return ExecutionRecoveryCandidate{}, err
	}
	return candidate, nil
}

func (lifecycle ExecutionLifecycle) ListRecoveryCandidates(
	ctx context.Context,
) ([]ExecutionRecoveryCandidate, error) {
	if lifecycle.store == nil {
		return nil, errors.New("execution lifecycle store is nil")
	}
	now := lifecycle.currentTime()
	recoverable, err := lifecycle.store.ListRecoverable(ctx, now)
	if err != nil {
		return nil, err
	}
	candidates := make([]ExecutionRecoveryCandidate, 0, len(recoverable))
	for _, metadata := range recoverable {
		candidate, err := executionRecoveryCandidateFromMetadata(
			metadata,
			now,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"trajectory %s returned by recoverable index: %w",
				metadata.ID,
				err,
			)
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func (lifecycle ExecutionLifecycle) FenceCancellation(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	if lifecycle.store == nil {
		return ExecutionView{}, errors.New("execution lifecycle store is nil")
	}
	if reason == "" {
		reason = "execution cancelled"
	}
	result, err := lifecycle.store.CancelExecution(
		ctx,
		sdk.TrajectoryExecutionCancelCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  executionID,
			Reason:       reason,
			At:           lifecycle.currentTime(),
		},
	)
	if err != nil {
		return ExecutionView{}, err
	}
	return LoadExecutionViewFromMetadata(ctx, lifecycle.store, result.Trajectory)
}

// Cancel requires a live runtime and performs the runtime-owned cancellation
// path, including terminal/restore trajectory entries and post-commit events.
func (control ExecutionControl) Cancel(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	if control.runtime == nil {
		return ExecutionView{}, errors.New("execution control runtime is nil")
	}
	return control.runtime.CancelExecution(ctx, trajectoryID, executionID, reason)
}

// CancelWithAvailableBoundary uses the strongest cancellation boundary exposed
// by this control. Runtime-backed controls own the full cancellation unwind;
// lifecycle-only controls durably fence the execution as cancelled.
func (control ExecutionControl) CancelWithAvailableBoundary(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	if control.runtime != nil {
		return control.Cancel(ctx, trajectoryID, executionID, reason)
	}
	return control.FenceCancellation(ctx, trajectoryID, executionID, reason)
}

// FenceCancellation durably marks an execution cancelled through this control's
// lifecycle store. It is the explicit state-only fallback when no runtime host
// can own the full cancellation completion.
func (control ExecutionControl) FenceCancellation(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	return control.lifecycle.FenceCancellation(
		ctx,
		trajectoryID,
		executionID,
		reason,
	)
}

func (control ExecutionControl) resolvedLifecycle() ExecutionLifecycle {
	if control.runtime == nil {
		return control.lifecycle
	}
	lifecycle := NewExecutionLifecycle(control.runtime.trajectories)
	lifecycle.now = control.lifecycle.now
	return lifecycle
}

func (control ExecutionControl) beginRuntimeRead() (func(), error) {
	if control.runtime == nil {
		return func() {}, nil
	}
	return control.runtime.beginTrajectoryWork()
}

func (lifecycle ExecutionLifecycle) currentTime() time.Time {
	if lifecycle.now != nil {
		now := lifecycle.now()
		if !now.IsZero() {
			return now.UTC()
		}
	}
	return time.Now().UTC()
}

// LoadExecutionView loads the current execution view for one trajectory.
func LoadExecutionView(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
) (ExecutionView, error) {
	return NewExecutionLifecycle(store).LoadView(ctx, trajectoryID)
}

// LoadExecutionViewFromMetadata projects trajectory metadata into the
// execution read model and, for succeeded executions, attaches the durable
// checkpoint result when one exists.
func LoadExecutionViewFromMetadata(
	ctx context.Context,
	store sdk.TrajectoryStore,
	metadata sdk.TrajectoryMetadata,
) (ExecutionView, error) {
	if metadata.Execution == nil {
		return ExecutionView{}, fmt.Errorf(
			"%w: %w: trajectory %s has no execution",
			ErrExecutionNotFound,
			sdk.ErrTrajectoryExecution,
			metadata.ID,
		)
	}
	view := ExecutionView{
		TrajectoryID: metadata.ID,
		Execution:    *metadata.Execution,
	}
	if metadata.Execution.State == sdk.TrajectoryExecutionSucceeded {
		result, err := LoadExecutionResult(ctx, store, metadata)
		if err != nil {
			return ExecutionView{}, err
		}
		view.Result = result
	}
	return view, nil
}

// LoadExecutionRecoveryCandidate loads the current non-terminal execution and
// computes how long a recovery host should wait before attempting to claim it.
func LoadExecutionRecoveryCandidate(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	now time.Time,
) (ExecutionRecoveryCandidate, error) {
	return ExecutionLifecycle{
		store: store,
		now:   func() time.Time { return now },
	}.LoadRecoveryCandidate(ctx, trajectoryID)
}

// ExecutionRecoveryCandidateFromMetadata projects metadata into the recovery
// read model when a non-terminal execution exists.
func ExecutionRecoveryCandidateFromMetadata(
	metadata sdk.TrajectoryMetadata,
	now time.Time,
) (ExecutionRecoveryCandidate, bool) {
	candidate, err := executionRecoveryCandidateFromMetadata(metadata, now)
	return candidate, err == nil
}

func executionRecoveryCandidateFromMetadata(
	metadata sdk.TrajectoryMetadata,
	now time.Time,
) (ExecutionRecoveryCandidate, error) {
	if metadata.Execution == nil {
		return ExecutionRecoveryCandidate{}, fmt.Errorf(
			"%w: %w: trajectory %s has no execution",
			ErrExecutionNotFound,
			sdk.ErrTrajectoryExecution,
			metadata.ID,
		)
	}
	if metadata.Execution.Terminal() {
		return ExecutionRecoveryCandidate{}, fmt.Errorf(
			"%w: %w: trajectory %s execution %s is %s",
			ErrExecutionNotRecoverable,
			sdk.ErrTrajectoryExecution,
			metadata.ID,
			metadata.Execution.ID,
			metadata.Execution.State,
		)
	}
	execution := *metadata.Execution
	return ExecutionRecoveryCandidate{
		TrajectoryID: metadata.ID,
		Execution:    execution,
		Delay:        execution.RecoveryDelay(now),
	}, nil
}

// FenceExecutionCancellation is the low-level durable cancellation fence for a
// trajectory execution when no runtime can own the completion boundary.
func FenceExecutionCancellation(
	ctx context.Context,
	store sdk.TrajectoryStore,
	trajectoryID string,
	executionID string,
	reason string,
	now time.Time,
) (ExecutionView, error) {
	return ExecutionLifecycle{
		store: store,
		now:   func() time.Time { return now },
	}.FenceCancellation(ctx, trajectoryID, executionID, reason)
}

// CancelExecution cancels one trajectory execution. Locally hosted executions
// are interrupted and allowed to commit their own cancelled unwind; executions
// not hosted by this runtime use the runtime-owned cancellation transition for
// the configured backend.
func (runtime *Runtime) CancelExecution(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	reason string,
) (ExecutionView, error) {
	if runtime == nil {
		return ExecutionView{}, errors.New("runtime is nil")
	}
	if reason == "" {
		reason = "execution cancelled"
	}
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return ExecutionView{}, err
	}
	defer releaseWork()
	cancelledHost, err := runtime.trajectoryExecution.cancelHostedAndWait(
		ctx,
		trajectoryID,
		executionID,
		fmt.Errorf("%s: %w", reason, context.Canceled),
	)
	if err != nil {
		return ExecutionView{}, err
	}
	if cancelledHost {
		return LoadExecutionView(
			ctx,
			runtime.trajectories,
			trajectoryID,
		)
	}
	view, err := runtime.cancelTrajectoryExecution(
		ctx,
		trajectoryID,
		executionID,
		reason,
		time.Now().UTC(),
	)
	if err != nil {
		return ExecutionView{}, err
	}
	runtime.trajectoryExecution.cancelHosted(
		trajectoryID,
		executionID,
		context.Canceled,
	)
	return view, nil
}

func (runtime *Runtime) continueExistingExecution(
	ctx context.Context,
	metadata sdk.TrajectoryMetadata,
) (Result, bool, error) {
	if metadata.Execution == nil {
		return Result{}, false, nil
	}
	if _, ok := ExecutionRecoveryCandidateFromMetadata(
		metadata,
		time.Now().UTC(),
	); ok {
		result, err := runtime.RecoverExecution(ctx, metadata.ID)
		return result, true, err
	}
	if metadata.Execution.State != sdk.TrajectoryExecutionSucceeded {
		return Result{}, true, fmt.Errorf(
			"trajectory %q execution %q ended in state %q: %s",
			metadata.ID,
			metadata.Execution.ID,
			metadata.Execution.State,
			metadata.Execution.LastError,
		)
	}
	result, err := LoadExecutionResult(ctx, runtime.trajectories, metadata)
	if err != nil {
		return Result{}, true, err
	}
	if result == nil {
		return Result{}, true, fmt.Errorf(
			"trajectory %q execution %q succeeded without a checkpoint result",
			metadata.ID,
			metadata.Execution.ID,
		)
	}
	return *result, true, nil
}
