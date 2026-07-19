package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// RecoveredExecution identifies one trajectory resumed by runtime recovery.
type RecoveredExecution struct {
	TrajectoryID string `json:"trajectory_id"`
	Result       Result `json:"result"`
}

func (runtime *Runtime) RecoverExecutions(
	ctx context.Context,
) ([]RecoveredExecution, error) {
	candidates, err := NewRuntimeExecutionControl(
		runtime,
	).ListRecoveryCandidates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list recoverable trajectory executions: %w", err)
	}
	results := make([]RecoveredExecution, 0, len(candidates))
	var recoveryErrors []error
	for _, candidate := range candidates {
		result, recoverErr := runtime.RecoverExecution(
			ctx,
			candidate.TrajectoryID,
		)
		if recoverErr != nil {
			if errors.Is(recoverErr, sdk.ErrTrajectoryClaimed) {
				continue
			}
			recoveryErrors = append(
				recoveryErrors,
				fmt.Errorf(
					"recover trajectory %s: %w",
					candidate.TrajectoryID,
					recoverErr,
				),
			)
			continue
		}
		results = append(results, RecoveredExecution{
			TrajectoryID: candidate.TrajectoryID,
			Result:       result,
		})
	}
	return results, errors.Join(recoveryErrors...)
}

func (runtime *Runtime) RecoverExecution(
	ctx context.Context,
	id string,
) (Result, error) {
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return Result{}, err
	}
	defer releaseWork()
	plan, err := runtime.prepareExecutionRecovery(ctx, id)
	if err != nil {
		return Result{}, err
	}
	defer plan.release()
	if err := plan.session.claimExecution(ctx); err != nil {
		return Result{}, err
	}
	return plan.session.runClaimedExecution(
		ctx,
		func(executionCtx context.Context) (Result, error) {
			return runtime.continueRecoveredExecution(executionCtx, plan)
		},
	)
}

type executionRecoveryPlan struct {
	metadata  sdk.TrajectoryMetadata
	execution sdk.TrajectoryExecution
	input     sdk.TrajectoryEntry
	session   *Session
	pin       *snapshotLease
}

func (plan executionRecoveryPlan) release() {
	if plan.pin == nil {
		return
	}
	plan.pin.release()
}

func (runtime *Runtime) prepareExecutionRecovery(
	ctx context.Context,
	id string,
) (executionRecoveryPlan, error) {
	metadata, candidate, err := runtime.waitExecutionRecoveryCandidate(ctx, id)
	if err != nil {
		return executionRecoveryPlan{}, err
	}
	storedExecution := candidate.Execution
	inputEntry, err := runtime.trajectories.LoadEntry(
		ctx,
		id,
		storedExecution.InputEntryID,
	)
	if err != nil {
		return executionRecoveryPlan{}, err
	}
	recordedEnvironment, err := executionResumeEnvironment(
		metadata.Environment,
		inputEntry,
	)
	if err != nil {
		return executionRecoveryPlan{}, err
	}
	config := SessionConfig{
		ID:           id,
		Provider:     storedExecution.Provider,
		System:       storedExecution.System,
		MaxTurns:     storedExecution.MaxTurns,
		ResumePolicy: ResumeExact,
	}
	if err := validateSessionConfig(runtime, &config); err != nil {
		return executionRecoveryPlan{}, err
	}
	projection, err := runtime.acquireExactResumeProjection(
		metadata.Environment,
		config,
		nil,
		recordedEnvironment,
	)
	if err != nil {
		return executionRecoveryPlan{}, err
	}
	executionPin := projection.Lease

	session := runtime.projectTrajectorySession(trajectorySessionProjection{
		Metadata:       metadata,
		Config:         projection.Config,
		Head:           metadata.Head,
		PinnedSnapshot: projection.snapshot(),
	})
	return executionRecoveryPlan{
		metadata:  metadata,
		execution: storedExecution,
		input:     inputEntry,
		session:   session,
		pin:       executionPin,
	}, nil
}

func (runtime *Runtime) waitExecutionRecoveryCandidate(
	ctx context.Context,
	id string,
) (sdk.TrajectoryMetadata, ExecutionRecoveryCandidate, error) {
	for {
		metadata, err := runtime.trajectories.LoadMetadata(ctx, id)
		if err != nil {
			return sdk.TrajectoryMetadata{}, ExecutionRecoveryCandidate{}, err
		}
		candidate, candidateErr := executionRecoveryCandidateFromMetadata(
			metadata,
			time.Now().UTC(),
		)
		if candidateErr != nil {
			return sdk.TrajectoryMetadata{}, ExecutionRecoveryCandidate{}, candidateErr
		}
		if candidate.Delay <= 0 {
			return metadata, candidate, nil
		}
		if err := runtime.waitExecutionRecoveryDelay(ctx, candidate); err != nil {
			return sdk.TrajectoryMetadata{}, ExecutionRecoveryCandidate{}, err
		}
	}
}

func (runtime *Runtime) waitExecutionRecoveryDelay(
	ctx context.Context,
	candidate ExecutionRecoveryCandidate,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithCancelCause(ctx)
	stopRuntimeCancel := runtime.trajectoryExecution.cancelAfter(cancel)
	defer func() {
		stopRuntimeCancel()
		cancel(context.Canceled)
	}()
	return candidate.Wait(waitCtx)
}

func (runtime *Runtime) continueRecoveredExecution(
	ctx context.Context,
	plan executionRecoveryPlan,
) (Result, error) {
	base, err := durability.LoadExecutionRecoveryBase(
		ctx,
		runtime.trajectories,
		plan.metadata,
		plan.input,
	)
	if err != nil {
		return Result{}, err
	}
	if err := plan.session.restoreExecutionHead(ctx, base.Head); err != nil {
		return Result{}, err
	}
	if base.Checkpoint != nil {
		session := plan.session
		session.applyCheckpointProjection(base.Checkpoint)
		recoveredResult := resultFromCheckpoint(
			base.CheckpointEntry,
			base.Checkpoint,
		)
		execution := &promptExecution{
			session:  session,
			messages: sdk.CloneMessages(recoveredResult.Messages),
			system:   base.Checkpoint.System,
			dependencies: append(
				[]string(nil),
				base.Checkpoint.Dependencies...,
			),
			result: *recoveredResult,
		}
		if base.Checkpoint.Action.Kind == sdk.ActionStop {
			cause := sdk.Cause{Code: sdk.CauseModelEnd}
			if base.Checkpoint.Action.Cause != nil {
				cause = *base.Checkpoint.Action.Cause
			}
			snapshotLease, acquireErr := session.acquireSnapshot()
			if acquireErr != nil {
				return Result{}, acquireErr
			}
			result, err := session.finish(
				ctx,
				snapshotLease.snapshot,
				execution.messages,
				execution.result,
				cause,
			)
			snapshotLease.release()
			session.applyMessageProjection(execution.messages)
			return result, err
		}
		return execution.runTurnsFrom(ctx, base.Checkpoint.Turns)
	}
	plan.session.applyMessageProjection(base.Messages)
	execution, err := newPromptExecutionFromAcceptedMessages(
		plan.session,
		base.Messages,
		base.Message,
	)
	if err != nil {
		return Result{}, fmt.Errorf(
			"trajectory execution input %s: %w",
			plan.input.ID,
			err,
		)
	}
	return execution.runFromStart(ctx)
}

func (session *Session) restoreExecutionHead(
	ctx context.Context,
	target string,
) error {
	moveHead, err := session.runtime.trajectoryHeadNeedsMove(
		ctx,
		session.config.ID,
		session.head,
		target,
	)
	if err != nil {
		return err
	}
	if !moveHead {
		return nil
	}
	lease, err := session.acquireSnapshot()
	if err != nil {
		return err
	}
	defer lease.release()
	move, events, err := session.runtime.prepareTrajectoryHeadMove(
		lease.snapshot,
		session.config.ID,
		session.head,
		target,
		sdk.TrajectoryKindRestore,
	)
	if err != nil {
		return err
	}
	if err := session.commitExecution(
		ctx,
		move.Entry,
		"",
		"",
		events,
	); err != nil {
		return fmt.Errorf("restore recoverable trajectory execution: %w", err)
	}
	return nil
}
