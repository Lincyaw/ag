package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lincyaw/ag/sdk"
)

const (
	executionCompositionDigestAttribute = "ag.runtime.composition_digest"
	executionSDKAPIVersionAttribute     = "ag.runtime.sdk_api_version"
)

func (runtime *Runtime) beginTrajectoryWork() error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.closed {
		return errors.New("runtime is closed")
	}
	runtime.trajectoryWait.Add(1)
	return nil
}

func (runtime *Runtime) endTrajectoryWork() {
	runtime.trajectoryWait.Done()
}

func (session *Session) beginExecution(
	ctx context.Context,
	userMessage sdk.Message,
) error {
	lease, err := session.runtime.acquireSnapshot()
	if err != nil {
		return err
	}
	generation := lease.snapshot.generation
	environment, err := newTrajectoryEnvironment(
		session.runtime,
		lease.snapshot,
		session.config,
	)
	lease.release()
	if err != nil {
		return err
	}

	executionID := sdk.NewID()
	raw, err := json.Marshal(userMessage)
	if err != nil {
		return fmt.Errorf("encode user_message trajectory entry: %w", err)
	}
	entry := sdk.TrajectoryEntry{
		ID:         sdk.NewID(),
		ParentID:   session.head,
		Kind:       sdk.TrajectoryKindUserMessage,
		Timestamp:  time.Now().UTC(),
		Generation: generation,
		Fields: sdk.TrajectoryEntryFields{
			ExecutionID: executionID,
		},
		Payload: raw,
		Attributes: map[string]string{
			executionCompositionDigestAttribute: environment.CompositionDigest,
			executionSDKAPIVersionAttribute: strconv.Itoa(
				environment.SDKAPIVersion,
			),
		},
	}
	metadata, err := session.runtime.trajectories.BeginExecution(
		ctx,
		session.config.ID,
		session.head,
		sdk.TrajectoryExecutionStart{
			ID:       executionID,
			Provider: session.config.Provider,
			System:   session.config.System,
			MaxTurns: session.config.MaxTurns,
		},
		entry,
	)
	if err != nil {
		return fmt.Errorf("begin trajectory execution: %w", err)
	}
	session.head = metadata.Head
	session.runtime.emitTrajectoryEvent(
		ctx,
		sdk.EventTrajectoryAppend,
		sdk.TrajectoryEventPayload{
			TrajectoryID: session.config.ID,
			EntryID:      entry.ID,
			EntryKind:    entry.Kind,
			Generation:   generation,
		},
	)
	return nil
}

func (session *Session) claimExecution(ctx context.Context) error {
	execution, err := session.runtime.trajectories.ClaimExecution(
		ctx,
		session.config.ID,
		session.runtime.trajectoryWorkerID,
		time.Now().UTC(),
		session.runtime.trajectoryLease,
	)
	if err != nil {
		return fmt.Errorf("claim trajectory execution: %w", err)
	}
	session.executionMu.Lock()
	session.executionID = execution.ID
	session.executionToken = execution.LeaseToken
	session.executionMu.Unlock()
	return nil
}

func (session *Session) activeExecution() (string, string) {
	session.executionMu.Lock()
	defer session.executionMu.Unlock()
	return session.executionID, session.executionToken
}

func (session *Session) executionOperationKey(
	kind string,
	coordinate string,
) string {
	executionID, _ := session.activeExecution()
	if executionID == "" {
		return session.head
	}
	sum := sha256.Sum256([]byte(
		executionID + "\x00" + kind + "\x00" + coordinate,
	))
	return hex.EncodeToString(sum[:])
}

func (session *Session) clearExecution(executionID string, token string) {
	session.executionMu.Lock()
	defer session.executionMu.Unlock()
	if session.executionID == executionID &&
		session.executionToken == token {
		session.executionID = ""
		session.executionToken = ""
	}
}

func (session *Session) executionHeartbeat(
	parent context.Context,
) (context.Context, func() error) {
	ctx, cancel := context.WithCancel(parent)
	stopRuntimeCancel := context.AfterFunc(
		session.runtime.trajectoryContext,
		cancel,
	)
	done := make(chan struct{})
	lost := make(chan error, 1)
	executionID, token := session.activeExecution()
	go func() {
		defer close(done)
		interval := session.runtime.trajectoryLease / 3
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_, err := session.runtime.trajectories.RenewExecution(
					ctx,
					session.config.ID,
					executionID,
					token,
					now.UTC(),
					session.runtime.trajectoryLease,
				)
				if err == nil {
					continue
				}
				activeID, activeToken := session.activeExecution()
				if activeID != executionID || activeToken != token {
					return
				}
				select {
				case lost <- err:
				default:
				}
				cancel()
				return
			}
		}
	}()
	return ctx, func() error {
		stopRuntimeCancel()
		cancel()
		<-done
		select {
		case err := <-lost:
			return fmt.Errorf("trajectory execution lease lost: %w", err)
		default:
			return nil
		}
	}
}

func (session *Session) commitExecution(
	ctx context.Context,
	entry sdk.TrajectoryEntry,
	state sdk.TrajectoryExecutionState,
	executionError string,
) error {
	executionID, token := session.activeExecution()
	if executionID == "" || token == "" {
		return errors.New("session has no claimed trajectory execution")
	}
	entry.Fields.ExecutionID = executionID
	commit := sdk.TrajectoryExecutionCommit{
		TrajectoryID: session.config.ID,
		ExecutionID:  executionID,
		LeaseToken:   token,
		ExpectedHead: session.head,
		State:        state,
		Error:        executionError,
	}
	if entry.ID != "" {
		commit.Entries = []sdk.TrajectoryEntry{entry}
	}
	metadata, err := session.runtime.commitTrajectoryExecution(ctx, commit)
	if err != nil {
		return err
	}
	session.head = metadata.Head
	if state != "" && state != sdk.TrajectoryExecutionRunning {
		session.clearExecution(executionID, token)
	}
	return nil
}

func (runtime *Runtime) commitTrajectoryExecution(
	ctx context.Context,
	commit sdk.TrajectoryExecutionCommit,
) (sdk.TrajectoryMetadata, error) {
	if runtime.storage.Capabilities().AtomicState {
		atomic, ok := runtime.storage.(sdk.AtomicStateBackend)
		if !ok {
			return sdk.TrajectoryMetadata{}, errors.New(
				"state backend advertises atomic state without implementing AtomicStateBackend",
			)
		}
		result, err := atomic.CommitExecutionStep(
			ctx,
			sdk.ExecutionStepCommit{Trajectory: commit},
		)
		return result.Trajectory, err
	}
	return runtime.trajectories.CommitExecution(ctx, commit)
}

func (session *Session) failExecution(
	ctx context.Context,
	cause error,
) error {
	executionID, token := session.activeExecution()
	if executionID == "" || token == "" {
		return nil
	}
	metadata, err := session.runtime.trajectories.LoadMetadata(
		ctx,
		session.config.ID,
	)
	if err != nil {
		return fmt.Errorf("load trajectory for failure restore: %w", err)
	}
	checkpointEntry, checkpoint, err := latestTrajectoryCheckpoint(
		ctx,
		session.runtime.trajectories,
		metadata,
	)
	if err != nil {
		return err
	}
	checkpointID := checkpointEntry.ID
	commit := sdk.TrajectoryExecutionCommit{
		TrajectoryID: session.config.ID,
		ExecutionID:  executionID,
		LeaseToken:   token,
		ExpectedHead: metadata.Head,
		State:        sdk.TrajectoryExecutionFailed,
	}
	if session.runtime.trajectoryContext.Err() != nil {
		commit.State = sdk.TrajectoryExecutionPending
	} else if errors.Is(cause, context.Canceled) ||
		errors.Is(cause, context.DeadlineExceeded) {
		commit.State = sdk.TrajectoryExecutionCancelled
	}
	if cause != nil {
		commit.Error = cause.Error()
	}
	restored, err := trajectoryHeadRestoresCheckpoint(
		ctx,
		session.runtime.trajectories,
		metadata.ID,
		metadata.Head,
		checkpointID,
	)
	if err != nil {
		return err
	}
	if !restored {
		raw, marshalErr := json.Marshal(map[string]string{
			"from": metadata.Head,
			"to":   checkpointID,
		})
		if marshalErr != nil {
			return marshalErr
		}
		commit.Entries = []sdk.TrajectoryEntry{{
			ID:        sdk.NewID(),
			ParentID:  checkpointID,
			Kind:      sdk.TrajectoryKindRestore,
			Timestamp: time.Now().UTC(),
			Fields: sdk.TrajectoryEntryFields{
				ExecutionID: executionID,
			},
			Payload: raw,
		}}
	}
	updated, err := session.runtime.commitTrajectoryExecution(ctx, commit)
	if err != nil {
		return fmt.Errorf("fail trajectory execution: %w", err)
	}
	session.head = updated.Head
	session.messages = cloneMessages(checkpointMessages(checkpoint))
	if checkpoint != nil {
		session.config.System = checkpoint.System
		if checkpoint.Provider != "" {
			session.config.Provider = checkpoint.Provider
		}
	}
	session.clearExecution(executionID, token)
	return nil
}

func executionStateForCause(cause sdk.Cause) sdk.TrajectoryExecutionState {
	switch cause.Code {
	case "provider_error", "hook_error":
		return sdk.TrajectoryExecutionFailed
	case "cancelled":
		return sdk.TrajectoryExecutionCancelled
	default:
		return sdk.TrajectoryExecutionSucceeded
	}
}
