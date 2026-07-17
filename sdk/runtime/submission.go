package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type PromptSubmission struct {
	mu        sync.Mutex
	session   *Session
	prompt    string
	execution sdk.TrajectoryExecution
	started   bool
}

func (session *Session) SubmitPrompt(
	ctx context.Context,
	prompt string,
) (*PromptSubmission, error) {
	if session == nil {
		return nil, errors.New("session is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if strings.TrimSpace(prompt) == "" {
		return nil, errors.New("prompt is empty")
	}
	execution := newPromptExecution(session, prompt)
	if err := session.beginExecution(ctx, execution.userMessage); err != nil {
		return nil, err
	}
	loadCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		5*time.Second,
	)
	defer cancel()
	metadata, err := session.runtime.trajectories.LoadMetadata(
		loadCtx,
		session.config.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("load submitted trajectory execution: %w", err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.Terminal() {
		return nil, fmt.Errorf(
			"%w: trajectory %s has no pending execution after submit",
			sdk.ErrTrajectoryExecution,
			session.config.ID,
		)
	}
	return &PromptSubmission{
		session:   session,
		prompt:    prompt,
		execution: *metadata.Execution,
	}, nil
}

func (submission *PromptSubmission) Execution() sdk.TrajectoryExecution {
	if submission == nil {
		return sdk.TrajectoryExecution{}
	}
	submission.mu.Lock()
	defer submission.mu.Unlock()
	return submission.execution
}

func (submission *PromptSubmission) Run(
	ctx context.Context,
) (result Result, returnErr error) {
	if submission == nil || submission.session == nil {
		return Result{}, errors.New("prompt submission is nil")
	}
	submission.mu.Lock()
	if submission.started {
		submission.mu.Unlock()
		return Result{}, errors.New("prompt submission has already started")
	}
	submission.started = true
	expectedExecutionID := submission.execution.ID
	submission.mu.Unlock()

	session := submission.session
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.claimExecution(ctx); err != nil {
		return Result{}, err
	}
	executionID, _ := session.activeExecution()
	if executionID != expectedExecutionID {
		return Result{}, fmt.Errorf(
			"claimed trajectory execution %q, expected %q",
			executionID,
			expectedExecutionID,
		)
	}
	execution := newPromptExecution(session, submission.prompt)
	execution.mutated = true
	defer func() {
		if returnErr == nil {
			return
		}
		restoreCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		returnErr = errors.Join(
			returnErr,
			session.failExecution(restoreCtx, returnErr),
		)
	}()
	executionCtx, stopHeartbeat := session.executionHeartbeat(ctx)
	defer func() {
		returnErr = errors.Join(returnErr, stopHeartbeat())
	}()

	var done bool
	result, done, returnErr = execution.start(executionCtx)
	if returnErr != nil || done {
		return result, returnErr
	}
	return execution.runTurns(executionCtx)
}
