package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

// PromptSubmission separates durable acceptance from execution hosting.
type PromptSubmission struct {
	mu        sync.Mutex
	session   *Session
	input     durability.ExecutionInput
	execution sdk.TrajectoryExecution
	started   bool
}

// SubmitPrompt resumes an existing trajectory according to config and durably
// accepts prompt as a new execution. Hosting the accepted execution remains a
// separate boundary through PromptSubmission.Run.
func (runtime *Runtime) SubmitPrompt(
	ctx context.Context,
	id string,
	config SessionConfig,
	prompt string,
) (*PromptSubmission, error) {
	session, err := runtime.ResumeSession(ctx, id, config)
	if err != nil {
		return nil, err
	}
	return session.SubmitPrompt(ctx, prompt)
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
	return session.submitPromptLocked(ctx, prompt)
}

func (session *Session) submitPromptLocked(
	ctx context.Context,
	prompt string,
) (*PromptSubmission, error) {
	if strings.TrimSpace(prompt) == "" {
		return nil, errors.New("prompt is empty")
	}
	releaseWork, err := session.runtime.beginTrajectoryWork()
	if err != nil {
		return nil, err
	}
	defer releaseWork()
	accepted, err := session.beginExecution(ctx, newPromptUserMessage(prompt))
	if err != nil {
		return nil, err
	}
	return &PromptSubmission{
		session:   session,
		input:     accepted.Input,
		execution: accepted.Execution,
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

// LoadExecutionView loads the current trajectory-backed execution view for the
// accepted submission. Unlike ExecutionView, this reflects terminal result
// projection after the submitted execution has run.
func (submission *PromptSubmission) LoadExecutionView(
	ctx context.Context,
) (ExecutionView, error) {
	if submission == nil || submission.session == nil {
		return ExecutionView{}, errors.New("prompt submission is nil")
	}
	session := submission.session
	if session.runtime == nil || session.runtime.trajectories == nil {
		return ExecutionView{}, errors.New("prompt submission runtime is nil")
	}
	return LoadExecutionView(ctx, session.runtime.trajectories, session.config.ID)
}

// ExecutionView returns the accepted execution snapshot kept for synchronous
// callers. Use LoadExecutionView when a current trajectory projection is needed.
func (submission *PromptSubmission) ExecutionView() ExecutionView {
	if submission == nil || submission.session == nil {
		return ExecutionView{}
	}
	submission.mu.Lock()
	defer submission.mu.Unlock()
	return ExecutionView{
		TrajectoryID: submission.session.config.ID,
		Execution:    submission.execution,
	}
}

func (submission *PromptSubmission) Run(
	ctx context.Context,
) (Result, error) {
	if submission == nil || submission.session == nil {
		return Result{}, errors.New("prompt submission is nil")
	}
	session := submission.session
	session.mu.Lock()
	defer session.mu.Unlock()
	return submission.runLocked(ctx)
}

// runLocked requires submission.session.mu to be held by the caller.
func (submission *PromptSubmission) runLocked(
	ctx context.Context,
) (Result, error) {
	submission.mu.Lock()
	if submission.started {
		submission.mu.Unlock()
		return Result{}, errors.New("prompt submission has already started")
	}
	submission.started = true
	expectedExecutionID := submission.execution.ID
	input := durability.NewExecutionInput(
		submission.input.Message,
		submission.input.Environment,
		submission.input.BaseMessages,
	)
	submission.mu.Unlock()

	session := submission.session
	releaseWork, err := session.runtime.beginTrajectoryWork()
	if err != nil {
		return Result{}, err
	}
	defer releaseWork()
	pin, restoreSnapshot, err := session.pinExecutionSnapshot(input.Environment)
	if err != nil {
		return Result{}, err
	}
	defer restoreSnapshot()
	defer pin.release()
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
	session.applyMessageProjection(input.BaseMessages)
	execution, err := newPromptExecutionFromInput(session, input)
	if err != nil {
		return Result{}, err
	}
	return session.runClaimedExecution(ctx, execution.runFromStart)
}
