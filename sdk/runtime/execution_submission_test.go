package runtime

// Execution tests cover durable prompt acceptance and hosting.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type acceptedEnvironmentProvider struct {
	model string
	calls atomic.Int64
}

func (provider *acceptedEnvironmentProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "accepted-environment", Model: provider.model}
}

func (provider *acceptedEnvironmentProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.calls.Add(1)
	return sdk.ModelResponse{
		Content:      "unexpected current composition",
		Model:        provider.model,
		FinishReason: "stop",
	}, nil
}

func acceptedEnvironmentPlugin(
	version string,
	provider *acceptedEnvironmentProvider,
) sdk.Plugin {
	return sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "accepted-environment-plugin",
			Version:     version,
			Description: "tests accepted execution environment binding",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("accepted-environment"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(provider)
		},
	}
}

func TestSubmitPromptPersistsBeforeExecutionStarts(t *testing.T) {
	ctx := t.Context()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	tool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(provider, tool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "submitted-prompt", Provider: "scripted", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "run asynchronously")
	if err != nil {
		t.Fatal(err)
	}
	execution := submission.Execution()
	if execution.ID == "" ||
		execution.State != sdk.TrajectoryExecutionPending ||
		provider.submissions != 0 {
		t.Fatalf(
			"submitted execution=%#v provider submissions=%d",
			execution,
			provider.submissions,
		)
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		session.ID(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != execution.ID ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("persisted execution = %#v", metadata.Execution)
	}
	result, err := submission.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "finished" || result.Turns != 2 {
		t.Fatalf("submission result = %#v", result)
	}
	view, err := submission.LoadExecutionView(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if view.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		view.Result == nil ||
		view.Result.Output != "finished" {
		t.Fatalf("completed submission view = %#v", view)
	}
	if _, err := submission.Run(ctx); err == nil {
		t.Fatal("second submission run succeeded")
	}
}

func TestRuntimeSubmitPromptResumesAndAcceptsExecution(t *testing.T) {
	ctx := t.Context()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	tool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(provider, tool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "runtime-submitted-prompt", Provider: "scripted", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := runtime.SubmitPrompt(
		ctx,
		session.ID(),
		SessionConfig{
			Provider:     "scripted",
			MaxTurns:     3,
			ResumePolicy: ResumeCurrent,
		},
		"resume and accept",
	)
	if err != nil {
		t.Fatal(err)
	}
	view, err := submission.LoadExecutionView(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if view.TrajectoryID != session.ID() ||
		view.Execution.ID == "" ||
		view.Execution.State != sdk.TrajectoryExecutionPending ||
		provider.submissions != 0 {
		t.Fatalf(
			"submission view=%#v provider submissions=%d",
			view,
			provider.submissions,
		)
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		session.ID(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != view.Execution.ID ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("persisted execution = %#v", metadata.Execution)
	}
}

func TestPromptSubmissionRunUsesAcceptedInputBaseMessages(t *testing.T) {
	ctx := t.Context()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &executionBaseMessageProvider{}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "submission-base-message",
			Version:     "1.0.0",
			Description: "records submitted execution base messages",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("base-message"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(provider)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "submitted-input-base",
		Provider: "base-message",
		System:   "base system",
		MaxTurns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	session.messages = []sdk.Message{{
		Role:    sdk.RoleUser,
		Content: "accepted base",
	}}
	submission, err := session.SubmitPrompt(ctx, "accepted prompt")
	if err != nil {
		t.Fatal(err)
	}
	session.messages = []sdk.Message{{
		Role:    sdk.RoleUser,
		Content: "polluted base",
	}}
	if _, err := submission.Run(ctx); err != nil {
		t.Fatal(err)
	}
	requests := provider.Requests()
	if len(requests) != 1 {
		t.Fatalf("provider requests = %d, want 1", len(requests))
	}
	messages := requests[0].Messages
	if len(messages) != 3 ||
		messages[0].Role != sdk.RoleSystem ||
		messages[1].Content != "accepted base" ||
		messages[2].Content != "accepted prompt" {
		t.Fatalf("submitted provider messages = %#v", messages)
	}
}

func TestPromptSubmissionRunUsesAcceptedCompositionEnvironment(t *testing.T) {
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       newTestStateBackend(),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	acceptedProvider := &acceptedEnvironmentProvider{model: "accepted-v1"}
	acceptedMount, err := runtime.Mount(
		ctx,
		sdk.Local(acceptedEnvironmentPlugin("1.0.0", acceptedProvider)),
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "submitted-input-environment",
		Provider: "accepted-environment",
		MaxTurns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "use accepted environment")
	if err != nil {
		t.Fatal(err)
	}
	if err := acceptedMount.Unmount(ctx); err != nil {
		t.Fatal(err)
	}
	currentProvider := &acceptedEnvironmentProvider{model: "current-v2"}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(acceptedEnvironmentPlugin("2.0.0", currentProvider)),
	); err != nil {
		t.Fatal(err)
	}

	_, err = submission.Run(ctx)
	if !errors.Is(err, ErrResumeEnvironmentMismatch) {
		t.Fatalf("submission run error = %v, want ErrResumeEnvironmentMismatch", err)
	}
	if calls := currentProvider.calls.Load(); calls != 0 {
		t.Fatalf("current provider calls = %d, want 0", calls)
	}
	if calls := acceptedProvider.calls.Load(); calls != 0 {
		t.Fatalf("accepted provider calls = %d, want 0", calls)
	}
	metadata, err := runtime.trajectories.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != submission.Execution().ID ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("execution after environment mismatch = %#v", metadata.Execution)
	}
}

func TestPromptSubmissionRunIgnoresUnrelatedCompositionChanges(t *testing.T) {
	ctx := t.Context()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       newTestStateBackend(),
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &acceptedEnvironmentProvider{model: "accepted-v1"}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(acceptedEnvironmentPlugin("1.0.0", provider)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "submitted-unrelated-environment",
		Provider: "accepted-environment",
		MaxTurns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "ignore unrelated marker")
	if err != nil {
		t.Fatal(err)
	}
	mountEnvironmentMarker(t, runtime, "accepted-unrelated-marker")

	result, err := submission.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "unexpected current composition" {
		t.Fatalf("submission result = %#v", result)
	}
	if calls := provider.calls.Load(); calls != 1 {
		t.Fatalf("accepted provider calls = %d, want 1", calls)
	}
}

func TestLoadExecutionRecoveryCandidateIncludesLeaseDelay(t *testing.T) {
	ctx := t.Context()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	tool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(provider, tool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "recovery-candidate", Provider: "scripted", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "recover later")
	if err != nil {
		t.Fatal(err)
	}
	execution := submission.Execution()
	now := time.Now().UTC()
	candidate, err := LoadExecutionRecoveryCandidate(
		ctx,
		backend.Trajectories(),
		session.ID(),
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.TrajectoryID != session.ID() ||
		candidate.Execution.ID != execution.ID ||
		candidate.Delay != 0 {
		t.Fatalf("pending recovery candidate = %#v", candidate)
	}

	claimed, err := backend.Trajectories().ClaimExecution(
		ctx,
		session.ID(),
		"other-worker",
		now,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err = LoadExecutionRecoveryCandidate(
		ctx,
		backend.Trajectories(),
		session.ID(),
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Execution.LeaseToken != claimed.LeaseToken ||
		candidate.Delay <= 0 {
		t.Fatalf("running recovery candidate = %#v", candidate)
	}

	if _, err := backend.Trajectories().CancelExecution(
		ctx,
		sdk.TrajectoryExecutionCancelCommit{
			TrajectoryID: session.ID(),
			ExecutionID:  claimed.ID,
			Reason:       "test complete",
			At:           now,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadExecutionRecoveryCandidate(
		ctx,
		backend.Trajectories(),
		session.ID(),
		now,
	); !errors.Is(err, sdk.ErrTrajectoryExecution) {
		t.Fatalf("terminal recovery candidate error = %v", err)
	}
}

func TestRecoverExecutionWaitsForRecoveryCandidateDelay(t *testing.T) {
	ctx := t.Context()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       backend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	provider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	tool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := runtime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(provider, tool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "recover-waits-for-lease", Provider: "scripted", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "recover after lease")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := backend.Trajectories().ClaimExecution(
		ctx,
		session.ID(),
		"other-worker",
		time.Now().UTC(),
		250*time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}

	recoverCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err = runtime.RecoverExecution(recoverCtx, session.ID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("recover execution error = %v, want context deadline", err)
	}
	metadata, err := backend.Trajectories().LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != submission.Execution().ID ||
		metadata.Execution.LeaseToken != claimed.LeaseToken {
		t.Fatalf("recover claimed execution = %#v", metadata.Execution)
	}
	if provider.submissions != 0 {
		t.Fatalf("provider submissions = %d, want 0", provider.submissions)
	}
}
