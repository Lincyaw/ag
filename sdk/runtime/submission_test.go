package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

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
	if _, err := submission.Run(ctx); err == nil {
		t.Fatal("second submission run succeeded")
	}
}
