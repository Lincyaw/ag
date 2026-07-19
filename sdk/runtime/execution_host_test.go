package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

type panicCloseBackend struct {
	sdk.StateBackend
}

func (backend *panicCloseBackend) Close(context.Context) error {
	panic("broken state close")
}

func TestExecutionHostCommandPanicStillClosesHost(t *testing.T) {
	t.Parallel()
	state := &closeCountingBackend{StateBackend: newTestStateBackend()}
	_, err := runExecutionHostCommand(
		context.Background(),
		ExecutionHost{State: state},
		func(context.Context) (sdk.Operation, error) {
			panic("broken host command")
		},
	)
	if err == nil || !strings.Contains(err.Error(), "broken host command") {
		t.Fatalf("command error = %v", err)
	}
	if got := state.closes.Load(); got != 1 {
		t.Fatalf("state closes = %d, want 1", got)
	}
}

func TestExecutionHostEnqueueContextInjectionClosesStateHost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newTestStateBackend()
	runtime, err := NewRuntime(RuntimeConfig{Storage: backend})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "host-context-injection",
	})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(ctx, "base prompt")
	if err != nil {
		t.Fatal(err)
	}
	state := &closeCountingBackend{StateBackend: backend}
	view, err := (ExecutionHost{State: state}).EnqueueContextInjection(
		ctx,
		session.ID(),
		submission.Execution().ID,
		sdk.ContextInjection{
			Priority: sdk.ContextInjectionNext,
			Mode:     sdk.ContextInjectionTaskNotification,
			Origin:   "test",
			Messages: []sdk.Message{{
				Role:    sdk.RoleUser,
				Content: "queued context",
			}},
		},
	)
	if err != nil {
		t.Fatalf("enqueue context injection: %v", err)
	}
	if view.Execution.ID != submission.Execution().ID {
		t.Fatalf("execution view = %#v", view)
	}
	if got := state.closes.Load(); got != 1 {
		t.Fatalf("state closes = %d, want 1", got)
	}
	queued, err := backend.ContextInjections().List(
		ctx,
		sdk.ContextInjectionQuery{
			TargetSessionID:   session.ID(),
			TargetExecutionID: submission.Execution().ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 ||
		len(queued[0].Messages) != 1 ||
		queued[0].Messages[0].Content != "queued context" {
		t.Fatalf("queued context injections = %#v", queued)
	}
}

func TestExecutionHostClosePanicBecomesError(t *testing.T) {
	t.Parallel()
	host := ExecutionHost{
		State: &panicCloseBackend{StateBackend: newTestStateBackend()},
	}
	err := host.Close(context.Background())
	if err == nil {
		t.Fatal("Close() error = nil")
	}
	if got := err.Error(); !strings.Contains(got, "close state backend panic") ||
		!strings.Contains(got, "broken state close") {
		t.Fatalf("Close() error = %v", err)
	}
}
