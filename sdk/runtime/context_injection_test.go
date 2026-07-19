package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestContextInjectionRejectsWrongTargetSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	runtime, err := NewRuntime(RuntimeConfig{Storage: newTestStateBackend()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "target-session"})
	if err != nil {
		t.Fatal(err)
	}

	err = session.EnqueueContextInjection(ctx, sdk.ContextInjection{
		TargetSessionID: "other-session",
		Messages: []sdk.Message{{
			Role:    sdk.RoleUser,
			Content: "wrong target",
		}},
	})
	if err == nil {
		t.Fatal("EnqueueContextInjection accepted wrong target session")
	}
	if !strings.Contains(
		err.Error(),
		`context injection targets session "other-session", not "target-session"`,
	) {
		t.Fatalf("wrong target error = %v", err)
	}
}
