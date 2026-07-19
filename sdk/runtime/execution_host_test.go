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
