package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type runtimeContextTestKey struct{}

func TestNewRuntimeContextPreservesValuesForOwnedAsyncWork(t *testing.T) {
	t.Parallel()
	parent := context.WithValue(
		context.Background(),
		runtimeContextTestKey{},
		"trace-value",
	)
	observed := make(chan any, 1)
	runtime, err := NewRuntimeContext(parent, RuntimeConfig{
		Storage: sdkstorage.NewMemoryStateBackend(),
		EventObserver: func(ctx context.Context, _ sdk.Event) {
			observed <- ctx.Value(runtimeContextTestKey{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})

	runtime.observeEvent(context.Background(), sdk.Event{
		ID:      "runtime-context-event",
		Name:    "runtime.context",
		Payload: json.RawMessage(`{}`),
	})

	select {
	case value := <-observed:
		if value != "trace-value" {
			t.Fatalf("observer context value = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not run")
	}
}

func TestNewRuntimeRejectsUnknownAgentForkPolicy(t *testing.T) {
	t.Parallel()
	_, err := NewRuntime(RuntimeConfig{
		Storage:         sdkstorage.NewMemoryStateBackend(),
		AgentForkPolicy: AgentForkPolicy("surprise"),
	})
	if err == nil || err.Error() != `unknown agent fork policy "surprise"` {
		t.Fatalf("runtime config error = %v", err)
	}
}
