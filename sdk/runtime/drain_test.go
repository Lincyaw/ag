package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type drainBoundaryProvider struct {
	mu       sync.Mutex
	entered  chan struct{}
	release  chan struct{}
	toolTurn bool
	requests []sdk.ModelRequest
}

func (*drainBoundaryProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "scripted", Model: "drain-test"}
}

func (provider *drainBoundaryProvider) Complete(
	ctx context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	provider.requests = append(provider.requests, sdk.ModelRequest{
		Messages: sdk.CloneMessages(request.Messages),
		Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
	})
	provider.mu.Unlock()
	if provider.toolTurn {
		if provider.entered != nil {
			close(provider.entered)
		}
		if provider.release != nil {
			select {
			case <-ctx.Done():
				return sdk.ModelResponse{}, ctx.Err()
			case <-provider.release:
			}
		}
		return sdk.ModelResponse{
			Model: "drain-test", FinishReason: "tool_calls",
			ToolCalls: []sdk.ToolCall{{
				ID: "drain-tool-call", Name: "echo",
				Arguments: json.RawMessage(`{"value":"checkpoint"}`),
			}},
		}, nil
	}
	return sdk.ModelResponse{
		Content: "recovered after drain", Model: "drain-test",
		FinishReason: "stop",
	}, nil
}

func (provider *drainBoundaryProvider) Requests() []sdk.ModelRequest {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	requests := make([]sdk.ModelRequest, len(provider.requests))
	copy(requests, provider.requests)
	return requests
}

type drainBoundaryTool struct{}

func (*drainBoundaryTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name: "echo", Description: "returns a durable drain checkpoint",
		Parameters: map[string]any{"type": "object"},
	}
}

func (*drainBoundaryTool) Call(
	context.Context,
	json.RawMessage,
) (sdk.ToolResult, error) {
	return sdk.ToolResult{Content: "checkpoint committed"}, nil
}

func TestRuntimeDrainHandsOffAfterCurrentModelTurn(t *testing.T) {
	state := newTestStateBackend()
	firstProvider := &drainBoundaryProvider{
		entered: make(chan struct{}), release: make(chan struct{}), toolTurn: true,
	}
	first, err := NewRuntime(RuntimeConfig{
		Storage: state, StorageOwnership: StorageBorrowed,
		OperationPoll: time.Millisecond, TrajectoryLease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Mount(
		t.Context(),
		sdk.Local(trajectoryRecoveryPlugin(firstProvider, &drainBoundaryTool{})),
	); err != nil {
		t.Fatal(err)
	}
	session, err := first.NewSession(t.Context(), SessionConfig{
		ID: "drain-boundary", Provider: "scripted", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(
			context.Background(),
			"stop after one complete model turn",
		)
		promptDone <- promptErr
	}()
	select {
	case <-firstProvider.entered:
	case <-time.After(time.Second):
		t.Fatal("provider call did not start")
	}
	drainDone := make(chan error, 1)
	go func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		drainDone <- first.Drain(drainCtx)
	}()
	select {
	case err := <-drainDone:
		t.Fatalf("drain returned before provider response boundary: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(firstProvider.release)
	select {
	case err := <-promptDone:
		if !errors.Is(err, ErrRuntimeDraining) {
			t.Fatalf("prompt error = %v, want runtime draining", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("prompt did not hand off after its current turn")
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	metadata, err := state.Trajectories().LoadMetadata(t.Context(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending ||
		metadata.Execution.LeaseToken != "" {
		t.Fatalf("drained execution = %#v", metadata.Execution)
	}
	firstTrajectory, err := state.Trajectories().Load(t.Context(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	var firstRequests, checkpoints int
	for _, entry := range firstTrajectory.Entries {
		switch entry.Kind {
		case sdk.TrajectoryKindProviderRequest:
			firstRequests++
		case sdk.TrajectoryKindCheckpoint:
			checkpoints++
		}
	}
	if firstRequests != 1 || checkpoints == 0 {
		t.Fatalf(
			"durable turn boundary requests=%d checkpoints=%d",
			firstRequests,
			checkpoints,
		)
	}
	closeRuntimeForDrainTest(t, first)

	secondProvider := &drainBoundaryProvider{}
	second, err := NewRuntime(RuntimeConfig{
		Storage: state, StorageOwnership: StorageBorrowed,
		OperationPoll: time.Millisecond, TrajectoryLease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeRuntimeForDrainTest(t, second) })
	if _, err := second.Mount(
		t.Context(),
		sdk.Local(trajectoryRecoveryPlugin(secondProvider, &drainBoundaryTool{})),
	); err != nil {
		t.Fatal(err)
	}
	result, err := second.RecoverExecution(t.Context(), session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "recovered after drain" || result.Turns != 2 {
		t.Fatalf("recovered result = %#v", result)
	}
	requests := secondProvider.Requests()
	if len(requests) != 1 {
		t.Fatalf("provider calls after recovery = %d, want 1", len(requests))
	}
	foundToolResult := false
	for _, message := range requests[0].Messages {
		if message.Role == sdk.RoleTool && message.Content == "checkpoint committed" {
			foundToolResult = true
		}
	}
	if !foundToolResult {
		t.Fatalf("recovery did not resume from drained checkpoint: %#v", requests[0])
	}
}

func closeRuntimeForDrainTest(t *testing.T, runtime *Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
