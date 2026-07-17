package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type trajectoryTestProvider struct {
	mu          sync.Mutex
	operations  map[string]Operation
	requests    []OperationRequest
	submissions int
	failNext    bool
}

func (provider *trajectoryTestProvider) Spec() ProviderSpec {
	return ProviderSpec{Name: "scripted", Model: "scripted-v1"}
}

func (provider *trajectoryTestProvider) SubmitCompletion(
	_ context.Context,
	request OperationRequest,
) (Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if operation, exists := provider.operations[request.IdempotencyKey]; exists {
		return operation, nil
	}
	provider.submissions++
	provider.requests = append(provider.requests, request)
	operation := Operation{
		ID:             fmt.Sprintf("provider-operation-%d", provider.submissions),
		IdempotencyKey: request.IdempotencyKey,
		State:          OperationPending,
		Revision:       1,
	}
	if provider.failNext {
		provider.failNext = false
		operation.Error = "fail-on-poll"
	}
	provider.operations[request.IdempotencyKey] = operation
	return operation, nil
}

func (provider *trajectoryTestProvider) PollCompletion(
	_ context.Context,
	id string,
	_ uint64,
) (Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for key, operation := range provider.operations {
		if operation.ID != id {
			continue
		}
		if operation.Error == "fail-on-poll" {
			operation.State = OperationFailed
			operation.Revision++
			operation.Error = "scripted provider failure"
			provider.operations[key] = operation
			return operation, nil
		}
		var response ModelResponse
		if id == "provider-operation-1" {
			response = ModelResponse{
				ToolCalls: []ToolCall{{
					ID:        "tool-call-1",
					Name:      "echo",
					Arguments: []byte(`{"value":"hello"}`),
				}},
				Model:        "scripted-v1",
				FinishReason: "tool_calls",
			}
		} else {
			response = ModelResponse{
				Content:      "finished",
				Model:        "scripted-v1",
				FinishReason: "stop",
			}
		}
		output, err := json.Marshal(response)
		if err != nil {
			return Operation{}, err
		}
		operation.State = OperationSucceeded
		operation.Revision++
		operation.Output = output
		provider.operations[key] = operation
		return operation, nil
	}
	return Operation{}, errors.New("provider operation not found")
}

func (provider *trajectoryTestProvider) CancelCompletion(
	_ context.Context,
	id string,
) (Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for key, operation := range provider.operations {
		if operation.ID == id {
			operation.State = OperationCancelled
			operation.Revision++
			provider.operations[key] = operation
			return operation, nil
		}
	}
	return Operation{}, errors.New("provider operation not found")
}

type trajectoryTestTool struct {
	mu         sync.Mutex
	operations map[string]Operation
	requests   []OperationRequest
}

func (tool *trajectoryTestTool) Spec() ToolSpec {
	return ToolSpec{
		Name:        "echo",
		Description: "returns its input",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *trajectoryTestTool) SubmitCall(
	_ context.Context,
	request OperationRequest,
) (Operation, error) {
	tool.mu.Lock()
	defer tool.mu.Unlock()
	if operation, exists := tool.operations[request.IdempotencyKey]; exists {
		return operation, nil
	}
	tool.requests = append(tool.requests, request)
	operation := Operation{
		ID:             "tool-operation-1",
		IdempotencyKey: request.IdempotencyKey,
		State:          OperationRunning,
		Revision:       1,
	}
	tool.operations[request.IdempotencyKey] = operation
	return operation, nil
}

func (tool *trajectoryTestTool) PollCall(
	_ context.Context,
	id string,
	_ uint64,
) (Operation, error) {
	tool.mu.Lock()
	defer tool.mu.Unlock()
	for key, operation := range tool.operations {
		if operation.ID == id {
			output, err := json.Marshal(ToolResult{Content: "hello"})
			if err != nil {
				return Operation{}, err
			}
			operation.State = OperationSucceeded
			operation.Revision++
			operation.Output = output
			tool.operations[key] = operation
			return operation, nil
		}
	}
	return Operation{}, errors.New("tool operation not found")
}

func (tool *trajectoryTestTool) CancelCall(
	context.Context,
	string,
) (Operation, error) {
	return Operation{}, errors.New("unexpected tool cancellation")
}

func TestSessionTrajectoryAsyncOperationsRestoreAndRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Trajectories:  store,
		OperationPoll: time.Millisecond,
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
	provider := &trajectoryTestProvider{operations: make(map[string]Operation)}
	tool := &trajectoryTestTool{operations: make(map[string]Operation)}
	plugin := PluginFunc{
		PluginManifest: Manifest{
			Name:        "scripted-agent",
			Version:     "1.0.0",
			Description: "async provider and tool for trajectory end-to-end testing",
			APIVersion:  APIVersion,
			Registers: []string{
				ProviderResource("scripted"),
				ToolResource("echo"),
			},
		},
		InstallFunc: func(_ context.Context, registrar Registrar) error {
			return errors.Join(
				registrar.RegisterProvider(provider),
				registrar.RegisterTool(tool),
			)
		},
	}
	if _, err := runtime.Mount(ctx, Local(plugin)); err != nil {
		t.Fatalf("mount: %v", err)
	}
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID:       "trajectory-session",
		Provider: "scripted",
		System:   "be deterministic",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	result, err := session.Prompt(ctx, "start")
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if result.Output != "finished" || result.Turns != 2 || result.ToolCalls != 1 {
		t.Fatalf("result = %#v", result)
	}

	trajectory, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	var providerRequestIDs, toolCallIDs, checkpoints []string
	for _, entry := range trajectory.Entries {
		switch entry.Kind {
		case TrajectoryKindProviderRequest:
			providerRequestIDs = append(providerRequestIDs, entry.ID)
		case TrajectoryKindToolCall:
			toolCallIDs = append(toolCallIDs, entry.ID)
		case TrajectoryKindCheckpoint:
			checkpoints = append(checkpoints, entry.ID)
		}
	}
	if len(providerRequestIDs) != 2 || len(toolCallIDs) != 1 || len(checkpoints) != 2 {
		t.Fatalf("trajectory entry IDs: providers=%v tools=%v checkpoints=%v", providerRequestIDs, toolCallIDs, checkpoints)
	}
	provider.mu.Lock()
	providerKeys := []string{
		provider.requests[0].IdempotencyKey,
		provider.requests[1].IdempotencyKey,
	}
	provider.mu.Unlock()
	tool.mu.Lock()
	toolKey := tool.requests[0].IdempotencyKey
	tool.mu.Unlock()
	if providerKeys[0] != providerRequestIDs[0] || providerKeys[1] != providerRequestIDs[1] || toolKey != toolCallIDs[0] {
		t.Fatalf("operation keys providers=%v tool=%q; trajectory providers=%v tool=%v", providerKeys, toolKey, providerRequestIDs, toolCallIDs)
	}

	stableHead := trajectory.Head
	provider.mu.Lock()
	provider.failNext = true
	provider.mu.Unlock()
	if _, err := session.Prompt(ctx, "this attempt fails"); err == nil {
		t.Fatal("failing provider prompt unexpectedly succeeded")
	}
	failed, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if failed.Head == stableHead {
		t.Fatal("failed attempt did not record a restore")
	}
	var failedUserID string
	for _, entry := range failed.Entries {
		if entry.Kind == TrajectoryKindUserMessage &&
			strings.Contains(string(entry.Payload), "this attempt fails") {
			failedUserID = entry.ID
		}
	}
	if failedUserID == "" {
		t.Fatal("failed attempt was not retained for audit")
	}
	failedBranch, err := failed.Branch(failed.Head)
	if err != nil {
		t.Fatal(err)
	}
	if failedBranch[len(failedBranch)-1].Kind != TrajectoryKindRestore {
		t.Fatalf("failed prompt head = %#v, want restore", failedBranch[len(failedBranch)-1])
	}
	for _, entry := range failedBranch {
		if entry.ID == failedUserID {
			t.Fatalf("failed entry %q remained on active branch", failedUserID)
		}
	}

	resumed, err := runtime.ResumeSession(ctx, session.ID(), SessionConfig{
		Provider: "scripted",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	restored, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if restored.Head != failed.Head {
		t.Fatalf("resume appended a redundant restore: %q -> %q", failed.Head, restored.Head)
	}
	branch, err := restored.Branch(restored.Head)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range branch {
		if entry.ID == failedUserID {
			t.Fatalf("restored branch retained failed tail entry %q", failedUserID)
		}
	}
	if got := resumed.Messages(); len(got) != len(result.Messages) {
		t.Fatalf("resumed messages = %d, want %d", len(got), len(result.Messages))
	}

	if err := resumed.Rollback(ctx, checkpoints[0]); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	rolledBack, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	rollbackBranch, err := rolledBack.Branch(rolledBack.Head)
	if err != nil {
		t.Fatal(err)
	}
	if rollbackBranch[len(rollbackBranch)-1].Kind != TrajectoryKindRollback {
		t.Fatalf("rollback head = %#v", rollbackBranch[len(rollbackBranch)-1])
	}
	for _, entry := range rollbackBranch {
		if entry.ID == checkpoints[1] {
			t.Fatal("rollback branch retained the later checkpoint")
		}
	}
	if got := resumed.Messages(); len(got) >= len(result.Messages) {
		t.Fatalf("rollback messages = %d, final messages = %d", len(got), len(result.Messages))
	}
}

func TestResumeWithoutCheckpointBranchesFromEmptyRoot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{Trajectories: store})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	}()
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "no-checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	failedTail := TrajectoryEntry{
		ID:        "failed-user-message",
		Kind:      TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   []byte(`{"role":"user","content":"never committed"}`),
	}
	if _, err := store.Append(ctx, session.ID(), "", failedTail); err != nil {
		t.Fatal(err)
	}
	resumed, err := runtime.ResumeSession(ctx, session.ID(), SessionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Messages()) != 0 {
		t.Fatalf("resumed uncommitted messages: %v", resumed.Messages())
	}
	trajectory, err := store.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(branch) != 1 || branch[0].Kind != TrajectoryKindRestore || branch[0].ParentID != "" {
		t.Fatalf("restored root branch = %#v", branch)
	}
}
