package runtime

// Durability tests cover trajectory branches, checkpoints, and recovery.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type trajectoryTestProvider struct {
	mu          sync.Mutex
	operations  map[string]sdk.Operation
	requests    []sdk.OperationRequest
	submissions int
	failNext    bool
}

type shutdownBlockingTrajectoryProvider struct {
	entered   chan struct{}
	once      sync.Once
	operation sdk.Operation
}

type trajectoryHandoffBackend struct {
	sdk.StateBackend
	trajectoryID string
}

type panicRenewExecutionStore struct {
	sdk.TrajectoryStore
	entered chan<- struct{}
}

func (store panicRenewExecutionStore) RenewExecution(
	context.Context,
	string,
	string,
	string,
	time.Time,
	time.Duration,
) (sdk.TrajectoryExecution, error) {
	select {
	case store.entered <- struct{}{}:
	default:
	}
	panic("broken trajectory heartbeat")
}

func (backend *trajectoryHandoffBackend) Close(ctx context.Context) error {
	metadata, err := backend.Trajectories().LoadMetadata(
		ctx,
		backend.trajectoryID,
	)
	if err != nil {
		return err
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		return fmt.Errorf(
			"storage closed before trajectory handoff: %#v",
			metadata.Execution,
		)
	}
	return nil
}

func TestExecutionHeartbeatPanicCancelsExecutionContext(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	store := panicRenewExecutionStore{
		TrajectoryStore: sdkstorage.NewMemoryTrajectoryStore(),
		entered:         entered,
	}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:         testStateBackendWithStores(store, nil),
		TrajectoryLease: 3 * time.Millisecond,
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
	session := &Session{
		runtime:        runtime,
		config:         SessionConfig{ID: "heartbeat-panic"},
		executionID:    "execution-heartbeat-panic",
		executionToken: "heartbeat-token",
	}
	executionCtx, stopHeartbeat := session.executionHeartbeat(context.Background())
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not renew execution")
	}
	select {
	case <-executionCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("heartbeat panic did not cancel execution context")
	}
	err = stopHeartbeat()
	if err == nil ||
		!strings.Contains(err.Error(), "trajectory execution lease lost") ||
		!strings.Contains(err.Error(), "broken trajectory heartbeat") {
		t.Fatalf("stop heartbeat error = %v", err)
	}
}

func (*shutdownBlockingTrajectoryProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "scripted", Model: "scripted-v1"}
}

func (provider *shutdownBlockingTrajectoryProvider) SubmitCompletion(
	_ context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	provider.operation = sdk.Operation{
		ID:             "shutdown-blocked-provider-operation",
		IdempotencyKey: request.IdempotencyKey,
		State:          sdk.OperationPending,
		Revision:       1,
	}
	provider.once.Do(func() { close(provider.entered) })
	return provider.operation, nil
}

func (provider *shutdownBlockingTrajectoryProvider) PollCompletion(
	_ context.Context,
	_ string,
	_ uint64,
) (sdk.Operation, error) {
	return provider.operation, nil
}

func (provider *shutdownBlockingTrajectoryProvider) CancelCompletion(
	_ context.Context,
	id string,
) (sdk.Operation, error) {
	cancelled := provider.operation
	cancelled.ID = id
	cancelled.State = sdk.OperationCancelled
	cancelled.Revision++
	return cancelled, nil
}

type postRollbackLoadFailStore struct {
	sdk.TrajectoryStore
	failLoad              atomic.Bool
	postRollbackLoadCalls atomic.Int64
}

func (store *postRollbackLoadFailStore) Append(
	ctx context.Context,
	id string,
	expectedHead string,
	entries ...sdk.TrajectoryEntry,
) (string, error) {
	head, err := store.TrajectoryStore.Append(ctx, id, expectedHead, entries...)
	if err == nil {
		for _, entry := range entries {
			if entry.Kind == sdk.TrajectoryKindRollback {
				store.failLoad.Store(true)
			}
		}
	}
	return head, err
}

func (store *postRollbackLoadFailStore) Load(
	ctx context.Context,
	id string,
) (sdk.Trajectory, error) {
	if store.failLoad.Load() {
		store.postRollbackLoadCalls.Add(1)
		return sdk.Trajectory{}, errors.New("post-rollback load failed")
	}
	return store.TrajectoryStore.Load(ctx, id)
}

func (provider *trajectoryTestProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "scripted", Model: "scripted-v1"}
}

func (provider *trajectoryTestProvider) SubmitCompletion(
	_ context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if operation, exists := provider.operations[request.IdempotencyKey]; exists {
		return operation, nil
	}
	provider.submissions++
	provider.requests = append(provider.requests, request)
	operation := sdk.Operation{
		ID:             fmt.Sprintf("provider-operation-%d", provider.submissions),
		IdempotencyKey: request.IdempotencyKey,
		State:          sdk.OperationPending,
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
) (sdk.Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for key, operation := range provider.operations {
		if operation.ID != id {
			continue
		}
		if operation.Error == "fail-on-poll" {
			operation.State = sdk.OperationFailed
			operation.Revision++
			operation.Error = "scripted provider failure"
			provider.operations[key] = operation
			return operation, nil
		}
		var response sdk.ModelResponse
		if id == "provider-operation-1" {
			response = sdk.ModelResponse{
				ToolCalls: []sdk.ToolCall{{
					ID:        "tool-call-1",
					Name:      "echo",
					Arguments: []byte(`{"value":"hello"}`),
				}},
				Model:        "scripted-v1",
				FinishReason: "tool_calls",
			}
		} else {
			response = sdk.ModelResponse{
				Content:      "finished",
				Model:        "scripted-v1",
				FinishReason: "stop",
			}
		}
		output, err := json.Marshal(response)
		if err != nil {
			return sdk.Operation{}, err
		}
		operation.State = sdk.OperationSucceeded
		operation.Revision++
		operation.Output = output
		provider.operations[key] = operation
		return operation, nil
	}
	return sdk.Operation{}, errors.New("provider operation not found")
}

func (provider *trajectoryTestProvider) CancelCompletion(
	_ context.Context,
	id string,
) (sdk.Operation, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for key, operation := range provider.operations {
		if operation.ID == id {
			operation.State = sdk.OperationCancelled
			operation.Revision++
			provider.operations[key] = operation
			return operation, nil
		}
	}
	return sdk.Operation{}, errors.New("provider operation not found")
}

type trajectoryTestTool struct {
	mu         sync.Mutex
	operations map[string]sdk.Operation
	requests   []sdk.OperationRequest
}

func (tool *trajectoryTestTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "echo",
		Description: "returns its input",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *trajectoryTestTool) SubmitCall(
	_ context.Context,
	request sdk.OperationRequest,
) (sdk.Operation, error) {
	tool.mu.Lock()
	defer tool.mu.Unlock()
	if operation, exists := tool.operations[request.IdempotencyKey]; exists {
		return operation, nil
	}
	tool.requests = append(tool.requests, request)
	operation := sdk.Operation{
		ID:             "tool-operation-1",
		IdempotencyKey: request.IdempotencyKey,
		State:          sdk.OperationRunning,
		Revision:       1,
	}
	tool.operations[request.IdempotencyKey] = operation
	return operation, nil
}

func (tool *trajectoryTestTool) PollCall(
	_ context.Context,
	id string,
	_ uint64,
) (sdk.Operation, error) {
	tool.mu.Lock()
	defer tool.mu.Unlock()
	for key, operation := range tool.operations {
		if operation.ID == id {
			output, err := json.Marshal(sdk.ToolResult{Content: "hello"})
			if err != nil {
				return sdk.Operation{}, err
			}
			operation.State = sdk.OperationSucceeded
			operation.Revision++
			operation.Output = output
			tool.operations[key] = operation
			return operation, nil
		}
	}
	return sdk.Operation{}, errors.New("tool operation not found")
}

func (tool *trajectoryTestTool) CancelCall(
	context.Context,
	string,
) (sdk.Operation, error) {
	return sdk.Operation{}, errors.New("unexpected tool cancellation")
}

func TestSessionTrajectoryAsyncOperationsRestoreAndRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:       testStateBackendWithStores(store, nil),
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
	provider := &trajectoryTestProvider{operations: make(map[string]sdk.Operation)}
	tool := &trajectoryTestTool{operations: make(map[string]sdk.Operation)}
	plugin := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "scripted-agent",
			Version:     "1.0.0",
			Description: "async provider and tool for trajectory end-to-end testing",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("scripted"),
				sdk.ToolResource("echo"),
			},
		},
		InstallFunc: func(_ context.Context, registrar sdk.Registrar) error {
			return errors.Join(
				registrar.RegisterProvider(provider),
				registrar.RegisterTool(tool),
			)
		},
	}
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
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
	if trajectory.Execution == nil ||
		trajectory.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		trajectory.Execution.LeaseToken != "" {
		t.Fatalf("completed trajectory execution = %#v", trajectory.Execution)
	}
	metadata, err := store.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	durableResult, err := LoadExecutionResult(ctx, store, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if durableResult == nil ||
		durableResult.Output != result.Output ||
		durableResult.Cause.Code != "model_end" {
		t.Fatalf("durable execution result = %#v", durableResult)
	}
	completedExecutionID := trajectory.Execution.ID
	var providerRequestIDs, providerOperationKeys []string
	var providerResponseCorrelations []string
	var toolCallIDs, toolOperationKeys, checkpoints []string
	var toolCallCorrelations, toolResultCorrelations []string
	var providerResponses, toolResults, decisions int
	for _, entry := range trajectory.Entries {
		if entry.Kind != sdk.TrajectoryKindRestore &&
			entry.Kind != sdk.TrajectoryKindRollback &&
			entry.Fields.ExecutionID != completedExecutionID {
			t.Fatalf(
				"entry %s execution_id = %q, want %q",
				entry.ID,
				entry.Fields.ExecutionID,
				completedExecutionID,
			)
		}
		switch entry.Kind {
		case sdk.TrajectoryKindProviderRequest:
			providerRequestIDs = append(providerRequestIDs, entry.ID)
			providerOperationKeys = append(
				providerOperationKeys,
				entry.Fields.OperationKey,
			)
			if entry.Fields.Turn == nil ||
				entry.Fields.Provider != "scripted" ||
				entry.Fields.Model != "scripted-v1" ||
				entry.Fields.OperationKey == "" ||
				entry.Fields.CorrelationID != entry.Fields.OperationKey {
				t.Fatalf("provider request fields = %#v", entry.Fields)
			}
		case sdk.TrajectoryKindProviderResponse:
			providerResponses++
			providerResponseCorrelations = append(
				providerResponseCorrelations,
				entry.Fields.CorrelationID,
			)
			if entry.Fields.Turn == nil ||
				entry.Fields.Provider != "scripted" ||
				entry.Fields.Model != "scripted-v1" ||
				entry.Fields.IsError == nil ||
				*entry.Fields.IsError ||
				entry.Fields.CorrelationID == "" {
				t.Fatalf("provider response fields = %#v", entry.Fields)
			}
		case sdk.TrajectoryKindToolCall:
			toolCallIDs = append(toolCallIDs, entry.ID)
			toolOperationKeys = append(
				toolOperationKeys,
				entry.Fields.OperationKey,
			)
			toolCallCorrelations = append(
				toolCallCorrelations,
				entry.Fields.CorrelationID,
			)
			if entry.Fields.Turn == nil ||
				entry.Fields.ToolName != "echo" ||
				entry.Fields.ToolCallID != "tool-call-1" ||
				entry.Fields.OperationKey == "" ||
				entry.Fields.CorrelationID == "" {
				t.Fatalf("tool call fields = %#v", entry.Fields)
			}
		case sdk.TrajectoryKindToolResult:
			toolResults++
			toolResultCorrelations = append(
				toolResultCorrelations,
				entry.Fields.CorrelationID,
			)
			if entry.Fields.Turn == nil ||
				entry.Fields.ToolName != "echo" ||
				entry.Fields.ToolCallID != "tool-call-1" ||
				entry.Fields.IsError == nil ||
				*entry.Fields.IsError ||
				entry.Fields.CorrelationID == "" {
				t.Fatalf("tool result fields = %#v", entry.Fields)
			}
		case sdk.TrajectoryKindDecision:
			decisions++
			if entry.Fields.Turn == nil ||
				entry.Fields.ActionKind == "" {
				t.Fatalf("decision fields = %#v", entry.Fields)
			}
		case sdk.TrajectoryKindCheckpoint:
			checkpoints = append(checkpoints, entry.ID)
		}
	}
	if len(providerRequestIDs) != 2 || len(toolCallIDs) != 1 || len(checkpoints) != 2 {
		t.Fatalf("trajectory entry IDs: providers=%v tools=%v checkpoints=%v", providerRequestIDs, toolCallIDs, checkpoints)
	}
	if providerResponses != 2 || toolResults != 1 || decisions != 2 {
		t.Fatalf(
			"fixed field entry counts: provider responses=%d tool results=%d decisions=%d",
			providerResponses,
			toolResults,
			decisions,
		)
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
	if providerKeys[0] != providerOperationKeys[0] ||
		providerKeys[1] != providerOperationKeys[1] ||
		toolKey != toolOperationKeys[0] {
		t.Fatalf(
			"operation keys providers=%v tool=%q; trajectory providers=%v tool=%v",
			providerKeys,
			toolKey,
			providerOperationKeys,
			toolOperationKeys,
		)
	}
	if providerResponseCorrelations[0] != providerOperationKeys[0] ||
		providerResponseCorrelations[1] != providerOperationKeys[1] ||
		toolCallCorrelations[0] != providerOperationKeys[0] ||
		toolResultCorrelations[0] != providerOperationKeys[0] {
		t.Fatalf(
			"round correlations provider responses=%v tool calls=%v tool results=%v provider operations=%v",
			providerResponseCorrelations,
			toolCallCorrelations,
			toolResultCorrelations,
			providerOperationKeys,
		)
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
	if failed.Execution == nil ||
		failed.Execution.State != sdk.TrajectoryExecutionFailed ||
		failed.Execution.LastError == "" ||
		failed.Execution.LeaseToken != "" {
		t.Fatalf("failed trajectory execution = %#v", failed.Execution)
	}
	if failed.Head == stableHead {
		t.Fatal("failed attempt did not record a restore")
	}
	var failedUserID string
	for _, entry := range failed.Entries {
		if entry.Kind == sdk.TrajectoryKindUserMessage &&
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
	if failedBranch[len(failedBranch)-1].Kind != sdk.TrajectoryKindRestore {
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
	if rollbackBranch[len(rollbackBranch)-1].Kind != sdk.TrajectoryKindRollback {
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
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(store, nil),
	})
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
	failedTail := sdk.TrajectoryEntry{
		ID:        "failed-user-message",
		Kind:      sdk.TrajectoryKindUserMessage,
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
	if len(branch) != 1 || branch[0].Kind != sdk.TrajectoryKindRestore || branch[0].ParentID != "" {
		t.Fatalf("restored root branch = %#v", branch)
	}
}

func TestSessionRollbackDoesNotReloadAfterCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := sdkstorage.NewMemoryTrajectoryStore()
	store := &postRollbackLoadFailStore{TrajectoryStore: base}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(store, nil),
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
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "rollback-no-reload"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if err := session.checkpointTrajectory(
		ctx,
		lease.snapshot,
		trajectoryCheckpointCommit{
			Messages: []sdk.Message{{
				Role:    sdk.RoleUser,
				Content: "checkpoint",
			}},
			Action: sdk.Action{Kind: sdk.ActionStep},
			System: "system",
		},
	); err != nil {
		t.Fatal(err)
	}
	trajectory, err := base.Load(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}

	if err := session.Rollback(ctx, trajectory.Head); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if got := store.postRollbackLoadCalls.Load(); got != 0 {
		t.Fatalf("post-rollback Load calls = %d, want 0", got)
	}
	if got := session.Messages(); len(got) != 1 || got[0].Content != "checkpoint" {
		t.Fatalf("session messages after rollback = %#v", got)
	}
}

func TestResumeDoesNotMaterializeTrajectory(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	base := sdkstorage.NewMemoryTrajectoryStore()
	store := &postRollbackLoadFailStore{TrajectoryStore: base}
	runtime, err := NewRuntime(RuntimeConfig{
		Storage: testStateBackendWithStores(store, nil),
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
	session, err := runtime.NewSession(ctx, SessionConfig{ID: "resume-no-load"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	if err := session.checkpointTrajectory(
		ctx,
		lease.snapshot,
		trajectoryCheckpointCommit{
			Messages: []sdk.Message{{
				Role:    sdk.RoleUser,
				Content: "checkpoint",
			}},
			Action: sdk.Action{Kind: sdk.ActionStep},
			System: "system",
		},
	); err != nil {
		t.Fatal(err)
	}
	store.failLoad.Store(true)

	resumed, err := runtime.ResumeSession(ctx, session.ID(), SessionConfig{})
	if err != nil {
		t.Fatalf("ResumeSession() error = %v", err)
	}
	if got := store.postRollbackLoadCalls.Load(); got != 0 {
		t.Fatalf("resume Load calls = %d, want 0", got)
	}
	if got := resumed.Messages(); len(got) != 1 ||
		got[0].Content != "checkpoint" {
		t.Fatalf("resumed messages = %#v", got)
	}
}

func TestTrajectoryAppendEventUsesAppendSnapshot(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	events := make(chan sdk.Event, 4)
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          newTestStateBackend(),
		StorageOwnership: StorageBorrowed,
		EventObserver: func(_ context.Context, event sdk.Event) {
			if event.Name == sdk.EventTrajectoryAppend {
				events <- event
			}
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
	session, err := runtime.NewSession(ctx, SessionConfig{
		ID: "append-snapshot-event",
	})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	eventGeneration := lease.snapshot.generation
	if _, err := runtime.Mount(ctx, sdk.Local(sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "generation-advance",
			Version:     "1.0.0",
			Description: "advances composition generation",
			APIVersion:  sdk.APIVersion,
		},
		InstallFunc: func(context.Context, sdk.Registrar) error {
			return nil
		},
	})); err != nil {
		t.Fatal(err)
	}
	if runtime.current.Load().generation == eventGeneration {
		t.Fatal("mount did not advance runtime generation")
	}
	if err := session.appendTrajectory(
		ctx,
		lease.snapshot,
		sdk.TrajectoryKindAgentStart,
		sdk.AgentStartPayload{},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Generation != eventGeneration {
			t.Fatalf(
				"trajectory event generation = %d, want append snapshot generation %d",
				event.Generation,
				eventGeneration,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("trajectory append event was not observed")
	}
}

func TestRecoverExecutionContinuesExpiredTrajectoryLease(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	directory := t.TempDir()
	store, err := sdkstorage.NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	firstRuntime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(store, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstProvider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	firstTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := firstRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(firstProvider, firstTool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := firstRuntime.NewSession(ctx, SessionConfig{
		ID:       "crash-recovery",
		Provider: "scripted",
		System:   "recover exactly",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "durable-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Fields: sdk.TrajectoryEntryFields{
			ExecutionID: "durable-execution",
		},
		Payload: json.RawMessage(
			`{"role":"user","content":"finish after restart"}`,
		),
	}
	if _, err := store.BeginExecution(
		ctx,
		session.ID(),
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "durable-execution",
			Provider: "scripted",
			System:   "recover exactly",
			MaxTurns: 3,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimExecution(
		ctx,
		session.ID(),
		"terminated-worker",
		time.Now().UTC().Add(-time.Minute),
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := firstRuntime.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	cancel()

	reopened, err := sdkstorage.NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(reopened, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
		if err := secondRuntime.Close(closeCtx); err != nil {
			t.Errorf("close recovered runtime: %v", err)
		}
	})
	secondProvider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	secondTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := secondRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(secondProvider, secondTool)),
	); err != nil {
		t.Fatal(err)
	}

	result, err := secondRuntime.RecoverExecution(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "finished" ||
		result.Turns != 2 ||
		result.ToolCalls != 1 {
		t.Fatalf("recovered result = %#v", result)
	}
	metadata, err := reopened.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != "durable-execution" ||
		metadata.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		metadata.Execution.LeaseToken != "" {
		t.Fatalf("recovered execution metadata = %#v", metadata.Execution)
	}
	if recoverable, err := reopened.ListRecoverable(
		ctx,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	} else if len(recoverable) != 0 {
		t.Fatalf("completed execution remained recoverable: %#v", recoverable)
	}
}

func TestRecoverExecutionFromDuckDBBackendAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "agent-state.duckdb")
	firstBackend, err := sdkstorage.NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	firstRuntime, err := NewRuntime(RuntimeConfig{
		Storage:       firstBackend,
		OperationPoll: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstProvider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	firstTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := firstRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(firstProvider, firstTool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := firstRuntime.NewSession(ctx, SessionConfig{
		ID:       "duckdb-crash-recovery",
		Provider: "scripted",
		System:   "recover from DuckDB",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := firstBackend.Trajectories()
	input := sdk.TrajectoryEntry{
		ID:        "duckdb-durable-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Fields: sdk.TrajectoryEntryFields{
			ExecutionID: "duckdb-durable-execution",
		},
		Payload: json.RawMessage(
			`{"role":"user","content":"finish from DuckDB"}`,
		),
	}
	if _, err := store.BeginExecution(
		ctx,
		session.ID(),
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "duckdb-durable-execution",
			Provider: "scripted",
			System:   "recover from DuckDB",
			MaxTurns: 3,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimExecution(
		ctx,
		session.ID(),
		"terminated-duckdb-worker",
		time.Now().UTC().Add(-time.Minute),
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := firstRuntime.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	cancel()

	secondBackend, err := sdkstorage.NewDuckDBStateBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime, err := NewRuntime(RuntimeConfig{
		Storage:       secondBackend,
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
		if err := secondRuntime.Close(closeCtx); err != nil {
			t.Errorf("close recovered DuckDB runtime: %v", err)
		}
	})
	secondProvider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	secondTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := secondRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(secondProvider, secondTool)),
	); err != nil {
		t.Fatal(err)
	}
	result, err := secondRuntime.RecoverExecution(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "finished" ||
		result.Turns != 2 ||
		result.ToolCalls != 1 {
		t.Fatalf("DuckDB recovered result = %#v", result)
	}
	metadata, err := secondBackend.Trajectories().LoadMetadata(
		ctx,
		session.ID(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionSucceeded {
		t.Fatalf(
			"DuckDB recovered execution metadata = %#v",
			metadata.Execution,
		)
	}
	analyzer, ok := secondBackend.Trajectories().(sdk.TrajectoryAnalyzer)
	if !ok {
		t.Fatal("DuckDB trajectory store does not expose indexed analysis")
	}
	requests, err := analyzer.AnalyzeEntries(
		ctx,
		sdk.TrajectoryEntryQuery{
			TrajectoryID: session.ID(),
			ExecutionID:  "duckdb-durable-execution",
			Kind:         sdk.TrajectoryKindProviderRequest,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 ||
		requests[0].Fields.OperationKey == "" ||
		requests[1].Fields.OperationKey == "" {
		t.Fatalf("indexed DuckDB provider requests = %#v", requests)
	}
}

func TestCallerCancellationTerminatesTrajectoryExecution(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(store, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
	provider := &shutdownBlockingTrajectoryProvider{
		entered: make(chan struct{}),
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
		ID:       "caller-cancelled",
		Provider: "scripted",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	promptCtx, cancelPrompt := context.WithCancel(ctx)
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(promptCtx, "cancel this request")
		promptDone <- promptErr
	}()
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("provider submission did not start")
	}
	cancelPrompt()
	select {
	case promptErr := <-promptDone:
		if !errors.Is(promptErr, context.Canceled) {
			t.Fatalf("prompt error = %v, want context cancellation", promptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not stop after caller cancellation")
	}

	metadata, err := store.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionCancelled ||
		metadata.Execution.LeaseToken != "" {
		t.Fatalf("cancelled execution metadata = %#v", metadata.Execution)
	}
	recoverable, err := store.ListRecoverable(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 0 {
		t.Fatalf("cancelled execution remained recoverable: %#v", recoverable)
	}
}

func TestRuntimeCancelExecutionInterruptsHostedTrajectoryExecution(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(store, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
	provider := &shutdownBlockingTrajectoryProvider{
		entered: make(chan struct{}),
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
		ID:       "runtime-cancelled",
		Provider: "scripted",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(
			context.Background(),
			"cancel from runtime",
		)
		promptDone <- promptErr
	}()
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("provider submission did not start")
	}
	metadata, err := store.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil {
		t.Fatalf("running execution metadata = %#v", metadata.Execution)
	}
	cancelled, err := runtime.CancelExecution(
		ctx,
		session.ID(),
		metadata.Execution.ID,
		"runtime requested cancellation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution view = %#v", cancelled)
	}
	select {
	case promptErr := <-promptDone:
		if !errors.Is(promptErr, context.Canceled) {
			t.Fatalf("prompt error = %v, want context cancellation", promptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not stop after runtime cancellation")
	}
}

func TestRuntimeCancelExecutionCompletesUnhostedTrajectoryExecution(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(store, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
	if err := store.Create(ctx, sdk.Trajectory{ID: "unhosted-cancel"}); err != nil {
		t.Fatal(err)
	}
	input := sdk.TrajectoryEntry{
		ID:        "unhosted-cancel-input",
		Kind:      sdk.TrajectoryKindUserMessage,
		Timestamp: time.Now().UTC(),
		Payload:   json.RawMessage(`{"role":"user","content":"stop"}`),
	}
	if _, err := store.BeginExecution(
		ctx,
		"unhosted-cancel",
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "unhosted-cancel-execution",
			Provider: "scripted",
			MaxTurns: 3,
		},
		input,
	); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimExecution(
		ctx,
		"unhosted-cancel",
		"remote-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := runtime.CancelExecution(
		ctx,
		"unhosted-cancel",
		claimed.ID,
		"runtime requested cancellation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution view = %#v", cancelled)
	}
	trajectory, err := store.Load(ctx, "unhosted-cancel")
	if err != nil {
		t.Fatal(err)
	}
	var terminal sdk.TrajectoryEntry
	for _, entry := range trajectory.Entries {
		if entry.Kind == sdk.TrajectoryKindTerminal {
			terminal = entry
			break
		}
	}
	if terminal.ID == "" || terminal.ParentID != input.ID {
		t.Fatalf("terminal entry = %#v", terminal)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(branch) == 0 ||
		branch[len(branch)-1].Kind != sdk.TrajectoryKindRestore {
		t.Fatalf("cancelled execution head branch = %#v", branch)
	}
	for _, entry := range branch {
		if entry.ID == terminal.ID {
			t.Fatalf("terminal entry %q remained on active branch", entry.ID)
		}
	}
}

func TestRuntimeCancelExecutionProjectsCheckpointResult(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := sdkstorage.NewMemoryTrajectoryStore()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(store, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
	const trajectoryID = "unhosted-cancel-checkpoint"
	if err := store.Create(ctx, sdk.Trajectory{ID: trajectoryID}); err != nil {
		t.Fatal(err)
	}
	input, err := newPayloadTrajectoryEntry(
		"",
		sdk.TrajectoryKindUserMessage,
		0,
		time.Now().UTC(),
		durability.NewExecutionInput(
			sdk.Message{Role: sdk.RoleUser, Content: "start"},
			sdk.TrajectoryEnvironment{},
			nil,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	input.ID = "unhosted-cancel-checkpoint-input"
	metadata, err := store.BeginExecution(
		ctx,
		trajectoryID,
		"",
		sdk.TrajectoryExecutionStart{
			ID:       "unhosted-cancel-checkpoint-execution",
			Provider: "scripted",
			MaxTurns: 3,
		},
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimExecution(
		ctx,
		trajectoryID,
		"remote-worker",
		time.Now().UTC(),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := newPayloadTrajectoryEntry(
		metadata.Head,
		sdk.TrajectoryKindCheckpoint,
		0,
		time.Now().UTC(),
		durability.Checkpoint{
			Messages: []sdk.Message{
				{Role: sdk.RoleUser, Content: "start"},
				{Role: sdk.RoleAssistant, Content: "partial output"},
			},
			Output:    "partial output",
			Provider:  "scripted",
			Turns:     1,
			ToolCalls: 2,
			Action:    sdk.Action{Kind: sdk.ActionStep},
			ContextInjections: []sdk.ContextInjection{{
				ID:                "cancel-context",
				Priority:          sdk.ContextInjectionNext,
				Mode:              sdk.ContextInjectionTaskNotification,
				Origin:            "test",
				TargetSessionID:   trajectoryID,
				TargetExecutionID: claimed.ID,
				IsMeta:            true,
				Messages: []sdk.Message{{
					Role:    sdk.RoleUser,
					Content: "queued before cancel",
				}},
			}},
			ConsumedContextInjectionIDs: []string{"cancel-context"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.ID = "unhosted-cancel-checkpoint-entry"
	checkpoint.Fields.ExecutionID = claimed.ID
	metadata, err = store.CommitExecution(
		ctx,
		sdk.TrajectoryExecutionCommit{
			TrajectoryID: trajectoryID,
			ExecutionID:  claimed.ID,
			LeaseToken:   claimed.LeaseToken,
			ExpectedHead: metadata.Head,
			Entries:      []sdk.TrajectoryEntry{checkpoint},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	cancelled, err := runtime.CancelExecution(
		ctx,
		trajectoryID,
		claimed.ID,
		"runtime requested cancellation",
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled ||
		cancelled.Result == nil {
		t.Fatalf("cancelled execution view = %#v", cancelled)
	}
	if cancelled.Result.Output != "partial output" ||
		cancelled.Result.Turns != 1 ||
		cancelled.Result.ToolCalls != 2 ||
		cancelled.Result.Cause.Code != sdk.CauseCancelled ||
		cancelled.Result.Cause.Detail != "runtime requested cancellation" ||
		!cancelled.Result.Cause.Final {
		t.Fatalf("cancelled checkpoint result = %#v", cancelled.Result)
	}
	if got := cancelled.Result.Messages; len(got) != 2 ||
		got[1].Content != "partial output" {
		t.Fatalf("cancelled result messages = %#v", got)
	}
	if got := cancelled.Result.ContextInjections; len(got) != 1 ||
		got[0].ID != "cancel-context" ||
		got[0].Mode != sdk.ContextInjectionTaskNotification ||
		got[0].Origin != "test" ||
		!got[0].IsMeta ||
		len(got[0].Messages) != 1 ||
		got[0].Messages[0].Content != "queued before cancel" {
		t.Fatalf("cancelled context injections = %#v", got)
	}
	trajectory, err := store.Load(ctx, trajectoryID)
	if err != nil {
		t.Fatal(err)
	}
	var terminal sdk.TrajectoryEntry
	for _, entry := range trajectory.Entries {
		if entry.Kind == sdk.TrajectoryKindTerminal {
			terminal = entry
			break
		}
	}
	if terminal.ID == "" || terminal.ParentID != metadata.Head {
		t.Fatalf("terminal entry = %#v, checkpoint head = %q", terminal, metadata.Head)
	}
	branch, err := trajectory.Branch(trajectory.Head)
	if err != nil {
		t.Fatal(err)
	}
	if len(branch) == 0 ||
		branch[len(branch)-1].Kind != sdk.TrajectoryKindRestore {
		t.Fatalf("cancelled execution head branch = %#v", branch)
	}
	for _, entry := range branch {
		if entry.ID == terminal.ID {
			t.Fatalf("terminal entry %q remained on active branch", entry.ID)
		}
	}
}

func TestRuntimeCloseHandsExecutionBackForImmediateRecovery(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	directory := t.TempDir()
	store, err := sdkstorage.NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	firstBackend := &trajectoryHandoffBackend{
		StateBackend: testStateBackendWithStores(store, nil),
		trajectoryID: "shutdown-handoff",
	}
	firstRuntime, err := NewRuntime(RuntimeConfig{
		Storage:         firstBackend,
		OperationPoll:   time.Millisecond,
		TrajectoryLease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	blockingProvider := &shutdownBlockingTrajectoryProvider{
		entered: make(chan struct{}),
	}
	firstTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := firstRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(blockingProvider, firstTool)),
	); err != nil {
		t.Fatal(err)
	}
	session, err := firstRuntime.NewSession(ctx, SessionConfig{
		ID:       "shutdown-handoff",
		Provider: "scripted",
		System:   "recover after shutdown",
		MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	promptDone := make(chan error, 1)
	go func() {
		_, promptErr := session.Prompt(
			context.Background(),
			"finish after graceful restart",
		)
		promptDone <- promptErr
	}()
	select {
	case <-blockingProvider.entered:
	case <-time.After(time.Second):
		t.Fatal("provider submission did not start")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := firstRuntime.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case promptErr := <-promptDone:
		if !errors.Is(promptErr, context.Canceled) {
			t.Fatalf("prompt error = %v, want context cancellation", promptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not stop during runtime close")
	}

	metadata, err := store.LoadMetadata(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending ||
		metadata.Execution.Owner != "" ||
		metadata.Execution.LeaseToken != "" ||
		metadata.Execution.System != "recover after shutdown" {
		t.Fatalf("shutdown execution metadata = %#v", metadata.Execution)
	}
	recoverable, err := store.ListRecoverable(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(recoverable) != 1 || recoverable[0].ID != session.ID() {
		t.Fatalf("immediately recoverable executions = %#v", recoverable)
	}

	reopened, err := sdkstorage.NewFileTrajectoryStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	secondRuntime, err := NewRuntime(RuntimeConfig{
		Storage:          testStateBackendWithStores(reopened, nil),
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
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
		if err := secondRuntime.Close(closeCtx); err != nil {
			t.Errorf("close recovered runtime: %v", err)
		}
	})
	secondProvider := &trajectoryTestProvider{
		operations: make(map[string]sdk.Operation),
	}
	secondTool := &trajectoryTestTool{
		operations: make(map[string]sdk.Operation),
	}
	if _, err := secondRuntime.Mount(
		ctx,
		sdk.Local(trajectoryRecoveryPlugin(secondProvider, secondTool)),
	); err != nil {
		t.Fatal(err)
	}
	result, err := secondRuntime.RecoverExecution(ctx, session.ID())
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "finished" ||
		result.Turns != 2 ||
		result.ToolCalls != 1 {
		t.Fatalf("recovered result = %#v", result)
	}
}

func trajectoryRecoveryPlugin(
	provider sdk.Provider,
	tool sdk.Tool,
) sdk.Plugin {
	return sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "scripted-agent",
			Version:     "1.0.0",
			Description: "provider and tool for trajectory recovery testing",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("scripted"),
				sdk.ToolResource("echo"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return errors.Join(
				registrar.RegisterProvider(provider),
				registrar.RegisterTool(tool),
			)
		},
	}
}

func TestExecutionOperationKeyIsStableAcrossRecoveryHeads(t *testing.T) {
	t.Parallel()
	session := &Session{
		head:           "attempt-head-1",
		executionID:    "execution-stable",
		executionToken: "lease-token",
	}
	first := session.executionOperationKey("provider", "1")
	session.head = "attempt-head-2"
	second := session.executionOperationKey("provider", "1")
	other := session.executionOperationKey("provider", "2")
	if first == "" || first != second || first == other {
		t.Fatalf(
			"operation keys first=%q second=%q other=%q",
			first,
			second,
			other,
		)
	}
}
