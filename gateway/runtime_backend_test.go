package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type gatewayTestProvider struct {
	block        chan struct{}
	closeStarted chan struct{}
	closeRelease chan struct{}
}

type gatewayContextInjectionProvider struct {
	mu       sync.Mutex
	requests []sdk.ModelRequest
}

type gatewayContextInjectionTool struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*gatewayTestProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "gateway-test", Model: "test"}
}

func TestGatewayExecutionErrorsPreserveRuntimeSemantics(t *testing.T) {
	t.Parallel()
	if err := gatewayExecutionViewError(
		agentruntime.ErrExecutionNotFound,
	); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("view not found error = %v", err)
	}
	if err := gatewayExecutionViewError(
		sdk.ErrTrajectoryExecution,
	); errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("trajectory conflict collapsed to not found: %v", err)
	}
	if err := gatewayRecoveryCandidateError(
		agentruntime.ErrExecutionNotRecoverable,
	); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("non-recoverable candidate error = %v", err)
	}
}

func (provider *gatewayTestProvider) Complete(
	ctx context.Context,
	_ sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	if provider.block != nil {
		select {
		case provider.block <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return sdk.ModelResponse{}, ctx.Err()
	}
	return sdk.ModelResponse{
		Content: "gateway result", FinishReason: "stop", Model: "test",
	}, nil
}

func (provider *gatewayTestProvider) Close(ctx context.Context) error {
	if provider.closeStarted == nil {
		return nil
	}
	close(provider.closeStarted)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-provider.closeRelease:
		return nil
	}
}

func (*gatewayContextInjectionProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "gateway-context", Model: "test"}
}

func (provider *gatewayContextInjectionProvider) Complete(
	_ context.Context,
	request sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	provider.mu.Lock()
	provider.requests = append(provider.requests, sdk.ModelRequest{
		Messages: sdk.CloneMessages(request.Messages),
		Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
	})
	callCount := len(provider.requests)
	provider.mu.Unlock()
	if callCount == 1 {
		return sdk.ModelResponse{
			ToolCalls: []sdk.ToolCall{{
				ID:        "gateway-wait-call",
				Name:      "gateway_wait_for_context",
				Arguments: json.RawMessage(`{}`),
			}},
		}, nil
	}
	return sdk.ModelResponse{
		Content:      "gateway context accepted",
		FinishReason: "stop",
		Model:        "test",
	}, nil
}

func (provider *gatewayContextInjectionProvider) Requests() []sdk.ModelRequest {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	requests := make([]sdk.ModelRequest, len(provider.requests))
	for index, request := range provider.requests {
		requests[index] = sdk.ModelRequest{
			Messages: sdk.CloneMessages(request.Messages),
			Tools:    append([]sdk.ToolSpec(nil), request.Tools...),
		}
	}
	return requests
}

func (*gatewayContextInjectionTool) Spec() sdk.ToolSpec {
	return sdk.ToolSpec{
		Name:        "gateway_wait_for_context",
		Description: "blocks until gateway injects context",
		Parameters:  map[string]any{"type": "object"},
	}
}

func (tool *gatewayContextInjectionTool) Call(
	ctx context.Context,
	_ json.RawMessage,
) (sdk.ToolResult, error) {
	tool.once.Do(func() { close(tool.entered) })
	select {
	case <-ctx.Done():
		return sdk.ToolResult{}, ctx.Err()
	case <-tool.release:
		return sdk.ToolResult{Content: "tool done"}, nil
	}
}

func TestRuntimeExecutionBackendSubmitsPollsAndCancels(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		testRuntimeExecutionBackendSuccess(t)
	})
	t.Run("cancel", func(t *testing.T) {
		testRuntimeExecutionBackendCancel(t)
	})
}

func TestRuntimeExecutionBackendRecoversPendingExecution(t *testing.T) {
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-recover", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testGatewayRuntimeBuilder(
		&gatewayTestProvider{},
	)(t.Context(), runtimeBuildSpec(session), state)
	if err != nil {
		t.Fatal(err)
	}
	runtimeSession, err := runtime.NewSession(
		t.Context(),
		agentruntime.SessionConfig{
			ID: session.ID, Provider: session.Provider,
			MaxTurns: session.MaxTurns,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := runtimeSession.SubmitPrompt(
		t.Context(),
		"recover me",
	)
	if err != nil {
		t.Fatal(err)
	}
	executionID := submission.Execution().ID
	if err := (agentruntime.ExecutionHost{
		Runtime: runtime,
		State:   state,
	}).Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	backend := newTestRuntimeExecutionBackendAt(
		t,
		root,
		&gatewayTestProvider{},
	)
	recovered, err := backend.Recover(
		t.Context(),
		session,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Execution.ID != executionID {
		t.Fatalf("recovered execution = %#v", recovered)
	}
	completed := waitGatewayExecution(
		t,
		backend,
		session,
		executionID,
	)
	if completed.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		completed.Result == nil ||
		completed.Result.Output != "gateway result" {
		t.Fatalf("completed recovery = %#v", completed)
	}
}

func TestRuntimeExecutionBackendEnqueuesContextIntoLiveHostedExecution(
	t *testing.T,
) {
	t.Parallel()
	states, err := NewFileSessionStateFactory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	provider := &gatewayContextInjectionProvider{}
	tool := &gatewayContextInjectionTool{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build:  testGatewayContextRuntimeBuilder(provider, tool),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	session := Session{
		ID: "gateway-context-session", UserID: "user-a",
		Provider: "gateway-context", MaxTurns: 3,
	}
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	submitted, err := backend.Submit(t.Context(), session, "base")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-tool.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("tool did not start")
	}
	enqueued, err := backend.EnqueueContextInjection(
		t.Context(),
		session,
		submitted.Execution.ID,
		sdk.ContextInjection{
			Priority: sdk.ContextInjectionNext,
			Mode:     sdk.ContextInjectionTaskNotification,
			Origin:   "gateway-test",
			Messages: []sdk.Message{{
				Role:    sdk.RoleUser,
				Content: "live gateway context",
			}},
		},
	)
	if err != nil {
		t.Fatalf("enqueue context injection: %v", err)
	}
	if enqueued.Execution.ID != submitted.Execution.ID {
		t.Fatalf("enqueued execution = %#v", enqueued)
	}
	close(tool.release)
	completed := waitGatewayExecution(
		t,
		backend,
		session,
		submitted.Execution.ID,
	)
	if completed.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		completed.Result == nil ||
		completed.Result.Output != "gateway context accepted" {
		t.Fatalf("completed execution = %#v", completed)
	}
	requests := provider.Requests()
	if len(requests) != 2 {
		t.Fatalf("provider requests = %#v", requests)
	}
	second := requests[1].Messages
	if len(second) == 0 ||
		second[len(second)-1].Content != "live gateway context" {
		t.Fatalf("second provider messages = %#v", second)
	}
}

func TestRuntimeExecutionBackendClosePreservesExecutionForRecovery(
	t *testing.T,
) {
	root := t.TempDir()
	entered := make(chan struct{}, 1)
	session := Session{
		ID: "runtime-close-recover", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	backend := newTestRuntimeExecutionBackendAt(
		t,
		root,
		&gatewayTestProvider{block: entered},
	)
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	submitted, err := backend.Submit(t.Context(), session, "wait")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}
	closeCtx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	if err := backend.Close(closeCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	metadata, loadErr := state.Trajectories().LoadMetadata(
		t.Context(),
		session.ID,
	)
	closeErr := state.Close(context.Background())
	if loadErr != nil || closeErr != nil {
		t.Fatal(errors.Join(loadErr, closeErr))
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != submitted.Execution.ID ||
		metadata.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("execution after backend close = %#v", metadata.Execution)
	}

	recoveredBackend := newTestRuntimeExecutionBackendAt(
		t,
		root,
		&gatewayTestProvider{},
	)
	if _, err := recoveredBackend.Recover(
		t.Context(),
		session,
	); err != nil {
		t.Fatal(err)
	}
	recovered := waitGatewayExecution(
		t,
		recoveredBackend,
		session,
		submitted.Execution.ID,
	)
	if recovered.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		recovered.Result == nil ||
		recovered.Result.Output != "gateway result" {
		t.Fatalf("recovered execution = %#v", recovered)
	}
}

func TestRuntimeExecutionBackendCurrentUsesActiveHostControl(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	countingStates := &countingStateFactory{StateBackendFactory: states}
	entered := make(chan struct{}, 1)
	session := Session{
		ID: "runtime-current-active-host", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: countingStates,
		Build:  testGatewayRuntimeBuilder(&gatewayTestProvider{block: entered}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	submitted, err := backend.Submit(t.Context(), session, "wait")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}

	opensBeforeCurrent := countingStates.opens.Load()
	current, err := backend.Current(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	if current.Execution.ID != submitted.Execution.ID {
		t.Fatalf("current execution = %#v", current)
	}
	if opensAfterCurrent := countingStates.opens.Load(); opensAfterCurrent != opensBeforeCurrent {
		t.Fatalf(
			"state opens after active Current() = %d, want %d",
			opensAfterCurrent,
			opensBeforeCurrent,
		)
	}
}

func TestRuntimeExecutionBackendCancelsUnhostedThroughRuntime(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-cancel-unhosted", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	executionID := createUnhostedGatewayExecution(
		t,
		states,
		session,
		&gatewayTestProvider{},
	)
	var builds atomic.Int64
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build: func(
			ctx context.Context,
			spec RuntimeBuildSpec,
			state sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			builds.Add(1)
			return testGatewayRuntimeBuilder(&gatewayTestProvider{})(
				ctx,
				spec,
				state,
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	cancelled, err := backend.Cancel(t.Context(), session, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if builds.Load() != 1 {
		t.Fatalf("runtime builds = %d, want 1", builds.Load())
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", cancelled)
	}
}

func TestRuntimeExecutionBackendCancelFallsBackToFenceWhenRuntimeBuildFails(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-cancel-fallback", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	executionID := createUnhostedGatewayExecution(
		t,
		states,
		session,
		&gatewayTestProvider{},
	)
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build: func(
			context.Context,
			RuntimeBuildSpec,
			sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			return nil, errors.New("runtime unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	cancelled, err := backend.Cancel(t.Context(), session, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", cancelled)
	}
}

func TestRuntimeExecutionBackendCancelDrainsRecoveryBeforeFallback(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-cancel-active-recovery", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	executionID := createUnhostedGatewayExecution(
		t,
		states,
		session,
		&gatewayTestProvider{},
	)
	firstBuildEntered := make(chan struct{})
	var (
		builds          atomic.Int64
		concurrentBuild atomic.Bool
		inBuild         atomic.Int64
	)
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build: func(
			ctx context.Context,
			spec RuntimeBuildSpec,
			state sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			if inBuild.Add(1) > 1 {
				concurrentBuild.Store(true)
			}
			defer inBuild.Add(-1)
			if builds.Add(1) == 1 {
				close(firstBuildEntered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return testGatewayRuntimeBuilder(&gatewayTestProvider{})(
				ctx,
				spec,
				state,
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	if _, err := backend.Recover(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstBuildEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery runtime build did not start")
	}
	cancelled, err := backend.Cancel(t.Context(), session, executionID)
	if err != nil {
		t.Fatal(err)
	}
	if concurrentBuild.Load() {
		t.Fatal("cancel built a fallback runtime before recovery host drained")
	}
	if builds.Load() != 2 {
		t.Fatalf("runtime builds = %d, want 2", builds.Load())
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", cancelled)
	}
}

func TestRuntimeExecutionBackendPollWaitsForHostClose(t *testing.T) {
	provider := &gatewayTestProvider{}
	backend := newTestRuntimeExecutionBackend(t, provider)
	session := Session{
		ID: "runtime-poll-close", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	provider.closeStarted = make(chan struct{})
	provider.closeRelease = make(chan struct{})
	submitted, err := backend.Submit(t.Context(), session, "hello")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.closeStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("execution host did not start closing")
	}

	result := make(chan error, 1)
	go func() {
		_, err := backend.Get(
			t.Context(),
			session,
			submitted.Execution.ID,
		)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("poll returned before execution host closed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(provider.closeRelease)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return after execution host closed")
	}
}

func TestRuntimeExecutionBackendReservesSessionBeforeDurableSubmit(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-submit-reservation", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testGatewayRuntimeBuilder(
		&gatewayTestProvider{},
	)(t.Context(), runtimeBuildSpec(session), state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.NewSession(
		t.Context(),
		agentruntime.SessionConfig{
			ID:       session.ID,
			Provider: session.Provider,
			MaxTurns: session.MaxTurns,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := (agentruntime.ExecutionHost{
		Runtime: runtime,
		State:   state,
	}).Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	buildEntered := make(chan struct{}, 1)
	releaseBuild := make(chan struct{})
	provider := &gatewayTestProvider{}
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build: func(
			ctx context.Context,
			spec RuntimeBuildSpec,
			state sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			select {
			case buildEntered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseBuild:
			}
			return testGatewayRuntimeBuilder(provider)(
				ctx,
				spec,
				state,
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})

	firstDone := make(chan struct {
		execution Execution
		err       error
	}, 1)
	go func() {
		execution, err := backend.Submit(
			context.Background(),
			session,
			"first",
		)
		firstDone <- struct {
			execution Execution
			err       error
		}{execution: execution, err: err}
	}()
	select {
	case <-buildEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime builder did not start")
	}
	state, err = states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	_, viewErr := agentruntime.LoadExecutionView(
		t.Context(),
		state.Trajectories(),
		session.ID,
	)
	closeErr := state.Close(context.Background())
	if !errors.Is(viewErr, sdk.ErrTrajectoryExecution) ||
		closeErr != nil {
		t.Fatalf(
			"execution view before durable submit error=%v close=%v",
			viewErr,
			closeErr,
		)
	}
	if _, err := backend.Submit(t.Context(), session, "second"); !errors.Is(
		err,
		ErrExecutionActive,
	) {
		t.Fatalf("second submit error = %v, want ErrExecutionActive", err)
	}
	close(releaseBuild)
	var first struct {
		execution Execution
		err       error
	}
	select {
	case first = <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first submit did not finish")
	}
	if first.err != nil {
		t.Fatal(first.err)
	}
	completed := waitGatewayExecution(
		t,
		backend,
		session,
		first.execution.Execution.ID,
	)
	if completed.Execution.State != sdk.TrajectoryExecutionSucceeded {
		t.Fatalf("completed execution = %#v", completed)
	}
}

func TestRuntimeExecutionBackendGatesCompositionDuringPreDurableReservation(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	t.Cleanup(func() {
		if err := directory.Close(context.Background()); err != nil {
			t.Errorf("close directory: %v", err)
		}
	})
	if _, err := directory.Register(
		t.Context(),
		testRegistration("file", "node-a"),
		registry.LeaseOptions{TTL: time.Minute},
	); err != nil {
		t.Fatal(err)
	}

	buildEntered := make(chan struct{}, 1)
	releaseBuild := make(chan struct{})
	var blockBuild atomic.Bool
	provider := &gatewayTestProvider{}
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build: func(
			ctx context.Context,
			spec RuntimeBuildSpec,
			state sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			if blockBuild.Load() {
				select {
				case buildEntered <- struct{}{}:
				default:
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-releaseBuild:
				}
			}
			return testGatewayRuntimeBuilder(provider)(
				ctx,
				spec,
				state,
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemorySessionStore()
	service, err := NewService(ServiceConfig{
		Store: store, Directory: directory, Executions: backend,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("close service: %v", err)
		}
	})
	session, err := service.CreateSession(t.Context(), Session{
		ID: "runtime-predurable-idle-guard", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	blockBuild.Store(true)
	submitDone := make(chan error, 1)
	go func() {
		_, err := service.SubmitMessage(
			context.Background(),
			"user-a",
			session.ID,
			"first",
		)
		submitDone <- err
	}()
	select {
	case <-buildEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("runtime builder did not start")
	}
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	_, viewErr := agentruntime.LoadExecutionView(
		t.Context(),
		state.Trajectories(),
		session.ID,
	)
	closeErr := state.Close(context.Background())
	if !errors.Is(viewErr, sdk.ErrTrajectoryExecution) ||
		closeErr != nil {
		t.Fatalf(
			"execution view before durable submit error=%v close=%v",
			viewErr,
			closeErr,
		)
	}

	attachCtx, cancelAttach := context.WithTimeout(
		t.Context(),
		100*time.Millisecond,
	)
	_, err = service.AttachPlugin(
		attachCtx,
		"user-a",
		session.ID,
		"file@node-a",
		session.Revision,
	)
	cancelAttach()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"pre-durable attach error = %v, want context deadline",
			err,
		)
	}

	cancelCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	_, err = backend.Cancel(cancelCtx, session, "not-yet-durable")
	cancel()
	if !errors.Is(err, ErrExecutionActive) {
		t.Fatalf("pre-durable cancel error = %v, want ErrExecutionActive", err)
	}

	close(releaseBuild)
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("submit did not finish")
	}
}

func TestRuntimeExecutionBackendValidatesSessionAfterReservation(
	t *testing.T,
) {
	t.Parallel()
	root := t.TempDir()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		ID: "runtime-validator-reservation", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testGatewayRuntimeBuilder(
		&gatewayTestProvider{},
	)(t.Context(), runtimeBuildSpec(session), state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.NewSession(
		t.Context(),
		agentruntime.SessionConfig{
			ID:       session.ID,
			Provider: session.Provider,
			MaxTurns: session.MaxTurns,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := (agentruntime.ExecutionHost{
		Runtime: runtime,
		State:   state,
	}).Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	validatorEntered := make(chan struct{}, 1)
	releaseValidator := make(chan struct{})
	validatorErr := errors.New("session plugin binding is stale")
	var buildCalls atomic.Int32
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		ValidateSession: func(ctx context.Context, _ Session) error {
			select {
			case validatorEntered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseValidator:
			}
			return validatorErr
		},
		Build: func(
			ctx context.Context,
			spec RuntimeBuildSpec,
			state sdk.StateBackend,
		) (*agentruntime.Runtime, error) {
			buildCalls.Add(1)
			return testGatewayRuntimeBuilder(&gatewayTestProvider{})(
				ctx,
				spec,
				state,
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := backend.Submit(context.Background(), session, "first")
		firstDone <- err
	}()
	select {
	case <-validatorEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("session validator did not start")
	}
	state, err = states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	_, viewErr := agentruntime.LoadExecutionView(
		t.Context(),
		state.Trajectories(),
		session.ID,
	)
	closeErr := state.Close(context.Background())
	if !errors.Is(viewErr, sdk.ErrTrajectoryExecution) ||
		closeErr != nil {
		t.Fatalf(
			"execution view before validator release error=%v close=%v",
			viewErr,
			closeErr,
		)
	}
	if _, err := backend.Submit(t.Context(), session, "second"); !errors.Is(
		err,
		ErrExecutionActive,
	) {
		t.Fatalf("second submit error = %v, want ErrExecutionActive", err)
	}
	close(releaseValidator)
	select {
	case err := <-firstDone:
		if !errors.Is(err, validatorErr) {
			t.Fatalf("first submit error = %v, want validator error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first submit did not finish")
	}
	if got := buildCalls.Load(); got != 0 {
		t.Fatalf("runtime build calls = %d, want 0 before validation succeeds", got)
	}
}

func testRuntimeExecutionBackendSuccess(t *testing.T) {
	backend := newTestRuntimeExecutionBackend(t, &gatewayTestProvider{})
	session := Session{
		ID: "runtime-success", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	submitted, err := backend.Submit(t.Context(), session, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Execution.ID == "" {
		t.Fatalf("submitted execution = %#v", submitted)
	}
	completed := waitGatewayExecution(
		t,
		backend,
		session,
		submitted.Execution.ID,
	)
	if completed.Execution.State != sdk.TrajectoryExecutionSucceeded ||
		completed.Result == nil ||
		completed.Result.Output != "gateway result" {
		t.Fatalf("completed execution = %#v", completed)
	}
}

func testRuntimeExecutionBackendCancel(t *testing.T) {
	entered := make(chan struct{}, 1)
	backend := newTestRuntimeExecutionBackend(
		t,
		&gatewayTestProvider{block: entered},
	)
	session := Session{
		ID: "runtime-cancel", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	}
	if err := backend.CreateSession(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	submitted, err := backend.Submit(t.Context(), session, "wait")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not start")
	}
	cancelled, err := backend.Cancel(
		t.Context(),
		session,
		submitted.Execution.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", cancelled)
	}
	current := waitGatewayExecution(
		t,
		backend,
		session,
		submitted.Execution.ID,
	)
	if current.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("current execution = %#v", current)
	}
}

func createUnhostedGatewayExecution(
	t *testing.T,
	states StateBackendFactory,
	session Session,
	provider sdk.Provider,
) string {
	t.Helper()
	state, err := states.Open(t.Context(), session)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testGatewayRuntimeBuilder(provider)(
		t.Context(),
		runtimeBuildSpec(session),
		state,
	)
	if err != nil {
		t.Fatal(err)
	}
	runtimeSession, err := runtime.NewSession(
		t.Context(),
		agentruntime.SessionConfig{
			ID:       session.ID,
			Provider: session.Provider,
			MaxTurns: session.MaxTurns,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := runtimeSession.SubmitPrompt(t.Context(), "cancel me")
	if err != nil {
		t.Fatal(err)
	}
	executionID := submission.Execution().ID
	if err := (agentruntime.ExecutionHost{
		Runtime: runtime,
		State:   state,
	}).Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return executionID
}

type countingStateFactory struct {
	StateBackendFactory
	opens atomic.Int64
}

func (factory *countingStateFactory) Open(
	ctx context.Context,
	session Session,
) (sdk.StateBackend, error) {
	state, err := factory.StateBackendFactory.Open(ctx, session)
	if err != nil {
		return nil, err
	}
	factory.opens.Add(1)
	return state, nil
}

func newTestRuntimeExecutionBackend(
	t *testing.T,
	provider sdk.Provider,
) ExecutionBackend {
	t.Helper()
	return newTestRuntimeExecutionBackendAt(t, t.TempDir(), provider)
}

func newTestRuntimeExecutionBackendAt(
	t *testing.T,
	root string,
	provider sdk.Provider,
) ExecutionBackend {
	t.Helper()
	states, err := NewFileSessionStateFactory(root)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build:  testGatewayRuntimeBuilder(provider),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)
		defer cancel()
		if err := backend.Close(ctx); err != nil {
			t.Errorf("close execution backend: %v", err)
		}
	})
	return backend
}

func testGatewayRuntimeBuilder(provider sdk.Provider) RuntimeBuilder {
	return func(
		ctx context.Context,
		_ RuntimeBuildSpec,
		state sdk.StateBackend,
	) (*agentruntime.Runtime, error) {
		runtime, err := agentruntime.NewRuntime(
			agentruntime.RuntimeConfig{
				Storage:          state,
				StorageOwnership: agentruntime.StorageBorrowed,
				OperationPoll:    time.Millisecond,
				TrajectoryLease:  time.Second,
			},
		)
		if err != nil {
			return nil, err
		}
		plugin := gatewayTestPlugin{PluginFunc: sdk.PluginFunc{
			PluginManifest: sdk.Manifest{
				Name: "gateway-provider", Version: "1.0.0",
				Description: "gateway runtime backend test provider",
				APIVersion:  sdk.APIVersion,
				Registers: []string{
					sdk.ProviderResource("gateway-test"),
				},
			},
			InstallFunc: func(
				_ context.Context,
				registrar sdk.Registrar,
			) error {
				return registrar.RegisterProvider(provider)
			},
		}, provider: provider}
		if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
			closeCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			closeErr := runtime.Close(closeCtx)
			cancel()
			return nil, errors.Join(err, closeErr)
		}
		return runtime, nil
	}
}

func testGatewayContextRuntimeBuilder(
	provider sdk.Provider,
	tool sdk.Tool,
) RuntimeBuilder {
	return func(
		ctx context.Context,
		_ RuntimeBuildSpec,
		state sdk.StateBackend,
	) (*agentruntime.Runtime, error) {
		runtime, err := agentruntime.NewRuntime(
			agentruntime.RuntimeConfig{
				Storage:          state,
				StorageOwnership: agentruntime.StorageBorrowed,
				OperationPoll:    time.Millisecond,
				TrajectoryLease:  time.Second,
			},
		)
		if err != nil {
			return nil, err
		}
		plugin := sdk.PluginFunc{
			PluginManifest: sdk.Manifest{
				Name:        "gateway-context",
				Version:     "1.0.0",
				Description: "gateway context injection test resources",
				APIVersion:  sdk.APIVersion,
				Registers: []string{
					sdk.ProviderResource("gateway-context"),
					sdk.ToolResource("gateway_wait_for_context"),
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
		if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
			closeCtx, cancel := context.WithTimeout(
				context.Background(),
				time.Second,
			)
			closeErr := runtime.Close(closeCtx)
			cancel()
			return nil, errors.Join(err, closeErr)
		}
		return runtime, nil
	}
}

type gatewayTestPlugin struct {
	sdk.PluginFunc
	provider sdk.Provider
}

func (plugin gatewayTestPlugin) Close(ctx context.Context) error {
	if closer, ok := plugin.provider.(interface {
		Close(context.Context) error
	}); ok {
		return closer.Close(ctx)
	}
	return nil
}

func waitGatewayExecution(
	t *testing.T,
	backend ExecutionBackend,
	session Session,
	executionID string,
) Execution {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		execution, err := backend.Get(
			t.Context(),
			session,
			executionID,
		)
		if err == nil && execution.Execution.Terminal() {
			return execution
		}
		if err != nil && !errors.Is(err, ErrExecutionNotFound) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution %s did not finish", executionID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
