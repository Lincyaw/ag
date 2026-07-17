package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type gatewayTestProvider struct {
	block        chan struct{}
	closeStarted chan struct{}
	closeRelease chan struct{}
}

func (*gatewayTestProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "gateway-test", Model: "test"}
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
	)(t.Context(), session, state)
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
	if err := closeExecutionHost(runtime, state); err != nil {
		t.Fatal(err)
	}

	backend := newTestRuntimeExecutionBackendAt(
		t,
		root,
		&gatewayTestProvider{},
	)
	recovered, err := backend.(ExecutionRecoveryBackend).Recover(
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
		_ Session,
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
