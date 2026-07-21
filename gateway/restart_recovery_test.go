package gateway

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

func TestGatewayRestartRecoversDurableBackgroundInput(t *testing.T) {
	root := t.TempDir()
	entered := make(chan struct{}, 1)
	first := openRestartRecoveryService(
		t,
		root,
		&gatewayTestProvider{block: entered},
	)
	session, err := first.CreateSession(t.Context(), Session{
		ID: "restart-recovery", UserID: "user-a",
		Provider: "gateway-test", MaxTurns: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := first.EnqueueInput(
		t.Context(),
		session.UserID,
		session.ID,
		AgentInput{ID: "durable-input", Content: "recover after restart"},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("background provider did not start")
	}
	dispatching := waitRestartRecoveryInput(
		t,
		first,
		session,
		queued.ID,
		AgentInputDispatching,
	)
	if dispatching.ExecutionID == "" {
		t.Fatal("dispatching input was not durably bound to an execution")
	}

	closeGatewayTestService(t, first)

	second := openRestartRecoveryService(t, root, &gatewayTestProvider{})
	t.Cleanup(func() { closeGatewayTestService(t, second) })
	recovered, err := second.RecoverSessions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 ||
		recovered[0].Execution.ID != dispatching.ExecutionID {
		t.Fatalf("recovered executions = %#v", recovered)
	}
	completed := waitRestartRecoveryInput(
		t,
		second,
		session,
		queued.ID,
		AgentInputSucceeded,
	)
	if completed.ExecutionID != dispatching.ExecutionID {
		t.Fatalf(
			"execution binding changed across restart: before=%q after=%q",
			dispatching.ExecutionID,
			completed.ExecutionID,
		)
	}
	page, err := second.ListSessions(
		t.Context(),
		session.UserID,
		sdk.PageRequest{Limit: sdk.MaxPageSize},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != session.ID {
		t.Fatalf("trajectories after restart = %#v", page.Items)
	}
	trajectory, err := second.LoadTrajectory(
		t.Context(), session.UserID, session.ID, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	if trajectory.Execution == nil ||
		trajectory.Execution.ID != dispatching.ExecutionID ||
		trajectory.Execution.State != sdk.TrajectoryExecutionSucceeded {
		t.Fatalf("trajectory execution after restart = %#v", trajectory.Execution)
	}
}

func openRestartRecoveryService(
	t *testing.T,
	root string,
	provider sdk.Provider,
) *Service {
	t.Helper()
	sessions, err := NewFileSessionStore(filepath.Join(root, "control"))
	if err != nil {
		t.Fatal(err)
	}
	events, err := NewFileEventStore(filepath.Join(root, "events"))
	if err != nil {
		t.Fatal(err)
	}
	interactionStore, err := NewFileInteractionStore(
		filepath.Join(root, "interactions"),
	)
	if err != nil {
		t.Fatal(err)
	}
	interactions, err := NewInteractionManager(interactionStore, events)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := NewFileInputStore(filepath.Join(root, "inputs"))
	if err != nil {
		t.Fatal(err)
	}
	stateURI := (&url.URL{
		Scheme: "sqlite",
		Path:   filepath.Join(root, "state.db"),
	}).String()
	states, err := NewStorageSessionStateFactory(stateURI)
	if err != nil {
		t.Fatal(err)
	}
	executions, err := NewRuntimeExecutionBackend(RuntimeExecutionConfig{
		States: states,
		Build:  testGatewayRuntimeBuilder(provider),
		Events: events,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceConfig{
		Store: sessions, Events: events, Inputs: inputs,
		Interactions: interactions,
		Directory:    registry.NewMemoryDirectory(registry.MemoryConfig{}),
		Executions:   executions, DefaultNamespace: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func waitRestartRecoveryInput(
	t *testing.T,
	service *Service,
	session Session,
	inputID string,
	want AgentInputState,
) AgentInput {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		input, err := service.GetInput(
			t.Context(), session.UserID, session.ID, inputID,
		)
		if err == nil && input.State == want {
			return input
		}
		time.Sleep(10 * time.Millisecond)
	}
	input, err := service.GetInput(
		t.Context(), session.UserID, session.ID, inputID,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("input state = %q, want %q: %#v", input.State, want, input)
	return AgentInput{}
}

func closeGatewayTestService(t *testing.T, service *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
