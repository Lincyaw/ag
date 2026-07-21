package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

func TestInputSupervisorRunsQueuedPromptsSerially(t *testing.T) {
	service, backend := newInputSupervisorTestService(t)
	createInputSupervisorTestSession(t, service, "serial")
	first, err := service.EnqueueInput(t.Context(), "user-a", "serial", AgentInput{
		ID: "input-first", Content: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.EnqueueInput(t.Context(), "user-a", "serial", AgentInput{
		ID: "input-second", Content: "second",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := receiveSubmittedPrompt(t, backend.submitted); got != "first" {
		t.Fatalf("first submitted prompt = %q", got)
	}
	select {
	case prompt := <-backend.submitted:
		t.Fatalf("second prompt submitted before boundary: %q", prompt)
	case <-time.After(150 * time.Millisecond):
	}
	backend.complete("serial", sdk.TrajectoryExecutionSucceeded)
	if got := receiveSubmittedPrompt(t, backend.submitted); got != "second" {
		t.Fatalf("second submitted prompt = %q", got)
	}
	backend.complete("serial", sdk.TrajectoryExecutionSucceeded)
	waitInputState(t, service, "serial", first.ID, AgentInputSucceeded)
	waitInputState(t, service, "serial", second.ID, AgentInputSucceeded)
}

func TestInputSupervisorHonorsDurablePauseAndResume(t *testing.T) {
	service, backend := newInputSupervisorTestService(t)
	session := createInputSupervisorTestSession(t, service, "paused")
	paused, err := service.SetSessionPaused(
		t.Context(), "user-a", session.ID, true, session.Revision,
	)
	if err != nil || !paused.Paused {
		t.Fatalf("pause session = %#v, %v", paused, err)
	}
	if _, err := service.EnqueueInput(
		t.Context(),
		"user-a",
		session.ID,
		AgentInput{ID: "while-paused", Content: "wait"},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case prompt := <-backend.submitted:
		t.Fatalf("prompt submitted while paused: %q", prompt)
	case <-time.After(150 * time.Millisecond):
	}
	resumed, err := service.SetSessionPaused(
		t.Context(), "user-a", session.ID, false, paused.Revision,
	)
	if err != nil || resumed.Paused {
		t.Fatalf("resume session = %#v, %v", resumed, err)
	}
	if got := receiveSubmittedPrompt(t, backend.submitted); got != "wait" {
		t.Fatalf("resumed submitted prompt = %q", got)
	}
	backend.complete(session.ID, sdk.TrajectoryExecutionSucceeded)
}

func TestInputSupervisorBindsRecoveredCompletedExecutionWithoutResubmitting(t *testing.T) {
	inputs := NewMemoryInputStore()
	queued, err := inputs.Enqueue(t.Context(), AgentInput{
		ID: "recovered-complete", SessionID: "recovered", Content: "once",
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, ok, err := inputs.AcquireNext(t.Context(), queued.SessionID)
	if err != nil || !ok {
		t.Fatalf("acquire input = %#v, %v, %v", acquired, ok, err)
	}
	service, backend := newInputSupervisorTestServiceWithInputs(t, inputs)
	createInputSupervisorTestSession(t, service, queued.SessionID)
	backend.setExecution(queued.SessionID, Execution{
		SessionID: queued.SessionID,
		Execution: sdk.TrajectoryExecution{
			ID: "execution-recovered", State: sdk.TrajectoryExecutionSucceeded,
			Revision: 2, InputEntryID: "input",
			CreatedAt: acquired.Input.UpdatedAt.Add(time.Millisecond),
			UpdatedAt: acquired.Input.UpdatedAt.Add(2 * time.Millisecond),
		},
	})
	service.supervisor.schedule(queued.SessionID)
	completed := waitInputStateValue(
		t, service, queued.SessionID, queued.ID, AgentInputSucceeded,
	)
	if completed.ExecutionID != "execution-recovered" {
		t.Fatalf("completed input = %#v", completed)
	}
	select {
	case prompt := <-backend.submitted:
		t.Fatalf("recovered input was submitted again: %q", prompt)
	default:
	}
}

func TestInputSupervisorDoesNotBindOlderActiveExecutionToRecoveredInput(t *testing.T) {
	inputs := NewMemoryInputStore()
	queued, err := inputs.Enqueue(t.Context(), AgentInput{
		ID: "recovered-after-old", SessionID: "old-active", Content: "after",
	})
	if err != nil {
		t.Fatal(err)
	}
	acquired, ok, err := inputs.AcquireNext(t.Context(), queued.SessionID)
	if err != nil || !ok {
		t.Fatalf("acquire input = %#v, %v, %v", acquired, ok, err)
	}
	service, backend := newInputSupervisorTestServiceWithInputs(t, inputs)
	createInputSupervisorTestSession(t, service, queued.SessionID)
	backend.setExecution(queued.SessionID, Execution{
		SessionID: queued.SessionID,
		Execution: sdk.TrajectoryExecution{
			ID: "execution-older", State: sdk.TrajectoryExecutionRunning,
			Revision: 1, InputEntryID: "old-input",
			CreatedAt: acquired.Input.UpdatedAt.Add(-time.Minute),
			UpdatedAt: acquired.Input.UpdatedAt.Add(-time.Minute),
		},
	})
	service.supervisor.schedule(queued.SessionID)
	select {
	case prompt := <-backend.submitted:
		t.Fatalf("input submitted before older execution completed: %q", prompt)
	case <-time.After(150 * time.Millisecond):
	}
	backend.complete(queued.SessionID, sdk.TrajectoryExecutionSucceeded)
	if got := receiveSubmittedPrompt(t, backend.submitted); got != queued.Content {
		t.Fatalf("submitted prompt = %q", got)
	}
	backend.complete(queued.SessionID, sdk.TrajectoryExecutionSucceeded)
	completed := waitInputStateValue(
		t, service, queued.SessionID, queued.ID, AgentInputSucceeded,
	)
	if completed.ExecutionID == "execution-older" {
		t.Fatalf("input bound to older execution: %#v", completed)
	}
}

func TestServiceDrainStopsAdmissionAndDrainsExecutionBackend(t *testing.T) {
	service, backend := newInputSupervisorTestService(t)
	session := createInputSupervisorTestSession(t, service, "draining")
	if err := service.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	drains := backend.drains
	backend.mu.Unlock()
	if drains != 1 {
		t.Fatalf("execution backend drains = %d, want 1", drains)
	}
	if _, err := service.EnqueueInput(
		t.Context(),
		session.UserID,
		session.ID,
		AgentInput{Content: "too late"},
	); !errors.Is(err, ErrGatewayDraining) {
		t.Fatalf("enqueue during drain error = %v", err)
	}
	if _, err := service.SubmitMessage(
		t.Context(), session.UserID, session.ID, "too late",
	); !errors.Is(err, ErrGatewayDraining) {
		t.Fatalf("submit during drain error = %v", err)
	}
}

func newInputSupervisorTestService(
	t *testing.T,
) (*Service, *queueExecutionBackend) {
	return newInputSupervisorTestServiceWithInputs(t, NewMemoryInputStore())
}

func newInputSupervisorTestServiceWithInputs(
	t *testing.T,
	inputs InputStore,
) (*Service, *queueExecutionBackend) {
	t.Helper()
	backend := &queueExecutionBackend{
		executions: make(map[string]Execution),
		submitted:  make(chan string, 8),
	}
	service, err := NewService(ServiceConfig{
		Store: NewMemorySessionStore(), Events: NewMemoryEventStore(),
		Inputs:           inputs,
		Directory:        registry.NewMemoryDirectory(registry.MemoryConfig{}),
		Executions:       backend,
		DefaultProvider:  "test",
		DefaultMaxTurns:  3,
		DefaultNamespace: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return service, backend
}

func createInputSupervisorTestSession(
	t *testing.T,
	service *Service,
	id string,
) Session {
	t.Helper()
	session, err := service.CreateSession(t.Context(), Session{
		ID: id, UserID: "user-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func receiveSubmittedPrompt(t *testing.T, submitted <-chan string) string {
	t.Helper()
	select {
	case prompt := <-submitted:
		return prompt
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for submitted prompt")
		return ""
	}
}

func waitInputState(
	t *testing.T,
	service *Service,
	sessionID string,
	inputID string,
	want AgentInputState,
) {
	waitInputStateValue(t, service, sessionID, inputID, want)
}

func waitInputStateValue(
	t *testing.T,
	service *Service,
	sessionID string,
	inputID string,
	want AgentInputState,
) AgentInput {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		input, err := service.GetInput(t.Context(), "user-a", sessionID, inputID)
		if err == nil && input.State == want {
			return input
		}
		time.Sleep(10 * time.Millisecond)
	}
	input, err := service.GetInput(t.Context(), "user-a", sessionID, inputID)
	t.Fatalf("input = %#v, %v; want state %s", input, err, want)
	return AgentInput{}
}

type queueExecutionBackend struct {
	mu         sync.Mutex
	executions map[string]Execution
	submitted  chan string
	next       int
	drains     int
}

func (*queueExecutionBackend) CreateSession(context.Context, Session) error {
	return nil
}

func (backend *queueExecutionBackend) Submit(
	ctx context.Context,
	session Session,
	content string,
) (Execution, error) {
	if err := ctx.Err(); err != nil {
		return Execution{}, err
	}
	backend.mu.Lock()
	if current, ok := backend.executions[session.ID]; ok &&
		!current.Execution.Terminal() {
		backend.mu.Unlock()
		return Execution{}, ErrExecutionActive
	}
	backend.next++
	now := time.Now().UTC()
	execution := Execution{
		SessionID: session.ID,
		Execution: sdk.TrajectoryExecution{
			ID:    fmt.Sprintf("execution-%d", backend.next),
			State: sdk.TrajectoryExecutionRunning, Revision: 1,
			InputEntryID: "input", CreatedAt: now, UpdatedAt: now,
		},
	}
	backend.executions[session.ID] = execution
	backend.mu.Unlock()
	select {
	case backend.submitted <- content:
	case <-ctx.Done():
		return Execution{}, ctx.Err()
	}
	return execution, nil
}

func (backend *queueExecutionBackend) EnqueueContextInjection(
	context.Context,
	Session,
	string,
	sdk.ContextInjection,
) (Execution, error) {
	return Execution{}, errors.New("unsupported")
}

func (backend *queueExecutionBackend) Recover(
	ctx context.Context,
	session Session,
) (Execution, error) {
	return backend.Current(ctx, session)
}

func (backend *queueExecutionBackend) Current(
	ctx context.Context,
	session Session,
) (Execution, error) {
	if err := ctx.Err(); err != nil {
		return Execution{}, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	execution, ok := backend.executions[session.ID]
	if !ok {
		return Execution{}, ErrExecutionNotFound
	}
	return execution, nil
}

func (backend *queueExecutionBackend) Get(
	ctx context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	execution, err := backend.Current(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	if execution.Execution.ID != executionID {
		return Execution{}, ErrExecutionNotFound
	}
	return execution, nil
}

func (backend *queueExecutionBackend) Cancel(
	_ context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	execution, ok := backend.executions[session.ID]
	if !ok || execution.Execution.ID != executionID {
		return Execution{}, ErrExecutionNotFound
	}
	execution.Execution.State = sdk.TrajectoryExecutionCancelled
	backend.executions[session.ID] = execution
	return execution, nil
}

func (*queueExecutionBackend) Close(context.Context) error { return nil }

func (backend *queueExecutionBackend) Drain(context.Context) error {
	backend.mu.Lock()
	backend.drains++
	backend.mu.Unlock()
	return nil
}

func (backend *queueExecutionBackend) complete(
	sessionID string,
	state sdk.TrajectoryExecutionState,
) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	execution := backend.executions[sessionID]
	execution.Execution.State = state
	execution.Execution.Revision++
	execution.Execution.UpdatedAt = time.Now().UTC()
	backend.executions[sessionID] = execution
}

func (backend *queueExecutionBackend) setExecution(
	sessionID string,
	execution Execution,
) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.executions[sessionID] = execution
}
