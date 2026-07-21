package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
	tuitypes "github.com/lincyaw/ag/internal/tui/types"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

func TestGatewayInteractiveSessionProjectsEventsAndResult(t *testing.T) {
	payload, err := json.Marshal(sdk.AgentEndPayload{
		Output: "finished", Turns: 2, ToolCalls: 1,
		Cause: sdk.Cause{Code: sdk.CauseModelEnd},
	})
	if err != nil {
		t.Fatal(err)
	}
	frontend := &fakeGatewayFrontend{
		eventCursor: gateway.EventCursor{Sequence: 4},
		pages: []gateway.EventPage{{
			Items: []gateway.AgentEvent{{Sequence: 4}},
		}},
		events: []gateway.AgentEvent{
			{
				Sequence: 5, SessionID: "session-a", ID: "event-5",
				Name: sdk.EventTurnStart, Payload: json.RawMessage(`{"turn":1}`),
			},
			{
				Sequence: 6, SessionID: "session-a", ID: "event-6",
				Name: sdk.EventAgentEnd, Generation: 3, Payload: payload,
			},
		},
	}
	observed := make(chan string, 2)
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: "session-a",
		observe: func(_ context.Context, event sdk.Event) {
			observed <- event.Name
		},
	}
	cursor, err := session.latestEventCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	observerCtx, stopObserver := context.WithCancel(t.Context())
	defer stopObserver()
	subscription, err := session.frontend.SubscribeEvents(
		observerCtx, session.sessionID, cursor,
	)
	if err != nil {
		t.Fatal(err)
	}
	go session.observeEvents(observerCtx, subscription)
	result, err := session.Prompt(t.Context(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "finished" || result.Turns != 2 ||
		result.Generation != 3 || result.Cause.Code != sdk.CauseModelEnd {
		t.Fatalf("result = %#v", result)
	}
	if frontend.subscribedAfter != 4 {
		t.Fatalf("subscribed after = %d, want 4", frontend.subscribedAfter)
	}
	for index, want := range []string{sdk.EventTurnStart, sdk.EventAgentEnd} {
		select {
		case got := <-observed:
			if got != want {
				t.Fatalf("observed event %d = %q, want %q", index, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for observed event %d", index)
		}
	}
}

func TestGatewayInteractiveSessionCancelsRemoteExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	frontend := &fakeGatewayFrontend{
		blockEvents:      make(chan struct{}),
		submitted:        make(chan struct{}),
		executionRunning: true,
	}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: "session-a",
	}
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, "stop")
		done <- err
	}()
	select {
	case <-frontend.submitted:
	case <-time.After(time.Second):
		t.Fatal("message was not submitted")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("prompt error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not stop after cancellation")
	}
	frontend.mu.Lock()
	cancelled := frontend.cancelledInput
	frontend.mu.Unlock()
	if cancelled != "input-a" {
		t.Fatalf("cancelled input = %q", cancelled)
	}
}

func TestGatewayInteractiveSessionDetachLeavesRemoteExecutionRunning(t *testing.T) {
	ctx, detach := context.WithCancelCause(t.Context())
	frontend := &fakeGatewayFrontend{
		blockEvents:      make(chan struct{}),
		submitted:        make(chan struct{}),
		executionRunning: true,
	}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: "session-a",
	}
	done := make(chan error, 1)
	go func() {
		_, err := session.Prompt(ctx, "keep going")
		done <- err
	}()
	select {
	case <-frontend.submitted:
	case <-time.After(time.Second):
		t.Fatal("message was not submitted")
	}
	detach(errInteractiveDetached)
	select {
	case err := <-done:
		if !errors.Is(err, errInteractiveDetached) {
			t.Fatalf("prompt error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("prompt did not detach")
	}
	frontend.mu.Lock()
	cancelled := frontend.cancelledInput
	frontend.mu.Unlock()
	if cancelled != "" {
		t.Fatalf("detaching cancelled remote input %q", cancelled)
	}
}

func TestGatewayInteractiveSessionReportsMissingTerminalExecution(t *testing.T) {
	frontend := &fakeGatewayFrontend{executionErr: errors.New("missing")}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: "session-a",
	}
	_, terminal, err := session.resultIfInputTerminal(t.Context(), gateway.AgentInput{
		ID: "input-a", SessionID: "session-a", State: gateway.AgentInputSucceeded,
		ExecutionID: "execution-a",
	})
	if !terminal || err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("terminal=%v error=%v", terminal, err)
	}
}

func TestHydrateGatewayModelReconnectsInputAndInteraction(t *testing.T) {
	frontend := &fakeGatewayFrontend{
		hydrationInputs: gateway.InputPage{Items: []gateway.AgentInput{{
			ID: "input-pending", SessionID: "session-a",
			Content: "background task", State: gateway.AgentInputQueued,
		}}},
		hydrationInteractions: gateway.InteractionPage{Items: []gateway.Interaction{{
			ID: "interaction-pending", SessionID: "session-a",
			ExecutionID: "execution-a", State: gateway.InteractionPending,
			Prompt: "Continue?", Revision: 1,
		}}},
	}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: "session-a",
	}
	model := newInteractiveModel(session, session.sessionID, newProgressStyles(false))
	if err := hydrateGatewayModel(t.Context(), frontend, session, &model); err != nil {
		t.Fatal(err)
	}
	if model.state != stateExecuting || len(model.initialCmds) != 1 ||
		len(model.execCancels) != 1 {
		t.Fatalf(
			"state=%v initial=%d tracked=%d",
			model.state,
			len(model.initialCmds),
			len(model.execCancels),
		)
	}
	if model.interaction == nil || model.interaction.ID != "interaction-pending" ||
		model.transcript.Len() != 2 {
		t.Fatalf(
			"interaction=%#v transcript=%q",
			model.interaction,
			model.transcript.Content(),
		)
	}
	for _, cancel := range model.execCancels {
		cancel(context.Canceled)
	}
}

func TestHydrateGatewayModelLoadsHistoryWithoutRepeatingPendingInput(t *testing.T) {
	trajectory := gatewayHydrationTestTrajectory(t, sdk.TrajectoryExecutionPending)
	frontend := &fakeGatewayFrontend{
		hydrationTrajectory: trajectory,
		hydrationInputs: gateway.InputPage{Items: []gateway.AgentInput{{
			ID: "input-current", SessionID: trajectory.ID,
			Content: "current question", State: gateway.AgentInputDispatching,
			ExecutionID: "execution-current",
		}}},
	}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: trajectory.ID,
	}
	model := newInteractiveModel(session, session.sessionID, newProgressStyles(false))

	if err := hydrateGatewayModel(t.Context(), frontend, session, &model); err != nil {
		t.Fatal(err)
	}
	want := []*tuitypes.Message{
		tuitypes.User("earlier question"),
		tuitypes.Agent(tuitypes.MessageTypeAssistant, "ag", "earlier answer"),
		tuitypes.User("current question"),
	}
	got := model.transcript.Messages()
	if len(got) != len(want) {
		t.Fatalf("messages = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index].Type != want[index].Type ||
			got[index].Content != want[index].Content {
			t.Fatalf("messages[%d] = %#v, want %#v", index, got[index], want[index])
		}
	}
	if len(model.history) != 2 || model.history[0] != "earlier question" ||
		model.history[1] != "current question" || model.historyIndex != 2 {
		t.Fatalf("history = %#v index=%d", model.history, model.historyIndex)
	}
	if model.state != stateExecuting || len(model.initialCmds) != 1 ||
		len(model.execCancels) != 1 {
		t.Fatalf(
			"state=%v initial=%d tracked=%d",
			model.state,
			len(model.initialCmds),
			len(model.execCancels),
		)
	}
	for _, cancel := range model.execCancels {
		cancel(context.Canceled)
	}
}

func TestHydrateGatewayModelDoesNotReconnectStaleTerminalInput(t *testing.T) {
	trajectory := gatewayHydrationTestTrajectory(
		t,
		sdk.TrajectoryExecutionSucceeded,
	)
	frontend := &fakeGatewayFrontend{
		hydrationTrajectory: trajectory,
		hydrationInputs: gateway.InputPage{Items: []gateway.AgentInput{{
			ID: "input-current", SessionID: trajectory.ID,
			Content: "current question", State: gateway.AgentInputDispatching,
			ExecutionID: "execution-current",
		}}},
	}
	session := &gatewayInteractiveSession{
		frontend: frontend, sessionID: trajectory.ID,
	}
	model := newInteractiveModel(session, session.sessionID, newProgressStyles(false))

	if err := hydrateGatewayModel(t.Context(), frontend, session, &model); err != nil {
		t.Fatal(err)
	}
	if model.state != stateInput || len(model.initialCmds) != 0 ||
		len(model.execCancels) != 0 {
		t.Fatalf(
			"state=%v initial=%d tracked=%d",
			model.state,
			len(model.initialCmds),
			len(model.execCancels),
		)
	}
	got := model.transcript.Messages()
	if len(got) != 3 || got[2].Content != "current question" {
		t.Fatalf("messages = %#v", got)
	}
}

func gatewayHydrationTestTrajectory(
	t *testing.T,
	state sdk.TrajectoryExecutionState,
) sdk.Trajectory {
	t.Helper()
	mustPayload := func(value any) json.RawMessage {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	return sdk.Trajectory{
		ID: "session-history", Head: "current-user",
		Execution: &sdk.TrajectoryExecution{
			ID: "execution-current", State: state,
			InputEntryID: "current-user",
		},
		Entries: []sdk.TrajectoryEntry{
			{
				ID: "earlier-user", TrajectoryID: "session-history",
				Kind: sdk.TrajectoryKindUserMessage,
				Payload: mustPayload(sdk.Message{
					Role: sdk.RoleUser, Content: "earlier question",
				}),
			},
			{
				ID: "earlier-response", TrajectoryID: "session-history",
				ParentID: "earlier-user", Kind: sdk.TrajectoryKindProviderResponse,
				Payload: mustPayload(sdk.AfterProviderPayload{
					Response: &sdk.ModelResponse{Content: "earlier answer"},
				}),
			},
			{
				ID: "current-user", TrajectoryID: "session-history",
				ParentID: "earlier-response", Kind: sdk.TrajectoryKindUserMessage,
				Fields: sdk.TrajectoryEntryFields{
					ExecutionID: "execution-current",
				},
				Payload: mustPayload(sdk.Message{
					Role: sdk.RoleUser, Content: "current question",
				}),
			},
		},
	}
}

type fakeGatewayFrontend struct {
	mu                    sync.Mutex
	pages                 []gateway.EventPage
	eventCursor           gateway.EventCursor
	events                []gateway.AgentEvent
	blockEvents           chan struct{}
	submitted             chan struct{}
	subscribedAfter       uint64
	cancelledInput        string
	executionErr          error
	executionRunning      bool
	hydrationInputs       gateway.InputPage
	hydrationInteractions gateway.InteractionPage
	hydrationTrajectory   sdk.Trajectory
}

func (frontend *fakeGatewayFrontend) GetEventCursor(
	context.Context,
	string,
) (gateway.EventCursor, error) {
	return frontend.eventCursor, nil
}

func (frontend *fakeGatewayFrontend) ListConversation(
	_ context.Context,
	_ string,
	_ string,
	query gateway.ConversationQuery,
) (gateway.ConversationPage, error) {
	messages, err := agentruntime.ProjectTrajectoryMessages(
		frontend.hydrationTrajectory,
	)
	if err != nil {
		return gateway.ConversationPage{}, err
	}
	items := make([]gateway.ConversationMessage, 0, len(messages))
	for _, message := range messages {
		if message.Role == sdk.RoleUser || message.Role == sdk.RoleAssistant {
			items = append(items, gateway.ConversationMessage{
				Role: message.Role, Content: message.Content,
			})
		}
	}
	page := gateway.ConversationPage{
		Head: frontend.hydrationTrajectory.Head,
		Execution: sdk.CloneTrajectoryExecution(
			frontend.hydrationTrajectory.Execution,
		),
	}
	start := int(query.After)
	if start >= len(items) {
		return page, nil
	}
	limit := query.Limit
	if limit <= 0 || start+limit > len(items) {
		limit = len(items) - start
	}
	page.Items = append(page.Items, items[start:start+limit]...)
	if start+limit < len(items) {
		page.Next = uint64(start + limit)
	}
	return page, nil
}

func (frontend *fakeGatewayFrontend) EnqueueInput(
	context.Context,
	string,
	string,
	string,
) (gateway.AgentInput, error) {
	frontend.mu.Lock()
	if frontend.submitted == nil {
		frontend.submitted = make(chan struct{})
	}
	submitted := frontend.submitted
	frontend.mu.Unlock()
	select {
	case <-submitted:
	default:
		close(submitted)
	}
	return gateway.AgentInput{
		ID: "input-a", SessionID: "session-a", Revision: 2,
		State: gateway.AgentInputDispatching, ExecutionID: "execution-a",
	}, nil
}

func (frontend *fakeGatewayFrontend) GetInput(
	context.Context,
	string,
	string,
) (gateway.AgentInput, error) {
	return gateway.AgentInput{
		ID: "input-a", SessionID: "session-a", Revision: 2,
		State: gateway.AgentInputDispatching, ExecutionID: "execution-a",
	}, nil
}

func (frontend *fakeGatewayFrontend) CancelInput(
	_ context.Context,
	_ string,
	inputID string,
	_ uint64,
) (gateway.AgentInput, error) {
	frontend.mu.Lock()
	frontend.cancelledInput = inputID
	frontend.mu.Unlock()
	return gateway.AgentInput{}, nil
}

func (frontend *fakeGatewayFrontend) ResolveInteraction(
	_ context.Context,
	_ string,
	interactionID string,
	_ uint64,
	answer gateway.InteractionAnswer,
) (gateway.Interaction, error) {
	return gateway.Interaction{
		ID: interactionID, State: gateway.InteractionResolved, Answer: &answer,
	}, nil
}

func (frontend *fakeGatewayFrontend) GetExecution(
	context.Context,
	string,
	string,
) (gateway.Execution, error) {
	if frontend.executionErr != nil {
		return gateway.Execution{}, frontend.executionErr
	}
	if frontend.executionRunning {
		return gateway.Execution{
			SessionID: "session-a",
			Execution: sdk.TrajectoryExecution{
				ID: "execution-a", State: sdk.TrajectoryExecutionRunning,
			},
		}, nil
	}
	return gateway.Execution{
		SessionID: "session-a",
		Execution: sdk.TrajectoryExecution{
			ID: "execution-a", State: sdk.TrajectoryExecutionSucceeded,
		},
		Result: &agentruntime.Result{
			Output: "finished", Turns: 2, Generation: 3,
			Cause: sdk.Cause{Code: sdk.CauseModelEnd},
		},
	}, nil
}

func (frontend *fakeGatewayFrontend) ListEvents(
	context.Context,
	string,
	gateway.EventQuery,
) (gateway.EventPage, error) {
	frontend.mu.Lock()
	defer frontend.mu.Unlock()
	if len(frontend.pages) == 0 {
		return gateway.EventPage{}, nil
	}
	page := frontend.pages[0]
	frontend.pages = frontend.pages[1:]
	return page, nil
}

func (frontend *fakeGatewayFrontend) SubscribeEvents(
	ctx context.Context,
	_ string,
	after uint64,
) (gatewayEventSubscription, error) {
	frontend.mu.Lock()
	frontend.subscribedAfter = after
	frontend.mu.Unlock()
	return &fakeGatewayEventSubscription{
		ctx: ctx, events: frontend.events, block: frontend.blockEvents,
	}, nil
}

func (frontend *fakeGatewayFrontend) ListInputs(
	context.Context,
	string,
	gateway.InputQuery,
) (gateway.InputPage, error) {
	return frontend.hydrationInputs, nil
}

func (frontend *fakeGatewayFrontend) ListInteractions(
	context.Context,
	string,
	gateway.InteractionQuery,
) (gateway.InteractionPage, error) {
	return frontend.hydrationInteractions, nil
}

type fakeGatewayEventSubscription struct {
	ctx    context.Context
	events []gateway.AgentEvent
	block  chan struct{}
}

func (subscription *fakeGatewayEventSubscription) Next() (gateway.AgentEvent, error) {
	if len(subscription.events) > 0 {
		event := subscription.events[0]
		subscription.events = subscription.events[1:]
		return event, nil
	}
	select {
	case <-subscription.ctx.Done():
		return gateway.AgentEvent{}, subscription.ctx.Err()
	case <-subscription.block:
		return gateway.AgentEvent{}, errors.New("event stream closed")
	}
}

func (*fakeGatewayEventSubscription) Close() error { return nil }
