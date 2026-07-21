package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
)

func TestAskUserToolWaitsForGatewayResolution(t *testing.T) {
	store := NewMemoryInteractionStore()
	events := NewMemoryEventStore()
	manager, err := NewInteractionManager(store, events)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	defer events.Close(context.Background())
	ctx := sdk.WithInvocation(t.Context(), sdk.Invocation{
		ID: "invocation-a", RootID: "invocation-a",
		SessionID: "session-a", ExecutionID: "execution-a",
	})
	result := make(chan sdk.ToolResult, 1)
	failure := make(chan error, 1)
	go func() {
		value, err := (askUserTool{manager: manager}).Call(
			ctx,
			json.RawMessage(`{"question":"Continue?","options":[{"id":"yes","label":"Yes"},{"id":"no","label":"No"}]}`),
		)
		if err != nil {
			failure <- err
			return
		}
		result <- value
	}()

	interaction := waitPendingInteraction(t, manager, "session-a")
	if interaction.Prompt != "Continue?" {
		t.Fatalf("interaction = %#v", interaction)
	}
	if _, err := manager.Resolve(
		t.Context(),
		interaction.SessionID,
		interaction.ID,
		interaction.Revision,
		InteractionAnswer{OptionID: "yes"},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-result:
		if value.Content != "Yes" || value.IsError {
			t.Fatalf("tool result = %#v", value)
		}
	case err := <-failure:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("ask_user did not resume")
	}

	page, err := events.List(
		t.Context(),
		"session-a",
		EventQuery{Limit: maxEventPageSize},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 ||
		page.Items[0].Name != GatewayEventInteractionRequested ||
		page.Items[1].Name != GatewayEventInteractionResolved {
		t.Fatalf("interaction events = %#v", page.Items)
	}
}

func TestAskUserToolCancelsPendingInteractionWithExecutionContext(t *testing.T) {
	store := NewMemoryInteractionStore()
	events := NewMemoryEventStore()
	manager, err := NewInteractionManager(store, events)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	defer events.Close(context.Background())
	base, cancel := context.WithCancel(t.Context())
	ctx := sdk.WithInvocation(base, sdk.Invocation{
		ID: "invocation-cancel", RootID: "invocation-cancel",
		SessionID: "session-cancel", ExecutionID: "execution-cancel",
	})
	done := make(chan error, 1)
	go func() {
		_, err := (askUserTool{manager: manager}).Call(
			ctx,
			json.RawMessage(`{"question":"Continue?"}`),
		)
		done <- err
	}()

	interaction := waitPendingInteraction(t, manager, "session-cancel")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("tool error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ask_user did not stop after execution cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, err := manager.Get(t.Context(), interaction.SessionID, interaction.ID)
		if err == nil && current.State == InteractionCancelled {
			page, listErr := events.List(
				t.Context(),
				interaction.SessionID,
				EventQuery{Limit: maxEventPageSize},
			)
			if listErr != nil || len(page.Items) != 2 ||
				page.Items[1].Name != GatewayEventInteractionCancelled {
				t.Fatalf("cancellation events = %#v, %v", page.Items, listErr)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("pending interaction was not durably cancelled")
}

func waitPendingInteraction(
	t *testing.T,
	manager *InteractionManager,
	sessionID string,
) Interaction {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		page, err := manager.List(t.Context(), sessionID, InteractionQuery{
			State: InteractionPending,
		})
		if err == nil && len(page.Items) > 0 {
			return page.Items[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("pending interaction was not created")
	return Interaction{}
}
