package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/lincyaw/ag/gateway"
	"github.com/lincyaw/ag/internal/tui/types"
	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

func TestInteractiveViewportPagesThroughLongContent(t *testing.T) {
	t.Parallel()
	model := longInteractiveModel()

	bottom := model.transcript.YOffset()
	if bottom == 0 {
		t.Fatal("test content did not overflow the viewport")
	}
	if !strings.Contains(model.transcript.Content(), "line 00") {
		t.Fatal("viewport discarded content above the visible region")
	}

	updated, _ := model.Update(tea.KeyPressMsg{Code: tea.KeyPgUp})
	model = updated.(interactiveModel)
	if model.transcript.YOffset() >= bottom {
		t.Fatalf("PageUp offset = %d, want less than %d", model.transcript.YOffset(), bottom)
	}

	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyPgDown})
	model = updated.(interactiveModel)
	if !model.transcript.AtBottom() {
		t.Fatalf("PageDown offset = %d, want bottom", model.transcript.YOffset())
	}
}

func TestInteractiveViewportKeepsManualScrollPositionOnRefresh(t *testing.T) {
	t.Parallel()
	model := longInteractiveModel()
	model.transcript.PageUp()
	want := model.transcript.YOffset()

	model.statusLine = "still working"
	model.state = stateExecuting
	model.rebuildViewport()

	if got := model.transcript.YOffset(); got != want {
		t.Fatalf("refresh offset = %d, want %d", got, want)
	}
}

func TestInteractiveViewportFollowsNewContentFromBottom(t *testing.T) {
	t.Parallel()
	model := longInteractiveModel()
	if !model.transcript.AtBottom() {
		t.Fatal("viewport should start at bottom")
	}

	model.transcript.Append(types.Notice("new content"))
	model.rebuildViewport()

	if !model.transcript.AtBottom() {
		t.Fatalf("refresh offset = %d, want bottom", model.transcript.YOffset())
	}
}

func TestInteractiveSubmitReturnsViewportToBottom(t *testing.T) {
	t.Parallel()
	model := longInteractiveModel()
	model.transcript.PageUp()
	model.input.SetValue("next message")

	updated, _ := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(interactiveModel)

	if !model.transcript.AtBottom() {
		t.Fatalf("submit offset = %d, want bottom", model.transcript.YOffset())
	}
}

func TestInteractiveViewEnablesMouseAndKeepsTerminalHistory(t *testing.T) {
	t.Parallel()
	model := longInteractiveModel()

	view := model.View()
	if view.MouseMode != tea.MouseModeAllMotion {
		t.Fatalf("mouse mode = %v, want all motion", view.MouseMode)
	}
	if view.AltScreen {
		t.Fatal("agent view isolated the transcript in the alternate screen")
	}

	bottom := model.transcript.YOffset()
	updated, _ := model.Update(tea.MouseWheelMsg{Button: tea.MouseWheelUp})
	model = updated.(interactiveModel)
	if model.transcript.YOffset() >= bottom {
		t.Fatalf("wheel-up offset = %d, want less than %d", model.transcript.YOffset(), bottom)
	}
}

func TestInteractionAnswerSelectsOptionByNumber(t *testing.T) {
	answer := interactionAnswer(gateway.Interaction{
		Options: []gateway.InteractionOption{
			{ID: "approve", Label: "Approve"},
			{ID: "deny", Label: "Deny"},
		},
	}, "2")
	if answer.OptionID != "deny" || answer.Text != "" {
		t.Fatalf("answer = %#v", answer)
	}
}

func TestInteractiveQueuesMoreInputWhileExecutionRuns(t *testing.T) {
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	model.input.SetValue("first")
	updated, _ := model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(interactiveModel)
	model.input.SetValue("second")
	updated, _ = model.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
	model = updated.(interactiveModel)
	if model.state != stateExecuting || len(model.execCancels) != 2 {
		t.Fatalf(
			"state=%v queued=%d",
			model.state,
			len(model.execCancels),
		)
	}
	if !strings.Contains(model.statusLine, "2 input(s) pending") {
		t.Fatalf("status line = %q", model.statusLine)
	}
}

func TestInteractiveHydratesToolOnlyTurnsIntoHistory(t *testing.T) {
	t.Parallel()
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	model.hydrateConversation([]sdk.Message{
		{Role: sdk.RoleUser, Content: "inspect the repository"},
		{
			Role:      sdk.RoleAssistant,
			ToolCalls: []sdk.ToolCall{{ID: "call-1", Name: "read_file"}},
		},
		{
			Role: sdk.RoleTool, ToolCallID: "call-1",
			Content: "42 lines",
		},
		{Role: sdk.RoleAssistant, Content: "Inspection complete."},
	})

	got := model.transcript.Messages()
	if len(got) != 3 {
		t.Fatalf("messages = %#v", got)
	}
	if got[1].Type != types.MessageTypeNotice ||
		got[1].Content != "read_file — 42 lines" {
		t.Fatalf("tool history = %#v", got[1])
	}
	if !strings.Contains(model.transcript.Content(), "Inspection complete") {
		t.Fatalf("transcript = %q", model.transcript.Content())
	}
}

func TestInteractiveClearsInteractionWhenLastExecutionStops(t *testing.T) {
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	model.state = stateExecuting
	model.execCancels["request"] = func(error) {}
	model.interaction = &gateway.Interaction{ID: "interaction"}
	updated, _ := model.Update(executionDoneMsg{requestID: "request"})
	model = updated.(interactiveModel)
	if model.interaction != nil || model.state != stateInput {
		t.Fatalf("interaction=%#v state=%v", model.interaction, model.state)
	}
}

func TestInteractiveBackgroundDetachesWithoutCancellingAgent(t *testing.T) {
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	var cause error
	model.state = stateExecuting
	model.execCancels["request"] = func(err error) { cause = err }

	updated, command := model.Update(tea.KeyPressMsg{Code: 'b', Mod: tea.ModCtrl})
	model = updated.(interactiveModel)

	if command == nil || !model.quitting || !model.detached {
		t.Fatalf(
			"command=%v quitting=%v detached=%v",
			command,
			model.quitting,
			model.detached,
		)
	}
	if !errors.Is(cause, errInteractiveDetached) {
		t.Fatalf("cancel cause = %v", cause)
	}
}

func TestInteractiveExitDetachesActiveAgent(t *testing.T) {
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	var cause error
	model.state = stateExecuting
	model.execCancels["request"] = func(err error) { cause = err }

	updated, command := model.Update(tea.KeyPressMsg{Code: 'd', Mod: tea.ModCtrl})
	model = updated.(interactiveModel)

	if command == nil || !model.detached ||
		!errors.Is(cause, errInteractiveDetached) {
		t.Fatalf(
			"command=%v detached=%v cause=%v",
			command,
			model.detached,
			cause,
		)
	}
}

func TestInteractiveAgentViewShowsTrajectoryContextAndStatus(t *testing.T) {
	model := newInteractiveModel(
		stubInteractiveSession{},
		"session",
		newProgressStyles(false),
	)
	model.hydrateSession(gateway.Session{
		ID: "trajectory-12345678", Provider: "openai",
		WorkspaceRoot: "/workspace/project", Paused: true,
	})
	if model.agentStatus() != agentStatusPaused {
		t.Fatalf("status = %q, want paused", model.agentStatus())
	}
	model.width = 120
	model.height = 20
	model.recalculateLayout()
	view := model.View()
	for _, expected := range []string{
		agentStatusPaused, "openai", "/workspace/project", "trajectory-",
	} {
		if !strings.Contains(view.Content, expected) {
			t.Fatalf("view %q missing %q", view.Content, expected)
		}
	}

	model.state = stateExecuting
	if model.agentStatus() != agentStatusRunning {
		t.Fatalf("executing status = %q, want running", model.agentStatus())
	}
	model.interaction = &gateway.Interaction{ID: "question"}
	if model.agentStatus() != agentStatusWaiting {
		t.Fatalf("interaction status = %q, want waiting", model.agentStatus())
	}
}

type stubInteractiveSession struct{}

func (stubInteractiveSession) ID() string { return "session" }

func (stubInteractiveSession) Prompt(
	context.Context,
	string,
) (agentruntime.Result, error) {
	return agentruntime.Result{}, nil
}

func longInteractiveModel() interactiveModel {
	model := newInteractiveModel(nil, "session", newProgressStyles(false))
	model.width = 80
	model.height = 12
	model.recalculateLayout()

	lines := make([]string, 40)
	for i := range lines {
		lines[i] = fmt.Sprintf("line %02d", i)
	}
	model.transcript.Load([]*types.Message{types.Agent(
		types.MessageTypeAssistant,
		"ag",
		strings.Join(lines, "\n"),
	)})
	model.rebuildViewport()
	return model
}
