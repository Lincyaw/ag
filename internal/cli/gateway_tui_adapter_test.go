package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/lincyaw/ag/gateway"
	cagentapp "github.com/lincyaw/ag/internal/cagent/app"
	cagentruntime "github.com/lincyaw/ag/internal/cagent/runtime"
	cagentsession "github.com/lincyaw/ag/internal/cagent/session"
	tuimessages "github.com/lincyaw/ag/internal/tui/messages"
	"github.com/lincyaw/ag/sdk"
)

func TestGatewayConversationSessionHydratesCompleteHistory(t *testing.T) {
	arguments := json.RawMessage(`{"path":"README.md"}`)
	trajectory := gateway.Session{
		ID:            "trajectory-history",
		Provider:      "test-model",
		WorkspaceRoot: t.TempDir(),
		CreatedAt:     time.Now().UTC(),
	}
	mirror := gatewayConversationSession(trajectory, []sdk.Message{
		{Role: sdk.RoleUser, Content: "inspect the repository"},
		{
			Role:    sdk.RoleAssistant,
			Content: "I will inspect it.",
			ToolCalls: []sdk.ToolCall{{
				ID: "call-1", Name: "read_file", Arguments: arguments,
			}},
		},
		{
			Role: sdk.RoleTool, ToolCallID: "call-1",
			Content: "repository contents", IsError: false,
		},
		{Role: sdk.RoleAssistant, Content: "The repository is ready."},
	})

	if mirror.ID != trajectory.ID {
		t.Fatalf("session ID = %q, want %q", mirror.ID, trajectory.ID)
	}
	messages := mirror.GetAllMessages()
	if len(messages) != 4 {
		t.Fatalf("message count = %d, want 4", len(messages))
	}
	if got := messages[0].Message.Content; got != "inspect the repository" {
		t.Fatalf("first message = %q", got)
	}
	if len(messages[1].Message.ToolCalls) != 1 ||
		messages[1].Message.ToolCalls[0].Function.Name != "read_file" {
		t.Fatalf("assistant tool calls = %#v", messages[1].Message.ToolCalls)
	}
	if got := messages[2].Message.ToolCallID; got != "call-1" {
		t.Fatalf("tool result call ID = %q", got)
	}
	if got := messages[3].Message.Content; got != "The repository is ready." {
		t.Fatalf("last message = %q", got)
	}
	if mirror.Title != "inspect the repository" {
		t.Fatalf("title = %q", mirror.Title)
	}
}

func TestGatewayInputContentPreservesAttachmentReferencesAndInlineText(t *testing.T) {
	content := gatewayInputContent("review these", []tuimessages.Attachment{
		{Name: "main.go", FilePath: "/tmp/project/main.go"},
		{Name: "paste", Content: "package main"},
	})
	for _, expected := range []string{
		"review these",
		`name="main.go"`,
		`path="/tmp/project/main.go"`,
		`name="paste"`,
		"package main",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("input %q does not contain %q", content, expected)
		}
	}
}

func TestGatewayInteractionAnswerUsesSemanticOptions(t *testing.T) {
	interaction := gateway.Interaction{Options: []gateway.InteractionOption{
		{ID: "allow_once", Label: "Allow"},
		{ID: "deny", Label: "Deny"},
	}}
	approved := gatewayInteractionAnswer(
		interaction,
		cagentruntime.ResumeApprove(),
	)
	if approved.OptionID != "allow_once" {
		t.Fatalf("approved option = %q", approved.OptionID)
	}
	rejected := gatewayInteractionAnswer(
		interaction,
		cagentruntime.ResumeReject("unsafe"),
	)
	if rejected.OptionID != "deny" || rejected.Text != "unsafe" {
		t.Fatalf("rejected answer = %#v", rejected)
	}
}

func TestGatewayTUIToolKeepsArgumentsAndSelectsRendererCategory(t *testing.T) {
	call, definition := gatewayTUITool(sdk.ToolCall{
		ID:        "call-shell",
		Name:      "exec_command",
		Arguments: json.RawMessage(`{"cmd":"pwd"}`),
	})
	if call.Function.Arguments != `{"cmd":"pwd"}` {
		t.Fatalf("arguments = %q", call.Function.Arguments)
	}
	if definition.Category != "shell" {
		t.Fatalf("category = %q", definition.Category)
	}
}

func TestGatewayTUIBindingTranslatesProviderResponse(t *testing.T) {
	session := cagentsession.New(cagentsession.WithID("translation"))
	application := cagentapp.New(t.Context(), session)
	binding := &gatewayTUIBinding{
		App:            application,
		Session:        session,
		pendingToolIDs: make(map[string]struct{}),
	}
	payload, err := json.Marshal(sdk.AfterProviderPayload{
		Provider: "test",
		Response: &sdk.ModelResponse{
			Content: "answer",
			ToolCalls: []sdk.ToolCall{{
				ID: "call-1", Name: "read_file",
				Arguments: json.RawMessage(`{"path":"README.md"}`),
			}},
			Usage: sdk.Usage{InputTokens: 10, OutputTokens: 4},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding.translate(gateway.AgentEvent{
		Name: sdk.EventAfterProvider, Payload: payload,
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := make(chan tea.Msg, 3)
	go application.SubscribeWith(ctx, func(message tea.Msg) {
		observed <- message
	})

	wantTypes := []any{
		&cagentruntime.AgentChoiceEvent{},
		&cagentruntime.PartialToolCallEvent{},
		&cagentruntime.TokenUsageEvent{},
	}
	for _, want := range wantTypes {
		select {
		case got := <-observed:
			switch want.(type) {
			case *cagentruntime.AgentChoiceEvent:
				if event, ok := got.(*cagentruntime.AgentChoiceEvent); !ok || event.Content != "answer" {
					t.Fatalf("agent choice = %#v", got)
				}
			case *cagentruntime.PartialToolCallEvent:
				if event, ok := got.(*cagentruntime.PartialToolCallEvent); !ok || event.ToolCall.Function.Name != "read_file" {
					t.Fatalf("tool call = %#v", got)
				}
			case *cagentruntime.TokenUsageEvent:
				if event, ok := got.(*cagentruntime.TokenUsageEvent); !ok || event.Usage.InputTokens != 10 ||
					event.Usage.OutputTokens != 4 {
					t.Fatalf("usage = %#v", got)
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %T", want)
		}
	}
}
