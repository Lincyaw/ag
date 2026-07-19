package compact

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/plugincontract"
	"github.com/lincyaw/ag/sdk"
)

func TestHookCompactsLargeBeforeProviderPayload(t *testing.T) {
	registrar := plugincontract.NewRegistrar()
	if err := New(Config{
		TriggerTokens:      64,
		TargetTokens:       48,
		KeepRecentMessages: 2,
		MaxMessageChars:    80,
		MaxToolResultChars: 80,
	}).Install(context.Background(), registrar); err != nil {
		t.Fatal(err)
	}
	hook := registrar.Hooks["auto-compact"].Value

	payload := sdk.BeforeProviderPayload{
		Turn:     3,
		Provider: "test",
		Messages: []sdk.Message{
			{Role: sdk.RoleUser, Content: strings.Repeat("alpha ", 80)},
			{Role: sdk.RoleAssistant, Content: "middle", ToolCalls: []sdk.ToolCall{{ID: "call-1", Name: "read_file", Arguments: json.RawMessage(`{"path":"a.go"}`)}}},
			{Role: sdk.RoleTool, ToolCallID: "call-1", Content: strings.Repeat("tool ", 80)},
			{Role: sdk.RoleUser, Content: "recent question"},
			{Role: sdk.RoleAssistant, Content: "recent answer"},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := hook.Handle(context.Background(), sdk.Event{
		Name:    sdk.EventBeforeProvider,
		Payload: raw,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(effect.Patch) == 0 {
		t.Fatal("expected messages patch")
	}
	var compacted []sdk.Message
	if err := json.Unmarshal(effect.Patch["messages"], &compacted); err != nil {
		t.Fatal(err)
	}
	if len(compacted) != 3 {
		t.Fatalf("compacted message count = %d, want 3", len(compacted))
	}
	if compacted[0].Role != sdk.RoleUser || !strings.Contains(compacted[0].Content, "<compact-summary>") {
		t.Fatalf("first message is not compact summary: %#v", compacted[0])
	}
	if !strings.Contains(compacted[0].Content, "read_file") {
		t.Fatalf("summary lost tool call context: %s", compacted[0].Content)
	}
	if compacted[1].Content != "recent question" || compacted[2].Content != "recent answer" {
		t.Fatalf("recent tail not preserved: %#v", compacted[1:])
	}
	if estimateMessagesTokens(compacted) >= estimateMessagesTokens(payload.Messages) {
		t.Fatalf("compaction did not reduce estimated tokens")
	}
}

func TestCompactMessagesBelowThresholdNoops(t *testing.T) {
	messages := []sdk.Message{{Role: sdk.RoleUser, Content: "small"}}
	compacted, ok := compactMessages(messages, Config{TriggerTokens: 1_000})
	if ok {
		t.Fatal("unexpected compaction")
	}
	if len(compacted) != 1 || compacted[0].Content != "small" {
		t.Fatalf("messages = %#v", compacted)
	}
}
