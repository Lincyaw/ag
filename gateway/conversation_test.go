package gateway

import (
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
)

func TestProjectConversationPageChunksAndPagesLargeMessages(t *testing.T) {
	content := strings.Repeat("x", 5<<20)
	payload, err := json.Marshal(map[string]any{
		"messages": []sdk.Message{{Role: sdk.RoleAssistant, Content: content}},
	})
	if err != nil {
		t.Fatal(err)
	}
	trajectory := sdk.Trajectory{
		ID: "large-conversation", Head: "checkpoint",
		Entries: []sdk.TrajectoryEntry{{
			ID: "checkpoint", TrajectoryID: "large-conversation",
			Kind: sdk.TrajectoryKindCheckpoint, Payload: payload,
		}},
	}

	first, err := projectConversationPage(
		trajectory,
		ConversationQuery{Limit: maxConversationPageSize},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Next == 0 || len(first.Items) < 1 {
		t.Fatalf("first page items=%d next=%d", len(first.Items), first.Next)
	}
	var restored strings.Builder
	for _, item := range first.Items {
		restored.WriteString(item.Content)
	}
	second, err := projectConversationPage(
		trajectory,
		ConversationQuery{After: first.Next, Limit: maxConversationPageSize},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range second.Items {
		restored.WriteString(item.Content)
	}
	if second.Next != 0 || restored.String() != content {
		t.Fatalf(
			"second next=%d restored=%d want=%d",
			second.Next,
			restored.Len(),
			len(content),
		)
	}
}

func TestConversationChunksPreserveUTF8(t *testing.T) {
	content := strings.Repeat("界", conversationChunkBytes/3+10)
	chunks := conversationChunks([]sdk.Message{{
		Role: sdk.RoleUser, Content: content,
	}})
	if len(chunks) != 2 || chunks[0].Continuation || !chunks[1].Continuation {
		t.Fatalf("chunks = %#v", chunks)
	}
	if !utf8.ValidString(chunks[0].Content) || !utf8.ValidString(chunks[1].Content) ||
		chunks[0].Content+chunks[1].Content != content {
		t.Fatal("UTF-8 content was split incorrectly")
	}
}

func TestConversationChunksRetainToolHistoryWithoutArguments(t *testing.T) {
	t.Parallel()
	chunks := conversationChunks([]sdk.Message{
		{
			Role: sdk.RoleAssistant,
			ToolCalls: []sdk.ToolCall{{
				ID: "call-1", Name: "read_file",
				Arguments: json.RawMessage(`{"path":"/large/private/value"}`),
			}},
		},
		{
			Role: sdk.RoleTool, ToolCallID: "call-1",
			Content: "file contents", IsError: true,
		},
	})
	if len(chunks) != 2 {
		t.Fatalf("chunks = %#v", chunks)
	}
	call := chunks[0]
	if call.Role != sdk.RoleAssistant || len(call.ToolCalls) != 1 ||
		call.ToolCalls[0].ID != "call-1" ||
		call.ToolCalls[0].Name != "read_file" {
		t.Fatalf("tool call projection = %#v", call)
	}
	encoded, err := json.Marshal(call)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private") {
		t.Fatalf("tool arguments leaked into conversation projection: %s", encoded)
	}
	result := chunks[1]
	if result.Role != sdk.RoleTool || result.ToolCallID != "call-1" ||
		result.Content != "file contents" || !result.IsError {
		t.Fatalf("tool result projection = %#v", result)
	}
}

func TestConversationChunksBoundToolResultPreviews(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("界", conversationToolPreviewBytes)
	chunks := conversationChunks([]sdk.Message{{
		Role: sdk.RoleTool, ToolCallID: "call-large", Content: content,
	}})
	if len(chunks) != 1 {
		t.Fatalf("chunks = %d, want 1", len(chunks))
	}
	preview := chunks[0].Content
	if !utf8.ValidString(preview) || !strings.HasSuffix(preview, "…") ||
		len(preview) > conversationToolPreviewBytes+len("…") {
		t.Fatalf("preview bytes=%d valid=%v suffix=%v", len(preview), utf8.ValidString(preview), strings.HasSuffix(preview, "…"))
	}
}
