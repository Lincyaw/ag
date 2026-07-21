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
