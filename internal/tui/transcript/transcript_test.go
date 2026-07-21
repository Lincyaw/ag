package transcript

import (
	"fmt"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/tui/types"
)

func TestLoadRetainsCompleteConversationWhileStartingAtBottom(t *testing.T) {
	t.Parallel()
	model := New()
	model.SetSize(60, 8)

	messages := make([]*types.Message, 0, 24)
	for index := range 12 {
		messages = append(
			messages,
			types.User(fmt.Sprintf("question %02d", index)),
			types.Agent(
				types.MessageTypeAssistant,
				"ag",
				fmt.Sprintf("answer %02d", index),
			),
		)
	}
	model.Load(messages)

	if model.Len() != len(messages) {
		t.Fatalf("message count = %d, want %d", model.Len(), len(messages))
	}
	content := model.Content()
	for _, expected := range []string{"question 00", "answer 00", "question 11", "answer 11"} {
		if !strings.Contains(content, expected) {
			t.Fatalf("complete transcript missing %q", expected)
		}
	}
	if !model.AtBottom() || model.YOffset() == 0 {
		t.Fatalf("offset = %d, want non-zero bottom", model.YOffset())
	}

	model.PageUp()
	if model.AtBottom() {
		t.Fatal("PageUp did not expose historical messages")
	}
}

func TestAppendPreservesManualScrollAndFollowsFromBottom(t *testing.T) {
	t.Parallel()
	model := New()
	model.SetSize(60, 6)
	model.Load([]*types.Message{types.Agent(
		types.MessageTypeAssistant,
		"ag",
		strings.Repeat("history line\n", 30),
	)})

	model.PageUp()
	wantOffset := model.YOffset()
	model.Append(types.Notice("background update"))
	if got := model.YOffset(); got != wantOffset {
		t.Fatalf("manual offset = %d after append, want %d", got, wantOffset)
	}

	model.GotoBottom()
	model.Append(types.User("next prompt"))
	if !model.AtBottom() {
		t.Fatalf("offset = %d, want transcript to follow append", model.YOffset())
	}
}
