package toolcommon

import (
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/cagent/tools"
	"github.com/lincyaw/ag/internal/tui/spinner"
	"github.com/lincyaw/ag/internal/tui/styles"
	"github.com/lincyaw/ag/internal/tui/types"
)

func testToolMessage() *types.Message {
	return types.ToolCallMessage(
		"agent",
		tools.ToolCall{
			ID:   "call-1",
			Type: "function",
			Function: tools.FunctionCall{
				Name:      "test_tool",
				Arguments: `{"query":"secret-arg"}`,
			},
		},
		tools.Tool{Name: "test_tool"},
		types.ToolStatusCompleted,
	)
}

func TestRenderToolHidesDetails(t *testing.T) {
	msg := testToolMessage()

	view := RenderTool(
		msg,
		spinner.New(spinner.ModeSpinnerOnly, styles.SpinnerDotsAccentStyle),
		"secret-arg",
		"secret-result",
		80,
		true,
	)

	if !strings.Contains(view, "test_tool") {
		t.Fatalf("expected tool name to remain visible, got %q", view)
	}
	if strings.Contains(view, "secret-arg") {
		t.Fatalf("expected args to be hidden, got %q", view)
	}
	if strings.Contains(view, "secret-result") {
		t.Fatalf("expected result to be hidden, got %q", view)
	}
}

func TestRenderToolShowsDetailsWhenExpanded(t *testing.T) {
	msg := testToolMessage()

	view := RenderTool(
		msg,
		spinner.New(spinner.ModeSpinnerOnly, styles.SpinnerDotsAccentStyle),
		"secret-arg",
		"secret-result",
		80,
		false,
	)

	if !strings.Contains(view, "secret-arg") {
		t.Fatalf("expected args to be visible, got %q", view)
	}
	if !strings.Contains(view, "secret-result") {
		t.Fatalf("expected result to be visible, got %q", view)
	}
}
