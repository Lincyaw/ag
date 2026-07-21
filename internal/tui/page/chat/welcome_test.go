package chat

import (
	"strings"
	"testing"
)

func TestWelcomeCardUsesActiveGatewayModelLine(t *testing.T) {
	lines := claudeWelcomeCardLines(
		"~/workspace/ag",
		"gpt-5.5-2026-04-24 · API Usage Billing",
		100,
	)
	view := strings.Join(lines, "\n")
	if !strings.Contains(view, "gpt-5.5-2026-04-24") {
		t.Fatalf("welcome card ignored active model: %s", view)
	}
	if strings.Contains(view, "Opus 4.8") {
		t.Fatalf("welcome card retained static model: %s", view)
	}
}
