package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/internal/cagent/chat"
	"github.com/lincyaw/ag/internal/cagent/session"
)

func TestTranscriptAndHTMLExportUseHydratedSession(t *testing.T) {
	sess := session.New(session.WithID("trajectory-export"), session.WithTitle("Export <test>"))
	sess.AddMessage(session.UserMessage("hello <script>"))
	sess.AddMessage(session.NewAgentMessage("ag", &chat.Message{
		Role: chat.MessageRoleAssistant, Content: "safe answer",
	}))
	application := New(t.Context(), sess)
	plain := application.PlainTextTranscript()
	if !strings.Contains(plain, "USER:\nhello <script>") ||
		!strings.Contains(plain, "ASSISTANT (ag):\nsafe answer") {
		t.Fatalf("plain transcript = %q", plain)
	}
	path, err := application.ExportHTML(
		t.Context(), filepath.Join(t.TempDir(), "transcript"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Ext(path) != ".html" {
		t.Fatalf("export path = %q", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(raw)
	if strings.Contains(content, "<script>") ||
		!strings.Contains(content, "hello &lt;script&gt;") {
		t.Fatalf("HTML export was not escaped: %s", content)
	}
}
