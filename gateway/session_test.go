package gateway

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestSessionRuntimeConfigIsPrivateAndCloned(t *testing.T) {
	session, err := normalizeSession(Session{
		ID: "runtime-profile", UserID: "user-a",
		RuntimeConfig: json.RawMessage(`{"plugin":"remote"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	cloned := cloneSession(session)
	cloned.RuntimeConfig[2] = 'X'
	if string(cloned.RuntimeConfig) == string(session.RuntimeConfig) {
		t.Fatal("runtime config was not cloned")
	}
	raw, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "runtime_config") ||
		strings.Contains(string(raw), "remote") {
		t.Fatalf("private runtime config leaked through JSON: %s", raw)
	}
}

func TestGatewaySessionAllowsUnlimitedTurns(t *testing.T) {
	session, err := normalizeSession(Session{
		ID: "unlimited", UserID: "user-a", MaxTurns: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.MaxTurns != 0 {
		t.Fatalf("max turns = %d", session.MaxTurns)
	}
}

func TestGatewaySessionRejectsNegativeTurns(t *testing.T) {
	if _, err := normalizeSession(Session{
		ID: "negative", UserID: "user-a", MaxTurns: -1,
	}); err == nil {
		t.Fatal("negative max turns accepted")
	}
}

func TestGatewaySessionRequiresAbsoluteWorkspaceRoot(t *testing.T) {
	if _, err := normalizeSession(Session{
		ID: "relative", UserID: "user-a", WorkspaceRoot: "workspace",
	}); err == nil {
		t.Fatal("relative workspace root accepted")
	}
	root := t.TempDir()
	session, err := normalizeSession(Session{
		ID: "absolute", UserID: "user-a",
		WorkspaceRoot: filepath.Join(root, "nested", ".."),
	})
	if err != nil {
		t.Fatal(err)
	}
	if session.WorkspaceRoot != filepath.Clean(root) {
		t.Fatalf("workspace root = %q", session.WorkspaceRoot)
	}
}
