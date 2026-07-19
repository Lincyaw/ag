package sdk

import (
	"strings"
	"testing"
)

func TestDenyToolPermissionBuildsStructuredBlock(t *testing.T) {
	effect := DenyToolPermission(PermissionRejection{
		Audience: PermissionRejectionSubagent,
		ToolName: "write_file",
		Reason:   "outside the allowed workspace",
	})
	if effect.Block == nil {
		t.Fatal("permission denial did not block")
	}
	if effect.Block.Kind != string(ToolErrorPermissionDenied) {
		t.Fatalf("block kind = %q, want permission denial", effect.Block.Kind)
	}
	message := effect.Block.Reason
	if !strings.Contains(message, "write_file") ||
		!strings.Contains(message, "allowed alternative") ||
		!strings.Contains(message, "outside the allowed workspace") {
		t.Fatalf("permission denial message = %q", message)
	}
}

func TestPermissionRejectionDefaultsToPolicyAudience(t *testing.T) {
	message := (PermissionRejection{}).Message()
	if !strings.Contains(message, "policy") ||
		strings.Contains(message, "user denied") {
		t.Fatalf("default permission message = %q", message)
	}
}
