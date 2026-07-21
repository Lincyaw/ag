package gateway

import (
	"errors"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestUpdateSessionPersistsDurableControlSettings(t *testing.T) {
	service, _ := newInputSupervisorTestService(t)
	autoCompact := true
	created, err := service.CreateSession(t.Context(), Session{
		ID: "settings", UserID: "user-a", Model: "fast",
		Models: []string{"fast", "pro"}, AutoCompact: &autoCompact,
		ThinkingLevel: "off",
	})
	if err != nil {
		t.Fatal(err)
	}
	title := "Durable controls"
	model := "pro"
	disabled := false
	thinking := "high"
	updated, err := service.UpdateSession(
		t.Context(),
		created.UserID,
		created.ID,
		created.Revision,
		SessionPatch{
			Title: &title, Model: &model, AutoCompact: &disabled,
			ThinkingLevel:  &thinking,
			PermissionRule: &PermissionRule{Kind: "ask", Pattern: "Bash(git push:*)"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Title != title || updated.Model != model ||
		updated.AutoCompact == nil || *updated.AutoCompact ||
		updated.ThinkingLevel != thinking ||
		len(updated.Permissions.Ask) != 1 ||
		updated.Permissions.Ask[0] != "Bash(git push:*)" {
		t.Fatalf("updated settings = %#v", updated)
	}
	persisted, err := service.GetSession(t.Context(), created.UserID, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Revision != updated.Revision || persisted.Model != model ||
		persisted.ThinkingLevel != thinking {
		t.Fatalf("persisted settings = %#v", persisted)
	}
	if _, err := service.UpdateSession(
		t.Context(), created.UserID, created.ID, created.Revision,
		SessionPatch{Title: &title},
	); !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("stale update error = %v", err)
	}
}

func TestPermissionPatternMatchesClaudeCommandPrefix(t *testing.T) {
	candidates := permissionCandidates(toolCallForPermissionTest(
		"bash", `{"command":"git push origin main"}`,
	))
	if got := matchingPermissionRule(
		[]string{"Bash(git push:*)"}, candidates,
	); got == "" {
		t.Fatalf("permission rule did not match candidates %#v", candidates)
	}
}

func toolCallForPermissionTest(name, arguments string) sdk.ToolCall {
	return sdk.ToolCall{Name: name, Arguments: []byte(arguments)}
}
