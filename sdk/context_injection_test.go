package sdk

import (
	"testing"
	"time"
)

func TestNormalizeContextInjectionAppliesDefaults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 19, 10, 0, 0, 0, time.FixedZone("test", 3*3600))

	normalized, err := NormalizeContextInjection(ContextInjection{
		Messages: []Message{{
			Role:    RoleUser,
			Content: "queued context",
		}},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.ID == "" {
		t.Fatal("normalized context injection ID is empty")
	}
	if normalized.Priority != ContextInjectionNext {
		t.Fatalf("priority = %q", normalized.Priority)
	}
	if normalized.Mode != ContextInjectionPrompt {
		t.Fatalf("mode = %q", normalized.Mode)
	}
	if !normalized.CreatedAt.Equal(now.UTC()) {
		t.Fatalf("created at = %s, want %s", normalized.CreatedAt, now.UTC())
	}
}

func TestNormalizeContextInjectionRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	_, err := NormalizeContextInjection(ContextInjection{
		Priority: "urgent",
		Messages: []Message{{
			Role:    RoleUser,
			Content: "queued context",
		}},
	}, time.Time{})
	if err == nil {
		t.Fatal("NormalizeContextInjection accepted invalid priority")
	}
}

func TestNormalizeContextInjectionValidatesTargetSession(t *testing.T) {
	t.Parallel()
	_, err := NormalizeContextInjection(ContextInjection{
		TargetSessionID: "not a resource name",
		Messages: []Message{{
			Role:    RoleUser,
			Content: "queued context",
		}},
	}, time.Time{})
	if err == nil {
		t.Fatal("NormalizeContextInjection accepted invalid target session")
	}
}
