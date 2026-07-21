package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestSQLiteTrajectoryInspectionDoesNotMaterializePayloads(t *testing.T) {
	t.Parallel()

	backend, err := newSQLiteStateBackend(
		filepath.Join(t.TempDir(), "state.db"),
		"inspection",
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Errorf("close backend: %v", err)
		}
	})
	store := backend.Trajectories()
	if err := store.Create(t.Context(), sdk.Trajectory{ID: "large-inspection"}); err != nil {
		t.Fatal(err)
	}
	largePayload := json.RawMessage(`"` + strings.Repeat("x", 8<<20) + `"`)
	first := sdk.TrajectoryEntry{
		ID:         "large-entry",
		Kind:       sdk.TrajectoryKindUserMessage,
		Payload:    largePayload,
		Attributes: map[string]string{"source": "test"},
		Audit: []sdk.EventAudit{{
			EventID: "audit-1", EventName: sdk.EventBeforeAgentStart,
		}},
	}
	head, err := store.Append(t.Context(), "large-inspection", "", first)
	if err != nil {
		t.Fatal(err)
	}
	second := sdk.TrajectoryEntry{
		ID:       "checkpoint-entry",
		ParentID: head,
		Kind:     sdk.TrajectoryKindCheckpoint,
		Payload:  json.RawMessage(`{"message_mode":"branch","turns":1,"action":{"kind":"step"}}`),
	}
	if _, err := store.Append(t.Context(), "large-inspection", head, second); err != nil {
		t.Fatal(err)
	}

	inspector, ok := store.(sdk.TrajectoryEntryInspector)
	if !ok {
		t.Fatal("SQLite trajectory store does not expose payload-free inspection")
	}
	metadata, entries, err := inspector.InspectTrajectoryEntries(
		t.Context(),
		"large-inspection",
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Head != second.ID || metadata.Checkpoint != second.ID ||
		metadata.EntryCount != 2 || len(entries) != 2 {
		t.Fatalf("inspection metadata=%#v entries=%#v", metadata, entries)
	}
	if entries[0].PayloadBytes != len(largePayload) ||
		entries[0].AttributeCount != 1 ||
		entries[0].AuditCount != 1 {
		t.Fatalf("large entry inspection = %#v", entries[0])
	}
	if entries[1].PayloadBytes != len(second.Payload) {
		t.Fatalf("checkpoint inspection = %#v", entries[1])
	}

	historical, entries, err := inspector.InspectTrajectoryEntries(
		t.Context(),
		"large-inspection",
		first.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if historical.Head != first.ID || historical.Checkpoint != "" ||
		historical.EntryCount != 1 || len(entries) != 1 {
		t.Fatalf("historical inspection metadata=%#v entries=%#v", historical, entries)
	}
}
