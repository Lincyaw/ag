package durability_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

func TestDecodeCheckpointReturnsOwnedState(t *testing.T) {
	t.Parallel()

	entry := sdk.TrajectoryEntry{
		ID: "checkpoint-1",
		Payload: mustJSON(t, durability.Checkpoint{
			Messages: []sdk.Message{{
				Role: sdk.RoleAssistant,
				ToolCalls: []sdk.ToolCall{{
					ID: "call-1", Name: "read",
				}},
			}},
			Dependencies: []string{"provider-1"},
		}),
	}
	checkpoint, err := durability.DecodeCheckpoint("trajectory-1", entry)
	if err != nil {
		t.Fatal(err)
	}

	checkpoint.Messages[0].ToolCalls[0].Name = "changed"
	checkpoint.Dependencies[0] = "changed"
	decodedAgain, err := durability.DecodeCheckpoint("trajectory-1", entry)
	if err != nil {
		t.Fatal(err)
	}
	if decodedAgain.Messages[0].ToolCalls[0].Name != "read" {
		t.Fatal("decoded checkpoint retained a caller mutation")
	}
	if decodedAgain.Dependencies[0] != "provider-1" {
		t.Fatal("decoded dependencies retained a caller mutation")
	}
}

func TestEntryFieldsProjectsDurableIndexes(t *testing.T) {
	t.Parallel()

	provider := durability.EntryFields(durability.ProviderRequest{
		Turn:         2,
		Provider:     "openai",
		Model:        "gpt-test",
		OperationKey: "provider-op",
	})
	if provider.Turn == nil || *provider.Turn != 2 ||
		provider.Provider != "openai" ||
		provider.Model != "gpt-test" ||
		provider.OperationKey != "provider-op" {
		t.Fatalf("provider fields = %#v", provider)
	}

	checkpoint := durability.EntryFields(durability.Checkpoint{
		Turns: 3,
		Action: sdk.Action{
			Kind: sdk.ActionStop,
			Cause: &sdk.Cause{
				Code: "complete",
			},
		},
	})
	if checkpoint.Turn == nil || *checkpoint.Turn != 2 ||
		checkpoint.ActionKind != sdk.ActionStop ||
		checkpoint.CauseCode != "complete" {
		t.Fatalf("checkpoint fields = %#v", checkpoint)
	}
}

func TestHeadRestoresCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	checkpointID := "checkpoint-1"
	restoreID := "restore-1"
	store := checkpointStore{
		entries: map[string]sdk.TrajectoryEntry{
			restoreID: {
				ID:       restoreID,
				ParentID: checkpointID,
				Kind:     sdk.TrajectoryKindRestore,
				Payload:  json.RawMessage(`{}`),
			},
		},
	}

	restored, err := durability.HeadRestoresCheckpoint(
		ctx,
		store,
		"trajectory-1",
		restoreID,
		checkpointID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !restored {
		t.Fatal("restore head was not recognized as the checkpoint branch")
	}
}

type checkpointStore struct {
	sdk.TrajectoryStore
	entries map[string]sdk.TrajectoryEntry
}

func (store checkpointStore) LoadEntry(
	_ context.Context,
	_ string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	entry, exists := store.entries[entryID]
	if !exists {
		return sdk.TrajectoryEntry{}, sdk.ErrTrajectoryEntryNotFound
	}
	return entry, nil
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
