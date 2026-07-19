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

func TestHeadRestoresAnchor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	anchorID := "fork-anchor-1"
	restoreID := "restore-1"
	store := checkpointStore{
		entries: map[string]sdk.TrajectoryEntry{
			restoreID: {
				ID:       restoreID,
				ParentID: anchorID,
				Kind:     sdk.TrajectoryKindRestore,
				Payload:  json.RawMessage(`{}`),
			},
		},
	}

	restored, err := durability.HeadRestoresAnchor(
		ctx,
		store,
		"trajectory-1",
		restoreID,
		anchorID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !restored {
		t.Fatal("restore head was not recognized as the anchor branch")
	}
}

func TestBranchMessagesDistinguishesLegacyAndEnvelopeInputs(t *testing.T) {
	t.Parallel()

	legacyBranch := []sdk.TrajectoryEntry{
		{
			ID:   "checkpoint-1",
			Kind: sdk.TrajectoryKindCheckpoint,
			Payload: mustJSON(t, durability.Checkpoint{
				Messages: []sdk.Message{{
					Role:    sdk.RoleAssistant,
					Content: "checkpoint",
				}},
			}),
		},
		{
			ID:       "legacy-user",
			ParentID: "checkpoint-1",
			Kind:     sdk.TrajectoryKindUserMessage,
			Payload: mustJSON(t, sdk.Message{
				Role:    sdk.RoleUser,
				Content: "legacy",
			}),
		},
	}
	messages, err := durability.BranchMessages("trajectory-1", legacyBranch)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 ||
		messages[0].Content != "checkpoint" ||
		messages[1].Content != "legacy" {
		t.Fatalf("legacy branch messages = %#v", messages)
	}

	envelopeBranch := []sdk.TrajectoryEntry{
		legacyBranch[0],
		{
			ID:       "envelope-user",
			ParentID: "checkpoint-1",
			Kind:     sdk.TrajectoryKindUserMessage,
			Payload: mustJSON(t, durability.ExecutionInput{
				Message: sdk.Message{
					Role:    sdk.RoleUser,
					Content: "envelope",
				},
				BaseMessages: []sdk.Message{{
					Role:    sdk.RoleAssistant,
					Content: "base",
				}},
				Environment: sdk.TrajectoryEnvironment{
					SDKAPIVersion:     sdk.APIVersion,
					CompositionDigest: "digest",
				},
			}),
		},
	}
	messages, err = durability.BranchMessages("trajectory-1", envelopeBranch)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 ||
		messages[0].Content != "base" ||
		messages[1].Content != "envelope" {
		t.Fatalf("envelope branch messages = %#v", messages)
	}
}

func TestLoadSessionResumeBaseProjectsForkAnchorWithoutOwnedCheckpoint(t *testing.T) {
	t.Parallel()

	checkpoint := sdk.TrajectoryEntry{
		ID:           "parent-checkpoint",
		TrajectoryID: "parent",
		Kind:         sdk.TrajectoryKindCheckpoint,
		Payload: mustJSON(t, durability.Checkpoint{
			Messages: []sdk.Message{{
				Role:    sdk.RoleAssistant,
				Content: "parent checkpoint",
			}},
		}),
	}
	store := checkpointStore{
		entries: map[string]sdk.TrajectoryEntry{
			checkpoint.ID: checkpoint,
		},
		branches: map[string][]sdk.TrajectoryEntry{
			"child\x00parent-checkpoint": {checkpoint},
		},
	}
	base, err := durability.LoadSessionResumeBase(
		context.Background(),
		store,
		sdk.TrajectoryMetadata{
			ID:            "child",
			ParentID:      "parent",
			ParentEntryID: checkpoint.ID,
			Checkpoint:    checkpoint.ID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if base.Head != checkpoint.ID || len(base.Messages) != 1 ||
		base.Messages[0].Content != "parent checkpoint" {
		t.Fatalf("fork resume base = %#v", base)
	}
	if base.Checkpoint != nil || base.CheckpointEntry.ID != "" {
		t.Fatalf("fork resume base retained inherited checkpoint: %#v", base)
	}
}

func TestLoadSessionResumeBaseUsesTerminalExecutionBaseHead(t *testing.T) {
	t.Parallel()

	baseEntry := sdk.TrajectoryEntry{
		ID:           "execution-base",
		TrajectoryID: "trajectory",
		Kind:         sdk.TrajectoryKindUserMessage,
		Payload: mustJSON(t, sdk.Message{
			Role:    sdk.RoleUser,
			Content: "base prompt",
		}),
	}
	checkpoint := sdk.TrajectoryEntry{
		ID:           "latest-checkpoint",
		TrajectoryID: "trajectory",
		ParentID:     baseEntry.ID,
		Kind:         sdk.TrajectoryKindCheckpoint,
		Payload: mustJSON(t, durability.Checkpoint{
			Messages: []sdk.Message{{
				Role:    sdk.RoleAssistant,
				Content: "latest checkpoint",
			}},
		}),
	}
	store := checkpointStore{
		entries: map[string]sdk.TrajectoryEntry{
			checkpoint.ID: checkpoint,
		},
		branches: map[string][]sdk.TrajectoryEntry{
			"trajectory\x00execution-base": {baseEntry},
		},
	}
	base, err := durability.LoadSessionResumeBase(
		context.Background(),
		store,
		sdk.TrajectoryMetadata{
			ID:         "trajectory",
			Checkpoint: checkpoint.ID,
			Execution: &sdk.TrajectoryExecution{
				ID:       "execution",
				State:    sdk.TrajectoryExecutionFailed,
				BaseHead: baseEntry.ID,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if base.Head != baseEntry.ID || len(base.Messages) != 1 ||
		base.Messages[0].Content != "base prompt" {
		t.Fatalf("terminal execution resume base = %#v", base)
	}
	if base.Checkpoint == nil || base.CheckpointEntry.ID != checkpoint.ID {
		t.Fatalf("terminal execution lost latest checkpoint context: %#v", base)
	}
}

func TestLoadExecutionCompletionBaseProjectsExecutionBaseHead(t *testing.T) {
	t.Parallel()

	baseEntry := sdk.TrajectoryEntry{
		ID:           "execution-base",
		TrajectoryID: "trajectory",
		Kind:         sdk.TrajectoryKindUserMessage,
		Payload: mustJSON(t, sdk.Message{
			Role:    sdk.RoleUser,
			Content: "base prompt",
		}),
	}
	store := checkpointStore{
		branches: map[string][]sdk.TrajectoryEntry{
			"trajectory\x00execution-base": {baseEntry},
		},
	}
	base, err := durability.LoadExecutionCompletionBase(
		context.Background(),
		store,
		sdk.TrajectoryMetadata{
			ID: "trajectory",
			Execution: &sdk.TrajectoryExecution{
				ID:       "execution",
				State:    sdk.TrajectoryExecutionRunning,
				BaseHead: baseEntry.ID,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if base.Head != baseEntry.ID || len(base.Messages) != 1 ||
		base.Messages[0].Content != "base prompt" {
		t.Fatalf("execution completion base = %#v", base)
	}
}

type checkpointStore struct {
	sdk.TrajectoryStore
	entries  map[string]sdk.TrajectoryEntry
	branches map[string][]sdk.TrajectoryEntry
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

func (store checkpointStore) LoadBranch(
	_ context.Context,
	trajectoryID string,
	head string,
) ([]sdk.TrajectoryEntry, error) {
	branch, exists := store.branches[trajectoryID+"\x00"+head]
	if !exists {
		return nil, sdk.ErrTrajectoryEntryNotFound
	}
	result := make([]sdk.TrajectoryEntry, len(branch))
	for index, entry := range branch {
		result[index] = sdk.CloneTrajectoryEntry(entry)
	}
	return result, nil
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
