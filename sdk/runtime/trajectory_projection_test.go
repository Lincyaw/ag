package runtime

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

type projectionEntryStore struct {
	sdk.TrajectoryStore
	entries map[string]sdk.TrajectoryEntry
	loaded  []string
}

func (store *projectionEntryStore) LoadEntry(
	_ context.Context,
	_ string,
	entryID string,
) (sdk.TrajectoryEntry, error) {
	store.loaded = append(store.loaded, entryID)
	entry, ok := store.entries[entryID]
	if !ok {
		return sdk.TrajectoryEntry{}, sdk.ErrTrajectoryEntryNotFound
	}
	return entry, nil
}

func TestProjectTrajectoryMessagesUsesActiveBranch(t *testing.T) {
	userPayload, err := json.Marshal(sdk.Message{
		Role: sdk.RoleUser, Content: "remember this",
	})
	if err != nil {
		t.Fatal(err)
	}
	responsePayload, err := json.Marshal(sdk.AfterProviderPayload{
		Turn: 0,
		Response: &sdk.ModelResponse{
			Content: "remembered",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	trajectory := sdk.Trajectory{
		ID: "trajectory-a", Head: "response",
		Entries: []sdk.TrajectoryEntry{
			{
				ID: "user", TrajectoryID: "trajectory-a",
				Kind: sdk.TrajectoryKindUserMessage, Payload: userPayload,
			},
			{
				ID: "response", TrajectoryID: "trajectory-a", ParentID: "user",
				Kind: sdk.TrajectoryKindProviderResponse, Payload: responsePayload,
			},
			{
				ID: "off-branch", TrajectoryID: "trajectory-a", ParentID: "user",
				Kind:    sdk.TrajectoryKindUserMessage,
				Payload: json.RawMessage(`{"role":"user","content":"ignore me"}`),
			},
		},
	}

	messages, err := ProjectTrajectoryMessages(trajectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 ||
		messages[0].Role != sdk.RoleUser || messages[0].Content != "remember this" ||
		messages[1].Role != sdk.RoleAssistant || messages[1].Content != "remembered" {
		t.Fatalf("messages = %#v", messages)
	}
}

func TestProjectStoredTrajectoryMessagesLoadsOnlyProjectionPayloads(t *testing.T) {
	t.Parallel()

	entry := func(id string, kind sdk.TrajectoryKind, payload any) sdk.TrajectoryEntry {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return sdk.TrajectoryEntry{
			ID: id, TrajectoryID: "trajectory-a", Kind: kind, Payload: raw,
		}
	}
	legacy := entry("legacy", sdk.TrajectoryKindCheckpoint, durability.Checkpoint{
		Messages: []sdk.Message{{Role: sdk.RoleUser, Content: "base"}},
	})
	request := entry("request", sdk.TrajectoryKindProviderRequest, map[string]any{
		"request": "large and irrelevant to conversation projection",
	})
	response := entry("response", sdk.TrajectoryKindProviderResponse, sdk.AfterProviderPayload{
		Response: &sdk.ModelResponse{Content: "answer"},
	})
	sparse := entry("sparse", sdk.TrajectoryKindCheckpoint, durability.Checkpoint{
		MessageMode: durability.CheckpointMessagesBranch,
		Action: sdk.Action{
			Kind: sdk.ActionInject,
			Messages: []sdk.Message{{
				Role: sdk.RoleUser, Content: "injected",
			}},
		},
	})
	store := &projectionEntryStore{entries: map[string]sdk.TrajectoryEntry{
		legacy.ID: legacy, request.ID: request, response.ID: response, sparse.ID: sparse,
	}}
	branch := []sdk.TrajectoryEntryInspection{
		{ID: "obsolete", Kind: sdk.TrajectoryKindCheckpoint},
		{ID: legacy.ID, Kind: legacy.Kind},
		{ID: request.ID, Kind: request.Kind},
		{ID: response.ID, Kind: response.Kind},
		{ID: sparse.ID, Kind: sparse.Kind},
	}

	messages, err := ProjectStoredTrajectoryMessages(
		t.Context(),
		store,
		"trajectory-a",
		branch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 || messages[0].Content != "base" ||
		messages[1].Content != "answer" || messages[2].Content != "injected" {
		t.Fatalf("messages = %#v", messages)
	}
	if !slices.Equal(store.loaded, []string{"sparse", "legacy", "response"}) {
		t.Fatalf("loaded entries = %v", store.loaded)
	}
}
