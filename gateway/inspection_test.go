package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

func TestProjectTrajectoryEntryPageDoesNotTransportPayloads(t *testing.T) {
	payload := json.RawMessage(`"` + strings.Repeat("x", 20<<20) + `"`)
	trajectory := sdk.Trajectory{
		ID: "large-trajectory", Head: "large-entry",
		Entries: []sdk.TrajectoryEntry{
			{
				ID: "large-entry", TrajectoryID: "large-trajectory",
				Kind: sdk.TrajectoryKindCheckpoint, Payload: payload,
			},
			{
				ID: "off-branch", TrajectoryID: "large-trajectory",
				Kind: sdk.TrajectoryKindUserMessage,
			},
		},
	}

	page, err := projectTrajectoryEntryPage(trajectory, TrajectoryEntryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if page.Trajectory.EntryCount != 1 || len(page.Items) != 1 ||
		page.Items[0].PayloadBytes != len(payload) {
		t.Fatalf("page = %#v", page)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 4096 || strings.Contains(string(encoded), strings.Repeat("x", 100)) {
		t.Fatalf("inspection response unexpectedly contains payload (%d bytes)", len(encoded))
	}
}
