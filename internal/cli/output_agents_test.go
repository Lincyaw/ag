package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
)

func TestProjectBackgroundAgentStatus(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name         string
		session      gateway.Session
		inputs       []gateway.AgentInput
		interactions []gateway.Interaction
		want         string
	}{
		{name: "idle", want: agentStatusIdle},
		{
			name: "queued",
			inputs: []gateway.AgentInput{{
				State: gateway.AgentInputQueued, UpdatedAt: now,
			}},
			want: agentStatusQueued,
		},
		{
			name: "running",
			inputs: []gateway.AgentInput{{
				State: gateway.AgentInputDispatching, UpdatedAt: now,
			}},
			want: agentStatusRunning,
		},
		{
			name:    "paused",
			session: gateway.Session{Paused: true},
			inputs: []gateway.AgentInput{{
				State: gateway.AgentInputQueued, UpdatedAt: now,
			}},
			want: agentStatusPaused,
		},
		{
			name: "waiting",
			inputs: []gateway.AgentInput{{
				State: gateway.AgentInputDispatching, UpdatedAt: now,
			}},
			interactions: []gateway.Interaction{{
				ID: "question", State: gateway.InteractionPending,
				UpdatedAt: now,
			}},
			want: agentStatusWaiting,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.session.ID = "agent"
			got := projectManagedTrajectory(
				test.session,
				test.inputs,
				test.interactions,
			)
			if got.Status != test.want {
				t.Fatalf("status = %q, want %q", got.Status, test.want)
			}
		})
	}
}

func TestManagedTrajectoryOutputUsesUnifiedTrajectoryVocabulary(t *testing.T) {
	now := time.Now().UTC()
	var stdout bytes.Buffer
	application := &app{stdout: &stdout, output: outputText}
	err := application.writeManagedTrajectories([]managedTrajectorySummary{{
		ID: "trajectory-a", Status: agentStatusRunning,
		WorkspaceRoot: "/workspace/a", UpdatedAt: now,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"trajectory-a", "running", "/workspace/a"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("output %q does not contain %q", stdout.String(), want)
		}
	}
	stdout.Reset()
	application.output = outputJSON
	if err := application.writeManagedTrajectories(nil); err != nil {
		t.Fatal(err)
	}
	var values []managedTrajectorySummary
	if err := json.Unmarshal(stdout.Bytes(), &values); err != nil {
		t.Fatal(err)
	}
}
