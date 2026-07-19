package sdk

import (
	"strings"
	"testing"
	"time"
)

func TestBuildInvocationGraphProjectsEdgesAndGraphOrder(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	records := []OperationRecord{
		invocationGraphRecord(
			"reducer",
			"workflow",
			[]string{"agent-a", "agent-b"},
			2,
			base.Add(time.Second),
		),
		invocationGraphRecord(
			"agent-b",
			"workflow",
			nil,
			1,
			base.Add(2*time.Second),
		),
		invocationGraphRecord(
			"workflow",
			"root",
			nil,
			0,
			base.Add(3*time.Second),
		),
		invocationGraphRecord(
			"agent-a",
			"workflow",
			nil,
			0,
			base.Add(4*time.Second),
		),
	}
	graph, err := BuildInvocationGraph("root", records)
	if err != nil {
		t.Fatal(err)
	}
	gotNodes := invocationNodeIDs(graph.Nodes)
	wantNodes := []string{
		"root",
		"workflow",
		"agent-a",
		"agent-b",
		"reducer",
	}
	if strings.Join(gotNodes, ",") != strings.Join(wantNodes, ",") {
		t.Fatalf("graph nodes = %v, want %v", gotNodes, wantNodes)
	}
	gotOperations := invocationRecordIDs(graph.OrderedOperations())
	wantOperations := wantNodes[1:]
	if strings.Join(gotOperations, ",") != strings.Join(wantOperations, ",") {
		t.Fatalf(
			"graph operations = %v, want %v",
			gotOperations,
			wantOperations,
		)
	}
	nodes := make(map[string]InvocationNode, len(graph.Nodes))
	for _, node := range graph.Nodes {
		nodes[node.ID] = node
	}
	if strings.Join(nodes["workflow"].Children, ",") !=
		"agent-a,agent-b,reducer" {
		t.Fatalf("workflow children = %#v", nodes["workflow"].Children)
	}
	if strings.Join(nodes["agent-a"].Dependents, ",") != "reducer" ||
		strings.Join(nodes["agent-b"].Dependents, ",") != "reducer" {
		t.Fatalf(
			"dependency edges: agent-a=%#v agent-b=%#v",
			nodes["agent-a"].Dependents,
			nodes["agent-b"].Dependents,
		)
	}
}

func invocationGraphRecord(
	id string,
	parentID string,
	dependencies []string,
	ordinal uint32,
	submittedAt time.Time,
) OperationRecord {
	return OperationRecord{
		Operation: Operation{
			ID:             id + "-operation",
			IdempotencyKey: id,
			State:          OperationSucceeded,
			Revision:       1,
			Output:         []byte(`{}`),
			SubmittedAt:    submittedAt,
		},
		Kind:     OperationKindAgent,
		Resource: "agent",
		Invocation: Invocation{
			ID:           id,
			RootID:       "root",
			ParentID:     parentID,
			SessionID:    "session",
			ExecutionID:  "execution",
			Dependencies: dependencies,
			Ordinal:      ordinal,
		},
	}
}

func invocationNodeIDs(nodes []InvocationNode) []string {
	result := make([]string, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node.ID)
	}
	return result
}

func invocationRecordIDs(records []OperationRecord) []string {
	result := make([]string, 0, len(records))
	for _, record := range records {
		result = append(result, record.Invocation.ID)
	}
	return result
}
