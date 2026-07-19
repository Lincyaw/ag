package sdk

import (
	"encoding/json"
	"testing"
)

func TestCloneMessagesSnapshotsToolCallArguments(t *testing.T) {
	t.Parallel()
	messages := []Message{{
		Content: "call",
		ToolCalls: []ToolCall{{
			ID:        "call",
			Arguments: json.RawMessage(`{"value":1}`),
		}},
	}}
	cloned := CloneMessages(messages)
	injected := Inject(messages...)

	messages[0].Content = "changed"
	messages[0].ToolCalls[0].Arguments[0] = '['

	if cloned[0].Content != "call" ||
		string(cloned[0].ToolCalls[0].Arguments) != `{"value":1}` {
		t.Fatalf("cloned messages changed with source: %#v", cloned)
	}
	if injected.Action.Messages[0].Content != "call" ||
		string(injected.Action.Messages[0].ToolCalls[0].Arguments) != `{"value":1}` {
		t.Fatalf("injected messages changed with source: %#v", injected)
	}
}

func TestCloneOperationRecordSnapshotsMutableFields(t *testing.T) {
	t.Parallel()
	record := OperationRecord{
		Operation: Operation{
			ID:             "operation",
			IdempotencyKey: "operation-key",
			Output:         json.RawMessage(`{"previous":true}`),
		},
		Input: json.RawMessage(`{"input":1}`),
		Invocation: Invocation{
			ID:           "node",
			RootID:       "root",
			SessionID:    "session",
			ExecutionID:  "execution",
			Dependencies: []string{"dependency"},
		},
		Execution: &OperationLease{Owner: "owner", Token: "token"},
	}
	cloned := CloneOperationRecord(record)

	record.Operation.Output[0] = '['
	record.Input[0] = '['
	record.Invocation.Dependencies[0] = "changed"
	record.Execution.Token = "changed"

	if string(cloned.Operation.Output) != `{"previous":true}` ||
		string(cloned.Input) != `{"input":1}` ||
		cloned.Invocation.Dependencies[0] != "dependency" ||
		cloned.Execution.Token != "token" {
		t.Fatalf("cloned operation record changed with source: %#v", cloned)
	}
}

func TestCloneOperationRequestSnapshotsMutableFields(t *testing.T) {
	t.Parallel()
	request := OperationRequest{
		IdempotencyKey: "operation-key",
		Input:          json.RawMessage(`{"input":1}`),
		Invocation: Invocation{
			ID:           "node",
			Dependencies: []string{"dependency"},
		},
	}
	cloned := CloneOperationRequest(request)

	request.Input[0] = '['
	request.Invocation.Dependencies[0] = "changed"

	if string(cloned.Input) != `{"input":1}` ||
		cloned.Invocation.Dependencies[0] != "dependency" {
		t.Fatalf("cloned operation request changed with source: %#v", cloned)
	}
}

func TestCloneTrajectoryEntrySnapshotsMutableFields(t *testing.T) {
	t.Parallel()
	turn := 1
	isError := true
	entry := TrajectoryEntry{
		ID: "entry",
		Fields: TrajectoryEntryFields{
			Turn:    &turn,
			IsError: &isError,
		},
		Payload: json.RawMessage(`{"payload":1}`),
		Audit: []EventAudit{{
			Steps: []HookAuditStep{{
				Attributes: map[string]string{"key": "value"},
			}},
		}},
		Attributes: map[string]string{"entry": "value"},
	}
	cloned := CloneTrajectoryEntry(entry)

	*entry.Fields.Turn = 2
	*entry.Fields.IsError = false
	entry.Payload[0] = '['
	entry.Audit[0].Steps[0].Attributes["key"] = "changed"
	entry.Attributes["entry"] = "changed"

	if *cloned.Fields.Turn != 1 ||
		!*cloned.Fields.IsError ||
		string(cloned.Payload) != `{"payload":1}` ||
		cloned.Audit[0].Steps[0].Attributes["key"] != "value" ||
		cloned.Attributes["entry"] != "value" {
		t.Fatalf("cloned trajectory entry changed with source: %#v", cloned)
	}
}

func TestCloneTrajectoryEnvironmentSnapshotsMutableFields(t *testing.T) {
	t.Parallel()
	environment := TrajectoryEnvironment{
		Plugins: []TrajectoryPlugin{{
			Name:      "plugin",
			Registers: []string{ToolResource("tool")},
		}},
		Tools: []ToolSpec{{
			Name: "tool",
			Parameters: map[string]any{
				"properties": map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"required": []any{"query"},
			},
		}},
		Agents: []AgentSpec{{
			Name:  "agent",
			Tools: []string{"tool"},
		}},
		Subscribers: []SubscriberSpec{{
			Name:   "subscriber",
			Events: []string{EventAgentEnd},
		}},
		Capabilities: []CapabilitySpec{{
			Name: "capability",
			InputSchema: map[string]any{
				"properties": map[string]any{
					"value": map[string]any{"type": "number"},
				},
			},
			OutputSchema: map[string]any{
				"required": []any{"value"},
			},
		}},
		Events: []EventContract{{
			Name:          "custom",
			MutableFields: []string{"payload"},
		}},
	}
	cloned := CloneTrajectoryEnvironment(environment)

	environment.Plugins[0].Registers[0] = "changed"
	environment.Tools[0].Parameters["properties"].(map[string]any)["query"].(map[string]any)["type"] = "number"
	environment.Tools[0].Parameters["required"].([]any)[0] = "changed"
	environment.Agents[0].Tools[0] = "changed"
	environment.Subscribers[0].Events[0] = "changed"
	environment.Capabilities[0].InputSchema["properties"].(map[string]any)["value"].(map[string]any)["type"] = "string"
	environment.Capabilities[0].OutputSchema["required"].([]any)[0] = "changed"
	environment.Events[0].MutableFields[0] = "changed"

	if cloned.Plugins[0].Registers[0] != ToolResource("tool") ||
		cloned.Tools[0].Parameters["properties"].(map[string]any)["query"].(map[string]any)["type"] != "string" ||
		cloned.Tools[0].Parameters["required"].([]any)[0] != "query" ||
		cloned.Agents[0].Tools[0] != "tool" ||
		cloned.Subscribers[0].Events[0] != EventAgentEnd ||
		cloned.Capabilities[0].InputSchema["properties"].(map[string]any)["value"].(map[string]any)["type"] != "number" ||
		cloned.Capabilities[0].OutputSchema["required"].([]any)[0] != "value" ||
		cloned.Events[0].MutableFields[0] != "payload" {
		t.Fatalf("cloned trajectory environment changed with source: %#v", cloned)
	}
}
