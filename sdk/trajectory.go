package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

var (
	ErrTrajectoryNotFound = errors.New("trajectory not found")
	ErrTrajectoryExists   = errors.New("trajectory already exists")
	ErrTrajectoryConflict = errors.New("trajectory head conflict")
	ErrTrajectoryVersion  = errors.New("unsupported trajectory schema version")
)

const (
	TrajectorySchemaVersion  uint32 = 1
	TrajectoryPayloadVersion uint32 = 1

	TrajectoryKindUserMessage      = "user_message"
	TrajectoryKindAgentStart       = "agent_start"
	TrajectoryKindProviderRequest  = "provider_request"
	TrajectoryKindProviderResponse = "provider_response"
	TrajectoryKindToolCall         = "tool_call"
	TrajectoryKindToolResult       = "tool_result"
	TrajectoryKindDecision         = "decision"
	TrajectoryKindCheckpoint       = "checkpoint"
	TrajectoryKindTerminal         = "terminal"
	TrajectoryKindRestore          = "restore"
	TrajectoryKindRollback         = "rollback"
)

type TrajectoryEntry struct {
	ID             string            `json:"id"`
	ParentID       string            `json:"parent_id,omitempty"`
	Kind           string            `json:"kind"`
	Timestamp      time.Time         `json:"timestamp"`
	Generation     uint64            `json:"generation,omitempty"`
	PayloadVersion uint32            `json:"payload_version"`
	Payload        json.RawMessage   `json:"payload"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

type TrajectoryPlugin struct {
	Name      string   `json:"name"`
	Version   string   `json:"version"`
	Registers []string `json:"registers,omitempty"`
}

type TrajectoryEnvironment struct {
	SDKAPIVersion     int                `json:"sdk_api_version"`
	RuntimeVersion    string             `json:"runtime_version,omitempty"`
	CreatedGeneration uint64             `json:"created_generation,omitempty"`
	RequestedProvider string             `json:"requested_provider,omitempty"`
	SystemDigest      string             `json:"system_digest,omitempty"`
	CompositionDigest string             `json:"composition_digest,omitempty"`
	Plugins           []TrajectoryPlugin `json:"plugins,omitempty"`
	Providers         []ProviderSpec     `json:"providers,omitempty"`
	Tools             []ToolSpec         `json:"tools,omitempty"`
	Hooks             []HookSpec         `json:"hooks,omitempty"`
	Subscribers       []SubscriberSpec   `json:"subscribers,omitempty"`
	Capabilities      []CapabilitySpec   `json:"capabilities,omitempty"`
	Events            []EventContract    `json:"events,omitempty"`
}

type Trajectory struct {
	SchemaVersion uint32                `json:"schema_version"`
	ID            string                `json:"id"`
	ParentID      string                `json:"parent_id,omitempty"`
	ParentEntryID string                `json:"parent_entry_id,omitempty"`
	CreatedAt     time.Time             `json:"created_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
	Head          string                `json:"head,omitempty"`
	Environment   TrajectoryEnvironment `json:"environment,omitempty"`
	Entries       []TrajectoryEntry     `json:"entries"`
}

type TrajectorySummary struct {
	SchemaVersion uint32    `json:"schema_version"`
	ID            string    `json:"id"`
	ParentID      string    `json:"parent_id,omitempty"`
	ParentEntryID string    `json:"parent_entry_id,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Head          string    `json:"head,omitempty"`
	EntryCount    int       `json:"entry_count"`
}

type TrajectoryPage struct {
	Items []TrajectorySummary `json:"items"`
	Next  string              `json:"next,omitempty"`
}

// TrajectoryStore is the only trajectory dependency accepted by Runtime.
// Files, databases, object stores, and network services are implementations of
// this port rather than execution-time special cases.
type TrajectoryStore interface {
	Create(context.Context, Trajectory) error
	Append(
		context.Context,
		string,
		string,
		...TrajectoryEntry,
	) (string, error)
	Load(context.Context, string) (Trajectory, error)
	List(context.Context) ([]TrajectorySummary, error)
	ListPage(context.Context, PageRequest) (TrajectoryPage, error)
	Delete(context.Context, string) error
}

func (trajectory Trajectory) Branch(head string) ([]TrajectoryEntry, error) {
	if head == "" {
		return nil, nil
	}
	index := make(map[string]TrajectoryEntry, len(trajectory.Entries))
	for _, entry := range trajectory.Entries {
		index[entry.ID] = entry
	}
	result := make([]TrajectoryEntry, 0, len(trajectory.Entries))
	seen := make(map[string]struct{})
	for cursor := head; cursor != ""; {
		if _, cycle := seen[cursor]; cycle {
			return nil, fmt.Errorf(
				"trajectory %q contains a cycle at %q",
				trajectory.ID,
				cursor,
			)
		}
		seen[cursor] = struct{}{}
		entry, exists := index[cursor]
		if !exists {
			return nil, fmt.Errorf(
				"trajectory %q branch references unknown entry %q",
				trajectory.ID,
				cursor,
			)
		}
		entry.Payload = append(json.RawMessage(nil), entry.Payload...)
		entry.Attributes = maps.Clone(entry.Attributes)
		result = append(result, entry)
		cursor = entry.ParentID
	}
	slices.Reverse(result)
	return result, nil
}
