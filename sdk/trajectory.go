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
	ErrTrajectoryNotFound      = errors.New("trajectory not found")
	ErrTrajectoryEntryNotFound = errors.New("trajectory entry not found")
	ErrTrajectoryExists        = errors.New("trajectory already exists")
	ErrTrajectoryConflict      = errors.New("trajectory head conflict")
	ErrTrajectoryReferenced    = errors.New("trajectory is referenced")
	ErrTrajectoryVersion       = errors.New("unsupported trajectory schema version")
	ErrTrajectoryExecution     = errors.New("trajectory execution conflict")
	ErrTrajectoryClaimed       = errors.New("trajectory execution is claimed")
	ErrTrajectoryFence         = errors.New("trajectory execution lease is no longer valid")
)

const (
	TrajectorySchemaVersion  uint32 = 2
	TrajectoryPayloadVersion uint32 = 1
)

type TrajectoryKind string

const (
	TrajectoryKindUserMessage      TrajectoryKind = "user_message"
	TrajectoryKindAgentStart       TrajectoryKind = "agent_start"
	TrajectoryKindProviderRequest  TrajectoryKind = "provider_request"
	TrajectoryKindProviderResponse TrajectoryKind = "provider_response"
	TrajectoryKindToolCall         TrajectoryKind = "tool_call"
	TrajectoryKindToolResult       TrajectoryKind = "tool_result"
	TrajectoryKindDecision         TrajectoryKind = "decision"
	TrajectoryKindCheckpoint       TrajectoryKind = "checkpoint"
	TrajectoryKindTerminal         TrajectoryKind = "terminal"
	TrajectoryKindRestore          TrajectoryKind = "restore"
	TrajectoryKindRollback         TrajectoryKind = "rollback"
)

type TrajectoryEntryFields struct {
	Turn          *int       `json:"turn,omitempty"`
	ExecutionID   string     `json:"execution_id,omitempty"`
	OperationKey  string     `json:"operation_key,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Provider      string     `json:"provider,omitempty"`
	Model         string     `json:"model,omitempty"`
	ToolName      string     `json:"tool_name,omitempty"`
	ToolCallID    string     `json:"tool_call_id,omitempty"`
	FinishReason  string     `json:"finish_reason,omitempty"`
	InputTokens   int64      `json:"input_tokens,omitempty"`
	OutputTokens  int64      `json:"output_tokens,omitempty"`
	IsError       *bool      `json:"is_error,omitempty"`
	CauseCode     string     `json:"cause_code,omitempty"`
	ActionKind    ActionKind `json:"action_kind,omitempty"`
}

type TrajectoryEntry struct {
	ID             string                `json:"id"`
	TrajectoryID   string                `json:"trajectory_id"`
	ParentID       string                `json:"parent_id,omitempty"`
	Ordinal        uint64                `json:"ordinal"`
	Depth          uint64                `json:"depth"`
	Kind           TrajectoryKind        `json:"kind"`
	Timestamp      time.Time             `json:"timestamp"`
	Generation     uint64                `json:"generation,omitempty"`
	Fields         TrajectoryEntryFields `json:"fields"`
	PayloadVersion uint32                `json:"payload_version"`
	Payload        json.RawMessage       `json:"payload"`
	Attributes     map[string]string     `json:"attributes,omitempty"`
}

type TrajectoryExecutionState string

const (
	TrajectoryExecutionPending   TrajectoryExecutionState = "pending"
	TrajectoryExecutionRunning   TrajectoryExecutionState = "running"
	TrajectoryExecutionSucceeded TrajectoryExecutionState = "succeeded"
	TrajectoryExecutionFailed    TrajectoryExecutionState = "failed"
	TrajectoryExecutionCancelled TrajectoryExecutionState = "cancelled"
)

type TrajectoryExecution struct {
	ID             string                   `json:"id"`
	State          TrajectoryExecutionState `json:"state"`
	Revision       uint64                   `json:"revision"`
	BaseHead       string                   `json:"base_head,omitempty"`
	InputEntryID   string                   `json:"input_entry_id"`
	Provider       string                   `json:"provider,omitempty"`
	System         string                   `json:"system,omitempty"`
	MaxTurns       int                      `json:"max_turns"`
	Owner          string                   `json:"owner,omitempty"`
	LeaseToken     string                   `json:"lease_token,omitempty"`
	LeaseExpiresAt time.Time                `json:"lease_expires_at,omitempty"`
	LastError      string                   `json:"last_error,omitempty"`
	CreatedAt      time.Time                `json:"created_at"`
	UpdatedAt      time.Time                `json:"updated_at"`
}

func (execution TrajectoryExecution) Terminal() bool {
	switch execution.State {
	case TrajectoryExecutionSucceeded,
		TrajectoryExecutionFailed,
		TrajectoryExecutionCancelled:
		return true
	default:
		return false
	}
}

type TrajectoryExecutionStart struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
	System   string `json:"system,omitempty"`
	MaxTurns int    `json:"max_turns"`
}

type TrajectoryExecutionCommit struct {
	TrajectoryID string                   `json:"trajectory_id"`
	ExecutionID  string                   `json:"execution_id"`
	LeaseToken   string                   `json:"lease_token"`
	ExpectedHead string                   `json:"expected_head,omitempty"`
	Entries      []TrajectoryEntry        `json:"entries,omitempty"`
	State        TrajectoryExecutionState `json:"state,omitempty"`
	Error        string                   `json:"error,omitempty"`
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
	Checkpoint    string                `json:"checkpoint,omitempty"`
	Execution     *TrajectoryExecution  `json:"execution,omitempty"`
	Environment   TrajectoryEnvironment `json:"environment,omitempty"`
	Entries       []TrajectoryEntry     `json:"entries"`
}

type TrajectoryMetadata struct {
	SchemaVersion   uint32                `json:"schema_version"`
	ID              string                `json:"id"`
	ParentID        string                `json:"parent_id,omitempty"`
	ParentEntryID   string                `json:"parent_entry_id,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	Head            string                `json:"head,omitempty"`
	Checkpoint      string                `json:"checkpoint,omitempty"`
	Execution       *TrajectoryExecution  `json:"execution,omitempty"`
	Environment     TrajectoryEnvironment `json:"environment,omitempty"`
	EntryCount      int                   `json:"entry_count"`
	OwnedEntryCount int                   `json:"owned_entry_count"`
}

type TrajectorySummary struct {
	SchemaVersion   uint32                   `json:"schema_version"`
	ID              string                   `json:"id"`
	ParentID        string                   `json:"parent_id,omitempty"`
	ParentEntryID   string                   `json:"parent_entry_id,omitempty"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
	Head            string                   `json:"head,omitempty"`
	Checkpoint      string                   `json:"checkpoint,omitempty"`
	ExecutionID     string                   `json:"execution_id,omitempty"`
	ExecutionState  TrajectoryExecutionState `json:"execution_state,omitempty"`
	EntryCount      int                      `json:"entry_count"`
	OwnedEntryCount int                      `json:"owned_entry_count"`
}

type TrajectoryPage struct {
	Items []TrajectorySummary `json:"items"`
	Next  string              `json:"next,omitempty"`
}

type TrajectoryEntryQuery struct {
	TrajectoryID  string         `json:"trajectory_id,omitempty"`
	ExecutionID   string         `json:"execution_id,omitempty"`
	OperationKey  string         `json:"operation_key,omitempty"`
	Kind          TrajectoryKind `json:"kind,omitempty"`
	Provider      string         `json:"provider,omitempty"`
	ToolName      string         `json:"tool_name,omitempty"`
	CorrelationID string         `json:"correlation_id,omitempty"`
	Limit         int            `json:"limit,omitempty"`
}

type TrajectoryAnalyzer interface {
	AnalyzeEntries(
		context.Context,
		TrajectoryEntryQuery,
	) ([]TrajectoryEntry, error)
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
	BeginExecution(
		context.Context,
		string,
		string,
		TrajectoryExecutionStart,
		TrajectoryEntry,
	) (TrajectoryMetadata, error)
	ClaimExecution(
		context.Context,
		string,
		string,
		time.Time,
		time.Duration,
	) (TrajectoryExecution, error)
	RenewExecution(
		context.Context,
		string,
		string,
		string,
		time.Time,
		time.Duration,
	) (TrajectoryExecution, error)
	CommitExecution(
		context.Context,
		TrajectoryExecutionCommit,
	) (TrajectoryMetadata, error)
	CancelExecution(
		context.Context,
		string,
		string,
		string,
		time.Time,
	) (TrajectoryMetadata, error)
	ListRecoverable(
		context.Context,
		time.Time,
	) ([]TrajectoryMetadata, error)
	LoadMetadata(context.Context, string) (TrajectoryMetadata, error)
	LoadEntry(context.Context, string, string) (TrajectoryEntry, error)
	LoadBranch(context.Context, string, string) ([]TrajectoryEntry, error)
	FindLatest(
		context.Context,
		string,
		string,
		TrajectoryKind,
	) (TrajectoryEntry, bool, error)
	// Load materializes inherited entries and locally owned entries into a
	// compatibility view. Runtime recovery should use the targeted read methods.
	Load(context.Context, string) (Trajectory, error)
	List(context.Context) ([]TrajectorySummary, error)
	ListPage(context.Context, PageRequest) (TrajectoryPage, error)
	Delete(context.Context, string) error
}

func (kind TrajectoryKind) Valid() bool {
	switch kind {
	case TrajectoryKindUserMessage,
		TrajectoryKindAgentStart,
		TrajectoryKindProviderRequest,
		TrajectoryKindProviderResponse,
		TrajectoryKindToolCall,
		TrajectoryKindToolResult,
		TrajectoryKindDecision,
		TrajectoryKindCheckpoint,
		TrajectoryKindTerminal,
		TrajectoryKindRestore,
		TrajectoryKindRollback:
		return true
	default:
		return false
	}
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
