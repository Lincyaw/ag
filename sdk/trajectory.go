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
	TrajectorySchemaVersion  uint32 = 3
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

type HookAuditOutcome string

const (
	HookAuditNoEffect HookAuditOutcome = "no_effect"
	HookAuditPatched  HookAuditOutcome = "patched"
	HookAuditBlocked  HookAuditOutcome = "blocked"
	HookAuditAction   HookAuditOutcome = "action"
	HookAuditError    HookAuditOutcome = "error"
	HookAuditSkipped  HookAuditOutcome = "skipped"
)

type EffectResolutionOutcome string

const (
	EffectResolutionNoEffect EffectResolutionOutcome = "no_effect"
	EffectResolutionPatched  EffectResolutionOutcome = "patched"
	EffectResolutionBlocked  EffectResolutionOutcome = "blocked"
	EffectResolutionAction   EffectResolutionOutcome = "action"
	EffectResolutionError    EffectResolutionOutcome = "error"
)

type BlockSummary struct {
	Reason string `json:"reason,omitempty"`
	Kind   string `json:"kind,omitempty"`
}

type ActionSummary struct {
	Kind         ActionKind `json:"kind,omitempty"`
	CauseCode    string     `json:"cause_code,omitempty"`
	CauseFinal   bool       `json:"cause_final,omitempty"`
	MessageCount int        `json:"message_count,omitempty"`
}

type EffectResolution struct {
	Outcome     EffectResolutionOutcome `json:"outcome,omitempty"`
	Block       *BlockSummary           `json:"block,omitempty"`
	BlockStep   *int                    `json:"block_step,omitempty"`
	Action      *ActionSummary          `json:"action,omitempty"`
	ActionSteps []int                   `json:"action_steps,omitempty"`
	ActionRule  string                  `json:"action_rule,omitempty"`
	PatchFields []string                `json:"patch_fields,omitempty"`
	Error       string                  `json:"error,omitempty"`
}

type HookAuditStep struct {
	Index         int               `json:"index"`
	Plugin        string            `json:"plugin,omitempty"`
	PluginVersion string            `json:"plugin_version,omitempty"`
	Hook          string            `json:"hook,omitempty"`
	Priority      Priority          `json:"priority,omitempty"`
	Sequence      uint64            `json:"sequence,omitempty"`
	FailurePolicy FailurePolicy     `json:"failure_policy,omitempty"`
	Duration      time.Duration     `json:"duration,omitempty"`
	InputHash     string            `json:"input_hash,omitempty"`
	OutputHash    string            `json:"output_hash,omitempty"`
	PatchFields   []string          `json:"patch_fields,omitempty"`
	Overwrites    []string          `json:"overwrites,omitempty"`
	Block         *BlockSummary     `json:"block,omitempty"`
	Action        *ActionSummary    `json:"action,omitempty"`
	Error         string            `json:"error,omitempty"`
	Outcome       HookAuditOutcome  `json:"outcome,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

type EventAudit struct {
	EventID    string           `json:"event_id,omitempty"`
	EventName  string           `json:"event_name,omitempty"`
	Generation uint64           `json:"generation,omitempty"`
	InputHash  string           `json:"input_hash,omitempty"`
	OutputHash string           `json:"output_hash,omitempty"`
	Steps      []HookAuditStep  `json:"steps,omitempty"`
	Resolution EffectResolution `json:"resolution,omitempty"`
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
	Audit          []EventAudit          `json:"audit,omitempty"`
	PayloadVersion uint32                `json:"payload_version"`
	Payload        json.RawMessage       `json:"payload"`
	Attributes     map[string]string     `json:"attributes,omitempty"`
}

// TrajectoryEntryInspection is the payload-free representation used by
// control-plane listing and diagnostics. PayloadBytes describes the durable
// payload without requiring a store to read or transport it.
type TrajectoryEntryInspection struct {
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
	PayloadBytes   int                   `json:"payload_bytes"`
	AuditCount     int                   `json:"audit_count,omitempty"`
	AttributeCount int                   `json:"attribute_count,omitempty"`
}

func CloneTrajectoryEntry(entry TrajectoryEntry) TrajectoryEntry {
	if entry.Fields.Turn != nil {
		turn := *entry.Fields.Turn
		entry.Fields.Turn = &turn
	}
	if entry.Fields.IsError != nil {
		isError := *entry.Fields.IsError
		entry.Fields.IsError = &isError
	}
	entry.Payload = append(json.RawMessage(nil), entry.Payload...)
	entry.Audit = CloneEventAudits(entry.Audit)
	entry.Attributes = maps.Clone(entry.Attributes)
	return entry
}

func CloneTrajectoryEntries(entries []TrajectoryEntry) []TrajectoryEntry {
	if entries == nil {
		return nil
	}
	result := make([]TrajectoryEntry, len(entries))
	for index, entry := range entries {
		result[index] = CloneTrajectoryEntry(entry)
	}
	return result
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

func (execution TrajectoryExecution) RecoveryDelay(
	now time.Time,
) time.Duration {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if execution.State == TrajectoryExecutionRunning &&
		execution.LeaseExpiresAt.After(now) {
		return execution.LeaseExpiresAt.Sub(now)
	}
	return 0
}

func (execution TrajectoryExecution) RecoverableAt(
	now time.Time,
) bool {
	return !execution.Terminal() && execution.RecoveryDelay(now) <= 0
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

type TrajectoryExecutionCancelCommit struct {
	TrajectoryID string            `json:"trajectory_id"`
	ExecutionID  string            `json:"execution_id"`
	ExpectedHead string            `json:"expected_head,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	At           time.Time         `json:"at,omitempty"`
	Entries      []TrajectoryEntry `json:"entries,omitempty"`
}

type TrajectoryExecutionCancelResult struct {
	Trajectory TrajectoryMetadata `json:"trajectory"`
	Changed    bool               `json:"changed"`
}

type TrajectoryPlugin struct {
	Name      string   `json:"name"`
	Version   string   `json:"version"`
	Registers []string `json:"registers,omitempty"`
}

func CloneTrajectoryPlugin(plugin TrajectoryPlugin) TrajectoryPlugin {
	plugin.Registers = slices.Clone(plugin.Registers)
	return plugin
}

type TrajectoryEnvironment struct {
	SDKAPIVersion          int                `json:"sdk_api_version"`
	RuntimeVersion         string             `json:"runtime_version,omitempty"`
	CreatedGeneration      uint64             `json:"created_generation,omitempty"`
	ParentSessionID        string             `json:"parent_session_id,omitempty"`
	OriginInvocationID     string             `json:"origin_invocation_id,omitempty"`
	OriginInvocationRootID string             `json:"origin_invocation_root_id,omitempty"`
	OriginForkInvocationID string             `json:"origin_fork_invocation_id,omitempty"`
	OriginMode             AgentSessionMode   `json:"origin_mode,omitempty"`
	RequestedProvider      string             `json:"requested_provider,omitempty"`
	SystemDigest           string             `json:"system_digest,omitempty"`
	CompositionDigest      string             `json:"composition_digest,omitempty"`
	Plugins                []TrajectoryPlugin `json:"plugins,omitempty"`
	Providers              []ProviderSpec     `json:"providers,omitempty"`
	Tools                  []ToolSpec         `json:"tools,omitempty"`
	Agents                 []AgentSpec        `json:"agents,omitempty"`
	Hooks                  []HookSpec         `json:"hooks,omitempty"`
	Subscribers            []SubscriberSpec   `json:"subscribers,omitempty"`
	Capabilities           []CapabilitySpec   `json:"capabilities,omitempty"`
	Events                 []EventContract    `json:"events,omitempty"`
}

func CloneTrajectoryEnvironment(environment TrajectoryEnvironment) TrajectoryEnvironment {
	environment.Plugins = cloneTrajectoryPlugins(environment.Plugins)
	environment.Providers = slices.Clone(environment.Providers)
	environment.Tools = cloneToolSpecs(environment.Tools)
	environment.Agents = cloneAgentSpecs(environment.Agents)
	environment.Hooks = slices.Clone(environment.Hooks)
	environment.Subscribers = cloneSubscriberSpecs(environment.Subscribers)
	environment.Capabilities = cloneCapabilitySpecs(environment.Capabilities)
	environment.Events = cloneEventContracts(environment.Events)
	return environment
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

func CloneTrajectory(trajectory Trajectory) Trajectory {
	trajectory.Execution = CloneTrajectoryExecution(trajectory.Execution)
	trajectory.Environment = CloneTrajectoryEnvironment(trajectory.Environment)
	if trajectory.Entries != nil {
		entries := make([]TrajectoryEntry, len(trajectory.Entries))
		for index, entry := range trajectory.Entries {
			entries[index] = CloneTrajectoryEntry(entry)
		}
		trajectory.Entries = entries
	}
	return trajectory
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

func CloneTrajectoryExecution(execution *TrajectoryExecution) *TrajectoryExecution {
	if execution == nil {
		return nil
	}
	result := *execution
	return &result
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

// TrajectoryEntryInspector materializes branch topology and indexed entry
// fields without loading payload blobs. It is optional so external and legacy
// stores retain source compatibility; control planes should prefer it whenever
// the concrete store provides it.
type TrajectoryEntryInspector interface {
	InspectTrajectoryEntries(
		context.Context,
		string,
		string,
	) (TrajectoryMetadata, []TrajectoryEntryInspection, error)
}

// TrajectoryCreator owns aggregate creation.
type TrajectoryCreator interface {
	Create(context.Context, Trajectory) error
}

// TrajectoryAppender owns appends made outside an active execution.
type TrajectoryAppender interface {
	Append(
		context.Context,
		string,
		string,
		...TrajectoryEntry,
	) (string, error)
}

// TrajectoryExecutionStore owns execution admission, leases, completion, and
// recovery. Keeping this contract separate prevents control-plane readers from
// depending on mutation methods.
type TrajectoryExecutionStore interface {
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
	// CancelExecution durably cancels the current execution. Entries, when
	// present, are committed with the cancellation so runtime-owned terminal and
	// restore records remain part of the trajectory aggregate.
	CancelExecution(
		context.Context,
		TrajectoryExecutionCancelCommit,
	) (TrajectoryExecutionCancelResult, error)
	ListRecoverable(
		context.Context,
		time.Time,
	) ([]TrajectoryMetadata, error)
}

// TrajectoryReader is the targeted branch/entry read port used by Runtime.
type TrajectoryReader interface {
	LoadMetadata(context.Context, string) (TrajectoryMetadata, error)
	LoadEntry(context.Context, string, string) (TrajectoryEntry, error)
	LoadBranch(context.Context, string, string) ([]TrajectoryEntry, error)
	FindLatest(
		context.Context,
		string,
		string,
		TrajectoryKind,
	) (TrajectoryEntry, bool, error)
}

// TrajectoryProjectionReader exposes materialized compatibility views. Runtime
// code should prefer TrajectoryReader so it does not load unrelated payloads.
type TrajectoryProjectionReader interface {
	// LoadBranchView materializes the trajectory projection visible at head.
	// Unlike Load, it only includes entries reachable from that branch head.
	LoadBranchView(context.Context, string, string) (Trajectory, error)
	// Load materializes inherited entries and locally owned entries into a
	// compatibility view. Runtime recovery should use the targeted read methods.
	Load(context.Context, string) (Trajectory, error)
}

// TrajectoryCatalog owns listing and retention operations.
type TrajectoryCatalog interface {
	List(context.Context) ([]TrajectorySummary, error)
	ListPage(context.Context, PageRequest) (TrajectoryPage, error)
	Delete(context.Context, string) error
}

// TrajectoryStore is the complete storage-adapter contract. Consumers should
// accept the narrow interfaces above whenever they need only part of it.
type TrajectoryStore interface {
	TrajectoryCreator
	TrajectoryAppender
	TrajectoryExecutionStore
	TrajectoryReader
	TrajectoryProjectionReader
	TrajectoryCatalog
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
		result = append(result, CloneTrajectoryEntry(entry))
		cursor = entry.ParentID
	}
	slices.Reverse(result)
	return result, nil
}

func CloneEventAudits(audits []EventAudit) []EventAudit {
	if audits == nil {
		return nil
	}
	result := make([]EventAudit, len(audits))
	for index, audit := range audits {
		result[index] = CloneEventAudit(audit)
	}
	return result
}

func CloneEventAudit(audit EventAudit) EventAudit {
	steps := audit.Steps
	audit.Steps = make([]HookAuditStep, len(steps))
	for index, step := range steps {
		audit.Steps[index] = cloneHookAuditStep(step)
	}
	audit.Resolution = cloneEffectResolution(audit.Resolution)
	return audit
}

func cloneHookAuditStep(step HookAuditStep) HookAuditStep {
	step.PatchFields = slices.Clone(step.PatchFields)
	step.Overwrites = slices.Clone(step.Overwrites)
	step.Attributes = maps.Clone(step.Attributes)
	if step.Block != nil {
		block := *step.Block
		step.Block = &block
	}
	if step.Action != nil {
		action := *step.Action
		step.Action = &action
	}
	return step
}

func cloneEffectResolution(resolution EffectResolution) EffectResolution {
	resolution.PatchFields = slices.Clone(resolution.PatchFields)
	resolution.ActionSteps = slices.Clone(resolution.ActionSteps)
	if resolution.Block != nil {
		block := *resolution.Block
		resolution.Block = &block
	}
	if resolution.BlockStep != nil {
		step := *resolution.BlockStep
		resolution.BlockStep = &step
	}
	if resolution.Action != nil {
		action := *resolution.Action
		resolution.Action = &action
	}
	return resolution
}

func cloneTrajectoryPlugins(plugins []TrajectoryPlugin) []TrajectoryPlugin {
	if plugins == nil {
		return nil
	}
	result := make([]TrajectoryPlugin, len(plugins))
	for index, plugin := range plugins {
		result[index] = CloneTrajectoryPlugin(plugin)
	}
	return result
}

func cloneToolSpecs(specs []ToolSpec) []ToolSpec {
	if specs == nil {
		return nil
	}
	result := make([]ToolSpec, len(specs))
	for index, spec := range specs {
		result[index] = CloneToolSpec(spec)
	}
	return result
}

func cloneAgentSpecs(specs []AgentSpec) []AgentSpec {
	if specs == nil {
		return nil
	}
	result := make([]AgentSpec, len(specs))
	for index, spec := range specs {
		result[index] = CloneAgentSpec(spec)
	}
	return result
}

func cloneSubscriberSpecs(specs []SubscriberSpec) []SubscriberSpec {
	if specs == nil {
		return nil
	}
	result := make([]SubscriberSpec, len(specs))
	for index, spec := range specs {
		result[index] = CloneSubscriberSpec(spec)
	}
	return result
}

func cloneCapabilitySpecs(specs []CapabilitySpec) []CapabilitySpec {
	if specs == nil {
		return nil
	}
	result := make([]CapabilitySpec, len(specs))
	for index, spec := range specs {
		result[index] = CloneCapabilitySpec(spec)
	}
	return result
}

func cloneEventContracts(contracts []EventContract) []EventContract {
	if contracts == nil {
		return nil
	}
	result := make([]EventContract, len(contracts))
	for index, contract := range contracts {
		result[index] = CloneEventContract(contract)
	}
	return result
}
