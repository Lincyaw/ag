package runtime

import (
	"context"
	"fmt"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

// Runtime is intentionally a separate package from the SDK contracts and state
// ports. These aliases keep the engine implementation readable while preserving
// a one-way dependency: runtime -> sdk.
type (
	Role             = sdk.Role
	ToolCall         = sdk.ToolCall
	Message          = sdk.Message
	Usage            = sdk.Usage
	ModelRequest     = sdk.ModelRequest
	ModelResponse    = sdk.ModelResponse
	ProviderSpec     = sdk.ProviderSpec
	Provider         = sdk.Provider
	SyncProvider     = sdk.SyncProvider
	AsyncProvider    = sdk.AsyncProvider
	ToolSpec         = sdk.ToolSpec
	ToolResult       = sdk.ToolResult
	Tool             = sdk.Tool
	SyncTool         = sdk.SyncTool
	AsyncTool        = sdk.AsyncTool
	CapabilitySpec   = sdk.CapabilitySpec
	Capability       = sdk.Capability
	SyncCapability   = sdk.SyncCapability
	AsyncCapability  = sdk.AsyncCapability
	Priority         = sdk.Priority
	FailurePolicy    = sdk.FailurePolicy
	HookSpec         = sdk.HookSpec
	Hook             = sdk.Hook
	SubscriberSpec   = sdk.SubscriberSpec
	Subscriber       = sdk.Subscriber
	SubscriberFunc   = sdk.SubscriberFunc
	HookFunc         = sdk.HookFunc
	EventContract    = sdk.EventContract
	Event            = sdk.Event
	Block            = sdk.Block
	Cause            = sdk.Cause
	ActionKind       = sdk.ActionKind
	Action           = sdk.Action
	Effect           = sdk.Effect
	Manifest         = sdk.Manifest
	Registrar        = sdk.Registrar
	Plugin           = sdk.Plugin
	PluginFunc       = sdk.PluginFunc
	Connection       = sdk.Connection
	Source           = sdk.Source
	PluginRegistry   = sdk.PluginRegistry
	OperationState   = sdk.OperationState
	OperationRequest = sdk.OperationRequest
	Operation        = sdk.Operation
	OperationKind    = sdk.OperationKind
	OperationRecord  = sdk.OperationRecord
	OperationLease   = sdk.OperationLease
	OperationStore   = sdk.OperationStore
	DeliveryState    = sdk.DeliveryState
	Delivery         = sdk.Delivery
	DeliveryStore    = sdk.DeliveryStore

	TrajectoryEntry       = sdk.TrajectoryEntry
	TrajectoryPlugin      = sdk.TrajectoryPlugin
	TrajectoryEnvironment = sdk.TrajectoryEnvironment
	Trajectory            = sdk.Trajectory
	TrajectorySummary     = sdk.TrajectorySummary
	TrajectoryStore       = sdk.TrajectoryStore

	StateBackend        = sdk.StateBackend
	StorageCapabilities = sdk.StorageCapabilities
	RetentionPolicy     = sdk.RetentionPolicy
	PruneResult         = sdk.PruneResult

	BeforeAgentStartPayload = sdk.BeforeAgentStartPayload
	AgentStartPayload       = sdk.AgentStartPayload
	TurnStartPayload        = sdk.TurnStartPayload
	BeforeProviderPayload   = sdk.BeforeProviderPayload
	AfterProviderPayload    = sdk.AfterProviderPayload
	BeforeToolPayload       = sdk.BeforeToolPayload
	ToolErrorPayload        = sdk.ToolErrorPayload
	AfterToolPayload        = sdk.AfterToolPayload
	DecidePayload           = sdk.DecidePayload
	TurnEndPayload          = sdk.TurnEndPayload
	AgentEndPayload         = sdk.AgentEndPayload
	PluginLifecyclePayload  = sdk.PluginLifecyclePayload
	TrajectoryEventPayload  = sdk.TrajectoryEventPayload

	MemoryOperationStore  = sdkstorage.MemoryOperationStore
	MemoryDeliveryStore   = sdkstorage.MemoryDeliveryStore
	MemoryTrajectoryStore = sdkstorage.MemoryTrajectoryStore
	FileOperationStore    = sdkstorage.FileOperationStore
)

const (
	APIVersion = sdk.APIVersion

	RoleSystem    = sdk.RoleSystem
	RoleUser      = sdk.RoleUser
	RoleAssistant = sdk.RoleAssistant
	RoleTool      = sdk.RoleTool

	PriorityPre    = sdk.PriorityPre
	PriorityNormal = sdk.PriorityNormal
	PriorityPost   = sdk.PriorityPost

	FailurePolicyFailClosed = sdk.FailurePolicyFailClosed
	FailurePolicyContinue   = sdk.FailurePolicyContinue

	ActionStep   = sdk.ActionStep
	ActionStop   = sdk.ActionStop
	ActionInject = sdk.ActionInject

	OperationPending   = sdk.OperationPending
	OperationRunning   = sdk.OperationRunning
	OperationSucceeded = sdk.OperationSucceeded
	OperationFailed    = sdk.OperationFailed
	OperationCancelled = sdk.OperationCancelled

	OperationKindProvider   = sdk.OperationKindProvider
	OperationKindTool       = sdk.OperationKindTool
	OperationKindCapability = sdk.OperationKindCapability
	OperationKindRun        = sdk.OperationKindRun

	DeliveryPending    = sdk.DeliveryPending
	DeliveryLeased     = sdk.DeliveryLeased
	DeliveryDelivered  = sdk.DeliveryDelivered
	DeliveryDeadLetter = sdk.DeliveryDeadLetter

	HostOutboxQueue  = sdk.HostOutboxQueue
	PluginInboxQueue = sdk.PluginInboxQueue

	TrajectorySchemaVersion  = sdk.TrajectorySchemaVersion
	TrajectoryPayloadVersion = sdk.TrajectoryPayloadVersion

	TrajectoryKindUserMessage      = sdk.TrajectoryKindUserMessage
	TrajectoryKindAgentStart       = sdk.TrajectoryKindAgentStart
	TrajectoryKindProviderRequest  = sdk.TrajectoryKindProviderRequest
	TrajectoryKindProviderResponse = sdk.TrajectoryKindProviderResponse
	TrajectoryKindToolCall         = sdk.TrajectoryKindToolCall
	TrajectoryKindToolResult       = sdk.TrajectoryKindToolResult
	TrajectoryKindDecision         = sdk.TrajectoryKindDecision
	TrajectoryKindCheckpoint       = sdk.TrajectoryKindCheckpoint
	TrajectoryKindTerminal         = sdk.TrajectoryKindTerminal
	TrajectoryKindRestore          = sdk.TrajectoryKindRestore
	TrajectoryKindRollback         = sdk.TrajectoryKindRollback

	EventBeforeAgentStart   = sdk.EventBeforeAgentStart
	EventAgentStart         = sdk.EventAgentStart
	EventTurnStart          = sdk.EventTurnStart
	EventBeforeProvider     = sdk.EventBeforeProvider
	EventAfterProvider      = sdk.EventAfterProvider
	EventBeforeTool         = sdk.EventBeforeTool
	EventToolError          = sdk.EventToolError
	EventAfterTool          = sdk.EventAfterTool
	EventDecide             = sdk.EventDecide
	EventTurnEnd            = sdk.EventTurnEnd
	EventAgentEnd           = sdk.EventAgentEnd
	EventPluginMounted      = sdk.EventPluginMounted
	EventPluginUnmounted    = sdk.EventPluginUnmounted
	EventTrajectoryAppend   = sdk.EventTrajectoryAppend
	EventTrajectoryRestore  = sdk.EventTrajectoryRestore
	EventTrajectoryRollback = sdk.EventTrajectoryRollback
)

var (
	ErrNoDelivery        = sdk.ErrNoDelivery
	ErrDeliveryLease     = sdk.ErrDeliveryLease
	ErrOperationClaimed  = sdk.ErrOperationClaimed
	ErrOperationConflict = sdk.ErrOperationConflict
	ErrOperationFence    = sdk.ErrOperationFence

	NewMemoryOperationStore  = sdkstorage.NewMemoryOperationStore
	NewMemoryDeliveryStore   = sdkstorage.NewMemoryDeliveryStore
	NewMemoryTrajectoryStore = sdkstorage.NewMemoryTrajectoryStore
	NewMemoryStateBackend    = sdkstorage.NewMemoryStateBackend
	NewFileOperationStore    = sdkstorage.NewFileOperationStore

	BuiltinEventContracts  = sdk.BuiltinEventContracts
	ProviderResource       = sdk.ProviderResource
	ToolResource           = sdk.ToolResource
	HookResource           = sdk.HookResource
	SubscriberResource     = sdk.SubscriberResource
	CapabilityResource     = sdk.CapabilityResource
	EventResource          = sdk.EventResource
	PluginResource         = sdk.PluginResource
	ResourceRevision       = sdk.ResourceRevision
	PluginResourceRevision = sdk.PluginResourceRevision
	Local                  = sdk.Local
)

func TypedHook[T any](
	spec HookSpec,
	handler func(context.Context, T) (Effect, error),
) Hook {
	return sdk.TypedHook(spec, handler)
}

func validateResourceName(kind, name string) error {
	return sdk.ValidateResourceName(kind, name)
}

func normalizeResources(resources []string) []string {
	return sdk.NormalizeResources(resources)
}

func validateOperation(operation Operation) error {
	return sdk.ValidateOperation(operation)
}

func cloneEvent(event Event) Event {
	return sdk.CloneEvent(event)
}

func cloneDelivery(delivery Delivery) Delivery {
	return sdk.CloneDelivery(delivery)
}

func sourceDescription(source Source) string {
	return sdk.SourceDescription(source)
}

type composedStateBackend struct {
	trajectories TrajectoryStore
	operations   OperationStore
	outbox       DeliveryStore
}

func (backend composedStateBackend) Trajectories() TrajectoryStore {
	return backend.trajectories
}

func (backend composedStateBackend) Operations() OperationStore {
	return backend.operations
}

func (backend composedStateBackend) Deliveries(name string) (DeliveryStore, error) {
	if name == HostOutboxQueue && backend.outbox != nil {
		return backend.outbox, nil
	}
	return nil, fmt.Errorf("composed state backend has no delivery queue %q", name)
}

func (composedStateBackend) Capabilities() StorageCapabilities {
	return StorageCapabilities{}
}

func (composedStateBackend) Namespace() string { return "legacy" }

func (composedStateBackend) Prune(
	context.Context,
	RetentionPolicy,
) (PruneResult, error) {
	return PruneResult{}, fmt.Errorf(
		"legacy composed state backend does not support maintenance",
	)
}

func (composedStateBackend) Health(ctx context.Context) error { return ctx.Err() }
func (composedStateBackend) Close(context.Context) error      { return nil }
func (composedStateBackend) String() string                   { return "composed://legacy" }
