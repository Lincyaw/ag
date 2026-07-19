package sdk

import (
	"context"
	"net/url"
	"time"
)

const (
	HostOutboxQueue  = "host-outbox"
	PluginInboxQueue = "plugin-inbox"
)

// StorageCapabilities make correctness properties explicit instead of asking
// callers to infer them from a backend's name.
type StorageCapabilities struct {
	Durable            bool `json:"durable"`
	MultiProcessSafe   bool `json:"multi_process_safe"`
	AtomicState        bool `json:"atomic_state"`
	Pagination         bool `json:"pagination"`
	Maintenance        bool `json:"maintenance"`
	OperationFencing   bool `json:"operation_fencing"`
	NamedQueues        bool `json:"named_queues"`
	NamespaceIsolation bool `json:"namespace_isolation"`
	EncryptedAtRest    bool `json:"encrypted_at_rest"`
}

type RetentionPolicy struct {
	OperationsBefore   time.Time `json:"operations_before,omitempty"`
	DeliveriesBefore   time.Time `json:"deliveries_before,omitempty"`
	TrajectoriesBefore time.Time `json:"trajectories_before,omitempty"`
}

type PruneResult struct {
	Operations   int `json:"operations"`
	Deliveries   int `json:"deliveries"`
	Trajectories int `json:"trajectories"`
}

// StateMutationDeliveries appends deliveries to one named outbox queue as part
// of the same durable state mutation as trajectory or execution updates.
type StateMutationDeliveries struct {
	Queue      string     `json:"queue"`
	Deliveries []Delivery `json:"deliveries"`
}

func CloneStateMutationDeliveries(
	group StateMutationDeliveries,
) StateMutationDeliveries {
	group.Deliveries = CloneDeliveries(group.Deliveries)
	return group
}

func CloneStateMutationOutbox(
	outbox []StateMutationDeliveries,
) []StateMutationDeliveries {
	if len(outbox) == 0 {
		return nil
	}
	result := make([]StateMutationDeliveries, len(outbox))
	for index, group := range outbox {
		result[index] = CloneStateMutationDeliveries(group)
	}
	return result
}

// ExecutionStepDeliveries is kept as a source-compatible alias for older
// callers. New atomic state APIs should use StateMutationDeliveries.
type ExecutionStepDeliveries = StateMutationDeliveries

func CloneExecutionStepDeliveries(
	group ExecutionStepDeliveries,
) ExecutionStepDeliveries {
	return CloneStateMutationDeliveries(group)
}

func CloneExecutionStepOutbox(
	outbox []ExecutionStepDeliveries,
) []ExecutionStepDeliveries {
	return CloneStateMutationOutbox(outbox)
}

// TrajectoryAppendCommit is the durable boundary for appending trajectory
// entries outside a claimed execution while preserving subscriber outbox
// projection with the same state transition.
type TrajectoryAppendCommit struct {
	TrajectoryID string                    `json:"trajectory_id"`
	ExpectedHead string                    `json:"expected_head"`
	Entries      []TrajectoryEntry         `json:"entries"`
	Outbox       []StateMutationDeliveries `json:"outbox,omitempty"`
}

type TrajectoryAppendResult struct {
	Trajectory TrajectoryMetadata `json:"trajectory"`
}

// ExecutionStartCommit is the durable boundary that accepts one user input and
// opens a trajectory execution. Implementations must commit the input,
// execution record, and delivery changes together or not at all.
type ExecutionStartCommit struct {
	TrajectoryID string                    `json:"trajectory_id"`
	ExpectedHead string                    `json:"expected_head,omitempty"`
	Start        TrajectoryExecutionStart  `json:"start"`
	Input        TrajectoryEntry           `json:"input"`
	Outbox       []StateMutationDeliveries `json:"outbox,omitempty"`
}

// ExecutionMutationCommit is the durable boundary for completing one claimed
// execution mutation. Implementations must commit the requested trajectory and
// outbox changes together or none of them.
type ExecutionMutationCommit struct {
	Trajectory TrajectoryExecutionCommit `json:"trajectory"`
	Outbox     []StateMutationDeliveries `json:"outbox,omitempty"`
}

type ExecutionMutationResult struct {
	Trajectory TrajectoryMetadata `json:"trajectory"`
}

// ExecutionStepCommit is kept as a source-compatible alias for older callers.
// New atomic state APIs should use ExecutionMutationCommit.
type ExecutionStepCommit = ExecutionMutationCommit

// ExecutionStepResult is kept as a source-compatible alias for older callers.
// New atomic state APIs should use ExecutionMutationResult.
type ExecutionStepResult = ExecutionMutationResult

// ExecutionCancelCommit is the durable boundary for externally cancelling one
// active execution. Implementations must commit the cancellation state,
// optional terminal trajectory entries, and delivery changes together or not at
// all.
type ExecutionCancelCommit struct {
	TrajectoryID string                    `json:"trajectory_id"`
	ExecutionID  string                    `json:"execution_id"`
	ExpectedHead string                    `json:"expected_head"`
	Reason       string                    `json:"reason,omitempty"`
	At           time.Time                 `json:"at,omitempty"`
	Entries      []TrajectoryEntry         `json:"entries,omitempty"`
	Outbox       []StateMutationDeliveries `json:"outbox,omitempty"`
}

func (commit ExecutionCancelCommit) TrajectoryCommit() TrajectoryExecutionCancelCommit {
	return TrajectoryExecutionCancelCommit{
		TrajectoryID: commit.TrajectoryID,
		ExecutionID:  commit.ExecutionID,
		ExpectedHead: commit.ExpectedHead,
		Reason:       commit.Reason,
		At:           commit.At,
		Entries:      CloneTrajectoryEntries(commit.Entries),
	}
}

type ExecutionCancelResult struct {
	Trajectory TrajectoryMetadata `json:"trajectory"`
	Changed    bool               `json:"changed"`
}

// StateBackend is a durability port resolved during application bootstrap,
// not an execution plugin. It must be
// available before plugin composition so recovery and durable delivery have a
// source of truth.
type StateBackend interface {
	Trajectories() TrajectoryStore
	Operations() OperationStore
	ContextInjections() ContextInjectionStore
	Deliveries(string) (DeliveryStore, error)
	Capabilities() StorageCapabilities
	Namespace() string
	Prune(context.Context, RetentionPolicy) (PruneResult, error)
	Health(context.Context) error
	Close(context.Context) error
	String() string
}

// AtomicStateBackend is an optional stronger StateBackend contract. Runtime
// uses it when AtomicState is advertised; callers of Runtime remain neutral to
// the concrete database. Backends must keep this interface and their
// Capabilities().AtomicState flag consistent.
type AtomicStateBackend interface {
	StateBackend
	AppendTrajectory(
		context.Context,
		TrajectoryAppendCommit,
	) (TrajectoryAppendResult, error)
	StartExecution(
		context.Context,
		ExecutionStartCommit,
	) (ExecutionMutationResult, error)
	CommitExecution(
		context.Context,
		ExecutionMutationCommit,
	) (ExecutionMutationResult, error)
	CancelExecution(
		context.Context,
		ExecutionCancelCommit,
	) (ExecutionCancelResult, error)
}

// StorageDriver resolves one URI scheme to a StateBackend. Applications can
// register database, network, S3, or other drivers without changing Runtime.
type StorageDriver interface {
	Scheme() string
	Open(context.Context, *url.URL) (StateBackend, error)
}
