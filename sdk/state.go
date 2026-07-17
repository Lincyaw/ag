package sdk

import (
	"context"
	"encoding/json"
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

// ExecutionStepOperation completes one previously claimed external operation
// as part of the same durable commit as its trajectory and delivery changes.
type ExecutionStepOperation struct {
	ID         string          `json:"id"`
	LeaseToken string          `json:"lease_token"`
	State      OperationState  `json:"state"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ExecutionStepDeliveryAck acknowledges a leased inbox delivery in an atomic
// execution-step commit.
type ExecutionStepDeliveryAck struct {
	Queue      string    `json:"queue"`
	ID         string    `json:"id"`
	LeaseToken string    `json:"lease_token"`
	At         time.Time `json:"at,omitempty"`
}

// ExecutionStepDeliveries appends deliveries to one named outbox queue.
type ExecutionStepDeliveries struct {
	Queue      string     `json:"queue"`
	Deliveries []Delivery `json:"deliveries"`
}

// ExecutionStepCommit is the durable boundary after an external LLM or tool
// call. Implementations must commit every requested mutation or none of them.
type ExecutionStepCommit struct {
	Trajectory TrajectoryExecutionCommit `json:"trajectory"`
	Operation  *ExecutionStepOperation   `json:"operation,omitempty"`
	InboxAck   *ExecutionStepDeliveryAck `json:"inbox_ack,omitempty"`
	Outbox     []ExecutionStepDeliveries `json:"outbox,omitempty"`
}

type ExecutionStepResult struct {
	Trajectory TrajectoryMetadata `json:"trajectory"`
	Operation  *OperationRecord   `json:"operation,omitempty"`
}

// StateBackend is a durability port resolved during application bootstrap,
// not an execution plugin. It must be
// available before plugin composition so recovery and durable delivery have a
// source of truth.
type StateBackend interface {
	Trajectories() TrajectoryStore
	Operations() OperationStore
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
// the concrete database.
type AtomicStateBackend interface {
	StateBackend
	CommitExecutionStep(
		context.Context,
		ExecutionStepCommit,
	) (ExecutionStepResult, error)
}

// StorageDriver resolves one URI scheme to a StateBackend. Applications can
// register database, network, S3, or other drivers without changing Runtime.
type StorageDriver interface {
	Scheme() string
	Open(context.Context, *url.URL) (StateBackend, error)
}
