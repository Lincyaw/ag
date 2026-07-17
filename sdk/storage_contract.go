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

// StateBackend is a bootstrap port, not an execution plugin. It must be
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

// StorageDriver resolves one URI scheme to a StateBackend. Applications can
// register database, network, S3, or other drivers without changing Runtime.
type StorageDriver interface {
	Scheme() string
	Open(context.Context, *url.URL) (StateBackend, error)
}
