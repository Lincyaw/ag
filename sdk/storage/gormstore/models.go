package gormstore

import "time"

// Trajectory is the aggregate root for one agent conversation.
type Trajectory struct {
	Namespace           string `gorm:"primaryKey"`
	ID                  string `gorm:"primaryKey"`
	SchemaVersion       uint32
	ParentID            string
	ParentEntryID       string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	Head                string
	Checkpoint          string
	EnvironmentJSON     string `gorm:"type:text"`
	InheritedEntryCount uint64
	OwnedEntryCount     uint64
}

func (Trajectory) TableName() string { return "ag_trajectories" }

// TrajectoryExecution tracks the current execution state for a trajectory.
type TrajectoryExecution struct {
	Namespace      string `gorm:"primaryKey"`
	TrajectoryID   string `gorm:"primaryKey"`
	ExecutionID    string
	State          string
	Revision       uint64
	BaseHead       string
	InputEntryID   string
	Provider       string
	SystemPrompt   string `gorm:"type:text"`
	MaxTurns       int
	Owner          string
	LeaseToken     string
	LeaseExpiresAt *time.Time
	LastError      string `gorm:"type:text"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (TrajectoryExecution) TableName() string { return "ag_trajectory_executions" }

// TrajectoryEntry is an immutable, append-only entry within a trajectory.
type TrajectoryEntry struct {
	Namespace      string `gorm:"primaryKey"`
	TrajectoryID   string `gorm:"primaryKey"`
	EntryID        string `gorm:"primaryKey;column:entry_id"`
	ParentID       string
	Ordinal        uint64
	Depth          uint64
	Kind           string
	RecordedAt     time.Time
	Generation     uint64
	ExecutionID    string
	OperationKey   string
	Turn           *int
	CorrelationID  string
	Provider       string
	Model          string
	ToolName       string
	ToolCallID     string
	FinishReason   string
	InputTokens    int64
	OutputTokens   int64
	IsError        *bool
	CauseCode      string
	ActionKind     string
	PayloadVersion uint32
	Payload        []byte  `gorm:"type:blob"`
	AttributesJSON *string `gorm:"type:text"`
	AuditJSON      *string `gorm:"type:text"`
}

func (TrajectoryEntry) TableName() string { return "ag_trajectory_entries" }

// Operation tracks durable operation state and worker leases.
type Operation struct {
	Namespace          string `gorm:"primaryKey"`
	ID                 string `gorm:"primaryKey"`
	IdempotencyKey     string
	Kind               string
	Resource           string
	ResourceRevision   string
	State              string
	Revision           uint64
	Input              []byte  `gorm:"type:blob"`
	InvocationJSON     string  `gorm:"type:text"`
	InvocationRootID   string
	InvocationParentID string
	InvocationGroupID  string
	Output             []byte `gorm:"type:blob"`
	OperationError     string `gorm:"type:text"`
	SubmittedAt        time.Time
	UpdatedAt          time.Time
	LeaseOwner         string
	LeaseToken         string
	LeaseExpiresAt     *time.Time
}

func (Operation) TableName() string { return "ag_operations" }

// Delivery is a queued event delivery for named outbox/inbox queues.
type Delivery struct {
	Namespace        string `gorm:"primaryKey"`
	Queue            string `gorm:"primaryKey"`
	ID               string `gorm:"primaryKey"`
	Sequence         uint64
	Plugin           string
	PluginVersion    string
	Subscription     string
	ResourceRevision string
	PartitionKey     string
	EventID          string
	EventName        string
	EventSessionID   string
	EventGeneration  uint64
	EventPayload     []byte `gorm:"type:blob"`
	State            string
	Attempt          int
	AvailableAt      *time.Time
	LeaseToken       string
	LeaseExpiresAt   *time.Time
	LastError        string `gorm:"type:text"`
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

func (Delivery) TableName() string { return "ag_deliveries" }

// ContextInjection is a queued model-visible payload for agent loops.
type ContextInjection struct {
	Namespace         string `gorm:"primaryKey"`
	ID                string `gorm:"primaryKey"`
	Sequence          uint64
	Priority          string
	Mode              string
	Origin            string
	TargetSessionID   string
	TargetExecutionID string
	IsMeta            bool
	Messages          []byte  `gorm:"type:blob"`
	AttributesJSON    *string `gorm:"type:text"`
	CreatedAt         time.Time
}

func (ContextInjection) TableName() string { return "ag_context_injections" }
