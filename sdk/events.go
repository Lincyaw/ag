package sdk

const (
	EventBeforeAgentStart   = "before_agent_start"
	EventAgentStart         = "agent_start"
	EventTurnStart          = "turn_start"
	EventBeforeProvider     = "before_provider"
	EventProviderOutcome    = "provider_outcome"
	EventAfterProvider      = "after_provider"
	EventBeforeTool         = "before_tool"
	EventToolError          = "tool_error"
	EventAfterTool          = "after_tool"
	EventDecide             = "decide"
	EventTurnEnd            = "turn_end"
	EventAgentEnd           = "agent_end"
	EventPluginMounted      = "plugin_mounted"
	EventPluginUnmounted    = "plugin_unmounted"
	EventTrajectoryAppend   = "trajectory_appended"
	EventTrajectoryRestore  = "trajectory_restored"
	EventTrajectoryRollback = "trajectory_rolled_back"
)

type BeforeAgentStartPayload struct {
	Messages []Message `json:"messages"`
	System   string    `json:"system,omitempty"`
}

type AgentStartPayload struct {
	Messages []Message `json:"messages"`
	System   string    `json:"system,omitempty"`
}

type TurnStartPayload struct {
	Turn int `json:"turn"`
}

type BeforeProviderPayload struct {
	Turn     int        `json:"turn"`
	Messages []Message  `json:"messages"`
	Provider string     `json:"provider"`
	System   string     `json:"system,omitempty"`
	Tools    []ToolSpec `json:"tools"`
}

type AfterProviderPayload struct {
	Turn     int            `json:"turn"`
	Provider string         `json:"provider"`
	Response *ModelResponse `json:"response,omitempty"`
	Error    string         `json:"error,omitempty"`
}

type ProviderOutcomeKind string

const (
	ProviderOutcomeCompleted ProviderOutcomeKind = "completed"
	ProviderOutcomeFailed    ProviderOutcomeKind = "failed"
)

type ProviderOutcomePayload struct {
	Turn          int                 `json:"turn"`
	Provider      string              `json:"provider"`
	OperationKey  string              `json:"operation_key,omitempty"`
	CorrelationID string              `json:"correlation_id,omitempty"`
	Sequence      uint64              `json:"sequence,omitempty"`
	Kind          ProviderOutcomeKind `json:"kind"`
	Response      *ModelResponse      `json:"response,omitempty"`
	Error         string              `json:"error,omitempty"`
}

type BeforeToolPayload struct {
	Turn int      `json:"turn"`
	Call ToolCall `json:"call"`
}

type ToolErrorKind string

const (
	ToolErrorUnknownTool      ToolErrorKind = "unknown_tool"
	ToolErrorExecutionFailed  ToolErrorKind = "execution_failed"
	ToolErrorInterrupted      ToolErrorKind = "interrupted"
	ToolErrorPermissionDenied ToolErrorKind = "permission_denied"
	ToolErrorBlocked          ToolErrorKind = "blocked"
)

type ToolErrorPayload struct {
	Turn   int           `json:"turn"`
	Call   ToolCall      `json:"call"`
	Kind   ToolErrorKind `json:"kind"`
	Reason string        `json:"reason"`
	Result ToolResult    `json:"result"`
}

type AfterToolPayload struct {
	Turn   int        `json:"turn"`
	Call   ToolCall   `json:"call"`
	Result ToolResult `json:"result"`
}

type DecidePayload struct {
	Turn        int           `json:"turn"`
	Default     Action        `json:"default"`
	Response    ModelResponse `json:"response"`
	ToolResults []ToolResult  `json:"tool_results,omitempty"`
}

type TurnEndPayload struct {
	Turn     int       `json:"turn"`
	Messages []Message `json:"messages"`
	Action   Action    `json:"action"`
}

type AgentEndPayload struct {
	Messages          []Message          `json:"messages"`
	ContextInjections []ContextInjection `json:"context_injections,omitempty"`
	Output            string             `json:"output,omitempty"`
	Turns             int                `json:"turns,omitempty"`
	ToolCalls         int                `json:"tool_calls,omitempty"`
	Cause             Cause              `json:"cause"`
}

type PluginLifecyclePayload struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Source     string `json:"source"`
	Generation uint64 `json:"generation"`
}

type TrajectoryEventPayload struct {
	TrajectoryID string         `json:"trajectory_id"`
	EntryID      string         `json:"entry_id,omitempty"`
	EntryKind    TrajectoryKind `json:"entry_kind,omitempty"`
	From         string         `json:"from,omitempty"`
	To           string         `json:"to,omitempty"`
	Generation   uint64         `json:"generation,omitempty"`
}
