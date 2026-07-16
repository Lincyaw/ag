package sdk

const (
	EventBeforeAgentStart   = "before_agent_start"
	EventAgentStart         = "agent_start"
	EventTurnStart          = "turn_start"
	EventBeforeProvider     = "before_provider"
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

var builtinEventContracts = []EventContract{
	{
		Name:          EventBeforeAgentStart,
		MutableFields: []string{"messages", "system"},
		AllowBlock:    true,
	},
	{Name: EventAgentStart},
	{Name: EventTurnStart},
	{
		Name:          EventBeforeProvider,
		MutableFields: []string{"messages", "provider", "system", "tools"},
	},
	{Name: EventAfterProvider},
	{
		Name:          EventBeforeTool,
		MutableFields: []string{"call"},
		AllowBlock:    true,
	},
	{
		Name:          EventToolError,
		MutableFields: []string{"result"},
	},
	{
		Name:          EventAfterTool,
		MutableFields: []string{"result"},
	},
	{Name: EventDecide, AllowAction: true},
	{Name: EventTurnEnd},
	{Name: EventAgentEnd},
	{Name: EventPluginMounted},
	{Name: EventPluginUnmounted},
	{Name: EventTrajectoryAppend},
	{Name: EventTrajectoryRestore},
	{Name: EventTrajectoryRollback},
}

func BuiltinEventContracts() []EventContract {
	result := make([]EventContract, len(builtinEventContracts))
	for index, contract := range builtinEventContracts {
		result[index] = contract
		result[index].MutableFields = append(
			[]string(nil),
			contract.MutableFields...,
		)
	}
	return result
}

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

type BeforeToolPayload struct {
	Turn int      `json:"turn"`
	Call ToolCall `json:"call"`
}

type ToolErrorPayload struct {
	Turn   int        `json:"turn"`
	Call   ToolCall   `json:"call"`
	Kind   string     `json:"kind"`
	Reason string     `json:"reason"`
	Result ToolResult `json:"result"`
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
	Messages []Message `json:"messages"`
	Cause    Cause     `json:"cause"`
}

type PluginLifecyclePayload struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Source     string `json:"source"`
	Generation uint64 `json:"generation"`
}

type TrajectoryEventPayload struct {
	TrajectoryID string `json:"trajectory_id"`
	EntryID      string `json:"entry_id,omitempty"`
	EntryKind    string `json:"entry_kind,omitempty"`
	From         string `json:"from,omitempty"`
	To           string `json:"to,omitempty"`
	Generation   uint64 `json:"generation,omitempty"`
}
