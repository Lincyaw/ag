package compact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

const (
	defaultTriggerTokens      = 96_000
	defaultTargetTokens       = 32_000
	defaultKeepRecentMessages = 16
	defaultMaxMessageChars    = 2_000
	defaultMaxToolResultChars = 4_000
)

// Config controls conservative prompt compaction before provider calls.
type Config struct {
	TriggerTokens      int
	TargetTokens       int
	KeepRecentMessages int
	MaxMessageChars    int
	MaxToolResultChars int
}

type plugin struct {
	config Config
}

func New(config Config) sdk.Plugin { return &plugin{config: config} }

func (plugin *plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "compact",
		Version:     "1.0.0",
		Description: "automatic extractive prompt compaction before provider calls to keep long sessions alive",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.HookResource("auto-compact")},
	}
}

func (plugin *plugin) Install(_ context.Context, registrar sdk.Registrar) error {
	config, err := normalizeConfig(plugin.config)
	if err != nil {
		return err
	}
	return registrar.RegisterHook(sdk.TypedHook[sdk.BeforeProviderPayload](
		sdk.HookSpec{
			Name:          "auto-compact",
			Event:         sdk.EventBeforeProvider,
			Priority:      sdk.PriorityPre,
			FailurePolicy: sdk.FailurePolicyContinue,
		},
		func(_ context.Context, payload sdk.BeforeProviderPayload) (sdk.Effect, error) {
			messages, compacted := compactMessages(payload.Messages, config)
			if !compacted {
				return sdk.Effect{}, nil
			}
			return sdk.Patch(map[string]any{"messages": messages})
		},
	))
}

func normalizeConfig(config Config) (Config, error) {
	if config.TriggerTokens == 0 {
		config.TriggerTokens = defaultTriggerTokens
	}
	if config.TargetTokens == 0 {
		config.TargetTokens = defaultTargetTokens
	}
	if config.KeepRecentMessages == 0 {
		config.KeepRecentMessages = defaultKeepRecentMessages
	}
	if config.MaxMessageChars == 0 {
		config.MaxMessageChars = defaultMaxMessageChars
	}
	if config.MaxToolResultChars == 0 {
		config.MaxToolResultChars = defaultMaxToolResultChars
	}
	if config.TriggerTokens < 1 || config.TargetTokens < 1 ||
		config.KeepRecentMessages < 1 || config.MaxMessageChars < 1 ||
		config.MaxToolResultChars < 1 {
		return Config{}, errors.New("compact limits must be positive")
	}
	return config, nil
}

func compactMessages(messages []sdk.Message, config Config) ([]sdk.Message, bool) {
	if estimateMessagesTokens(messages) <= config.TriggerTokens || len(messages) < 3 {
		return sdk.CloneMessages(messages), false
	}

	tailStart := chooseTailStart(messages, config.KeepRecentMessages)
	if tailStart <= 0 || tailStart >= len(messages) {
		return sdk.CloneMessages(messages), false
	}
	older := messages[:tailStart]
	tail := boundedMessages(messages[tailStart:], config)

	summaryBudget := config.TargetTokens - estimateMessagesTokens(tail)
	if summaryBudget < 512 {
		summaryBudget = 512
	}
	summary := summarizeOlderMessages(older, summaryBudget, config)
	compacted := append([]sdk.Message{{
		Role:    sdk.RoleUser,
		Content: summary,
	}}, tail...)
	return compacted, true
}

func chooseTailStart(messages []sdk.Message, keepRecent int) int {
	start := len(messages) - keepRecent
	if start < 1 {
		start = 1
	}
	for start > 0 && messages[start].Role == sdk.RoleTool {
		start--
	}
	return start
}

func summarizeOlderMessages(messages []sdk.Message, budgetTokens int, config Config) string {
	budgetChars := budgetTokens * 4
	prefix := fmt.Sprintf(
		"<compact-summary>\nThe earlier conversation was compacted automatically to stay within the model context. It covered %d prior messages. Recent uncompressed messages follow this summary.\n",
		len(messages),
	)
	suffix := "</compact-summary>"
	available := budgetChars - len(prefix) - len(suffix) - 128
	if available < 512 {
		available = 512
	}

	lines := make([]string, 0, len(messages))
	used := 0
	omitted := 0
	for index := len(messages) - 1; index >= 0; index-- {
		line := formatMessageSummary(index, messages[index], config)
		if used+len(line)+1 > available && len(lines) > 0 {
			omitted = index + 1
			break
		}
		if len(line) > available && len(lines) == 0 {
			line = truncate(line, available)
		}
		lines = append(lines, line)
		used += len(line) + 1
	}
	reverse(lines)

	var builder strings.Builder
	builder.WriteString(prefix)
	if omitted > 0 {
		builder.WriteString(fmt.Sprintf("[... %d oldest messages omitted from compact summary ...]\n", omitted))
	}
	for _, line := range lines {
		builder.WriteString(line)
		builder.WriteByte('\n')
	}
	builder.WriteString(suffix)
	return builder.String()
}

func formatMessageSummary(index int, message sdk.Message, config Config) string {
	contentLimit := config.MaxMessageChars
	if message.Role == sdk.RoleTool {
		contentLimit = config.MaxToolResultChars
	}
	content := strings.TrimSpace(message.Content)
	content = strings.ReplaceAll(content, "\x00", "")
	content = truncate(content, contentLimit)

	var parts []string
	parts = append(parts, fmt.Sprintf("[%03d] role=%s", index+1, message.Role))
	if message.ToolCallID != "" {
		parts = append(parts, "tool_call_id="+message.ToolCallID)
	}
	if len(message.ToolCalls) > 0 {
		parts = append(parts, "tool_calls="+summarizeToolCalls(message.ToolCalls, config.MaxMessageChars))
	}
	if message.IsError {
		parts = append(parts, "is_error=true")
	}
	if content != "" {
		parts = append(parts, "content="+quoteOneLine(content))
	}
	return strings.Join(parts, " ")
}

func summarizeToolCalls(calls []sdk.ToolCall, limit int) string {
	raw, err := json.Marshal(calls)
	if err != nil {
		return fmt.Sprintf("%d call(s)", len(calls))
	}
	return quoteOneLine(truncate(string(raw), limit))
}

func boundedMessages(messages []sdk.Message, config Config) []sdk.Message {
	bounded := sdk.CloneMessages(messages)
	for index := range bounded {
		limit := config.MaxMessageChars * 2
		switch bounded[index].Role {
		case sdk.RoleUser:
			limit = config.MaxMessageChars * 4
		case sdk.RoleTool:
			limit = config.MaxToolResultChars
		}
		bounded[index].Content = truncate(bounded[index].Content, limit)
	}
	return bounded
}

func estimateMessagesTokens(messages []sdk.Message) int {
	chars := 0
	for _, message := range messages {
		chars += len(message.Content) + len(message.ToolCallID) + 16
		for _, call := range message.ToolCalls {
			chars += len(call.ID) + len(call.Name) + len(call.Arguments) + 16
		}
	}
	if chars == 0 {
		return 0
	}
	return (chars + 3) / 4
}

func quoteOneLine(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	return string(raw)
}

func truncate(value string, limit int) string {
	if limit < 0 || len(value) <= limit {
		return value
	}
	if limit < 32 {
		return value[:limit]
	}
	return value[:limit-32] + "...[truncated]"
}

func reverse(values []string) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
