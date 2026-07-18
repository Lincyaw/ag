package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/lincyaw/ag/sdk"
)

func summarizeTask(messages []sdk.Message) string {
	for index := len(messages) - 1; index >= 0; index-- {
		message := messages[index]
		if message.Role == sdk.RoleUser && strings.TrimSpace(message.Content) != "" {
			return summarizeText(message.Content, 120)
		}
	}
	return ""
}

func summarizeAnswer(response *sdk.ModelResponse) string {
	if response == nil {
		return "response ready"
	}
	var parts []string
	if response.Usage.OutputTokens > 0 {
		parts = append(parts, fmt.Sprintf("%d output token(s)", response.Usage.OutputTokens))
	}
	if response.FinishReason != "" {
		parts = append(parts, "finish="+response.FinishReason)
	}
	if len(parts) == 0 {
		return "response ready"
	}
	return strings.Join(parts, ", ")
}

func summarizeToolPlan(calls []sdk.ToolCall) string {
	if len(calls) == 0 {
		return "no tool use"
	}
	items := make([]string, 0, len(calls))
	for index, call := range calls {
		if index >= 4 {
			items = append(items, fmt.Sprintf("+%d more", len(calls)-index))
			break
		}
		intent := inferToolIntent(call)
		if intent.Subject == "" {
			items = append(items, intent.PlanVerb)
		} else {
			items = append(items, intent.PlanVerb+" "+intent.Subject)
		}
	}
	return strings.Join(items, ", ")
}

func summarizeToolStart(call sdk.ToolCall) (label, detail, technical string) {
	intent := inferToolIntent(call)
	detail = emptyAs(intent.Subject, intent.Name)
	technical = "tool=" + emptyAs(call.Name, "unknown")
	if args := summarizeArguments(call.Arguments); args != "" {
		technical += " args=" + args
	}
	return intent.ActiveVerb, detail, technical
}

func summarizeToolFinish(
	call sdk.ToolCall,
	result sdk.ToolResult,
) (label, detail, technical string) {
	intent := inferToolIntent(call)
	measure := summarizeResultMeasure(result.Content)
	technical = "tool=" + emptyAs(call.Name, "unknown") + " result=" +
		summarizeToolResult(result)
	if result.IsError {
		label = "Failed"
		detail = emptyAs(intent.Subject, intent.Name)
		if preview := summarizeText(result.Content, 120); preview != "" {
			detail += ": " + preview
		}
		return label, detail, technical
	}
	label = intent.DoneVerb
	detail = emptyAs(intent.Subject, intent.Name)
	if measure != "" {
		detail += " (" + measure + ")"
	}
	return label, detail, technical
}

type toolIntent struct {
	Name       string
	ActiveVerb string
	PlanVerb   string
	DoneVerb   string
	Subject    string
}

func inferToolIntent(call sdk.ToolCall) toolIntent {
	name := friendlyToolName(call.Name)
	active, plan, done := toolVerbs(call.Name)
	return toolIntent{
		Name:       name,
		ActiveVerb: active,
		PlanVerb:   plan,
		DoneVerb:   done,
		Subject:    summarizeToolSubject(call.Arguments),
	}
}

func toolVerbs(name string) (active, plan, done string) {
	normalized := strings.ToLower(name)
	switch {
	case strings.Contains(normalized, "read"):
		return "Reading", "read", "Read"
	case strings.Contains(normalized, "list"):
		return "Listing", "list", "Listed"
	case strings.Contains(normalized, "search") ||
		strings.Contains(normalized, "grep") ||
		strings.Contains(normalized, "find"):
		return "Searching", "search", "Searched"
	case strings.Contains(normalized, "write") ||
		strings.Contains(normalized, "edit") ||
		strings.Contains(normalized, "patch") ||
		strings.Contains(normalized, "update"):
		return "Editing", "edit", "Edited"
	case strings.Contains(normalized, "create") ||
		strings.Contains(normalized, "new"):
		return "Creating", "create", "Created"
	case strings.Contains(normalized, "delete") ||
		strings.Contains(normalized, "remove") ||
		strings.Contains(normalized, "prune"):
		return "Deleting", "delete", "Deleted"
	case strings.Contains(normalized, "bash") ||
		strings.Contains(normalized, "shell") ||
		strings.Contains(normalized, "exec") ||
		strings.Contains(normalized, "run"):
		return "Running", "run", "Ran"
	case strings.Contains(normalized, "fetch") ||
		strings.Contains(normalized, "open") ||
		strings.Contains(normalized, "http") ||
		strings.Contains(normalized, "request"):
		return "Fetching", "fetch", "Fetched"
	default:
		return "Using", "use", "Used"
	}
}

func summarizeToolSubject(raw json.RawMessage) string {
	args := decodeArgumentObject(raw)
	if len(args) == 0 {
		return ""
	}
	path := firstArgumentString(args,
		"path", "file", "filename", "filepath", "target", "dir", "directory", "cwd",
	)
	query := firstArgumentString(args, "query", "pattern", "search", "text")
	command := firstArgumentString(args, "command", "cmd", "script")
	url := firstArgumentString(args, "url", "uri", "endpoint")
	identifier := firstArgumentString(args, "id", "ref_id", "name")
	switch {
	case query != "" && path != "":
		return strconv.Quote(summarizeText(query, 60)) + " in " + summarizeText(path, 80)
	case query != "":
		return strconv.Quote(summarizeText(query, 80))
	case path != "":
		return summarizeText(path, 100)
	case command != "":
		return strconv.Quote(summarizeText(command, 100))
	case url != "":
		return summarizeText(url, 100)
	case identifier != "":
		return summarizeText(identifier, 100)
	default:
		return summarizeArguments(raw)
	}
}

func decodeArgumentObject(raw json.RawMessage) map[string]any {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}

func firstArgumentString(args map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := args[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				return typed
			}
		case float64:
			return strconv.FormatFloat(typed, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(typed)
		default:
			raw, err := json.Marshal(typed)
			if err == nil && len(raw) > 0 {
				return string(raw)
			}
		}
	}
	return ""
}

func friendlyToolName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "tool"
	}
	name = strings.ReplaceAll(name, "_", " ")
	name = strings.ReplaceAll(name, "-", " ")
	return name
}

func shortIdentifier(value string) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= 12 {
		return value
	}
	return string(runes[:12])
}

func summarizeResultMeasure(content string) string {
	if content == "" {
		return "no output"
	}
	return fmt.Sprintf("%s, %d line(s)", formatBytes(len(content)), lineCount(content))
}

func summarizeModelResponse(response sdk.ModelResponse) string {
	var parts []string
	if response.FinishReason != "" {
		parts = append(parts, "finish="+response.FinishReason)
	}
	if response.Usage.InputTokens > 0 || response.Usage.OutputTokens > 0 {
		parts = append(parts, fmt.Sprintf(
			"tokens=%d+%d",
			response.Usage.InputTokens,
			response.Usage.OutputTokens,
		))
	}
	if strings.TrimSpace(response.Content) != "" {
		parts = append(parts, summarizeText(response.Content, 180))
	}
	if len(parts) == 0 {
		return "model returned"
	}
	return strings.Join(parts, "  ")
}

func summarizeToolCalls(calls []sdk.ToolCall) string {
	if len(calls) == 0 {
		return "no tool calls"
	}
	values := make([]string, 0, len(calls))
	for index, call := range calls {
		if index >= 4 {
			values = append(values, fmt.Sprintf("+%d more", len(calls)-index))
			break
		}
		value := emptyAs(call.Name, "tool")
		if summary := summarizeArguments(call.Arguments); summary != "" {
			value += " " + summary
		}
		values = append(values, value)
	}
	return strings.Join(values, "; ")
}

func summarizeArguments(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return summarizeText(string(raw), 220)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return summarizeJSONValue(value, 220)
	}
	keys := slices.Sorted(maps.Keys(object))
	values := make([]string, 0, len(keys))
	for index, key := range keys {
		if index >= 6 {
			values = append(values, fmt.Sprintf("+%d more", len(keys)-index))
			break
		}
		values = append(values, key+"="+summarizeJSONValue(object[key], 90))
	}
	return strings.Join(values, " ")
}

func summarizeJSONValue(value any, limit int) string {
	switch typed := value.(type) {
	case string:
		return strconv.Quote(summarizeText(typed, limit))
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	case nil:
		return "null"
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return "<value>"
		}
		return summarizeText(string(raw), limit)
	}
}

func summarizeToolResult(result sdk.ToolResult) string {
	prefix := fmt.Sprintf(
		"%s, %d line(s)",
		formatBytes(len(result.Content)),
		lineCount(result.Content),
	)
	if strings.TrimSpace(result.Content) == "" {
		return prefix
	}
	return prefix + ": " + strconv.Quote(summarizeText(result.Content, 220))
}

func summarizeText(value string, limit int) string {
	value = strings.Join(strings.Fields(tableCell(value)), " ")
	if value == "" {
		return ""
	}
	return fitProgressText(value, limit)
}

func fitProgressText(value string, limit int) string {
	if limit <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return string(runes[:limit-3]) + "..."
}

func fitLines(lines []string, limit int) string {
	if limit <= 0 || len(lines) <= limit {
		return strings.Join(lines, "\n")
	}
	if limit == 1 {
		return fitProgressText(lines[0], 80)
	}
	result := slices.Clone(lines[:limit])
	result[limit-1] = "..."
	return strings.Join(result, "\n")
}

func lineCount(value string) int {
	if value == "" {
		return 0
	}
	return strings.Count(value, "\n") + 1
}

func formatBytes(value int) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%d B", value)
	}
	if value < unit*unit {
		return fmt.Sprintf("%.1f KiB", float64(value)/unit)
	}
	return fmt.Sprintf("%.1f MiB", float64(value)/(unit*unit))
}
