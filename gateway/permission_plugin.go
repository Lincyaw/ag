package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/lincyaw/ag/sdk"
)

// NewPermissionPlugin enforces the durable trajectory permission rules at the
// runtime's before_tool boundary. Explicit deny wins over ask, which wins over
// allow; unmatched calls retain the runtime's existing allow behaviour.
func NewPermissionPlugin(
	manager *InteractionManager,
	rules PermissionRules,
) sdk.Plugin {
	return permissionPlugin{manager: manager, rules: clonePermissionRules(rules)}
}

type permissionPlugin struct {
	manager *InteractionManager
	rules   PermissionRules
}

func (permissionPlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "gateway_permissions",
		Version:     "1.0.0",
		Description: "durable trajectory-scoped tool permissions",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.HookResource("gateway-tool-permissions")},
	}
}

func (plugin permissionPlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	if plugin.manager == nil {
		return errors.New("gateway permission interaction manager is nil")
	}
	return registrar.RegisterHook(sdk.HookFunc{
		HookSpec: sdk.HookSpec{
			Name:          "gateway-tool-permissions",
			Event:         sdk.EventBeforeTool,
			Priority:      sdk.PriorityPre,
			FailurePolicy: sdk.FailurePolicyFailClosed,
		},
		HandleFunc: plugin.handle,
	})
}

func (plugin permissionPlugin) handle(
	ctx context.Context,
	event sdk.Event,
) (sdk.Effect, error) {
	var payload sdk.BeforeToolPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return sdk.Effect{}, fmt.Errorf("decode permission tool call: %w", err)
	}
	candidates := permissionCandidates(payload.Call)
	if matchingPermissionRule(plugin.rules.Deny, candidates) != "" {
		return sdk.BlockWith(
			fmt.Sprintf("permission denied for tool %s", payload.Call.Name),
			string(sdk.ToolErrorBlocked),
		), nil
	}
	pattern := matchingPermissionRule(plugin.rules.Ask, candidates)
	if pattern == "" {
		return sdk.Effect{}, nil
	}
	interaction := permissionInteraction(event.SessionID, payload.Call, pattern)
	answer, err := plugin.manager.Request(ctx, interaction)
	if err != nil {
		return sdk.Effect{}, err
	}
	if answer.OptionID == "allow" {
		return sdk.Effect{}, nil
	}
	return sdk.BlockWith(
		fmt.Sprintf("permission denied for tool %s", payload.Call.Name),
		string(sdk.ToolErrorBlocked),
	), nil
}

func permissionInteraction(
	sessionID string,
	call sdk.ToolCall,
	pattern string,
) Interaction {
	raw, _ := json.Marshal(struct {
		SessionID string
		Call      sdk.ToolCall
		Pattern   string
	}{sessionID, call, pattern})
	digest := sha256.Sum256(raw)
	summary := permissionArgumentSummary(call.Arguments)
	prompt := fmt.Sprintf("Allow %s?", call.Name)
	if summary != "" {
		prompt = fmt.Sprintf("Allow %s(%s)?", call.Name, summary)
	}
	return Interaction{
		ID:          fmt.Sprintf("permission-%x", digest[:12]),
		SessionID:   sessionID,
		ExecutionID: fmt.Sprintf("permission-execution-%x", digest[:12]),
		Kind:        InteractionPermission,
		Prompt:      prompt,
		Options: []InteractionOption{
			{ID: "allow", Label: "Allow once"},
			{ID: "deny", Label: "Deny"},
		},
	}
}

func permissionCandidates(call sdk.ToolCall) []string {
	name := strings.TrimSpace(call.Name)
	if name == "" {
		return nil
	}
	candidates := []string{name}
	if summary := permissionArgumentSummary(call.Arguments); summary != "" {
		candidates = append(candidates, name+"("+summary+")")
	}
	return candidates
}

func permissionArgumentSummary(raw json.RawMessage) string {
	var arguments map[string]any
	if json.Unmarshal(raw, &arguments) != nil {
		return strings.TrimSpace(string(raw))
	}
	for _, key := range []string{
		"command", "cmd", "file_path", "path", "url", "query", "pattern",
	} {
		if value, ok := arguments[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func matchingPermissionRule(rules, candidates []string) string {
	for _, rule := range rules {
		pattern := strings.TrimSpace(rule)
		if pattern == "" {
			continue
		}
		// Claude Code uses ':' before the wildcard as a command-prefix
		// separator (for example Bash(git status:*)).
		pattern = strings.ReplaceAll(pattern, ":*)", "*)")
		for _, candidate := range candidates {
			if permissionGlobMatch(pattern, candidate) {
				return rule
			}
		}
	}
	return ""
}

func permissionGlobMatch(pattern, value string) bool {
	patternRunes := []rune(strings.ToLower(strings.TrimSpace(pattern)))
	valueRunes := []rune(strings.ToLower(strings.TrimSpace(value)))
	previous := make([]bool, len(valueRunes)+1)
	previous[0] = true
	for _, token := range patternRunes {
		current := make([]bool, len(valueRunes)+1)
		if token == '*' {
			current[0] = previous[0]
			for index := 1; index <= len(valueRunes); index++ {
				current[index] = previous[index] || current[index-1]
			}
		} else {
			for index := 1; index <= len(valueRunes); index++ {
				current[index] = previous[index-1] &&
					(token == '?' || unicode.ToLower(token) == unicode.ToLower(valueRunes[index-1]))
			}
		}
		previous = current
	}
	return previous[len(valueRunes)]
}
