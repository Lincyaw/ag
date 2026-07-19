package sdk

import "strings"

// PermissionRejectionAudience selects the model-visible wording for a denied
// tool permission.
type PermissionRejectionAudience string

const (
	// PermissionRejectionPolicy avoids attributing the denial to the user.
	PermissionRejectionPolicy     PermissionRejectionAudience = "policy"
	PermissionRejectionForeground PermissionRejectionAudience = "foreground"
	PermissionRejectionSubagent   PermissionRejectionAudience = "subagent"
)

// PermissionRejection describes a denied tool permission in SDK terms so
// hooks, gateways, and presenters do not hand-roll incompatible tool results.
type PermissionRejection struct {
	Audience PermissionRejectionAudience `json:"audience,omitempty"`
	ToolName string                      `json:"tool_name,omitempty"`
	Reason   string                      `json:"reason,omitempty"`
}

// Effective returns the explicit audience, or the SDK's policy-neutral default.
func (audience PermissionRejectionAudience) Effective() PermissionRejectionAudience {
	if audience == "" {
		return PermissionRejectionPolicy
	}
	return audience
}

// Message renders the model-visible denial message for this rejection.
func (rejection PermissionRejection) Message() string {
	reason := strings.TrimSpace(rejection.Reason)
	tool := strings.TrimSpace(rejection.ToolName)
	message := permissionRejectionBaseMessage(
		rejection.Audience.Effective(),
		tool,
	)
	if reason != "" {
		message += " Reason: " + reason
	}
	return message
}

// DenyToolPermission returns a blocking hook effect for a denied tool use.
func DenyToolPermission(rejection PermissionRejection) Effect {
	return BlockWith(
		rejection.Message(),
		string(ToolErrorPermissionDenied),
	)
}

func permissionRejectionBaseMessage(
	audience PermissionRejectionAudience,
	tool string,
) string {
	switch audience {
	case PermissionRejectionForeground:
		if tool != "" {
			return "The user denied permission to use " + tool + ". Stop and wait for the user to decide how to proceed."
		}
		return "The user denied this tool use. Stop and wait for the user to decide how to proceed."
	case PermissionRejectionSubagent:
		if tool != "" {
			return "Permission to use " + tool + " was denied. Try an allowed alternative or report the limitation."
		}
		return "Permission for this tool use was denied. Try an allowed alternative or report the limitation."
	default:
		if tool != "" {
			return "Permission to use " + tool + " was denied by policy. Use an allowed alternative or report the limitation."
		}
		return "Permission for this tool use was denied by policy. Use an allowed alternative or report the limitation."
	}
}
