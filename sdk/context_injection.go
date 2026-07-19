package sdk

import (
	"maps"
	"time"
)

type ContextInjectionPriority string

const (
	// ContextInjectionNow is ordered ahead of other pending context at the next
	// runtime-owned drain boundary. Live runtimes may also use it to interrupt
	// cancellable tool waits without terminating the whole execution.
	ContextInjectionNow   ContextInjectionPriority = "now"
	ContextInjectionNext  ContextInjectionPriority = "next"
	ContextInjectionLater ContextInjectionPriority = "later"
)

type ContextInjectionMode string

const (
	ContextInjectionPrompt           ContextInjectionMode = "prompt"
	ContextInjectionHook             ContextInjectionMode = "hook"
	ContextInjectionPermission       ContextInjectionMode = "permission"
	ContextInjectionTaskNotification ContextInjectionMode = "task_notification"
	ContextInjectionInterAgent       ContextInjectionMode = "inter_agent"
	ContextInjectionLocalCommand     ContextInjectionMode = "local_command"
	ContextInjectionSystem           ContextInjectionMode = "system"
)

// ContextInjection is a model-visible payload queued by the host, hooks, or
// runtime services for insertion at an execution boundary.
type ContextInjection struct {
	ID         string                   `json:"id,omitempty"`
	Priority   ContextInjectionPriority `json:"priority,omitempty"`
	Mode       ContextInjectionMode     `json:"mode,omitempty"`
	Origin     string                   `json:"origin,omitempty"`
	IsMeta     bool                     `json:"is_meta,omitempty"`
	Messages   []Message                `json:"messages"`
	Attributes map[string]string        `json:"attributes,omitempty"`
	CreatedAt  time.Time                `json:"created_at,omitempty"`
}

func CloneContextInjection(injection ContextInjection) ContextInjection {
	injection.Messages = CloneMessages(injection.Messages)
	injection.Attributes = maps.Clone(injection.Attributes)
	return injection
}

func CloneContextInjections(
	injections []ContextInjection,
) []ContextInjection {
	if injections == nil {
		return nil
	}
	result := make([]ContextInjection, len(injections))
	for index, injection := range injections {
		result[index] = CloneContextInjection(injection)
	}
	return result
}
