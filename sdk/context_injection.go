package sdk

import (
	"errors"
	"fmt"
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

// Valid reports whether priority is one of the SDK-defined queue priorities.
// The empty value is accepted because normalization applies the default.
func (priority ContextInjectionPriority) Valid() bool {
	switch priority {
	case "", ContextInjectionNow, ContextInjectionNext, ContextInjectionLater:
		return true
	default:
		return false
	}
}

// Effective returns the explicit priority, or the SDK default when omitted.
func (priority ContextInjectionPriority) Effective() ContextInjectionPriority {
	if priority == "" {
		return ContextInjectionNext
	}
	return priority
}

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

// Valid reports whether mode is one of the SDK-defined context injection modes.
// The empty value is accepted because normalization applies the default.
func (mode ContextInjectionMode) Valid() bool {
	switch mode {
	case "",
		ContextInjectionPrompt,
		ContextInjectionHook,
		ContextInjectionPermission,
		ContextInjectionTaskNotification,
		ContextInjectionInterAgent,
		ContextInjectionLocalCommand,
		ContextInjectionSystem:
		return true
	default:
		return false
	}
}

// Effective returns the explicit mode, or the SDK default when omitted.
func (mode ContextInjectionMode) Effective() ContextInjectionMode {
	if mode == "" {
		return ContextInjectionPrompt
	}
	return mode
}

// ContextInjection is a model-visible payload queued by the host, hooks, or
// runtime services for insertion at an execution boundary.
type ContextInjection struct {
	ID                string                   `json:"id,omitempty"`
	Priority          ContextInjectionPriority `json:"priority,omitempty"`
	Mode              ContextInjectionMode     `json:"mode,omitempty"`
	Origin            string                   `json:"origin,omitempty"`
	TargetSessionID   string                   `json:"target_session_id,omitempty"`
	TargetExecutionID string                   `json:"target_execution_id,omitempty"`
	IsMeta            bool                     `json:"is_meta,omitempty"`
	Messages          []Message                `json:"messages"`
	Attributes        map[string]string        `json:"attributes,omitempty"`
	CreatedAt         time.Time                `json:"created_at,omitempty"`
}

// NormalizeContextInjection validates a queued model-visible payload and applies
// SDK defaults for ID, priority, mode, and creation time.
func NormalizeContextInjection(
	injection ContextInjection,
	now time.Time,
) (ContextInjection, error) {
	if injection.ID == "" {
		injection.ID = NewID()
	} else if err := ValidateResourceName("context injection", injection.ID); err != nil {
		return ContextInjection{}, err
	}
	if !injection.Priority.Valid() {
		return ContextInjection{}, fmt.Errorf(
			"unknown context injection priority %q",
			injection.Priority,
		)
	}
	injection.Priority = injection.Priority.Effective()
	if !injection.Mode.Valid() {
		return ContextInjection{}, fmt.Errorf(
			"unknown context injection mode %q",
			injection.Mode,
		)
	}
	injection.Mode = injection.Mode.Effective()
	if injection.TargetSessionID != "" {
		if err := ValidateResourceName(
			"context injection target session",
			injection.TargetSessionID,
		); err != nil {
			return ContextInjection{}, err
		}
	}
	if injection.TargetExecutionID != "" {
		if err := ValidateResourceName(
			"context injection target execution",
			injection.TargetExecutionID,
		); err != nil {
			return ContextInjection{}, err
		}
	}
	if len(injection.Messages) == 0 {
		return ContextInjection{}, errors.New(
			"context injection contains no messages",
		)
	}
	if injection.CreatedAt.IsZero() {
		if now.IsZero() {
			now = time.Now()
		}
		injection.CreatedAt = now.UTC()
	} else {
		injection.CreatedAt = injection.CreatedAt.UTC()
	}
	return CloneContextInjection(injection), nil
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
