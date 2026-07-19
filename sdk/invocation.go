package sdk

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

type invocationContextKey struct{}

func WithInvocation(
	ctx context.Context,
	invocation Invocation,
) context.Context {
	return context.WithValue(
		ctx,
		invocationContextKey{},
		CloneInvocation(invocation),
	)
}

func InvocationFromContext(
	ctx context.Context,
) (Invocation, bool) {
	invocation, ok := ctx.Value(
		invocationContextKey{},
	).(Invocation)
	return CloneInvocation(invocation), ok
}

// Invocation identifies one semantic execution node in an invocation graph.
//
// ID is stable across retries. RootID identifies the root agent execution,
// ParentID records causal ownership, GroupID identifies concurrently submitted
// siblings, and Dependencies records explicit DAG edges. SessionID and
// ExecutionID connect the graph to the stateful trajectory that produced it.
//
// A zero Invocation is valid for callers that do not participate in structured
// execution. Once ID is set, RootID, SessionID, and ExecutionID are required.
type Invocation struct {
	ID              string   `json:"id,omitempty"`
	RootID          string   `json:"root_id,omitempty"`
	ParentID        string   `json:"parent_id,omitempty"`
	GroupID         string   `json:"group_id,omitempty"`
	SessionID       string   `json:"session_id,omitempty"`
	TargetSessionID string   `json:"target_session_id,omitempty"`
	ExecutionID     string   `json:"execution_id,omitempty"`
	Dependencies    []string `json:"dependencies,omitempty"`
	Ordinal         uint32   `json:"ordinal,omitempty"`
}

func (invocation Invocation) Empty() bool {
	return invocation.ID == "" &&
		invocation.RootID == "" &&
		invocation.ParentID == "" &&
		invocation.GroupID == "" &&
		invocation.SessionID == "" &&
		invocation.TargetSessionID == "" &&
		invocation.ExecutionID == "" &&
		len(invocation.Dependencies) == 0 &&
		invocation.Ordinal == 0
}

func ValidateInvocation(invocation Invocation) error {
	if invocation.Empty() {
		return nil
	}
	if invocation.ID == "" {
		return errors.New("invocation ID is empty")
	}
	required := []struct {
		kind  string
		value string
	}{
		{"invocation", invocation.ID},
		{"invocation root", invocation.RootID},
		{"invocation session", invocation.SessionID},
		{"invocation execution", invocation.ExecutionID},
	}
	for _, field := range required {
		if field.value == "" {
			return fmt.Errorf("%s is empty", field.kind)
		}
		if err := ValidateResourceName(field.kind, field.value); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		kind  string
		value string
	}{
		{"invocation parent", invocation.ParentID},
		{"invocation group", invocation.GroupID},
		{"invocation target session", invocation.TargetSessionID},
	} {
		if field.value != "" {
			if err := ValidateResourceName(field.kind, field.value); err != nil {
				return err
			}
		}
	}
	seen := make(map[string]struct{}, len(invocation.Dependencies))
	for _, dependency := range invocation.Dependencies {
		if err := ValidateResourceName(
			"invocation dependency",
			dependency,
		); err != nil {
			return err
		}
		if dependency == invocation.ID {
			return fmt.Errorf(
				"invocation %q cannot depend on itself",
				invocation.ID,
			)
		}
		if _, duplicate := seen[dependency]; duplicate {
			return fmt.Errorf(
				"invocation %q contains duplicate dependency %q",
				invocation.ID,
				dependency,
			)
		}
		seen[dependency] = struct{}{}
	}
	return nil
}

func CloneInvocation(invocation Invocation) Invocation {
	invocation.Dependencies = slices.Clone(invocation.Dependencies)
	return invocation
}
