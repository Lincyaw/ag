package sdk

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
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

type InvocationGraph struct {
	RootID     string            `json:"root_id"`
	Operations []OperationRecord `json:"operations"`
}

type InvocationOperationStore interface {
	ListByInvocationRoot(
		context.Context,
		string,
	) ([]OperationRecord, error)
}

// LoadInvocationGraph projects durable operation records into one causal
// invocation graph. The root agent trajectory execution may not itself have an
// operation record; RootID still identifies it and every child node points to
// it through Invocation.RootID.
func LoadInvocationGraph(
	ctx context.Context,
	store OperationStore,
	rootID string,
) (InvocationGraph, error) {
	if store == nil {
		return InvocationGraph{}, errors.New("operation store is nil")
	}
	if err := ValidateResourceName(
		"invocation root",
		rootID,
	); err != nil {
		return InvocationGraph{}, err
	}
	var records []OperationRecord
	var err error
	if indexed, ok := store.(InvocationOperationStore); ok {
		records, err = indexed.ListByInvocationRoot(ctx, rootID)
	} else {
		records, err = store.List(ctx)
	}
	if err != nil {
		return InvocationGraph{}, err
	}
	graph := InvocationGraph{RootID: rootID}
	for _, record := range records {
		if record.Invocation.RootID == rootID {
			graph.Operations = append(
				graph.Operations,
				cloneInvocationGraphRecord(record),
			)
		}
	}
	slices.SortFunc(
		graph.Operations,
		func(left, right OperationRecord) int {
			if order := left.Operation.SubmittedAt.Compare(
				right.Operation.SubmittedAt,
			); order != 0 {
				return order
			}
			return strings.Compare(
				left.Operation.ID,
				right.Operation.ID,
			)
		},
	)
	return graph, nil
}

func cloneInvocationGraphRecord(record OperationRecord) OperationRecord {
	record.Input = append([]byte(nil), record.Input...)
	record.Invocation = CloneInvocation(record.Invocation)
	record.Operation.Output = append(
		[]byte(nil),
		record.Operation.Output...,
	)
	if record.Execution != nil {
		execution := *record.Execution
		record.Execution = &execution
	}
	return record
}
