package sdk

import (
	"context"
	"strings"
	"testing"
)

func TestValidateInvocation(t *testing.T) {
	t.Parallel()
	valid := Invocation{
		ID:           "tool-call",
		RootID:       "root",
		ParentID:     "parent",
		GroupID:      "siblings",
		SessionID:    "session",
		ExecutionID:  "execution",
		Dependencies: []string{"provider-call"},
		Ordinal:      1,
	}
	if err := ValidateInvocation(valid); err != nil {
		t.Fatalf("validate invocation: %v", err)
	}
	invalid := valid
	invalid.Dependencies = []string{invalid.ID}
	if err := ValidateInvocation(invalid); err == nil ||
		!strings.Contains(err.Error(), "cannot depend on itself") {
		t.Fatalf("self dependency error = %v", err)
	}
	clone := CloneInvocation(valid)
	clone.Dependencies[0] = "changed"
	if valid.Dependencies[0] != "provider-call" {
		t.Fatal("CloneInvocation aliased dependencies")
	}
	ctx := WithInvocation(context.Background(), valid)
	fromContext, ok := InvocationFromContext(ctx)
	if !ok || fromContext.ID != valid.ID {
		t.Fatalf("context invocation = %#v, %v", fromContext, ok)
	}
	fromContext.Dependencies[0] = "mutated"
	again, _ := InvocationFromContext(ctx)
	if again.Dependencies[0] != "provider-call" {
		t.Fatal("InvocationFromContext exposed mutable state")
	}
}
