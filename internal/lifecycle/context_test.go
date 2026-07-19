package lifecycle

import (
	"context"
	"testing"
)

type contextTestKey string

func TestWithValuesOverlaysAndFallsBack(t *testing.T) {
	parentKey := contextTestKey("parent")
	sharedKey := contextTestKey("shared")
	parent := context.WithValue(context.Background(), parentKey, "parent-value")
	parent = context.WithValue(parent, sharedKey, "parent-shared")
	values := context.WithValue(context.Background(), sharedKey, "value-shared")

	ctx := WithValues(parent, values)
	if value := ctx.Value(parentKey); value != "parent-value" {
		t.Fatalf("parent value = %#v", value)
	}
	if value := ctx.Value(sharedKey); value != "value-shared" {
		t.Fatalf("shared value = %#v", value)
	}
}
