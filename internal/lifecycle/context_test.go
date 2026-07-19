package lifecycle

import (
	"context"
	"errors"
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

func TestExpectedCancellationRequiresOnlyCancellationErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if !ExpectedCancellation(
		ctx,
		errors.Join(context.Canceled, context.DeadlineExceeded),
	) {
		t.Fatal("pure cancellation join was not recognized")
	}
	if ExpectedCancellation(
		ctx,
		errors.Join(context.Canceled, errors.New("close failed")),
	) {
		t.Fatal("joined close failure was hidden as cancellation")
	}
	if ExpectedCancellation(context.Background(), context.Canceled) {
		t.Fatal("uncancelled context was treated as expected cancellation")
	}
}
