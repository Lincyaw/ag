package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestActiveHostRegistryCloseWaitCanBeRetried(t *testing.T) {
	t.Parallel()
	registry := newActiveHostRegistry()
	slot, _ := newActiveHostSlot(context.Background(), "execution-1")
	if existing, err := registry.reserve("session-1", slot); err != nil || existing != nil {
		t.Fatalf("reserve existing=%v error=%v", existing, err)
	}
	runtimes, started := registry.beginClose()
	if !started {
		t.Fatal("beginClose did not start")
	}
	if len(runtimes) != 0 {
		t.Fatalf("beginClose runtimes = %d, want 0", len(runtimes))
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.waitClosed(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("first waitClosed error = %v, want context canceled", err)
	}

	registry.complete("session-1", slot)
	waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := registry.waitClosed(waitCtx); err != nil {
		t.Fatalf("retry waitClosed: %v", err)
	}
	if err := registry.waitClosed(context.Background()); err != nil {
		t.Fatalf("idempotent waitClosed: %v", err)
	}
}
