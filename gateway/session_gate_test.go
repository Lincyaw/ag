package gateway

import (
	"context"
	"errors"
	"testing"
)

func TestSessionGateReleasesUnusedEntries(t *testing.T) {
	gate := newSessionGate()
	unlock, err := gate.lock(t.Context(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(gate.locks); got != 1 {
		t.Fatalf("gate entries while locked = %d, want 1", got)
	}
	unlock()
	if got := len(gate.locks); got != 0 {
		t.Fatalf("gate entries after unlock = %d, want 0", got)
	}
}

func TestSessionGateReleasesCancelledWaiters(t *testing.T) {
	gate := newSessionGate()
	unlock, err := gate.lock(t.Context(), "session-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := gate.lock(ctx, "session-a"); !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf("cancelled lock error = %v", err)
	}
	if got := len(gate.locks); got != 1 {
		t.Fatalf("gate entries after waiter cancel = %d, want 1", got)
	}
	unlock()
	if got := len(gate.locks); got != 0 {
		t.Fatalf("gate entries after final unlock = %d, want 0", got)
	}
}
