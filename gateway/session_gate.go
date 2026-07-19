package gateway

import (
	"context"
	"sync"
)

type sessionGate struct {
	mu    sync.Mutex
	locks map[string]*sessionGateLock
}

type sessionGateLock struct {
	token chan struct{}
	refs  int
}

func newSessionGate() *sessionGate {
	return &sessionGate{locks: make(map[string]*sessionGateLock)}
}

func (gate *sessionGate) lock(
	ctx context.Context,
	sessionID string,
) (func(), error) {
	if gate == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	guard := gate.retain(sessionID)
	select {
	case <-ctx.Done():
		gate.release(sessionID, guard)
		return nil, ctx.Err()
	case <-guard.token:
		return func() {
			guard.token <- struct{}{}
			gate.release(sessionID, guard)
		}, nil
	}
}

func (gate *sessionGate) retain(sessionID string) *sessionGateLock {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	guard := gate.locks[sessionID]
	if guard == nil {
		guard = &sessionGateLock{token: make(chan struct{}, 1)}
		guard.token <- struct{}{}
		gate.locks[sessionID] = guard
	}
	guard.refs++
	return guard
}

func (gate *sessionGate) release(
	sessionID string,
	guard *sessionGateLock,
) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if guard == nil || gate.locks[sessionID] != guard {
		return
	}
	guard.refs--
	if guard.refs == 0 {
		delete(gate.locks, sessionID)
	}
}
