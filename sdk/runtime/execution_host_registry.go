package runtime

import (
	"context"
	"fmt"
	"sync"

	"github.com/lincyaw/ag/sdk"
)

type hostedExecutionRegistry struct {
	mu    sync.Mutex
	hosts map[string]*hostedExecution
}

type hostedExecution struct {
	cancel         context.CancelCauseFunc
	enqueueContext func(context.Context, sdk.ContextInjection) error
	done           chan struct{}
}

func newHostedExecutionRegistry() *hostedExecutionRegistry {
	return &hostedExecutionRegistry{
		hosts: make(map[string]*hostedExecution),
	}
}

func hostedExecutionKey(trajectoryID string, executionID string) string {
	return trajectoryID + "\x00" + executionID
}

func (registry *hostedExecutionRegistry) register(
	trajectoryID string,
	executionID string,
	cancel context.CancelCauseFunc,
	enqueueContext func(context.Context, sdk.ContextInjection) error,
) func() {
	key := hostedExecutionKey(trajectoryID, executionID)
	hosted := &hostedExecution{
		cancel:         cancel,
		enqueueContext: enqueueContext,
		done:           make(chan struct{}),
	}
	registry.mu.Lock()
	registry.hosts[key] = hosted
	registry.mu.Unlock()
	return func() {
		registry.mu.Lock()
		if registry.hosts[key] == hosted {
			delete(registry.hosts, key)
		}
		registry.mu.Unlock()
		close(hosted.done)
	}
}

func (registry *hostedExecutionRegistry) enqueueContext(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	injection sdk.ContextInjection,
) error {
	hosted, ok := registry.load(trajectoryID, executionID)
	if !ok {
		return hostedExecutionNotFoundError(trajectoryID, executionID)
	}
	if hosted.enqueueContext == nil {
		return fmt.Errorf(
			"%w: trajectory %s execution %s has no context injection boundary",
			ErrExecutionNotFound,
			trajectoryID,
			executionID,
		)
	}
	select {
	case <-hosted.done:
		return hostedExecutionNotFoundError(trajectoryID, executionID)
	default:
	}
	if err := hosted.enqueueContext(ctx, injection); err != nil {
		return err
	}
	select {
	case <-hosted.done:
		return hostedExecutionNotFoundError(trajectoryID, executionID)
	default:
		return nil
	}
}

func hostedExecutionNotFoundError(
	trajectoryID string,
	executionID string,
) error {
	return fmt.Errorf(
		"%w: trajectory %s execution %s is not hosted",
		ErrExecutionNotFound,
		trajectoryID,
		executionID,
	)
}

func (registry *hostedExecutionRegistry) load(
	trajectoryID string,
	executionID string,
) (*hostedExecution, bool) {
	if registry == nil {
		return nil, false
	}
	key := hostedExecutionKey(trajectoryID, executionID)
	registry.mu.Lock()
	hosted := registry.hosts[key]
	registry.mu.Unlock()
	if hosted == nil {
		return nil, false
	}
	return hosted, true
}

func (registry *hostedExecutionRegistry) cancelAndWait(
	ctx context.Context,
	trajectoryID string,
	executionID string,
	cause error,
) (bool, error) {
	hosted, ok := registry.load(trajectoryID, executionID)
	if !ok {
		return false, nil
	}
	hosted.cancel(cause)
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case <-hosted.done:
		return true, nil
	}
}

func (registry *hostedExecutionRegistry) cancel(
	trajectoryID string,
	executionID string,
	cause error,
) {
	hosted, ok := registry.load(trajectoryID, executionID)
	if ok {
		hosted.cancel(cause)
	}
}
