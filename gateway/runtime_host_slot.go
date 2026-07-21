package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"

	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

type activeHostRegistry struct {
	mu        sync.Mutex
	hosts     map[string]*activeHostSlot
	closed    bool
	closeOnce sync.Once
	closeDone chan struct{}
}

// activeHostSlot tracks a process-local runtime host for one session. It is
// deliberately not the durable execution source of truth; trajectory storage is.
type activeHostSlot struct {
	executionID string
	cancel      context.CancelFunc
	done        chan struct{}
	runtime     *agentruntime.Runtime
	control     agentruntime.ExecutionControl
	state       activeHostSlotState
}

type activeHostSlotState uint8

const (
	activeHostSlotReserved activeHostSlotState = iota
	activeHostSlotBound
)

type activeHostCancelMode uint8

const (
	activeHostCancelUnhosted activeHostCancelMode = iota
	activeHostCancelReserved
	activeHostCancelBound
)

type activeHostCancelPlan struct {
	mode    activeHostCancelMode
	control agentruntime.ExecutionControl
	cancel  context.CancelFunc
	done    <-chan struct{}
}

type activeHostReadPlan struct {
	control agentruntime.ExecutionControl
	done    <-chan struct{}
}

type activeHostContextPlan struct {
	control agentruntime.ExecutionControl
}

func newActiveHostRegistry() *activeHostRegistry {
	return &activeHostRegistry{
		hosts:     make(map[string]*activeHostSlot),
		closeDone: make(chan struct{}),
	}
}

func newActiveHostSlot(
	ctx context.Context,
	executionID string,
) (*activeHostSlot, context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	return &activeHostSlot{
		executionID: executionID,
		cancel:      cancel,
		done:        make(chan struct{}),
		state:       activeHostSlotReserved,
	}, runCtx
}

func (registry *activeHostRegistry) reserve(
	sessionID string,
	slot *activeHostSlot,
) (*activeHostSlot, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, errors.New("gateway execution backend is closed")
	}
	if existing := registry.hosts[sessionID]; existing != nil {
		return existing, existing.activeError()
	}
	registry.hosts[sessionID] = slot
	return nil, nil
}

func (registry *activeHostRegistry) bind(
	sessionID string,
	slot *activeHostSlot,
	executionID string,
	runtime *agentruntime.Runtime,
	control agentruntime.ExecutionControl,
) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return errors.New("gateway execution backend closed before host bind")
	}
	if existing := registry.hosts[sessionID]; existing != slot {
		if existing == nil {
			return errors.New(
				"gateway execution reservation was lost before host bind",
			)
		}
		return existing.activeError()
	}
	slot.bindHost(executionID, runtime, control)
	return nil
}

func (registry *activeHostRegistry) readPlan(
	sessionID string,
) (activeHostReadPlan, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	slot := registry.hosts[sessionID]
	if slot == nil {
		return activeHostReadPlan{}, nil
	}
	if !slot.hasAcceptedExecution() {
		return activeHostReadPlan{}, slot.activeError()
	}
	if slot.state != activeHostSlotBound {
		return activeHostReadPlan{}, nil
	}
	return activeHostReadPlan{
		control: slot.control,
		done:    slot.done,
	}, nil
}

func (registry *activeHostRegistry) cancelPlan(
	sessionID string,
	executionID string,
) (activeHostCancelPlan, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	slot := registry.hosts[sessionID]
	return slot.cancelPlan(executionID)
}

func (registry *activeHostRegistry) contextPlan(
	sessionID string,
	executionID string,
) (activeHostContextPlan, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	slot := registry.hosts[sessionID]
	return slot.contextPlan(executionID)
}

func (registry *activeHostRegistry) beginClose() (
	[]*agentruntime.Runtime,
	bool,
) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	started := !registry.closed
	registry.closed = true
	runtimes := make([]*agentruntime.Runtime, 0, len(registry.hosts))
	for _, slot := range registry.hosts {
		if slot.runtime != nil {
			runtimes = append(runtimes, slot.runtime)
		}
	}
	registry.closeIfIdleLocked()
	return runtimes, started
}

func (registry *activeHostRegistry) waitClosed(ctx context.Context) error {
	done := registry.closeDone
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (registry *activeHostRegistry) complete(
	sessionID string,
	slot *activeHostSlot,
) {
	registry.mu.Lock()
	if current := registry.hosts[sessionID]; current == slot {
		delete(registry.hosts, sessionID)
	}
	close(slot.done)
	registry.closeIfIdleLocked()
	registry.mu.Unlock()
}

func (registry *activeHostRegistry) closeIfIdleLocked() {
	if registry.closed && len(registry.hosts) == 0 {
		registry.closeOnce.Do(func() { close(registry.closeDone) })
	}
}

func (slot *activeHostSlot) hasAcceptedExecution() bool {
	return slot != nil && slot.executionID != ""
}

func (slot *activeHostSlot) matchesExecution(executionID string) bool {
	return slot != nil && executionID != "" && slot.executionID == executionID
}

func (slot *activeHostSlot) bindHost(
	executionID string,
	runtime *agentruntime.Runtime,
	control agentruntime.ExecutionControl,
) {
	slot.executionID = executionID
	slot.runtime = runtime
	slot.control = control
	slot.state = activeHostSlotBound
}

func (slot *activeHostSlot) activeError() error {
	if !slot.hasAcceptedExecution() {
		return ErrExecutionActive
	}
	return fmt.Errorf("%w: %s", ErrExecutionActive, slot.executionID)
}

func (slot *activeHostSlot) cancelPlan(
	executionID string,
) (activeHostCancelPlan, error) {
	if slot == nil {
		return activeHostCancelPlan{mode: activeHostCancelUnhosted}, nil
	}
	if !slot.matchesExecution(executionID) {
		return activeHostCancelPlan{}, slot.activeError()
	}
	mode := activeHostCancelReserved
	if slot.state == activeHostSlotBound {
		mode = activeHostCancelBound
	}
	return activeHostCancelPlan{
		mode:    mode,
		cancel:  slot.cancel,
		done:    slot.done,
		control: slot.control,
	}, nil
}

func (slot *activeHostSlot) contextPlan(
	executionID string,
) (activeHostContextPlan, error) {
	if slot == nil {
		return activeHostContextPlan{}, ErrExecutionNotFound
	}
	if !slot.matchesExecution(executionID) {
		return activeHostContextPlan{}, slot.activeError()
	}
	if slot.state != activeHostSlotBound {
		return activeHostContextPlan{}, ErrExecutionActive
	}
	return activeHostContextPlan{control: slot.control}, nil
}

func (plan activeHostCancelPlan) cancelActive() {
	if plan.cancel != nil {
		plan.cancel()
	}
}

func (plan activeHostCancelPlan) cancelExecution(
	ctx context.Context,
	hosted func(agentruntime.ExecutionControl) (
		agentruntime.ExecutionView,
		error,
	),
	unhosted func() (agentruntime.ExecutionView, error),
) (agentruntime.ExecutionView, error) {
	if plan.drainsBeforeControl() {
		plan.cancelActive()
		if err := plan.wait(ctx); err != nil {
			return agentruntime.ExecutionView{}, err
		}
	}

	var view agentruntime.ExecutionView
	var err error
	if plan.usesHostedControl() {
		view, err = hosted(plan.control)
	} else {
		view, err = unhosted()
	}
	if err != nil {
		return agentruntime.ExecutionView{}, err
	}

	if !plan.drainsBeforeControl() {
		plan.cancelActive()
	}
	if err := plan.wait(ctx); err != nil {
		return agentruntime.ExecutionView{}, err
	}
	return view, nil
}

func (plan activeHostCancelPlan) drainsBeforeControl() bool {
	return plan.mode == activeHostCancelReserved
}

func (plan activeHostCancelPlan) usesHostedControl() bool {
	return plan.mode == activeHostCancelBound
}

func (plan activeHostCancelPlan) wait(ctx context.Context) error {
	if plan.done == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-plan.done:
		return nil
	}
}

func (plan activeHostReadPlan) active() bool {
	return plan.done != nil
}

func (plan activeHostReadPlan) loadView(
	ctx context.Context,
	trajectoryID string,
) (agentruntime.ExecutionView, error) {
	return plan.control.LoadView(ctx, trajectoryID)
}

func (plan activeHostReadPlan) wait(ctx context.Context) error {
	if plan.done == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-plan.done:
		return nil
	}
}
