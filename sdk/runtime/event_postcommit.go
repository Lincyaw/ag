package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lincyaw/ag/sdk"
)

type postCommitEventPlan struct {
	snapshot *registrySnapshot
	event    sdk.Event
	delivery postCommitDelivery
	subject  postCommitSubject
}

// postCommitSubject keeps domain identity separate from the legacy Event
// SessionID envelope used for subscriber partitioning and protocol compatibility.
type postCommitSubject struct {
	envelopeSessionID string
	logIDField        string
	logID             string
}

func (plan postCommitEventPlan) dispatchAfterCommit(
	ctx context.Context,
	runtime *Runtime,
) {
	if runtime == nil || plan.event.ID == "" {
		return
	}
	if _, err := runtime.dispatchPreparedEvent(
		ctx,
		plan.snapshot,
		plan.event,
		postCommitEventDispatchOptions(plan.delivery),
	); err != nil {
		runtime.logger.WarnContext(
			ctx,
			"post-commit event failed",
			plan.logArgs(err)...,
		)
	}
}

func (plan postCommitEventPlan) logArgs(err error) []any {
	args := []any{"event", plan.event.Name}
	args = append(args, plan.subject.logArgs()...)
	return append(args, "error", err)
}

func (subject postCommitSubject) logArgs() []any {
	if subject.logIDField == "" {
		return nil
	}
	return []any{subject.logIDField, subject.logID}
}

type postCommitEventBundle []postCommitEventPlan

type hostOutboxDeliverySource interface {
	hostOutboxDeliveries() []sdk.Delivery
}

func (bundle postCommitEventBundle) dispatchAfterCommit(
	ctx context.Context,
	runtime *Runtime,
) {
	ctx = afterDispatchEventContext(ctx)
	for _, plan := range bundle {
		plan.dispatchAfterCommit(ctx, runtime)
	}
}

func (bundle postCommitEventBundle) hostOutboxDeliveries() []sdk.Delivery {
	var deliveries []sdk.Delivery
	for _, plan := range bundle {
		if plan.event.ID == "" {
			continue
		}
		deliveries = append(
			deliveries,
			plan.delivery.hostOutboxDeliveries()...,
		)
	}
	return deliveries
}

// leasedPostCommitEventBundle keeps the composition snapshot alive until the
// durable mutation has committed and its post-commit events have dispatched.
type leasedPostCommitEventBundle struct {
	events postCommitEventBundle
	lease  *snapshotLease
}

func (bundle *leasedPostCommitEventBundle) append(
	plans ...postCommitEventPlan,
) {
	bundle.events = append(bundle.events, plans...)
}

func (bundle leasedPostCommitEventBundle) dispatchAfterCommit(
	ctx context.Context,
	runtime *Runtime,
) {
	bundle.events.dispatchAfterCommit(ctx, runtime)
}

func (bundle *leasedPostCommitEventBundle) dispatchAfterCommitAndRelease(
	ctx context.Context,
	runtime *Runtime,
) {
	if bundle == nil {
		return
	}
	defer bundle.release()
	bundle.dispatchAfterCommit(ctx, runtime)
}

func (bundle leasedPostCommitEventBundle) hostOutboxDeliveries() []sdk.Delivery {
	return bundle.events.hostOutboxDeliveries()
}

func (bundle *leasedPostCommitEventBundle) release() {
	if bundle == nil || bundle.lease == nil {
		return
	}
	bundle.lease.release()
	bundle.lease = nil
}

func (runtime *Runtime) atomicMutationHostOutbox(
	deliveries []sdk.Delivery,
) ([]sdk.StateMutationDeliveries, error) {
	if len(deliveries) == 0 {
		return nil, nil
	}
	if runtime.atomicState == nil {
		return nil, errors.New(
			"host outbox requires an atomic state backend",
		)
	}
	return sdk.CloneStateMutationOutbox([]sdk.StateMutationDeliveries{{
		Queue:      sdk.HostOutboxQueue,
		Deliveries: deliveries,
	}}), nil
}

func (runtime *Runtime) stateMutationHostOutbox(
	source hostOutboxDeliverySource,
) ([]sdk.StateMutationDeliveries, error) {
	if source == nil {
		return nil, nil
	}
	return runtime.atomicMutationHostOutbox(source.hostOutboxDeliveries())
}

// postCommitDeliveryBoundary is the commit-boundary decision for subscriber
// deliveries attached to an event prepared around a durable state mutation.
// The boundary is not an event classification: events whose hooks can affect
// the final event payload must enqueue after dispatch, even on atomic storage.
type postCommitDeliveryBoundary uint8

const (
	postCommitDeliveryBoundaryAfterDispatch postCommitDeliveryBoundary = iota
	postCommitDeliveryBoundaryHostOutbox
)

type postCommitDelivery struct {
	afterDispatch bool
	hostOutbox    []sdk.Delivery
}

func newPostCommitDelivery(
	snapshot *registrySnapshot,
	event sdk.Event,
	boundary postCommitDeliveryBoundary,
) postCommitDelivery {
	if boundary == postCommitDeliveryBoundaryHostOutbox &&
		subscriberDeliveryStableBeforeDispatch(snapshot, event.Name) {
		return postCommitDelivery{
			hostOutbox: planSubscriberDeliveries(
				snapshot,
				event,
				time.Now().UTC(),
			),
		}
	}
	return postCommitDelivery{afterDispatch: true}
}

func (delivery postCommitDelivery) enqueueAfterDispatch() bool {
	return delivery.afterDispatch
}

func (delivery postCommitDelivery) hostOutboxDeliveries() []sdk.Delivery {
	return sdk.CloneDeliveries(delivery.hostOutbox)
}

func preparePostCommitEventPlan(
	snapshot *registrySnapshot,
	eventName string,
	subject postCommitSubject,
	payload any,
	deliveryBoundary postCommitDeliveryBoundary,
) (postCommitEventPlan, error) {
	event, err := newDispatchEvent(
		snapshot,
		eventName,
		subject.envelopeSessionID,
		payload,
	)
	if err != nil {
		return postCommitEventPlan{}, fmt.Errorf(
			"prepare post-commit %s event: %w",
			eventName,
			err,
		)
	}
	return postCommitEventPlan{
		snapshot: snapshot,
		event:    event,
		delivery: newPostCommitDelivery(
			snapshot,
			event,
			deliveryBoundary,
		),
		subject: subject,
	}, nil
}

func postCommitSessionSubject(sessionID string) postCommitSubject {
	return postCommitSubject{
		envelopeSessionID: sessionID,
		logIDField:        "session_id",
		logID:             sessionID,
	}
}

func postCommitTrajectorySubject(trajectoryID string) postCommitSubject {
	return postCommitSubject{
		envelopeSessionID: trajectoryID,
		logIDField:        "trajectory_id",
		logID:             trajectoryID,
	}
}

func postCommitPluginSubject(plugin string) postCommitSubject {
	return postCommitSubject{
		logIDField: "plugin",
		logID:      plugin,
	}
}

func (runtime *Runtime) mutationPostCommitDeliveryBoundary() postCommitDeliveryBoundary {
	if runtime.atomicState == nil {
		return postCommitDeliveryBoundaryAfterDispatch
	}
	return postCommitDeliveryBoundaryHostOutbox
}

func (session *Session) executionPostCommitDeliveryBoundary() postCommitDeliveryBoundary {
	executionID, token := session.activeExecution()
	if executionID == "" || token == "" {
		return postCommitDeliveryBoundaryAfterDispatch
	}
	return session.runtime.mutationPostCommitDeliveryBoundary()
}

func (session *Session) prepareExecutionEventPlan(
	snapshot *registrySnapshot,
	eventName string,
	payload any,
) (postCommitEventPlan, error) {
	return session.runtime.prepareSessionEventPlan(
		snapshot,
		eventName,
		session.config.ID,
		payload,
		session.executionPostCommitDeliveryBoundary(),
	)
}

func (runtime *Runtime) prepareSessionEventPlan(
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
	deliveryBoundary postCommitDeliveryBoundary,
) (postCommitEventPlan, error) {
	return preparePostCommitEventPlan(
		snapshot,
		eventName,
		postCommitSessionSubject(sessionID),
		payload,
		deliveryBoundary,
	)
}

func (runtime *Runtime) prepareTrajectoryEventPlan(
	snapshot *registrySnapshot,
	eventName string,
	payload sdk.TrajectoryEventPayload,
) (postCommitEventPlan, error) {
	return preparePostCommitEventPlan(
		snapshot,
		eventName,
		postCommitTrajectorySubject(payload.TrajectoryID),
		payload,
		runtime.mutationPostCommitDeliveryBoundary(),
	)
}

func (runtime *Runtime) preparePluginLifecycleEventPlan(
	snapshot *registrySnapshot,
	eventName string,
	payload sdk.PluginLifecyclePayload,
) (postCommitEventPlan, error) {
	return preparePostCommitEventPlan(
		snapshot,
		eventName,
		postCommitPluginSubject(payload.Name),
		payload,
		postCommitDeliveryBoundaryAfterDispatch,
	)
}

func subscriberDeliveryStableBeforeDispatch(
	snapshot *registrySnapshot,
	eventName string,
) bool {
	if snapshot == nil {
		return false
	}
	owned, exists := snapshot.events[eventName]
	if !exists {
		return false
	}
	contract := owned.contract
	return !contract.AllowsEffect()
}
