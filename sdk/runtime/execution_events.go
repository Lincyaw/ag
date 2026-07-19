package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/lincyaw/ag/internal/lifecycle"
	"github.com/lincyaw/ag/internal/pluginpolicy"
	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// DispatchResult is the merged policy effect for one execution event.
type DispatchResult struct {
	Event       sdk.Event
	Block       *sdk.Block
	Actions     []sdk.Action
	Audit       sdk.EventAudit
	actionSteps []int
}

type eventDispatchOptions struct {
	postCommit                       bool
	enqueueSubscriberDeliveries      bool
	warnSubscriberDeliveryFailures   bool
	runtimeOwnedSubscriberDeliveries bool
}

func emitEventDispatchOptions() eventDispatchOptions {
	return eventDispatchOptions{enqueueSubscriberDeliveries: true}
}

func executionEventDispatchOptions() eventDispatchOptions {
	return eventDispatchOptions{
		enqueueSubscriberDeliveries:      true,
		warnSubscriberDeliveryFailures:   true,
		runtimeOwnedSubscriberDeliveries: true,
	}
}

func postCommitEventDispatchOptions(
	delivery postCommitDelivery,
) eventDispatchOptions {
	return eventDispatchOptions{
		postCommit:                       true,
		enqueueSubscriberDeliveries:      delivery.enqueueAfterDispatch(),
		runtimeOwnedSubscriberDeliveries: true,
	}
}

func (options eventDispatchOptions) isPostCommit() bool {
	return options.postCommit
}

func (options eventDispatchOptions) enqueueSubscribers() bool {
	return options.enqueueSubscriberDeliveries
}

func (options eventDispatchOptions) returnSubscriberFailures() bool {
	return !options.warnSubscriberDeliveryFailures
}

func (options eventDispatchOptions) subscriberDeliveryContext(
	ctx context.Context,
) context.Context {
	if options.runtimeOwnedSubscriberDeliveries {
		return afterDispatchEventContext(ctx)
	}
	return ctx
}

func afterDispatchEventContext(ctx context.Context) context.Context {
	return lifecycle.Detached(ctx)
}

func (runtime *Runtime) Emit(
	ctx context.Context,
	eventName string,
	sessionID string,
	payload any,
) (DispatchResult, error) {
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return DispatchResult{}, err
	}
	defer lease.release()
	return runtime.dispatchEmitEvent(
		ctx,
		lease.snapshot,
		eventName,
		sessionID,
		payload,
	)
}

func (runtime *Runtime) dispatchEmitEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
) (DispatchResult, error) {
	return runtime.dispatchEvent(
		ctx,
		snapshot,
		eventName,
		sessionID,
		payload,
		emitEventDispatchOptions(),
	)
}

func (runtime *Runtime) dispatchExecutionEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
) (DispatchResult, error) {
	return runtime.dispatchEvent(
		ctx,
		snapshot,
		eventName,
		sessionID,
		payload,
		executionEventDispatchOptions(),
	)
}

func dispatchMutableExecutionEvent[T any](
	runtime *Runtime,
	ctx context.Context,
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload T,
) (T, DispatchResult, error) {
	dispatched, err := runtime.dispatchEvent(
		ctx,
		snapshot,
		eventName,
		sessionID,
		payload,
		executionEventDispatchOptions(),
	)
	var patched T
	if err != nil {
		return patched, dispatched, err
	}
	if err := decodePayload(dispatched.Event, &patched); err != nil {
		return patched, dispatched, err
	}
	return patched, dispatched, nil
}

func decodePayload(event sdk.Event, target any) error {
	if err := json.Unmarshal(event.Payload, target); err != nil {
		return fmt.Errorf("decode %s event payload: %w", event.Name, err)
	}
	return nil
}

func (runtime *Runtime) dispatchEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
	options eventDispatchOptions,
) (DispatchResult, error) {
	event, err := newDispatchEvent(snapshot, eventName, sessionID, payload)
	if err != nil {
		return DispatchResult{}, err
	}
	return runtime.dispatchPreparedEvent(ctx, snapshot, event, options)
}

func newDispatchEvent(
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
) (sdk.Event, error) {
	if snapshot == nil {
		return sdk.Event{}, errors.New("event registry snapshot is nil")
	}
	if _, exists := snapshot.events[eventName]; !exists {
		return sdk.Event{}, fmt.Errorf("event %q is not registered", eventName)
	}
	raw, err := marshalEventPayload(payload)
	if err != nil {
		return sdk.Event{}, fmt.Errorf("encode %s event: %w", eventName, err)
	}
	return sdk.Event{
		ID:         sdk.NewID(),
		Name:       eventName,
		SessionID:  sessionID,
		Generation: snapshot.generation,
		Payload:    raw,
	}, nil
}

func (runtime *Runtime) dispatchPreparedEvent(
	ctx context.Context,
	snapshot *registrySnapshot,
	event sdk.Event,
	options eventDispatchOptions,
) (DispatchResult, error) {
	if snapshot == nil {
		return DispatchResult{}, errors.New("event registry snapshot is nil")
	}
	owned, exists := snapshot.events[event.Name]
	if !exists {
		return DispatchResult{}, fmt.Errorf("event %q is not registered", event.Name)
	}
	hooks := snapshot.hooks[event.Name]

	ctx, span := runtime.tracer.Start(
		ctx,
		"event "+event.Name,
		trace.WithAttributes(
			attribute.String("agentm.event.name", event.Name),
			attribute.String("agentm.event.id", event.ID),
			attribute.Int64("agentm.registry.generation", int64(snapshot.generation)),
			attribute.Int("agentm.event.hook_count", len(hooks)),
		),
	)
	defer span.End()
	runtime.events.Add(
		ctx,
		1,
		metric.WithAttributes(attribute.String("agentm.event.name", event.Name)),
	)

	result := DispatchResult{
		Event: event,
		Audit: newEventAudit(event),
	}
	patchedBy := make(map[string]int)
	for index, ownedHook := range hooks {
		step := newHookAuditStep(index, ownedHook, event.Payload)
		start := time.Now()
		effect, hookErr := runtime.invokeHook(ctx, ownedHook, event)
		step.Duration = time.Since(start)
		step.OutputHash = step.InputHash
		if hookErr != nil {
			err := fmt.Errorf(
				"hook %q from plugin %q failed on event %q: %w",
				ownedHook.spec.Name,
				ownedHook.owner.manifest.Name,
				event.Name,
				hookErr,
			)
			if handled, err := runtime.handleHookDispatchError(
				ctx,
				span,
				&result,
				event,
				ownedHook,
				hookDispatchFailure{
					step:                       step,
					auditErr:                   hookErr,
					dispatchErr:                err,
					continueMessage:            "plugin hook failed; continuing",
					allowFailurePolicyContinue: true,
				},
				options,
			); err != nil {
				return result, err
			} else if handled {
				continue
			}
		}
		step.PatchFields = sortedPatchFields(effect.Patch)
		step.Overwrites = overwrittenPatchFields(step.PatchFields, patchedBy)
		step.Block = summarizeBlock(effect.Block)
		step.Action = summarizeAction(effect.Action)
		if err := validateEffect(owned.contract, effect); err != nil {
			err = fmt.Errorf(
				"hook %q from plugin %q returned invalid effect: %w",
				ownedHook.spec.Name,
				ownedHook.owner.manifest.Name,
				err,
			)
			if handled, err := runtime.handleHookDispatchError(
				ctx,
				span,
				&result,
				event,
				ownedHook,
				hookDispatchFailure{
					step:            step,
					auditErr:        err,
					dispatchErr:     err,
					continueMessage: "plugin hook returned invalid post-commit effect; continuing",
				},
				options,
			); err != nil {
				return result, err
			} else if handled {
				continue
			}
		}
		if len(effect.Patch) > 0 {
			raw, err := applyPatch(
				event.Payload,
				effect.Patch,
				owned.contract.MutableFields,
			)
			if err != nil {
				err = fmt.Errorf(
					"hook %q from plugin %q returned invalid patch: %w",
					ownedHook.spec.Name,
					ownedHook.owner.manifest.Name,
					err,
				)
				if handled, err := runtime.handleHookDispatchError(
					ctx,
					span,
					&result,
					event,
					ownedHook,
					hookDispatchFailure{
						step:            step,
						auditErr:        err,
						dispatchErr:     err,
						continueMessage: "plugin hook returned invalid post-commit patch; continuing",
					},
					options,
				); err != nil {
					return result, err
				} else if handled {
					continue
				}
			}
			event.Payload = raw
			result.Event.Payload = raw
			for _, field := range step.PatchFields {
				patchedBy[field] = index
			}
		}
		step.OutputHash = payloadHash(event.Payload)
		step.Outcome = hookAuditOutcome(effect)
		result.Audit.Steps = append(result.Audit.Steps, step)
		if result.Block == nil && effect.Block != nil {
			block := *effect.Block
			result.Block = &block
			result.Audit.Resolution = sdk.EffectResolution{
				Outcome:     sdk.EffectResolutionBlocked,
				Block:       summarizeBlock(effect.Block),
				BlockStep:   intPtr(index),
				PatchFields: auditPatchFields(result.Audit),
			}
			appendSkippedHookAuditSteps(
				&result.Audit,
				hooks,
				index+1,
				event.Payload,
			)
			break
		}
		if effect.Action != nil {
			result.Actions = append(result.Actions, sdk.CloneAction(*effect.Action))
			result.actionSteps = append(result.actionSteps, index)
		}
	}
	finalizeDispatchAudit(&result)
	if options.enqueueSubscribers() {
		deliveryCtx := options.subscriberDeliveryContext(ctx)
		if err := runtime.enqueueSubscribers(
			deliveryCtx,
			snapshot,
			result.Event,
		); err != nil {
			recordSpanError(span, err)
			if options.returnSubscriberFailures() {
				return DispatchResult{}, err
			}
			runtime.logger.WarnContext(
				ctx,
				"subscriber delivery enqueue failed",
				"event",
				event.Name,
				"event_id",
				event.ID,
				"error",
				err,
			)
		}
	}
	runtime.observeEvent(ctx, result.Event)
	return result, nil
}

type hookDispatchFailure struct {
	step                       sdk.HookAuditStep
	auditErr                   error
	dispatchErr                error
	continueMessage            string
	allowFailurePolicyContinue bool
}

func (runtime *Runtime) handleHookDispatchError(
	ctx context.Context,
	span trace.Span,
	result *DispatchResult,
	event sdk.Event,
	owned ownedHook,
	failure hookDispatchFailure,
	options eventDispatchOptions,
) (bool, error) {
	step := failure.step
	step.Error = failure.auditErr.Error()
	step.Outcome = sdk.HookAuditError
	result.Audit.Steps = append(result.Audit.Steps, step)
	if options.isPostCommit() ||
		(failure.allowFailurePolicyContinue &&
			owned.spec.FailurePolicy == sdk.FailurePolicyContinue) {
		runtime.logger.WarnContext(
			ctx,
			failure.continueMessage,
			"plugin",
			owned.owner.manifest.Name,
			"hook",
			owned.spec.Name,
			"event",
			event.Name,
			"error",
			failure.auditErr,
		)
		return true, nil
	}
	result.Audit.Resolution = errorEffectResolution(failure.dispatchErr)
	result.Audit.OutputHash = payloadHash(event.Payload)
	recordSpanError(span, failure.dispatchErr)
	return false, failure.dispatchErr
}

func newEventAudit(event sdk.Event) sdk.EventAudit {
	hash := payloadHash(event.Payload)
	return sdk.EventAudit{
		EventID:    event.ID,
		EventName:  event.Name,
		Generation: event.Generation,
		InputHash:  hash,
		OutputHash: hash,
		Resolution: sdk.EffectResolution{
			Outcome: sdk.EffectResolutionNoEffect,
		},
	}
}

func newHookAuditStep(
	index int,
	owned ownedHook,
	payload json.RawMessage,
) sdk.HookAuditStep {
	hash := payloadHash(payload)
	return sdk.HookAuditStep{
		Index:         index,
		Plugin:        owned.owner.manifest.Name,
		PluginVersion: owned.owner.manifest.Version,
		Hook:          owned.spec.Name,
		Priority:      owned.spec.Priority,
		Sequence:      owned.seq,
		FailurePolicy: owned.spec.FailurePolicy,
		InputHash:     hash,
		OutputHash:    hash,
		Outcome:       sdk.HookAuditNoEffect,
	}
}

func appendSkippedHookAuditSteps(
	audit *sdk.EventAudit,
	hooks []ownedHook,
	start int,
	payload json.RawMessage,
) {
	for index := start; index < len(hooks); index++ {
		step := newHookAuditStep(index, hooks[index], payload)
		step.Outcome = sdk.HookAuditSkipped
		audit.Steps = append(audit.Steps, step)
	}
}

func finalizeDispatchAudit(result *DispatchResult) {
	result.Audit.OutputHash = payloadHash(result.Event.Payload)
	if result.Audit.Resolution.Outcome == sdk.EffectResolutionBlocked ||
		result.Audit.Resolution.Outcome == sdk.EffectResolutionError {
		if len(result.Audit.Resolution.PatchFields) == 0 {
			result.Audit.Resolution.PatchFields = auditPatchFields(result.Audit)
		}
		return
	}
	if len(result.Actions) > 0 {
		result.Audit.Resolution = sdk.EffectResolution{
			Outcome:     sdk.EffectResolutionAction,
			Action:      summarizeAction(&result.Actions[len(result.Actions)-1]),
			ActionSteps: slices.Clone(result.actionSteps),
			ActionRule:  "proposed",
			PatchFields: auditPatchFields(result.Audit),
		}
		return
	}
	if hasHookAuditOutcome(result.Audit, sdk.HookAuditError) {
		result.Audit.Resolution = sdk.EffectResolution{
			Outcome:     sdk.EffectResolutionError,
			PatchFields: auditPatchFields(result.Audit),
		}
		return
	}
	if fields := auditPatchFields(result.Audit); len(fields) > 0 {
		result.Audit.Resolution = sdk.EffectResolution{
			Outcome:     sdk.EffectResolutionPatched,
			PatchFields: fields,
		}
		return
	}
	result.Audit.Resolution = sdk.EffectResolution{
		Outcome: sdk.EffectResolutionNoEffect,
	}
}

func hasHookAuditOutcome(
	audit sdk.EventAudit,
	outcome sdk.HookAuditOutcome,
) bool {
	for _, step := range audit.Steps {
		if step.Outcome == outcome {
			return true
		}
	}
	return false
}

func hookAuditOutcome(effect sdk.Effect) sdk.HookAuditOutcome {
	if effect.Block != nil {
		return sdk.HookAuditBlocked
	}
	if effect.Action != nil {
		return sdk.HookAuditAction
	}
	if len(effect.Patch) > 0 {
		return sdk.HookAuditPatched
	}
	return sdk.HookAuditNoEffect
}

func sortedPatchFields(patch map[string]json.RawMessage) []string {
	if len(patch) == 0 {
		return nil
	}
	fields := make([]string, 0, len(patch))
	for field := range patch {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields
}

func overwrittenPatchFields(
	fields []string,
	patchedBy map[string]int,
) []string {
	var overwrites []string
	for _, field := range fields {
		if _, exists := patchedBy[field]; exists {
			overwrites = append(overwrites, field)
		}
	}
	return overwrites
}

func auditPatchFields(audit sdk.EventAudit) []string {
	seen := make(map[string]struct{})
	for _, step := range audit.Steps {
		for _, field := range step.PatchFields {
			seen[field] = struct{}{}
		}
	}
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields
}

func summarizeBlock(block *sdk.Block) *sdk.BlockSummary {
	if block == nil {
		return nil
	}
	return &sdk.BlockSummary{
		Reason: block.Reason,
		Kind:   block.Kind,
	}
}

func summarizeAction(action *sdk.Action) *sdk.ActionSummary {
	if action == nil {
		return nil
	}
	summary := &sdk.ActionSummary{
		Kind:         action.Kind,
		MessageCount: len(action.Messages),
	}
	if action.Cause != nil {
		summary.CauseCode = action.Cause.Code
		summary.CauseFinal = action.Cause.Final
	}
	return summary
}

func errorEffectResolution(err error) sdk.EffectResolution {
	return sdk.EffectResolution{
		Outcome: sdk.EffectResolutionError,
		Error:   err.Error(),
	}
}

func intPtr(value int) *int {
	return &value
}

func payloadHash(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (runtime *Runtime) observeEvent(ctx context.Context, event sdk.Event) {
	runtime.observer.dispatch(runtime, ctx, event)
}

type eventObserverRuntime struct {
	observe     func(context.Context, sdk.Event)
	context     context.Context
	cancel      context.CancelFunc
	wait        sync.WaitGroup
	stoppedOnce sync.Once
	stopped     chan struct{}
}

func (observer *eventObserverRuntime) dispatch(
	runtime *Runtime,
	ctx context.Context,
	event sdk.Event,
) {
	if observer.observe == nil {
		return
	}
	observe := observer.observe
	observed := sdk.CloneEvent(event)
	observerCtx := lifecycle.WithValues(
		observer.context,
		afterDispatchEventContext(ctx),
	)
	releaseObserver, ok := observer.begin(runtime)
	if !ok {
		return
	}
	go func() {
		defer releaseObserver()
		defer func() {
			if recovered := recover(); recovered != nil {
				runtime.logger.WarnContext(
					observerCtx,
					"runtime event observer panicked",
					"event",
					observed.Name,
					"panic",
					recovered,
				)
			}
		}()
		observe(observerCtx, observed)
	}()
}

func (observer *eventObserverRuntime) begin(runtime *Runtime) (func(), bool) {
	return runtime.beginRuntimeWork(&observer.wait)
}

func (observer *eventObserverRuntime) stop() {
	if observer.cancel != nil {
		observer.cancel()
	}
}

func (observer *eventObserverRuntime) waitBestEffortStopped(
	ctx context.Context,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		timeout = lifecycle.DefaultFinalizationTimeout
	}
	done := observer.stoppedSignal()
	if ctx == nil {
		ctx = context.Background()
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-done:
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf(
			"runtime event observers did not stop: %w",
			waitCtx.Err(),
		)
	}
}

func (observer *eventObserverRuntime) stoppedSignal() <-chan struct{} {
	observer.stoppedOnce.Do(func() {
		if observer.stopped == nil {
			observer.stopped = make(chan struct{})
		}
		go func() {
			observer.wait.Wait()
			close(observer.stopped)
		}()
	})
	return observer.stopped
}

func (runtime *Runtime) invokeHook(
	ctx context.Context,
	owned ownedHook,
	event sdk.Event,
) (effect sdk.Effect, err error) {
	ctx, span := runtime.tracer.Start(
		ctx,
		"hook "+owned.spec.Name,
		trace.WithAttributes(
			attribute.String("agentm.plugin.name", owned.owner.manifest.Name),
			attribute.String("agentm.hook.name", owned.spec.Name),
			attribute.String("agentm.hook.event", owned.spec.Event),
			attribute.Int("agentm.hook.priority", int(owned.spec.Priority)),
		),
	)
	defer span.End()
	runtime.hooks.Add(
		ctx,
		1,
		metric.WithAttributes(
			attribute.String("agentm.plugin.name", owned.owner.manifest.Name),
			attribute.String("agentm.hook.name", owned.spec.Name),
			attribute.String("agentm.hook.event", owned.spec.Event),
		),
	)
	effect, err = pluginpolicy.HandleHook(ctx, owned.value, owned.spec, event)
	if err != nil {
		recordSpanError(span, err)
	}
	return effect, err
}

func validateEffect(contract sdk.EventContract, effect sdk.Effect) error {
	if len(effect.Patch) > 0 {
		allowed := make(map[string]struct{}, len(contract.MutableFields))
		for _, field := range contract.MutableFields {
			allowed[field] = struct{}{}
		}
		for field := range effect.Patch {
			if _, exists := allowed[field]; !exists {
				return fmt.Errorf(
					"event %q does not allow field %q to be patched",
					contract.Name,
					field,
				)
			}
		}
	}
	if effect.Block != nil {
		if !contract.AllowBlock {
			return fmt.Errorf("event %q does not allow blocking", contract.Name)
		}
		if effect.Block.Reason == "" {
			return errors.New("block reason is empty")
		}
	}
	if effect.Action != nil {
		if !contract.AllowAction {
			return fmt.Errorf("event %q does not allow actions", contract.Name)
		}
		switch effect.Action.Kind {
		case sdk.ActionStep:
			if effect.Action.Cause != nil || len(effect.Action.Messages) != 0 {
				return errors.New("step action cannot carry cause or messages")
			}
		case sdk.ActionStop:
			if effect.Action.Cause == nil || effect.Action.Cause.Code == "" {
				return errors.New("stop action requires a cause")
			}
		case sdk.ActionInject:
			if len(effect.Action.Messages) == 0 {
				return errors.New("inject action requires messages")
			}
		default:
			return fmt.Errorf("unknown action kind %q", effect.Action.Kind)
		}
	}
	return nil
}

func applyPatch(
	payload json.RawMessage,
	patch map[string]json.RawMessage,
	mutableFields []string,
) (json.RawMessage, error) {
	allowed := append([]string(nil), mutableFields...)
	slices.Sort(allowed)
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, fmt.Errorf("decode event payload for patch: %w", err)
	}
	for field, value := range patch {
		if _, found := slices.BinarySearch(allowed, field); !found {
			return nil, fmt.Errorf("field %q is not mutable", field)
		}
		if !json.Valid(value) {
			return nil, fmt.Errorf("patch field %q is not valid JSON", field)
		}
		object[field] = append(json.RawMessage(nil), value...)
	}
	result, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("encode patched event payload: %w", err)
	}
	return result, nil
}

func marshalEventPayload(payload any) (json.RawMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, errors.New("event payload must encode as a JSON object")
	}
	return raw, nil
}
