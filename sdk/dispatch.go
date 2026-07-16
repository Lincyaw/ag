package sdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type DispatchResult struct {
	Event   Event
	Block   *Block
	Actions []Action
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
	return runtime.dispatch(
		ctx,
		lease.snapshot,
		eventName,
		sessionID,
		payload,
	)
}

func (runtime *Runtime) dispatch(
	ctx context.Context,
	snapshot *registrySnapshot,
	eventName string,
	sessionID string,
	payload any,
) (DispatchResult, error) {
	owned, exists := snapshot.events[eventName]
	if !exists {
		return DispatchResult{}, fmt.Errorf("event %q is not registered", eventName)
	}
	raw, err := marshalEventPayload(payload)
	if err != nil {
		return DispatchResult{}, fmt.Errorf("encode %s event: %w", eventName, err)
	}
	event := Event{
		ID:         newDispatchID(),
		Name:       eventName,
		SessionID:  sessionID,
		Generation: snapshot.generation,
		Payload:    raw,
	}
	hooks := snapshot.hooks[eventName]

	ctx, span := runtime.tracer.Start(
		ctx,
		"event "+eventName,
		trace.WithAttributes(
			attribute.String("agentm.event.name", eventName),
			attribute.String("agentm.event.id", event.ID),
			attribute.Int64("agentm.registry.generation", int64(snapshot.generation)),
			attribute.Int("agentm.event.hook_count", len(hooks)),
		),
	)
	defer span.End()
	runtime.events.Add(
		ctx,
		1,
		metric.WithAttributes(attribute.String("agentm.event.name", eventName)),
	)

	result := DispatchResult{Event: event}
	for _, ownedHook := range hooks {
		effect, hookErr := runtime.invokeHook(ctx, ownedHook, event)
		if hookErr != nil {
			if ownedHook.spec.FailurePolicy == FailurePolicyContinue {
				runtime.logger.WarnContext(
					ctx,
					"plugin hook failed; continuing",
					"plugin",
					ownedHook.owner.manifest.Name,
					"hook",
					ownedHook.spec.Name,
					"event",
					eventName,
					"error",
					hookErr,
				)
				continue
			}
			err := fmt.Errorf(
				"hook %q from plugin %q failed on event %q: %w",
				ownedHook.spec.Name,
				ownedHook.owner.manifest.Name,
				eventName,
				hookErr,
			)
			recordSpanError(span, err)
			return DispatchResult{}, err
		}
		if err := validateEffect(owned.contract, effect); err != nil {
			err = fmt.Errorf(
				"hook %q from plugin %q returned invalid effect: %w",
				ownedHook.spec.Name,
				ownedHook.owner.manifest.Name,
				err,
			)
			recordSpanError(span, err)
			return DispatchResult{}, err
		}
		if len(effect.Patch) > 0 {
			raw, err = applyPatch(raw, effect.Patch, owned.contract.MutableFields)
			if err != nil {
				recordSpanError(span, err)
				return DispatchResult{}, err
			}
			event.Payload = raw
			result.Event.Payload = raw
		}
		if result.Block == nil && effect.Block != nil {
			block := *effect.Block
			result.Block = &block
		}
		if effect.Action != nil {
			result.Actions = append(result.Actions, *effect.Action)
		}
	}
	runtime.enqueueSubscribers(snapshot, result.Event)
	return result, nil
}

func (runtime *Runtime) invokeHook(
	ctx context.Context,
	owned ownedHook,
	event Event,
) (effect Effect, err error) {
	if owned.spec.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, owned.spec.Timeout)
		defer cancel()
	}
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
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())
			recordSpanError(span, err)
		}
	}()
	effect, err = owned.hook.Handle(ctx, event)
	if err != nil {
		recordSpanError(span, err)
	}
	return effect, err
}

func validateEffect(contract EventContract, effect Effect) error {
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
		case ActionStep:
			if effect.Action.Cause != nil || len(effect.Action.Messages) != 0 {
				return errors.New("step action cannot carry cause or messages")
			}
		case ActionStop:
			if effect.Action.Cause == nil || effect.Action.Cause.Code == "" {
				return errors.New("stop action requires a cause")
			}
		case ActionInject:
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

func newDispatchID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func recordSpanError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

func (runtime *Runtime) InvokeCapability(
	ctx context.Context,
	name string,
	input json.RawMessage,
) (json.RawMessage, error) {
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return nil, err
	}
	defer lease.release()
	owned, exists := lease.snapshot.capabilities[name]
	if !exists {
		return nil, fmt.Errorf("capability %q is not registered", name)
	}
	ctx, span := runtime.tracer.Start(
		ctx,
		"capability "+name,
		trace.WithAttributes(
			attribute.String("agentm.capability.name", name),
			attribute.String("agentm.plugin.name", owned.owner.manifest.Name),
		),
	)
	defer span.End()
	asynchronous, ok := owned.capability.(AsyncCapability)
	if !ok {
		err := fmt.Errorf("capability %q has no asynchronous execution implementation", name)
		recordSpanError(span, err)
		return nil, err
	}
	initial, err := asynchronous.SubmitInvoke(ctx, OperationRequest{
		IdempotencyKey: newDispatchID(),
		Input:          append(json.RawMessage(nil), input...),
	})
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("submit capability %q: %w", name, err)
	}
	operation, err := runtime.awaitOperation(
		ctx,
		initial,
		asynchronous.PollInvoke,
		asynchronous.CancelInvoke,
	)
	if err != nil {
		recordSpanError(span, err)
		return nil, fmt.Errorf("invoke capability %q: %w", name, err)
	}
	output := operation.Output
	if !json.Valid(output) {
		err := fmt.Errorf("capability %q returned invalid JSON", name)
		recordSpanError(span, err)
		return nil, err
	}
	return append(json.RawMessage(nil), output...), nil
}
