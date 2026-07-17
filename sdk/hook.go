package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Priority int

const (
	PriorityPre    Priority = 100
	PriorityNormal Priority = 500
	PriorityPost   Priority = 900
)

type FailurePolicy string

const (
	FailurePolicyFailClosed FailurePolicy = "fail_closed"
	FailurePolicyContinue   FailurePolicy = "continue"
)

type HookSpec struct {
	Name          string        `json:"name"`
	Event         string        `json:"event"`
	Priority      Priority      `json:"priority"`
	FailurePolicy FailurePolicy `json:"failure_policy"`
	Timeout       time.Duration `json:"timeout"`
}

type Hook interface {
	Spec() HookSpec
	Handle(context.Context, Event) (Effect, error)
}

type SubscriberSpec struct {
	Name    string        `json:"name"`
	Events  []string      `json:"events"`
	Timeout time.Duration `json:"timeout"`
}

type Subscriber interface {
	Spec() SubscriberSpec
	Receive(context.Context, Delivery) error
}

type SubscriberFunc struct {
	SubscriberSpec
	ReceiveFunc func(context.Context, Delivery) error
}

func (subscriber SubscriberFunc) Spec() SubscriberSpec {
	return subscriber.SubscriberSpec
}

func (subscriber SubscriberFunc) Receive(
	ctx context.Context,
	delivery Delivery,
) error {
	if subscriber.ReceiveFunc == nil {
		return errors.New("subscriber receiver is nil")
	}
	return subscriber.ReceiveFunc(ctx, delivery)
}

type HookFunc struct {
	HookSpec
	HandleFunc func(context.Context, Event) (Effect, error)
}

func (hook HookFunc) Spec() HookSpec {
	return hook.HookSpec
}

func (hook HookFunc) Handle(ctx context.Context, event Event) (Effect, error) {
	if hook.HandleFunc == nil {
		return Effect{}, errors.New("hook handler is nil")
	}
	return hook.HandleFunc(ctx, event)
}

func TypedHook[T any](
	spec HookSpec,
	handler func(context.Context, T) (Effect, error),
) Hook {
	return HookFunc{
		HookSpec: spec,
		HandleFunc: func(ctx context.Context, event Event) (Effect, error) {
			var payload T
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return Effect{}, fmt.Errorf(
					"decode %s event for hook %s: %w",
					event.Name,
					spec.Name,
					err,
				)
			}
			return handler(ctx, payload)
		},
	}
}

type EventContract struct {
	Name          string   `json:"name"`
	MutableFields []string `json:"mutable_fields,omitempty"`
	AllowBlock    bool     `json:"allow_block,omitempty"`
	AllowAction   bool     `json:"allow_action,omitempty"`
}

type Event struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	SessionID  string          `json:"session_id,omitempty"`
	Generation uint64          `json:"generation"`
	Payload    json.RawMessage `json:"payload"`
}

type Block struct {
	Reason string `json:"reason"`
	Kind   string `json:"kind,omitempty"`
}

type Cause struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
	Final  bool   `json:"final,omitempty"`
}

type ActionKind string

const (
	ActionStep   ActionKind = "step"
	ActionStop   ActionKind = "stop"
	ActionInject ActionKind = "inject"
)

type Action struct {
	Kind     ActionKind `json:"kind"`
	Cause    *Cause     `json:"cause,omitempty"`
	Messages []Message  `json:"messages,omitempty"`
}

type Effect struct {
	Patch  map[string]json.RawMessage `json:"patch,omitempty"`
	Block  *Block                     `json:"block,omitempty"`
	Action *Action                    `json:"action,omitempty"`
}

func Patch(values map[string]any) (Effect, error) {
	patch := make(map[string]json.RawMessage, len(values))
	for name, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			return Effect{}, fmt.Errorf("encode patch field %q: %w", name, err)
		}
		patch[name] = raw
	}
	return Effect{Patch: patch}, nil
}

func BlockWith(reason, kind string) Effect {
	return Effect{Block: &Block{Reason: reason, Kind: kind}}
}

func Step() Effect {
	return Effect{Action: &Action{Kind: ActionStep}}
}

func Stop(cause Cause) Effect {
	return Effect{Action: &Action{Kind: ActionStop, Cause: &cause}}
}

func Inject(messages ...Message) Effect {
	return Effect{
		Action: &Action{
			Kind:     ActionInject,
			Messages: append([]Message(nil), messages...),
		},
	}
}
