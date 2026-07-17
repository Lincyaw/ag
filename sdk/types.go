package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

const APIVersion = 1

var resourceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type Message struct {
	Role       Role       `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type Usage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
}

type ModelRequest struct {
	Messages []Message  `json:"messages"`
	Tools    []ToolSpec `json:"tools"`
}

type ModelResponse struct {
	Content      string     `json:"content,omitempty"`
	ToolCalls    []ToolCall `json:"tool_calls,omitempty"`
	Model        string     `json:"model,omitempty"`
	FinishReason string     `json:"finish_reason,omitempty"`
	Usage        Usage      `json:"usage"`
}

type ProviderSpec struct {
	Name  string `json:"name"`
	Model string `json:"model"`
}

type Provider interface {
	Spec() ProviderSpec
}

type SyncProvider interface {
	Provider
	Complete(context.Context, ModelRequest) (ModelResponse, error)
}

type AsyncProvider interface {
	Provider
	SubmitCompletion(context.Context, OperationRequest) (Operation, error)
	PollCompletion(context.Context, string, uint64) (Operation, error)
	CancelCompletion(context.Context, string) (Operation, error)
}

type ToolSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type ToolResult struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

type Tool interface {
	Spec() ToolSpec
}

type SyncTool interface {
	Tool
	Call(context.Context, json.RawMessage) (ToolResult, error)
}

type AsyncTool interface {
	Tool
	SubmitCall(context.Context, OperationRequest) (Operation, error)
	PollCall(context.Context, string, uint64) (Operation, error)
	CancelCall(context.Context, string) (Operation, error)
}

type CapabilitySpec struct {
	Name         string         `json:"name"`
	Description  string         `json:"description"`
	InputSchema  map[string]any `json:"input_schema"`
	OutputSchema map[string]any `json:"output_schema"`
}

type Capability interface {
	Spec() CapabilitySpec
}

type SyncCapability interface {
	Capability
	Invoke(context.Context, json.RawMessage) (json.RawMessage, error)
}

type AsyncCapability interface {
	Capability
	SubmitInvoke(context.Context, OperationRequest) (Operation, error)
	PollInvoke(context.Context, string, uint64) (Operation, error)
	CancelInvoke(context.Context, string) (Operation, error)
}

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

func (contract EventContract) active() bool {
	return len(contract.MutableFields) > 0 ||
		contract.AllowBlock ||
		contract.AllowAction
}

func (contract EventContract) Active() bool {
	return contract.active()
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

type Manifest struct {
	Name          string   `json:"name"`
	Version       string   `json:"version"`
	Description   string   `json:"description"`
	APIVersion    int      `json:"api_version"`
	MinAPIVersion int      `json:"min_api_version,omitempty"`
	MaxAPIVersion int      `json:"max_api_version,omitempty"`
	Requires      []string `json:"requires,omitempty"`
	Conflicts     []string `json:"conflicts,omitempty"`
	Registers     []string `json:"registers,omitempty"`
}

func (manifest Manifest) Validate() error {
	if err := validateResourceName("plugin", manifest.Name); err != nil {
		return err
	}
	if strings.TrimSpace(manifest.Version) == "" {
		return errors.New("plugin version is empty")
	}
	if strings.TrimSpace(manifest.Description) == "" {
		return errors.New("plugin description is empty")
	}
	minimum, maximum := manifest.APIRange()
	if minimum < 1 || maximum < minimum {
		return fmt.Errorf(
			"plugin %q has invalid API version range %d..%d",
			manifest.Name,
			minimum,
			maximum,
		)
	}
	if APIVersion < minimum || APIVersion > maximum {
		return fmt.Errorf(
			"plugin %q API versions %d..%d are incompatible with SDK API version %d",
			manifest.Name,
			minimum,
			maximum,
			APIVersion,
		)
	}
	return validateUniqueStrings(
		manifest.Name,
		append(
			append([]string(nil), manifest.Registers...),
			append(manifest.Requires, manifest.Conflicts...)...,
		),
	)
}

func (manifest Manifest) APIRange() (int, int) {
	minimum := manifest.APIVersion
	maximum := manifest.APIVersion
	if manifest.MinAPIVersion != 0 {
		minimum = manifest.MinAPIVersion
	}
	if manifest.MaxAPIVersion != 0 {
		maximum = manifest.MaxAPIVersion
	}
	return minimum, maximum
}

type Registrar interface {
	RegisterProvider(Provider) error
	RegisterTool(Tool) error
	RegisterHook(Hook) error
	RegisterSubscriber(Subscriber) error
	RegisterCapability(Capability) error
	RegisterEvent(EventContract) error
}

type Plugin interface {
	Manifest() Manifest
	Install(context.Context, Registrar) error
}

type PluginFunc struct {
	PluginManifest Manifest
	InstallFunc    func(context.Context, Registrar) error
}

func (plugin PluginFunc) Manifest() Manifest {
	return plugin.PluginManifest
}

func (plugin PluginFunc) Install(
	ctx context.Context,
	registrar Registrar,
) error {
	if plugin.InstallFunc == nil {
		return errors.New("plugin install function is nil")
	}
	return plugin.InstallFunc(ctx, registrar)
}

type Connection interface {
	Plugin
	Close(context.Context) error
}

type Source interface {
	Open(context.Context) (Connection, error)
	String() string
}

func ProviderResource(name string) string {
	return "provider:" + name
}

func ToolResource(name string) string {
	return "tool:" + name
}

func HookResource(name string) string {
	return "hook:" + name
}

func SubscriberResource(name string) string {
	return "subscriber:" + name
}

func CapabilityResource(name string) string {
	return "capability:" + name
}

func EventResource(name string) string {
	return "event:" + name
}

func PluginResource(name string) string {
	return "plugin:" + name
}

func normalizeResources(resources []string) []string {
	normalized := append([]string(nil), resources...)
	slices.Sort(normalized)
	return slices.Compact(normalized)
}

func NormalizeResources(resources []string) []string {
	return normalizeResources(resources)
}

func validateResourceName(kind, name string) error {
	if !resourceNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%s name %q must match %s",
			kind,
			name,
			resourceNamePattern,
		)
	}
	return nil
}

func ValidateResourceName(kind, name string) error {
	return validateResourceName(kind, name)
}

func validateUniqueStrings(owner string, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("plugin %q contains an empty resource reference", owner)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf(
				"plugin %q contains duplicate resource reference %q",
				owner,
				value,
			)
		}
		seen[value] = struct{}{}
	}
	return nil
}
