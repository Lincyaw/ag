package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag/agent"

var (
	ErrMaxTurns = errors.New("maximum agent turns reached")
	namePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

type Message struct {
	Role       Role
	Content    string
	ToolCalls  []ToolCall
	ToolCallID string
}

type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

type ModelRequest struct {
	Messages []Message
	Tools    []ToolSpec
}

type ModelResponse struct {
	Content      string
	ToolCalls    []ToolCall
	Model        string
	FinishReason string
	Usage        Usage
}

type Provider interface {
	Name() string
	Model() string
	Complete(context.Context, ModelRequest) (ModelResponse, error)
}

type ToolSpec struct {
	Name        string
	Description string
	Parameters  map[string]any
}

type Tool interface {
	Spec() ToolSpec
	Call(context.Context, json.RawMessage) (string, error)
}

type Host interface {
	RegisterProvider(Provider) error
	RegisterTool(Tool) error
}

type Plugin interface {
	Name() string
	Install(Host) error
}

type PluginFunc struct {
	PluginName string
	InstallFn  func(Host) error
}

func (p PluginFunc) Name() string {
	return p.PluginName
}

func (p PluginFunc) Install(host Host) error {
	if p.InstallFn == nil {
		return errors.New("plugin install function is nil")
	}
	return p.InstallFn(host)
}

type Config struct {
	MaxTurns int
	Logger   *slog.Logger
	Tracer   trace.Tracer
	Meter    metric.Meter
}

type Result struct {
	Output    string
	Messages  []Message
	Turns     int
	ToolCalls int
}

type Agent struct {
	provider   Provider
	tools      map[string]Tool
	toolSpecs  []ToolSpec
	maxTurns   int
	logger     *slog.Logger
	tracer     trace.Tracer
	runs       metric.Int64Counter
	modelCalls metric.Int64Counter
	toolCalls  metric.Int64Counter
}

type registry struct {
	provider Provider
	tools    map[string]Tool
}

func (r *registry) RegisterProvider(provider Provider) error {
	if provider == nil {
		return errors.New("provider is nil")
	}
	if strings.TrimSpace(provider.Name()) == "" {
		return errors.New("provider name is empty")
	}
	if strings.TrimSpace(provider.Model()) == "" {
		return errors.New("provider model is empty")
	}
	if r.provider != nil {
		return fmt.Errorf(
			"provider %q already registered; cannot register %q",
			r.provider.Name(),
			provider.Name(),
		)
	}
	r.provider = provider
	return nil
}

func (r *registry) RegisterTool(tool Tool) error {
	if tool == nil {
		return errors.New("tool is nil")
	}
	spec := tool.Spec()
	if err := validateToolSpec(spec); err != nil {
		return err
	}
	if _, exists := r.tools[spec.Name]; exists {
		return fmt.Errorf("tool %q already registered", spec.Name)
	}
	r.tools[spec.Name] = tool
	return nil
}

func validateToolSpec(spec ToolSpec) error {
	if !namePattern.MatchString(spec.Name) {
		return fmt.Errorf(
			"tool name %q must match %s",
			spec.Name,
			namePattern.String(),
		)
	}
	if strings.TrimSpace(spec.Description) == "" {
		return fmt.Errorf("tool %q description is empty", spec.Name)
	}
	if spec.Parameters == nil {
		return fmt.Errorf("tool %q parameters schema is nil", spec.Name)
	}
	return nil
}

func New(config Config, plugins ...Plugin) (*Agent, error) {
	if config.MaxTurns == 0 {
		config.MaxTurns = 8
	}
	if config.MaxTurns < 1 {
		return nil, errors.New("max turns must be positive")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Tracer == nil {
		config.Tracer = otel.Tracer(instrumentationName)
	}
	if config.Meter == nil {
		config.Meter = otel.Meter(instrumentationName)
	}

	reg := &registry{tools: make(map[string]Tool)}
	seenPlugins := make(map[string]struct{}, len(plugins))
	for _, plugin := range plugins {
		if plugin == nil {
			return nil, errors.New("plugin is nil")
		}
		name := strings.TrimSpace(plugin.Name())
		if name == "" {
			return nil, errors.New("plugin name is empty")
		}
		if _, exists := seenPlugins[name]; exists {
			return nil, fmt.Errorf("plugin %q installed twice", name)
		}
		seenPlugins[name] = struct{}{}
		if err := plugin.Install(reg); err != nil {
			return nil, fmt.Errorf("install plugin %q: %w", name, err)
		}
	}
	if reg.provider == nil {
		return nil, errors.New("no provider registered")
	}

	toolSpecs := make([]ToolSpec, 0, len(reg.tools))
	for _, tool := range reg.tools {
		toolSpecs = append(toolSpecs, tool.Spec())
	}
	sort.Slice(toolSpecs, func(i, j int) bool {
		return toolSpecs[i].Name < toolSpecs[j].Name
	})

	runs, err := config.Meter.Int64Counter(
		"agent.runs",
		metric.WithDescription("Number of agent runs."),
	)
	if err != nil {
		return nil, fmt.Errorf("create agent.runs counter: %w", err)
	}
	modelCalls, err := config.Meter.Int64Counter(
		"agent.model.calls",
		metric.WithDescription("Number of model calls."),
	)
	if err != nil {
		return nil, fmt.Errorf("create agent.model.calls counter: %w", err)
	}
	toolCalls, err := config.Meter.Int64Counter(
		"agent.tool.calls",
		metric.WithDescription("Number of tool calls."),
	)
	if err != nil {
		return nil, fmt.Errorf("create agent.tool.calls counter: %w", err)
	}

	return &Agent{
		provider:   reg.provider,
		tools:      reg.tools,
		toolSpecs:  toolSpecs,
		maxTurns:   config.MaxTurns,
		logger:     config.Logger,
		tracer:     config.Tracer,
		runs:       runs,
		modelCalls: modelCalls,
		toolCalls:  toolCalls,
	}, nil
}

func (a *Agent) Run(ctx context.Context, prompt, system string) (Result, error) {
	if strings.TrimSpace(prompt) == "" {
		return Result{}, errors.New("prompt is empty")
	}

	ctx, span := a.tracer.Start(
		ctx,
		"invoke_agent",
		trace.WithAttributes(
			semconv.GenAIOperationNameInvokeAgent,
			semconv.GenAIAgentNameKey.String("ag"),
			attribute.Int("agent.max_turns", a.maxTurns),
			attribute.Int("agent.prompt_chars", len(prompt)),
		),
	)
	defer span.End()

	a.runs.Add(ctx, 1)
	messages := make([]Message, 0, 2+a.maxTurns*2)
	if strings.TrimSpace(system) != "" {
		messages = append(messages, Message{Role: RoleSystem, Content: system})
	}
	messages = append(messages, Message{Role: RoleUser, Content: prompt})

	result := Result{Messages: messages}
	a.logger.InfoContext(
		ctx,
		"agent run started",
		"provider",
		a.provider.Name(),
		"model",
		a.provider.Model(),
		"tools",
		len(a.toolSpecs),
	)

	for turn := 0; turn < a.maxTurns; turn++ {
		response, err := a.complete(ctx, turn, messages)
		if err != nil {
			recordError(span, err)
			result.Messages = append([]Message(nil), messages...)
			result.Turns = turn + 1
			return result, err
		}

		messages = append(messages, Message{
			Role:      RoleAssistant,
			Content:   response.Content,
			ToolCalls: append([]ToolCall(nil), response.ToolCalls...),
		})
		result.Turns = turn + 1

		if len(response.ToolCalls) == 0 {
			result.Output = response.Content
			result.Messages = append([]Message(nil), messages...)
			a.logger.InfoContext(
				ctx,
				"agent run completed",
				"turns",
				result.Turns,
				"tool_calls",
				result.ToolCalls,
				"finish_reason",
				response.FinishReason,
			)
			return result, nil
		}

		for _, call := range response.ToolCalls {
			content, callErr := a.callTool(ctx, turn, call)
			if callErr != nil {
				content = "tool error: " + callErr.Error()
			}
			messages = append(messages, Message{
				Role:       RoleTool,
				Content:    content,
				ToolCallID: call.ID,
			})
			result.ToolCalls++
		}
	}

	err := fmt.Errorf("%w: %d", ErrMaxTurns, a.maxTurns)
	recordError(span, err)
	result.Messages = append([]Message(nil), messages...)
	return result, err
}

func (a *Agent) complete(
	ctx context.Context,
	turn int,
	messages []Message,
) (ModelResponse, error) {
	ctx, span := a.tracer.Start(
		ctx,
		"chat",
		trace.WithAttributes(
			semconv.GenAIOperationNameChat,
			semconv.GenAIProviderNameKey.String(a.provider.Name()),
			semconv.GenAIRequestModelKey.String(a.provider.Model()),
			attribute.Int("agent.turn", turn),
			attribute.Int("agent.message_count", len(messages)),
			attribute.Int("agent.tool_count", len(a.toolSpecs)),
		),
	)
	defer span.End()

	a.modelCalls.Add(
		ctx,
		1,
		metric.WithAttributes(
			semconv.GenAIProviderNameKey.String(a.provider.Name()),
			semconv.GenAIRequestModelKey.String(a.provider.Model()),
		),
	)
	response, err := a.provider.Complete(ctx, ModelRequest{
		Messages: append([]Message(nil), messages...),
		Tools:    append([]ToolSpec(nil), a.toolSpecs...),
	})
	if err != nil {
		recordError(span, err)
		return ModelResponse{}, fmt.Errorf("model completion: %w", err)
	}

	span.SetAttributes(
		semconv.GenAIResponseModelKey.String(response.Model),
		semconv.GenAIUsageInputTokensKey.Int64(response.Usage.InputTokens),
		semconv.GenAIUsageOutputTokensKey.Int64(response.Usage.OutputTokens),
		attribute.String("gen_ai.response.finish_reason", response.FinishReason),
		attribute.Int("agent.response.tool_calls", len(response.ToolCalls)),
	)
	return response, nil
}

func (a *Agent) callTool(
	ctx context.Context,
	turn int,
	call ToolCall,
) (string, error) {
	ctx, span := a.tracer.Start(
		ctx,
		"execute_tool "+call.Name,
		trace.WithAttributes(
			semconv.GenAIOperationNameExecuteTool,
			semconv.GenAIToolNameKey.String(call.Name),
			attribute.String("gen_ai.tool.call.id", call.ID),
			attribute.Int("agent.turn", turn),
		),
	)
	defer span.End()

	a.toolCalls.Add(
		ctx,
		1,
		metric.WithAttributes(semconv.GenAIToolNameKey.String(call.Name)),
	)
	tool, exists := a.tools[call.Name]
	if !exists {
		err := fmt.Errorf("unknown tool %q", call.Name)
		recordError(span, err)
		a.logger.WarnContext(ctx, "tool call rejected", "tool", call.Name, "error", err)
		return "", err
	}

	a.logger.InfoContext(ctx, "tool call started", "tool", call.Name, "call_id", call.ID)
	output, err := tool.Call(ctx, call.Arguments)
	if err != nil {
		recordError(span, err)
		a.logger.WarnContext(ctx, "tool call failed", "tool", call.Name, "error", err)
		return "", err
	}
	a.logger.InfoContext(ctx, "tool call completed", "tool", call.Name)
	return output, nil
}

func recordError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
