package otelplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/lincyaw/ag/sdk"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag/plugins/otel"

type Config struct {
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
}

type plugin struct {
	logger            *slog.Logger
	tracer            trace.Tracer
	runs              metric.Int64Counter
	providerCalls     metric.Int64Counter
	toolCalls         metric.Int64Counter
	trajectoryEntries metric.Int64Counter
	mu                sync.Mutex
	runSpans          map[string]trace.Span
	turnSpans         map[string]trace.Span
	providerSpans     map[string]trace.Span
	toolSpans         map[string]trace.Span
}

func New(config Config) (sdk.Plugin, error) {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.Tracer == nil {
		config.Tracer = otel.Tracer(instrumentationName)
	}
	if config.Meter == nil {
		config.Meter = otel.Meter(instrumentationName)
	}
	runs, err := config.Meter.Int64Counter(
		"agentm.agent.runs",
		metric.WithDescription("Agent runs observed from lifecycle events."),
	)
	if err != nil {
		return nil, fmt.Errorf("create run counter: %w", err)
	}
	providerCalls, err := config.Meter.Int64Counter(
		"agentm.provider.calls",
		metric.WithDescription("Provider operations observed from lifecycle events."),
	)
	if err != nil {
		return nil, fmt.Errorf("create provider counter: %w", err)
	}
	toolCalls, err := config.Meter.Int64Counter(
		"agentm.tool.calls",
		metric.WithDescription("Tool operations observed from lifecycle events."),
	)
	if err != nil {
		return nil, fmt.Errorf("create tool counter: %w", err)
	}
	trajectoryEntries, err := config.Meter.Int64Counter(
		"agentm.trajectory.entries",
		metric.WithDescription("Durable trajectory entries observed from events."),
	)
	if err != nil {
		return nil, fmt.Errorf("create trajectory counter: %w", err)
	}
	return &plugin{
		logger:            config.Logger,
		tracer:            config.Tracer,
		runs:              runs,
		providerCalls:     providerCalls,
		toolCalls:         toolCalls,
		trajectoryEntries: trajectoryEntries,
		runSpans:          make(map[string]trace.Span),
		turnSpans:         make(map[string]trace.Span),
		providerSpans:     make(map[string]trace.Span),
		toolSpans:         make(map[string]trace.Span),
	}, nil
}

func (plugin *plugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "otel",
		Version:     "1.0.0",
		Description: "asynchronous OpenTelemetry projection of runtime events",
		APIVersion:  sdk.APIVersion,
		Registers:   []string{sdk.SubscriberResource("otel-events")},
	}
}

func (plugin *plugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	return registrar.RegisterSubscriber(sdk.SubscriberFunc{
		SubscriberSpec: sdk.SubscriberSpec{
			Name: "otel-events",
			Events: []string{
				sdk.EventBeforeAgentStart,
				sdk.EventTurnStart,
				sdk.EventBeforeProvider,
				sdk.EventAfterProvider,
				sdk.EventBeforeTool,
				sdk.EventToolError,
				sdk.EventAfterTool,
				sdk.EventTurnEnd,
				sdk.EventAgentEnd,
				sdk.EventPluginMounted,
				sdk.EventPluginUnmounted,
				sdk.EventTrajectoryAppend,
				sdk.EventTrajectoryRestore,
				sdk.EventTrajectoryRollback,
			},
		},
		ReceiveFunc: plugin.receive,
	})
}

func (plugin *plugin) Close(context.Context) error {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	for _, spans := range []map[string]trace.Span{
		plugin.toolSpans,
		plugin.providerSpans,
		plugin.turnSpans,
		plugin.runSpans,
	} {
		for key, span := range spans {
			span.SetStatus(codes.Error, "observability plugin closed before terminal event")
			span.End()
			delete(spans, key)
		}
	}
	return nil
}

func (plugin *plugin) receive(ctx context.Context, delivery sdk.Delivery) error {
	event := delivery.Event
	switch event.Name {
	case sdk.EventBeforeAgentStart:
		plugin.startRun(ctx, event)
	case sdk.EventTurnStart:
		var payload sdk.TurnStartPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.startTurn(ctx, event, payload)
	case sdk.EventBeforeProvider:
		var payload sdk.BeforeProviderPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.startProvider(ctx, event, payload)
	case sdk.EventAfterProvider:
		var payload sdk.AfterProviderPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.endProvider(event, payload)
	case sdk.EventBeforeTool:
		var payload sdk.BeforeToolPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.startTool(ctx, event, payload)
	case sdk.EventToolError:
		var payload sdk.ToolErrorPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.markToolError(event, payload)
	case sdk.EventAfterTool:
		var payload sdk.AfterToolPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.endTool(event, payload)
	case sdk.EventTurnEnd:
		var payload sdk.TurnEndPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.endSpan(plugin.turnSpans, turnKey(event.SessionID, payload.Turn))
	case sdk.EventAgentEnd:
		var payload sdk.AgentEndPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.endRun(event, payload)
	case sdk.EventTrajectoryAppend:
		var payload sdk.TrajectoryEventPayload
		if err := decode(event, &payload); err != nil {
			return err
		}
		plugin.trajectoryEntries.Add(
			ctx,
			1,
			metric.WithAttributes(attribute.String(
				"agentm.trajectory.entry.kind",
				string(payload.EntryKind),
			)),
		)
	case sdk.EventTrajectoryRestore, sdk.EventTrajectoryRollback:
		plugin.logger.InfoContext(ctx, "trajectory lifecycle", "event", event.Name, "trajectory_id", event.SessionID)
	case sdk.EventPluginMounted, sdk.EventPluginUnmounted:
		plugin.logger.InfoContext(ctx, "plugin lifecycle", "event", event.Name)
	}
	return nil
}

func (plugin *plugin) startRun(ctx context.Context, event sdk.Event) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	_, span := plugin.tracer.Start(
		ctx,
		"agent.run",
		trace.WithAttributes(
			attribute.String("agentm.session.id", event.SessionID),
			attribute.Int64("agentm.registry.generation", int64(event.Generation)),
		),
	)
	replaceSpanLocked(plugin.runSpans, event.SessionID, span)
	plugin.runs.Add(ctx, 1)
}

func (plugin *plugin) startTurn(ctx context.Context, event sdk.Event, payload sdk.TurnStartPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	parent := plugin.parentContext(ctx, event.SessionID)
	_, span := plugin.tracer.Start(parent, "agent.turn", trace.WithAttributes(attribute.Int("agentm.turn", payload.Turn)))
	replaceSpanLocked(
		plugin.turnSpans,
		turnKey(event.SessionID, payload.Turn),
		span,
	)
}

func (plugin *plugin) startProvider(ctx context.Context, event sdk.Event, payload sdk.BeforeProviderPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	parent := plugin.parentContext(ctx, event.SessionID)
	if turn := plugin.turnSpans[turnKey(event.SessionID, payload.Turn)]; turn != nil {
		parent = trace.ContextWithSpan(parent, turn)
	}
	_, span := plugin.tracer.Start(
		parent,
		"gen_ai.chat "+payload.Provider,
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", "chat"),
			attribute.String("gen_ai.provider.name", payload.Provider),
			attribute.Int("agentm.turn", payload.Turn),
			attribute.Int("agentm.message_count", len(payload.Messages)),
			attribute.Int("agentm.tool_count", len(payload.Tools)),
		),
	)
	replaceSpanLocked(
		plugin.providerSpans,
		turnKey(event.SessionID, payload.Turn),
		span,
	)
	plugin.providerCalls.Add(ctx, 1, metric.WithAttributes(attribute.String("gen_ai.provider.name", payload.Provider)))
}

func (plugin *plugin) endProvider(event sdk.Event, payload sdk.AfterProviderPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	key := turnKey(event.SessionID, payload.Turn)
	span := plugin.providerSpans[key]
	if span == nil {
		return
	}
	if payload.Error != "" {
		err := errors.New(payload.Error)
		span.RecordError(err)
		span.SetStatus(codes.Error, payload.Error)
	}
	if payload.Response != nil {
		span.SetAttributes(
			attribute.String("gen_ai.response.model", payload.Response.Model),
			attribute.String("gen_ai.response.finish_reason", payload.Response.FinishReason),
			attribute.Int64("gen_ai.usage.input_tokens", payload.Response.Usage.InputTokens),
			attribute.Int64("gen_ai.usage.output_tokens", payload.Response.Usage.OutputTokens),
			attribute.Int("agentm.response.tool_calls", len(payload.Response.ToolCalls)),
		)
	}
	span.End()
	delete(plugin.providerSpans, key)
}

func (plugin *plugin) startTool(ctx context.Context, event sdk.Event, payload sdk.BeforeToolPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	parent := plugin.parentContext(ctx, event.SessionID)
	if turn := plugin.turnSpans[turnKey(event.SessionID, payload.Turn)]; turn != nil {
		parent = trace.ContextWithSpan(parent, turn)
	}
	_, span := plugin.tracer.Start(
		parent,
		"gen_ai.tool "+payload.Call.Name,
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", "execute_tool"),
			attribute.String("gen_ai.tool.name", payload.Call.Name),
			attribute.String("gen_ai.tool.call.id", payload.Call.ID),
			attribute.Int("agentm.turn", payload.Turn),
		),
	)
	replaceSpanLocked(
		plugin.toolSpans,
		toolKey(event.SessionID, payload.Call.ID),
		span,
	)
	plugin.toolCalls.Add(ctx, 1, metric.WithAttributes(attribute.String("gen_ai.tool.name", payload.Call.Name)))
}

func (plugin *plugin) markToolError(event sdk.Event, payload sdk.ToolErrorPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	if span := plugin.toolSpans[toolKey(event.SessionID, payload.Call.ID)]; span != nil {
		err := errors.New(payload.Reason)
		span.RecordError(err)
		span.SetStatus(codes.Error, string(payload.Kind))
	}
}

func (plugin *plugin) endTool(event sdk.Event, payload sdk.AfterToolPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	key := toolKey(event.SessionID, payload.Call.ID)
	span := plugin.toolSpans[key]
	if span == nil {
		return
	}
	span.SetAttributes(attribute.Bool("agentm.tool.result.error", payload.Result.IsError))
	if payload.Result.IsError {
		span.SetStatus(codes.Error, "tool result is an error")
	}
	span.End()
	delete(plugin.toolSpans, key)
}

func (plugin *plugin) endRun(event sdk.Event, payload sdk.AgentEndPayload) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	plugin.endSessionChildrenLocked(event.SessionID)
	span := plugin.runSpans[event.SessionID]
	if span == nil {
		return
	}
	span.SetAttributes(attribute.String("agentm.cause.code", payload.Cause.Code))
	if payload.Cause.Code != "model_end" && payload.Cause.Code != "max_turns" {
		span.SetStatus(codes.Error, payload.Cause.Code)
	}
	span.End()
	delete(plugin.runSpans, event.SessionID)
}

func (plugin *plugin) endSessionChildrenLocked(sessionID string) {
	prefix := sessionID + "/"
	for _, spans := range []map[string]trace.Span{plugin.toolSpans, plugin.providerSpans, plugin.turnSpans} {
		for key, span := range spans {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				span.SetStatus(codes.Error, "agent ended before paired event")
				span.End()
				delete(spans, key)
			}
		}
	}
}

func (plugin *plugin) parentContext(ctx context.Context, sessionID string) context.Context {
	if run := plugin.runSpans[sessionID]; run != nil {
		return trace.ContextWithSpan(ctx, run)
	}
	return ctx
}

func (plugin *plugin) endSpan(spans map[string]trace.Span, key string) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	if span := spans[key]; span != nil {
		span.End()
		delete(spans, key)
	}
}

func replaceSpanLocked(
	spans map[string]trace.Span,
	key string,
	replacement trace.Span,
) {
	if previous := spans[key]; previous != nil {
		previous.SetStatus(codes.Error, "start event repeated before prior span ended")
		previous.End()
	}
	spans[key] = replacement
}

func decode(event sdk.Event, target any) error {
	if err := json.Unmarshal(event.Payload, target); err != nil {
		return fmt.Errorf("decode %s event %s: %w", event.Name, event.ID, err)
	}
	return nil
}

func turnKey(sessionID string, turn int) string {
	return fmt.Sprintf("%s/%d", sessionID, turn)
}

func toolKey(sessionID, callID string) string {
	return sessionID + "/" + callID
}
