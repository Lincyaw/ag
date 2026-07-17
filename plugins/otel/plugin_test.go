package otelplugin

import (
	"context"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestPluginProjectsOrderedEventsIntoSpansAndMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	defer func() {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	}()
	metricReader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	defer func() {
		if err := meterProvider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown meter provider: %v", err)
		}
	}()
	plugin, err := New(Config{
		Tracer: tracerProvider.Tracer("test"),
		Meter:  meterProvider.Meter("test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := sdkstorage.NewMemoryStateBackend()
	deliveries, err := backend.Deliveries(sdk.HostOutboxQueue)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := agentruntime.NewRuntime(agentruntime.RuntimeConfig{
		Storage:         backend,
		DeliveryWorkers: 4,
		DeliveryPoll:    time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := runtime.Close(closeCtx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	}()
	if _, err := runtime.Mount(ctx, sdk.Local(plugin)); err != nil {
		t.Fatalf("mount OTel plugin: %v", err)
	}

	sessionID := "otel-session"
	emit := func(name string, payload any) {
		t.Helper()
		if _, emitErr := runtime.Emit(ctx, name, sessionID, payload); emitErr != nil {
			t.Fatalf("emit %s: %v", name, emitErr)
		}
	}
	emit(sdk.EventBeforeAgentStart, sdk.BeforeAgentStartPayload{})
	emit(sdk.EventTurnStart, sdk.TurnStartPayload{Turn: 0})
	emit(sdk.EventBeforeProvider, sdk.BeforeProviderPayload{
		Turn:     0,
		Provider: "scripted",
		Messages: []sdk.Message{{Role: sdk.RoleUser, Content: "secret not exported"}},
		Tools:    []sdk.ToolSpec{{Name: "echo", Description: "echo", Parameters: map[string]any{}}},
	})
	emit(sdk.EventAfterProvider, sdk.AfterProviderPayload{
		Turn:     0,
		Provider: "scripted",
		Response: &sdk.ModelResponse{
			Model:        "scripted-v1",
			FinishReason: "tool_calls",
			Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 3},
		},
	})
	emit(sdk.EventBeforeTool, sdk.BeforeToolPayload{
		Turn: 0,
		Call: sdk.ToolCall{ID: "call-1", Name: "echo", Arguments: []byte(`{"secret":"not exported"}`)},
	})
	emit(sdk.EventAfterTool, sdk.AfterToolPayload{
		Turn:   0,
		Call:   sdk.ToolCall{ID: "call-1", Name: "echo"},
		Result: sdk.ToolResult{Content: "secret result not exported"},
	})
	emit(sdk.EventTurnEnd, sdk.TurnEndPayload{Turn: 0, Action: sdk.Action{Kind: sdk.ActionStop}})
	emit(sdk.EventTrajectoryAppend, sdk.TrajectoryEventPayload{
		TrajectoryID: sessionID,
		EntryID:      "entry-1",
		EntryKind:    sdk.TrajectoryKindCheckpoint,
	})
	emit(sdk.EventAgentEnd, sdk.AgentEndPayload{Cause: sdk.Cause{Code: "model_end"}})

	eventually(t, time.Second, func() bool {
		records, listErr := deliveries.List(ctx)
		if listErr != nil || len(records) < 10 {
			return false
		}
		for _, delivery := range records {
			if delivery.State != sdk.DeliveryDelivered {
				return false
			}
		}
		return true
	})

	ended := spanRecorder.Ended()
	if len(ended) != 4 {
		t.Fatalf("ended spans = %d, want 4: %#v", len(ended), ended)
	}
	byName := make(map[string]sdktrace.ReadOnlySpan, len(ended))
	for _, span := range ended {
		byName[span.Name()] = span
	}
	run := byName["agent.run"]
	turn := byName["agent.turn"]
	provider := byName["gen_ai.chat scripted"]
	tool := byName["gen_ai.tool echo"]
	if run == nil || turn == nil || provider == nil || tool == nil {
		t.Fatalf("span names = %v", spanNames(ended))
	}
	if turn.Parent().SpanID() != run.SpanContext().SpanID() {
		t.Fatal("turn span is not a child of run span")
	}
	if provider.Parent().SpanID() != turn.SpanContext().SpanID() ||
		tool.Parent().SpanID() != turn.SpanContext().SpanID() {
		t.Fatal("provider/tool spans are not children of the turn span")
	}
	for _, span := range ended {
		for _, attribute := range span.Attributes() {
			value := attribute.Value.AsString()
			if value == "secret not exported" || value == "secret result not exported" {
				t.Fatalf("sensitive content exported on span %q", span.Name())
			}
		}
	}

	var metrics metricdata.ResourceMetrics
	if err := metricReader.Collect(ctx, &metrics); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	want := map[string]int64{
		"agentm.agent.runs":         1,
		"agentm.provider.calls":     1,
		"agentm.tool.calls":         1,
		"agentm.trajectory.entries": 1,
	}
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			if expected, exists := want[metric.Name]; exists {
				if total != expected {
					t.Fatalf("metric %s = %d, want %d", metric.Name, total, expected)
				}
				delete(want, metric.Name)
			}
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing metrics: %v", want)
	}
}

func TestDuplicateStartEventsEndReplacedSpans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	t.Cleanup(func() {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})
	plugin, err := New(Config{Tracer: tracerProvider.Tracer("duplicate-test")})
	if err != nil {
		t.Fatal(err)
	}
	event := sdk.Event{ID: "event", SessionID: "session"}
	for range 2 {
		plugin.startRun(ctx, event)
		plugin.startTurn(ctx, event, sdk.TurnStartPayload{Turn: 0})
		plugin.startProvider(ctx, event, sdk.BeforeProviderPayload{
			Turn: 0, Provider: "provider",
		})
		plugin.startTool(ctx, event, sdk.BeforeToolPayload{
			Turn: 0,
			Call: sdk.ToolCall{ID: "call", Name: "tool"},
		})
	}
	if err := plugin.Close(ctx); err != nil {
		t.Fatal(err)
	}

	counts := make(map[string]int)
	for _, span := range spanRecorder.Ended() {
		counts[span.Name()]++
	}
	for _, name := range []string{
		"agent.run",
		"agent.turn",
		"gen_ai.chat provider",
		"gen_ai.tool tool",
	} {
		if counts[name] != 2 {
			t.Fatalf("ended %q spans = %d, want 2; all=%v", name, counts[name], counts)
		}
	}
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	result := make([]string, len(spans))
	for index, span := range spans {
		result[index] = span.Name()
	}
	return result
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
