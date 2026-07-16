package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLoggerAddsTraceCorrelation(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(Config{Writer: &output, Format: "json"})
	if err != nil {
		t.Fatal(err)
	}

	traceID, err := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("0102030405060708")
	if err != nil {
		t.Fatal(err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID,
		SpanID:  spanID,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)

	logger.InfoContext(ctx, "hello")

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["trace_id"] != traceID.String() {
		t.Fatalf("trace_id = %v", record["trace_id"])
	}
	if record["span_id"] != spanID.String() {
		t.Fatalf("span_id = %v", record["span_id"])
	}
}

func TestWithHandlerFansOutAttributesAndGroups(t *testing.T) {
	var primary, additional bytes.Buffer
	logger, err := New(Config{Writer: &primary, Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	logger = WithHandler(logger, slog.NewJSONHandler(&additional, nil))
	logger.With("component", "runtime").WithGroup("operation").Info(
		"completed",
		"id",
		"operation-1",
	)
	for name, output := range map[string]*bytes.Buffer{
		"primary": &primary, "additional": &additional,
	} {
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		if record["component"] != "runtime" {
			t.Fatalf("%s component = %#v", name, record["component"])
		}
		operation, ok := record["operation"].(map[string]any)
		if !ok || operation["id"] != "operation-1" {
			t.Fatalf("%s operation = %#v", name, record["operation"])
		}
	}
}
