package telemetry

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestExporterEnabled(t *testing.T) {
	tests := []struct {
		name      string
		selector  string
		endpoint  bool
		want      bool
		wantError bool
	}{
		{name: "disabled by default", want: false},
		{name: "endpoint enables exporter", endpoint: true, want: true},
		{name: "explicit otlp", selector: "otlp", want: true},
		{name: "explicit none", selector: "none", endpoint: true, want: false},
		{name: "unknown fails", selector: "console", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := exporterEnabled(test.selector, test.endpoint)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("enabled = %v, want %v", got, test.want)
			}
		})
	}
}

func TestValidateHTTPProtocol(t *testing.T) {
	tests := []struct {
		name           string
		signalSpecific string
		common         string
		wantError      bool
	}{
		{name: "default"},
		{name: "common HTTP", common: "http/protobuf"},
		{
			name:           "signal overrides common",
			signalSpecific: "http/protobuf",
			common:         "grpc",
		},
		{name: "grpc rejected", common: "grpc", wantError: true},
		{name: "HTTP JSON rejected", common: "http/json", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateHTTPProtocol(test.signalSpecific, test.common)
			if (err != nil) != test.wantError {
				t.Fatalf("error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestSetupDisabledIgnoresExporterEnvironment(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "unsupported")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_LOGS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	runtime, err := Setup(t.Context(), Config{
		ServiceName: "disabled-test",
		Disabled:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Tracer == nil || runtime.Meter == nil || runtime.LogHandler != nil {
		t.Fatalf("disabled runtime = %#v", runtime)
	}
	if err := runtime.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupFailureDoesNotPublishPartialProviders(t *testing.T) {
	original := otel.GetTracerProvider()
	sentinel := noop.NewTracerProvider()
	otel.SetTracerProvider(sentinel)
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	certificatePath := filepath.Join(t.TempDir(), "ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	})
	if err := os.WriteFile(certificatePath, certificate, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_LOGS_EXPORTER", "none")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE", certificatePath)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf")

	runtime, err := Setup(context.Background(), Config{ServiceName: "telemetry-test"})
	if err == nil {
		if runtime != nil {
			_ = runtime.Shutdown(context.Background())
		}
		t.Fatal("Setup() error = nil, want invalid metric endpoint error")
	}
	if runtime != nil {
		t.Fatalf("Setup() runtime = %#v, want nil", runtime)
	}
	if got := otel.GetTracerProvider(); got != sentinel {
		t.Fatalf("global tracer provider changed after failed setup: %#v", got)
	}
}

func TestRuntimeShutdownRunsClosersOnceInReverseOrder(t *testing.T) {
	shutdownError := errors.New("shutdown failed")
	var calls []string
	runtime := &Runtime{shutdowns: []func(context.Context) error{
		func(context.Context) error {
			calls = append(calls, "first")
			return shutdownError
		},
		func(context.Context) error {
			calls = append(calls, "second")
			return nil
		},
	}}

	for range 2 {
		if err := runtime.Shutdown(context.Background()); !errors.Is(err, shutdownError) {
			t.Fatalf("Shutdown() error = %v, want %v", err, shutdownError)
		}
	}
	if got := strings.Join(calls, ","); got != "second,first" {
		t.Fatalf("shutdown calls = %q, want %q", got, "second,first")
	}
}

func TestSetupExportsCorrelatedSlogAsOTLPProtobuf(t *testing.T) {
	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/logs" {
			http.Error(writer, "unexpected path", http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		requests <- body
		writer.Header().Set("Content-Type", "application/x-protobuf")
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_METRICS_EXPORTER", "none")
	t.Setenv("OTEL_LOGS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", server.URL+"/v1/logs")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL", "http/protobuf")

	runtime, err := Setup(context.Background(), Config{
		ServiceName: "telemetry-test", ServiceVersion: "1.0.0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.LogHandler == nil {
		t.Fatal("OTLP log handler was not configured")
	}
	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	slog.New(runtime.LogHandler).InfoContext(ctx, "exported-log", "operation.id", "operation-1")
	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}

	select {
	case raw := <-requests:
		var request collectorlog.ExportLogsServiceRequest
		if err := proto.Unmarshal(raw, &request); err != nil {
			t.Fatalf("decode OTLP logs: %v", err)
		}
		if len(request.ResourceLogs) != 1 || len(request.ResourceLogs[0].ScopeLogs) != 1 ||
			len(request.ResourceLogs[0].ScopeLogs[0].LogRecords) != 1 {
			t.Fatalf("OTLP log request = %#v", request.ResourceLogs)
		}
		record := request.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		if record.Body.GetStringValue() != "exported-log" ||
			string(record.TraceId) != string(traceID[:]) || string(record.SpanId) != string(spanID[:]) {
			t.Fatalf("OTLP record = %#v", record)
		}
		foundAttribute := false
		for _, attribute := range record.Attributes {
			if attribute.Key == "operation.id" && attribute.Value.GetStringValue() == "operation-1" {
				foundAttribute = true
			}
		}
		if !foundAttribute {
			t.Fatalf("OTLP attributes = %#v", record.Attributes)
		}
	case <-time.After(time.Second):
		t.Fatal("OTLP log exporter did not send a request")
	}
}
