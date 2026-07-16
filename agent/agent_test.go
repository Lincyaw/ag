package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"slices"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type scriptedProvider struct {
	calls int
	t     *testing.T
}

func (p *scriptedProvider) Name() string {
	return "scripted"
}

func (p *scriptedProvider) Model() string {
	return "test-model"
}

func (p *scriptedProvider) Complete(
	_ context.Context,
	request ModelRequest,
) (ModelResponse, error) {
	p.calls++
	switch p.calls {
	case 1:
		return ModelResponse{
			Model:        p.Model(),
			FinishReason: "tool_calls",
			ToolCalls: []ToolCall{{
				ID:        "call-1",
				Name:      "echo",
				Arguments: json.RawMessage(`{"value":"hello"}`),
			}},
		}, nil
	case 2:
		last := request.Messages[len(request.Messages)-1]
		if last.Role != RoleTool || last.Content != "hello" {
			p.t.Fatalf("unexpected tool result message: %#v", last)
		}
		return ModelResponse{
			Content:      "done",
			Model:        p.Model(),
			FinishReason: "stop",
		}, nil
	default:
		p.t.Fatalf("unexpected model call %d", p.calls)
		return ModelResponse{}, nil
	}
}

type echoTool struct{}

func (echoTool) Spec() ToolSpec {
	return ToolSpec{
		Name:        "echo",
		Description: "Return the supplied value.",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"value": map[string]any{"type": "string"},
			},
			"required":             []string{"value"},
			"additionalProperties": false,
		},
	}
}

func (echoTool) Call(_ context.Context, raw json.RawMessage) (string, error) {
	var args struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return "", err
	}
	return args.Value, nil
}

func TestAgentRunsModelToolModelLoopWithTracing(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
	)
	defer func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	model := &scriptedProvider{t: t}
	app, err := New(
		Config{
			MaxTurns: 4,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
			Tracer:   provider.Tracer("test"),
		},
		PluginFunc{
			PluginName: "model",
			InstallFn: func(host Host) error {
				return host.RegisterProvider(model)
			},
		},
		PluginFunc{
			PluginName: "tools",
			InstallFn: func(host Host) error {
				return host.RegisterTool(echoTool{})
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := app.Run(context.Background(), "say hello", "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "done" {
		t.Fatalf("output = %q, want done", result.Output)
	}
	if result.Turns != 2 || result.ToolCalls != 1 {
		t.Fatalf("unexpected result counters: %#v", result)
	}

	var names []string
	for _, span := range recorder.Ended() {
		names = append(names, span.Name())
	}
	for _, want := range []string{"invoke_agent", "chat", "execute_tool echo"} {
		if !slices.Contains(names, want) {
			t.Fatalf("missing span %q in %v", want, names)
		}
	}
}
