# ag

A deliberately small, pluggable command-line agent extracted from AgentM's
core architectural idea:

> The kernel is mechanism. Providers and tools are plugins.

The project keeps the infrastructure production-shaped while leaving product
features out:

- compile-time Go plugins with a tiny `Plugin -> Host` registration contract;
- a provider-neutral model/tool loop;
- the official OpenAI Go SDK (no hand-written API HTTP or SSE protocol);
- native `log/slog` structured logging;
- native OpenTelemetry traces and metrics with optional OTLP/HTTP export;
- trace/span IDs injected into logs;
- OTel-instrumented HTTP transport for model requests;
- two bounded, read-only workspace tools.

It intentionally does not include a TUI, persistence, hot reload, multi-agent
orchestration, write tools, approvals, context compaction, or background jobs.

## Core flow

```text
plugins install
    ├── provider
    └── tools
          ↓
user prompt → provider → assistant
                         ├── no tool calls → return text
                         └── tool calls → execute → append results → provider
```

The `agent` package imports no concrete provider or tool package. Replacing the
model or tool set means changing only the composition root in
`cmd/ag`.

Go plugins are normal packages linked into the binary, not `.so` files. This is
intentional: the contracts stay type-checked and the binary remains portable
across macOS, Linux, and Windows.

## Build and run

Requires Go 1.24+.

```bash
go build -o bin/ag ./cmd/ag

OPENAI_API_KEY=... \
  ./bin/ag \
  --cwd . \
  --model gpt-5-mini \
  -p "Read README.md and summarize the architecture."
```

For an OpenAI-compatible endpoint:

```bash
OPENAI_API_KEY=... \
OPENAI_BASE_URL=http://localhost:8000/v1 \
OPENAI_MODEL=my-model \
  go run ./cmd/ag -p "List the files in this workspace."
```

Secrets are read from the environment by the official SDK; there is no API-key
CLI flag.

Business output is written to stdout. Logs, warnings, and errors are written to
stderr.

## Logging

The defaults are JSON logs at `info` level:

```bash
LOG_LEVEL=debug LOG_FORMAT=text \
  go run ./cmd/ag -p "List the root directory."
```

When a log record is emitted inside a span, it includes `trace_id` and
`span_id`. Prompt text, tool arguments, API keys, and response bodies are not
logged.

## OpenTelemetry

Without OTLP configuration, OTel uses its standard no-op providers.

Setting a shared or signal-specific endpoint enables OTLP/HTTP export:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=ag

go run ./cmd/ag -p "Read README.md."
```

The binary includes the stable `http/protobuf` exporters. Setting an OTLP
protocol to `grpc` or `http/json` fails at startup instead of being silently
ignored.

You can control each signal explicitly:

```bash
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp

# Or disable one signal even when a shared endpoint is configured.
export OTEL_METRICS_EXPORTER=none
```

The stable trace hierarchy is:

```text
invoke_agent
  ├── chat
  │    └── HTTP client span from otelhttp
  ├── execute_tool read_file
  └── chat
       └── HTTP client span from otelhttp
```

The kernel also records `agent.runs`, `agent.model.calls`, and
`agent.tool.calls` counters. OTel log export is not a hard dependency because
the Go OTLP log exporter is still experimental; `slog` remains the durable log
contract and carries trace correlation.

## Plugin contract

The complete plugin surface is intentionally small:

```go
type Plugin interface {
    Name() string
    Install(Host) error
}

type Host interface {
    RegisterProvider(Provider) error
    RegisterTool(Tool) error
}
```

A provider supplies one model completion operation. A tool supplies a JSON
Schema and one call operation. The host rejects duplicate providers, duplicate
tools, malformed names, and incomplete schemas during startup.

## Why Chat Completions in the OpenAI plugin?

The official SDK documents Responses as the primary OpenAI API, but Chat
Completions remains supported and maps directly onto a provider-neutral,
replayable message transcript. That keeps the kernel stateless and also
preserves compatibility with many OpenAI-compatible endpoints. The HTTP client,
authentication, retries, serialization, and response parsing still come from
the official SDK.

## Verify

```bash
gofmt -w .
go vet ./...
go test ./...
go build ./cmd/ag
```
