# AgentM SDK (`ag`)

`ag` is a small, durable, protocol-first agent runtime distilled from AgentM.
The kernel owns mechanisms; providers, tools, policy hooks, event subscribers,
and optional capabilities are plugins.

The same plugin can run in either mode:

- in-process through `sdk.Local(plugin)`;
- out-of-process through the versioned protobuf/gRPC protocol.

Go is a convenience SDK, not the cross-language boundary. Python, Rust, Java,
or any other language can implement `pluginrpc/v1/plugin.proto` and operate as
an independent process.

## What is implemented

- transactional plugin mount/unmount with ownership and immutable snapshots;
- pluggable memory/file/etcd registration and discovery with renewable,
  fenced leases and revision polling;
- async-first provider, tool, and capability operations with
  `Submit / Poll / Cancel`, revision CAS, idempotency, and durable stores;
- same-turn tool fanout with concurrent execution and stable result joins;
- same-process declarative agents, fork/new child trajectories, recursive
  invocation graphs, and structured fanout/DAG workflows;
- short synchronous control hooks plus durable asynchronous event subscribers;
- host outbox and remote-plugin inbox with leases, retries, deduplication, and
  per-trajectory ordering;
- transactional trajectory executions with durable input, renewable worker
  leases, crash/graceful-shutdown recovery, checkpoints, branch inspection,
  and rollback;
- a multi-user HTTP gateway with session-scoped plugin composition,
  asynchronous submit/poll/cancel, and startup execution recovery;
- DuckDB trajectory storage with immutable rows, native fixed-field indexes,
  transactional execution fencing, and indexed analysis queries;
- OpenTelemetry transport instrumentation plus an asynchronous semantic OTel
  subscriber plugin;
- Cobra CLI/config contract with `flag > AGENTM_* > config file > default`
  precedence;
- local OpenAI plus local/standalone root-confined file and bounded bash plugins.

Delivery and operation execution are at-least-once. Plugin side effects must be
idempotent; the SDK does not claim exactly-once execution.

## Build

Requires the Go version declared in `go.mod`.

Build `ag` and link it to the executable used by `~/.ag/ag`:

```bash
make
```

This produces `bin/ag` and links `~/.ag/bin/ag-core` to it. The paths can be
overridden when needed:

```bash
make AG_HOME=/custom/ag-home
make build
make test
make clean
```

Standalone plugin binaries can still be built directly:

```bash
go build -o bin/agentm-plugin-file ./cmd/agentm-plugin-file
go build -o bin/agentm-plugin-bash ./cmd/agentm-plugin-bash
```

## Run locally

The default mounts OpenAI, the read-only file plugin, and the asynchronous OTel
subscriber. The official OpenAI SDK reads the API key from the environment;
there is deliberately no API-key CLI flag. Runtime logs are appended to
`~/.ag/logs/ag.log` by default; pass `--log-console` to additionally show them
on stderr.

```bash
OPENAI_API_KEY=... bin/ag run \
  --cwd . \
  --model gpt-5-mini \
  --session example \
  --prompt "Read README.md and summarize the architecture."
```

Enable bounded shell execution explicitly:

```bash
bin/ag run --bash --prompt "Run go test ./... and explain failures."
```

Enable atomic file writes explicitly with `--write`. File paths stay confined
to the configured root after symlink resolution. Bash inherits no ambient
environment except explicit safe defaults and repeated `--env KEY=VALUE`
entries in the standalone binary.

## Run plugins as independent processes

Each process prints one ready JSON record containing its actual URI. It owns a
durable operation store and inbox beneath `--state-dir`.

```bash
bin/agentm-plugin-file \
  --listen 127.0.0.1:9001 \
  --root . \
  --state-dir .state/file

bin/agentm-plugin-bash \
  --listen 127.0.0.1:9002 \
  --root . \
  --state-dir .state/bash
```

Mount them explicitly in the CLI:

```bash
bin/ag run \
  --file=false --bash=false \
  --plugin file=grpc://127.0.0.1:9001 \
  --plugin bash=grpc://127.0.0.1:9002 \
  --prompt "Inspect the workspace, then run the tests."
```

The [independent Python example](examples/python-plugin/README.md) implements
the same protocol without importing the Go SDK. It includes provider, tool,
capability, hook, subscriber, async operation, and registry lease behavior.

Remote aliases share the plugin namespace with local plugins. Disable a local
plugin (for example, `--file=false`) before mounting a remote plugin under the
same name; `ag plugin inspect grpc://host:port` can inspect a URI directly.

Use `--registry-uri` and `--lease-ttl` on a standalone plugin to register and
renew a discovery lease. Discovery never implies execution: `ag plugin
discover` lists active leases, while `ag run` mounts only explicitly configured
plugins. `--tls-cert` and `--tls-key` enable a `grpcs://` server.

Run the local durable registry and select an instance explicitly:

```bash
ag registry serve

ag plugin discover --name file
ag run --file=false \
  --plugin file@file-node-a \
  --prompt "Inspect the workspace"
```

Use `--registry-backend etcd://host:2379/ag/registry` for a distributed
registry. See [docs/registry.md](docs/registry.md) for identity, lease, Poll,
backend, and compaction semantics.

## CLI

```text
ag run
ag config show
ag config path
ag plugin list
ag plugin discover
ag plugin inspect <name[@instance-id]|uri>
ag registry serve
ag gateway serve
ag trajectory list
ag trajectory show <id> [--head <entry-id>]
ag trajectory rollback <id> <checkpoint-id>
ag invocation show <root-invocation-id>
ag state inspect
ag state prune --before <RFC3339-or-duration>
ag version
```

Use a local DuckDB trajectory backend with:

```text
ag --storage 'duckdb:///absolute/path/agent-state.duckdb' run
```

Business output is written to stdout. Diagnostics and structured logs are
written to stderr. The default output is human-readable text; explicitly pass
`-o json` (or `--output json`) to any command for stable machine-readable
output. JSON `ag run` output includes the generated trajectory ID. Use `ag run
--resume <id>` to restore the last committed checkpoint and continue.

See [docs/cli.md](docs/cli.md) for the text/JSON schemas, stream boundary, and
exit-status contract. See [docs/gateway.md](docs/gateway.md) for the
multi-session HTTP workflow, plugin switching, cancellation, and recovery
semantics.

Configuration files may be TOML, YAML, or JSON. The default path is shown by
`ag config path`; `AGENTM_CONFIG` or `--config` selects another file. Secret
values are not represented in the config schema or `ag config show`.

## OpenTelemetry

OTLP/HTTP traces, metrics, and opt-in logs follow standard `OTEL_*` environment variables.
Runtime transport/mount/dispatch instrumentation is mechanism-level. Semantic
run, turn, provider, tool, and trajectory telemetry is projected from durable
events by `plugins/otel`; it is not hard-coded into the agent loop.

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp
export OTEL_LOGS_EXPORTER=otlp
```

The Go logs SDK/exporter is currently beta/experimental upstream, so logs stay
disabled unless a logs endpoint or `OTEL_LOGS_EXPORTER=otlp` is configured.
When enabled, `slog` records are fanned out to stderr and an asynchronous OTLP
batch processor.

## Verify

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go build ./cmd/ag ./cmd/agentm-plugin-file ./cmd/agentm-plugin-bash
```

The integration suite builds and starts real Go and Python standalone plugin
processes, performs protobuf `Submit / Poll / Cancel` calls, verifies lease
renewal and cleanup, and exercises CLI trajectory resume/rollback through a
real OpenAI-compatible HTTP server.

Start with [docs/architecture.md](docs/architecture.md) for the domain map and
application entry points. See
[docs/pluggable-sdk.md](docs/pluggable-sdk.md) for the normative SDK contract
and [decisions.md](decisions.md) for accepted design decisions.
