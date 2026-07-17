# Gateway

`ag gateway serve` is the multi-user presenter for the SDK runtime. It owns
durable session definitions, validates session-scoped plugin bindings against
the registry, and runs each message as an asynchronous trajectory execution.

The gateway does not hot-reload a shared runtime. A session's plugin
composition is immutable while one execution is active. After that execution
is cancelled or reaches a terminal state, the user may attach or replace a
plugin. The next message builds a new short-lived runtime from the updated
composition and resumes the last committed trajectory checkpoint.

## Configuration

The default configuration path is `~/.ag/config.toml`. A minimal gateway
configuration is:

```toml
[agent]
provider = "openai"
system = "You are a concise agent."
max_turns = 8
timeout = "5m"

[openai]
enabled = true
model = "deepseek-v4-flash"
base_url = "https://ark.cn-beijing.volces.com/api/plan/v3"
max_retries = 2

[plugins]
registry_uri = "grpc://127.0.0.1:9090"
registry_namespace = "default"

[gateway]
listen = "127.0.0.1:8080"
read_header_timeout = "5s"
idle_timeout = "1m"
shutdown_timeout = "10s"
```

When omitted, `gateway.directory` defaults to the resolved
`$HOME/.ag/gateway` path. A configured path is interpreted literally; shell
tilde expansion does not apply inside TOML.

The OpenAI-compatible API key is read from `OPENAI_API_KEY`; it is not stored
in or printed by the `ag` configuration schema.

Start the control plane, one or more standalone plugins, and the gateway:

```bash
ag registry serve

agentm-plugin-file \
  --listen 127.0.0.1:9001 \
  --registry-uri grpc://127.0.0.1:9090 \
  --instance-id file-a \
  --root .

ag gateway serve
```

The gateway writes one human-readable readiness record by default. Use
`ag gateway serve -o json` for a stable machine-readable record.

## HTTP flow

Every user-scoped request currently requires `X-AG-User-ID`. This is an
injectable authentication boundary in the Go package. The built-in command is
intended for loopback use or a trusted reverse proxy that removes any incoming
copy of this header and injects an authenticated identity.

Create a session. Omitted provider, system, and max-turn values use the
configured agent defaults:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/sessions \
  -H 'X-AG-User-ID: alice' \
  -H 'Content-Type: application/json' \
  -d '{"id":"alice-main"}'
```

Discover active plugins and attach one using the session's current revision:

```bash
curl -sS 'http://127.0.0.1:8080/v1/plugins?name=file' \
  -H 'X-AG-User-ID: alice'

curl -sS -X POST \
  http://127.0.0.1:8080/v1/sessions/alice-main/plugins \
  -H 'X-AG-User-ID: alice' \
  -H 'Content-Type: application/json' \
  -d '{"selector":"file@file-a","expected_revision":1}'
```

Submitting a message returns `202 Accepted` with a durable execution ID:

```bash
curl -sS -X POST \
  http://127.0.0.1:8080/v1/sessions/alice-main/messages \
  -H 'X-AG-User-ID: alice' \
  -H 'Content-Type: application/json' \
  -d '{"content":"Inspect the repository."}'
```

Poll or cancel that exact execution:

```bash
curl -sS \
  http://127.0.0.1:8080/v1/sessions/alice-main/executions/EXECUTION_ID \
  -H 'X-AG-User-ID: alice'

curl -sS -X POST \
  http://127.0.0.1:8080/v1/sessions/alice-main/executions/EXECUTION_ID/cancel \
  -H 'X-AG-User-ID: alice'
```

Cancellation is persisted before the in-memory context is stopped. The cancel
response waits for the old execution host to quiesce, so a successful response
is also the boundary after which plugin composition may safely change. Attach
or replace a plugin with the latest session revision, then submit another
message. That message resumes from the last successful checkpoint and uses the
new composition.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness |
| `POST` | `/v1/sessions` | Create a durable user session |
| `GET` | `/v1/sessions` | List the current user's sessions |
| `GET` | `/v1/sessions/{session}` | Read one owned session |
| `GET` | `/v1/plugins` | Discover leased plugin instances |
| `POST` | `/v1/sessions/{session}/plugins` | Attach or replace a plugin |
| `DELETE` | `/v1/sessions/{session}/plugins/{plugin}` | Detach a plugin |
| `POST` | `/v1/sessions/{session}/messages` | Submit an asynchronous execution |
| `GET` | `/v1/sessions/{session}/executions/{execution}` | Poll one execution |
| `POST` | `/v1/sessions/{session}/executions/{execution}/cancel` | Durably cancel one execution |

Session control records are stored under
`gateway.directory/control/sessions.json`. Each session receives an isolated
state namespace under `gateway.directory/state`; trajectory, operation, and
named delivery queue state remain separate SDK ports behind that namespace.

On startup, the gateway scans sessions for pending or lease-expired
executions. It rebuilds the persisted session composition and schedules
recovery. A still-valid worker lease is fenced until it expires. Completed
results are reconstructed from durable trajectory checkpoints, so polling does
not depend on process-local result memory.
