# Background Agent Manager

The `gateway` package is an internal application boundary, not a public daemon
command. `ag run` and `ag trajectory` import its manager API. On first use they
health-check the current background process and, when necessary, start a
detached copy of `ag` through a private environment protocol. There is no
`ag gateway serve` command and no gateway URL flag.

The manager owns durable trajectory hosting: serialized input queues,
execution lifecycle and recovery, plugin composition, pending user
interactions, and an ordered event log. The TUI owns terminal input, rendering,
scrolling, key bindings, and interaction presentation. It obtains the durable
active-branch conversation projection through gRPC and never opens trajectory
storage directly.

## One identity

A trajectory is the only durable user-facing identity.

- `ag run` creates a trajectory and attaches a view.
- `ag run <trajectory-id-or-prefix>` attaches another view, hydrating its
  historical conversation before following new events.
- `ctrl+b` or `ctrl+d` detaches the view while execution remains hosted.
- `ag trajectory list` lists attachable work and its projected state.
- `submit`, `pause`, `resume`, `cancel`, `wait`, `show`, and `rollback` all
  address the same trajectory ID.

An execution is one run within that trajectory. A view is a transient
attachment. The manager's historical Go `Session` structure is an internal
control record; the gRPC contract and CLI do not introduce a second session ID.

## Automatic process lifecycle

Local state lives under `gateway.directory`, which defaults to
`$HOME/.ag/gateway`. Process coordination uses:

```text
gateway.directory/
  managed/
    start.lock
    config.json
    ready.json
    gateway.stderr.log
  control/         # legacy non-SQL session migration source only
  events/          # legacy non-SQL event adapter only
  inputs/          # legacy non-SQL input migration source only
  interactions/    # legacy non-SQL interaction migration source only
```

Runtime durability is selected once through `state.backend_uri`, outside the
manager's control files. The manager derives one namespace per trajectory from
that URI. PostgreSQL is recommended when the manager is persistent, shared, or
remote. With no explicit URI, new local installations use one SQLite database
under `state.directory`; existing DuckDB or legacy file state is opened only
for compatibility.

With a SQLite or PostgreSQL state URI, the complete gateway control plane uses
the same URI, namespace, and GORM schema as runtime state. Sessions, input
queues, interactions, and reconnect events live in normalized
`ag_gateway_*` tables; revision and sequence transitions update one row instead
of rewriting a file aggregate. On first SQL startup, valid legacy control,
input, and interaction files are imported idempotently and then retained only
as rollback evidence.

The reconnect cursor log is a projection for attached views, not the agent's
memory or the source of historical conversation. Trajectory entries remain the
authoritative facts. Before persistence, the gateway removes repeated
conversation snapshots from events such as `turn_end`; reconnect pages are
also bounded by encoded bytes rather than only by item count. The former
`events/events.json` aggregate is read only by the legacy adapter. New legacy
writes are append-only in
`events.journal.jsonl`, so the whole aggregate is not decoded and rewritten on
every runtime event. Existing `events.json` files are left in place and are not
bulk-imported into SQL because their repeated payloads may be very large.

Startup takes an inter-process lock, checks the ready record through gRPC
health, writes a mode-`0600` runtime configuration, and launches the current
executable in a new process session. Concurrent CLIs therefore converge on one
manager. The child binds a random loopback port, reports a `grpc://` target in
`ready.json`, recovers durable executions and queued inputs, and outlives any
one TUI.

Recovery uses the trajectory entry index to find the newest compatible snapshot
and then fetches only message-producing deltas. It does not materialize provider
request payloads or obsolete checkpoints. This is the same projection used by
conversation paging, so restart cost follows the visible branch rather than the
total byte size of historical trajectory rows.

If a recorded process fails its health check, the launcher first verifies that
the ready record belongs to the requested gateway directory, asks that exact
PID to stop, and waits for it to exit before replacing the ready record. It does
not start a second server against the same control directory.

The child mode is selected only by private `AG_INTERNAL_GATEWAY_*` environment
variables. It is intentionally absent from Cobra help and `--dump-schema`.

## gRPC boundary

The protocol is [gatewayrpc/v1/gateway.proto](../gatewayrpc/v1/gateway.proto).
Local and remote clients use the same service. `grpc://` uses plaintext HTTP/2;
`grpcs://` uses TLS and the system trust roots by default.

Short reads and administrative mutations are unary RPCs, including:

- create/get/list/load/rollback trajectory;
- enqueue/get/list/cancel input;
- pause/resume dispatch;
- get/list/resolve interaction;
- get/cancel execution and inject context;
- attach/detach a trajectory plugin.

`Connect` is a persistent bidirectional stream. Its first client frame is
`OpenView(user_id, trajectory_id, after_event)`. The manager replies with
`ViewReady`, then multiplexes two classes of frames on that stream:

- client commands such as enqueue input, cancel, resolve interaction, or
  pause/resume, each carrying a request ID;
- server responses and durable ordered events.

gRPC keeps one HTTP/2 connection alive and multiplexes unary calls with the
view stream. Detaching closes only the stream. It does not cancel the input or
terminate the background runtime. Event sequence numbers remain durable
cursors, so a future reconnect can replay everything after the last observed
event.

This replaces the former HTTP/SSE split. There is no WebSocket transport and no
HTTP REST compatibility endpoint.

## Local and remote configuration

The default is zero-configuration local management:

```toml
[gateway]
target = ""
directory = "/absolute/path/to/.ag/gateway"
shutdown_timeout = "10s"

[state]
backend_uri = "postgresql://agent:password@db/agentm?sslmode=require"
namespace = "team-a"
```

`target = ""` means “health-check or start the local manager.” To use a remote
embedding of `gatewayrpc.NewGRPCServer`, configure one target without changing
CLI arguments:

```toml
[gateway]
target = "grpcs://agents.example.com:443"
```

The repository does not expose a foreground gateway command. A remote host is
an application embedding the `gateway`, `gatewayrpc`, and bootstrap packages;
it supplies listener ownership, TLS credentials, and authentication policy.

## Queue and controls

Each accepted input progresses through `queued`, `dispatching`, and one of
`succeeded`, `failed`, or `cancelled`. A trajectory dispatches at most one input
at a time.

Pause prevents the next dispatch but does not interrupt an execution already in
progress. Cancel requests cancel every non-terminal queued input and the active
execution. Cancellation is persisted before the runtime context is stopped.
`wait` opens a view stream before checking queue state, avoiding the race where
completion occurs between a state read and event subscription.

The `ask_user` tool creates a durable pending interaction. A view resolves it
with an expected revision; cancellation durably cancels the pending interaction
as part of execution teardown.

## Process state and restart

The gateway follows a web-server model. One request or recovered execution is a
goroutine hosted by the gateway process; a background agent is not a dedicated
OS process. Its durable identity is the trajectory plus its input queue,
execution lease, operations, interactions, and event cursor.

In-memory host slots, provider streams, cancellation functions, and shell child
processes are disposable. On restart the manager reloads control records,
reconstructs runtime hosts, waits out any still-valid execution lease, and
continues from the last committed trajectory boundary. Tokens or subprocess
output that had not crossed a durable boundary can be replayed or lost, and
external side effects remain at-least-once/idempotency concerns rather than an
exactly-once guarantee.

SIGTERM uses two phases controlled by `gateway.shutdown_timeout`. First the
gRPC server stops accepting calls and the service enters draining: new
trajectories and inputs receive `Unavailable`, while each active execution is
allowed to finish its current model turn, including tool results and the turn
checkpoint. It then releases its execution lease as pending, so the replacement
process resumes at the next model call. A response that already reached this
checkpoint is not replayed. If the drain deadline expires, `Close` cancels the
remaining provider/tool contexts and performs the ordinary recovery handoff;
that forced path may resume from an earlier committed checkpoint.

## Composition and recovery

A newly created trajectory persists its own runtime profile: local plugin
switches and limits, workspace policy, model selection, and configured remote
plugin references. The manager's process configuration remains responsible for
credentials, logging, listeners, and storage. This lets one long-lived manager
host differently configured agents without whichever CLI started it first
silently defining every later trajectory. The private profile is stored with
the control record but omitted from list/get JSON responses.

A trajectory's plugin composition is immutable while an execution is active.
After the execution becomes terminal, a plugin can be attached, replaced, or
detached; the next prompt builds a short-lived runtime from the new composition
and resumes the last committed checkpoint.

Each trajectory persists its absolute workspace root, so one manager can host
work from multiple repositories. When no remote plugin registry is configured,
the manager embeds the configured durable registry backend.

On startup the manager reads control records and asks the execution backend to
recover each trajectory. The runtime/store remains authoritative for execution
state, fencing, results, and recoverability. Gateway memory only prevents two
active hosts for the same trajectory; it does not invent another lifecycle.

Trajectory `show` and `rollback` also go through gRPC. `show` pages compact
entry summaries at a fixed branch head; it never sends the full aggregate or
repeated checkpoint payloads as one RPC message. Conversation hydration pages
user/assistant content separately with UTF-8-safe chunks and a response byte
budget. The execution backend opens the same per-trajectory state namespace
used by background execution, and rollback first enforces the idle boundary.
Rollback responses omit entries. The CLI never opens a parallel state store for
these operations.
