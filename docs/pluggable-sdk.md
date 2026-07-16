# Pluggable SDK design

Status: active implementation contract

API version: 1

Go baseline: 1.26

## Product definition

`ag` is a protocol-first, SDK-hosted agent runtime. The executable in `cmd/`
is one presenter and composition example, not the product boundary. A future
gateway is another peer presenter built only from the public SDK.

The kernel owns only mechanisms that cannot be moved into a plugin:

- immutable registry snapshots and generation publication;
- transactional plugin mount and ownership-based unmount;
- deterministic hook dispatch;
- the provider/tool agent loop;
- context cancellation and lifecycle;
- durable trajectory commit, restore, fork, and rollback mechanisms;
- complete lifecycle events needed by optional observability projections.

Providers, tools, policies, context rewriting, approval gates, retry behavior,
and new capabilities are plugins. A new policy or capability must not require a
kernel edit when it can be expressed through an existing execution boundary.

The wire protocol, not the Go package, is the cross-language plugin contract.
A plugin may be written in Go, Python, Rust, Java, or any language capable of
serving the protocol. It can run and be tested without importing the Go SDK.
The initial deployment modes are local registration in the host process and a
remote RPC connection. Both are ordinary `Source` implementations and have
identical mount, ownership, dispatch, and unmount behavior.

## Public dependency direction

```text
application / CLI / gateway
            |
            v
           sdk
            |
            v
      plugin protocol
            ^
            |
 standalone plugins and optional language helpers

sdk implementation -> internal mechanisms only
kernel -> no concrete plugin package
```

Presenters import the public `sdk` package and never runtime internals.
Plugins implement the versioned protocol. In-process Go plugins may use public
SDK interfaces as a convenience, but must never import runtime internals or
another plugin package.

## Unified plugin entry

The plugin entry is split into three concepts:

```go
type PluginRegistry struct { ... }
type PluginDriver interface { ... }
type Source interface { ... }
```

`PluginRegistry` stores named registrations and performs discovery.

Registration/discovery is the control plane. Mounting is the execution plane.
A discovered plugin is never executed merely because it appeared in a
registry. Composition policy explicitly selects which registrations to mount.

Registry backends support leased registrations. A standalone plugin starts its
protocol server, registers its endpoint and public metadata, renews its lease
while healthy, unregisters on graceful shutdown, and expires automatically
after missed renewals. The initial backend is in-memory and can be exposed
through the gRPC registry service. Persistent or external service-discovery
backends implement the same port without changing `Runtime`.

`PluginDriver` resolves one URI scheme into a `Source`. The initial drivers are:

- direct in-process `Source` registrations;
- `grpc://` for an insecure development endpoint;
- `grpcs://` for a TLS remote endpoint.

Future `exec://` and `wasm://` support is added by registering another driver,
not by changing `Runtime`.

`Source.Open` returns a `Connection`. A connection exposes the same logical
`Plugin` contract regardless of whether it is an in-process object or an RPC
proxy.

```text
Registry.Resolve
      |
      v
    Source.Open
      |
      v
   Connection
      |
      v
 Runtime.Mount
```

## Plugin contract

Each plugin has a manifest:

```go
type Manifest struct {
    Name        string
    Version     string
    Description string
    APIVersion  int
    Requires    []string
    Conflicts   []string
    Registers   []string
}
```

Contribution resource IDs are stable strings:

```text
provider:<name>
tool:<name>
hook:<name>
capability:<name>
event:<name>
plugin:<name>
```

During mount, the plugin installs into a staging registrar. The runtime compares
the exact staged resource set with `Manifest.Registers`. Missing, extra, or
duplicate registrations fail before publication.

The initial contribution kinds are:

- `Provider`: model completion;
- `Tool`: JSON-schema-described operation;
- `Hook`: synchronous policy effect required by the current boundary;
- `Subscriber`: passive asynchronous event consumer;
- `Capability`: generic JSON request/response operation;
- `EventContract`: custom event declaration.

Provider and Tool are specialized capabilities because the agent loop needs
their typed contracts directly.

## Transactional mount and unmount

Mount is copy-on-write:

```text
open source
  -> validate manifest
  -> install into staging registrar
  -> validate declared/actual resources
  -> validate requirements and conflicts
  -> clone current snapshot
  -> add owned contributions
  -> publish one new generation atomically
```

Any error before publication leaves the active registry unchanged.

Every contribution records its mount owner. Unmount removes all owned
contributions in one new snapshot. The unmount is rejected if another mounted
plugin would lose a required resource.

An active turn leases one immutable snapshot. The provider request advertises
tools from that snapshot, and returned tool calls execute against the same
snapshot. A mount or unmount committed during the turn becomes visible at the
next turn boundary.

Retired plugin connections close only after every snapshot lease that references
them has been released.

## Event and effect contract

Local and RPC plugins must have identical semantics. Therefore hooks do not
mutate Go pointers. They receive a serialized event envelope and return an
explicit effect:

```go
type Effect struct {
    Patch  map[string]json.RawMessage
    Block  *Block
    Action *Action
}
```

`EventContract.MutableFields` is the allow-list for top-level patches.
`AllowBlock` and `AllowAction` separately authorize control effects. The runtime
rejects undeclared effects.

Hooks execute serially:

```text
PRE (100) -> NORMAL (500) -> POST (900)
```

Registration order is stable inside one priority. Each patch is applied before
the next hook receives the event.

Hooks are synchronous only because their Patch, Block, or Action is required
before the loop can cross the boundary. They have an explicit short timeout;
active hooks default to `fail_closed`, so a broken permission or mutation
policy cannot silently disappear.

Passive observation is not a Hook. A `Subscriber` receives an immutable event
from a durable outbox through its inbox. Delivery is asynchronous and cannot
delay or fail the producer. Subscribers cannot patch, block, or choose an
action. The OpenTelemetry projection is the first built-in subscriber plugin.

Each delivery has a stable delivery ID, event ID, subscription ID, attempt,
and timestamp. Delivery is at-least-once: the receiver durably deduplicates by
delivery ID before acknowledging. A missing acknowledgement causes retry with
bounded exponential backoff and eventually a dead-letter state. Ordering is
preserved per trajectory, not globally.

The initial kernel event boundaries are:

```text
before_agent_start
agent_start
turn_start
before_provider
after_provider
before_tool
tool_error
after_tool
decide
turn_end
agent_end
plugin_mounted
plugin_unmounted
trajectory_appended
trajectory_restored
trajectory_rolled_back
```

Plugins may register custom events and applications may emit them through the
SDK.

## Asynchronous operation contract

Provider, tool, and generic capability execution use one operation state
machine, regardless of local or RPC deployment:

```text
Submit(command, idempotency_key)
  -> pending | running | succeeded | failed | cancelled
Poll(operation_id, revision)
Watch(operation_id, after_revision)  # optional streaming optimization
Cancel(operation_id)
```

`Submit` returns quickly with a stable operation ID. A local implementation may
return an already-succeeded operation. A remote plugin persists the command in
its inbox before acknowledging Submit, executes it independently, and writes
state transitions/results to its outbox. Poll is the portable baseline; Watch
only reduces polling latency and must be reconnectable from a revision.

The host records submitted, suspended, resumed, and terminal operation states
in trajectory. A CLI request need not hold the agent loop in memory: a worker
can resume the trajectory when the operation result arrives. Deadlines and
cancellation are themselves persisted commands; cancellation is best-effort
once an external side effect has started.

Operation IDs and idempotency keys are distinct. Retrying Submit with the same
idempotency key returns the original operation. Receivers may execute a command
more than once after a crash unless the concrete effect is idempotent or uses a
plugin-owned transaction. The SDK therefore guarantees at-least-once delivery,
not exactly-once effects.

## Inbox and outbox

Both host and standalone plugin expose the same logical queues:

```text
host outbox --event/command--> plugin inbox
host inbox  <--state/result--- plugin outbox
```

An outbox record is appended before publication. An inbox record is persisted
and deduplicated before acknowledgement. Dispatch workers lease records, renew
while processing, retry expired leases, and move repeatedly failing records to
a dead-letter state. Queue storage is a pluggable SDK port; the first durable
implementation targets one process/host and the RPC protocol remains suitable
for external brokers later.

## Trajectory contract

A trajectory is the durable, append-only record needed to reconstruct agent
state. It is distinct from telemetry.

```go
type TrajectoryEntry struct {
    ID         string
    ParentID   string
    Kind       string
    Timestamp  time.Time
    Generation uint64
    Payload    json.RawMessage
}

type TrajectoryStore interface {
    Create(...)
    Append(... expectedHead ...)
    Load(...)
    List(...)
}
```

Entries form a tree through `ParentID`; each trajectory has one active head.
Append uses compare-and-swap on the expected head so two writers cannot
silently overwrite each other.

Every user message, provider response, tool result, policy decision, and
terminal cause is recorded. A clean turn ends with a committed checkpoint.
Restore materializes only the branch ending at the latest selected committed
checkpoint; incomplete tail entries remain observable but are not replayed.

Rollback never deletes history. It appends a rollback entry whose parent is
the selected committed entry and moves the active head atomically. Fork creates
a new trajectory with lineage back to the source trajectory and entry.

Rollback restores agent/runtime state only. External effects such as file
writes or network calls require an explicit compensating capability from the
owning plugin; the kernel never pretends those effects were undone.

`TrajectoryStore` is pluggable. The SDK provides an in-memory implementation
for tests and a durable local implementation for CLI/gateway use.

The loop emits complete events carrying trajectory, entry, turn, plugin, and
generation identifiers. An OpenTelemetry plugin subscribes to those events and
projects semantic traces, metrics, and logs. The core loop does not construct
semantic spans. Transport and dispatch adapters may retain mechanism-level
instrumentation local to those mechanisms. OTel data may be sampled or
exported asynchronously; it is never the sole recovery source.

## Loop decision model

The kernel computes a default action:

- no tool calls: `Stop(model_end)`;
- tool results appended: `Step`;
- hard turn cap: `Stop(max_turns, final=true)`.

The `decide` event may return:

- `Step`;
- `Stop(cause)`;
- `Inject(messages)`.

For a final kernel cause, hook actions are observed but cannot override the
default. Otherwise all `Inject` messages are concatenated in hook order. With
no injection the last `Stop` wins, then any `Step`, then the default action.

## RPC protocol

The wire contract is protobuf in `pluginrpc/v1/plugin.proto`, served with
gRPC. The protocol exposes:

- `Describe`;
- provider completion;
- tool invocation;
- hook invocation;
- generic capability invocation.

The registry service exposes leased `Register`, `Renew`, `Unregister`, and
`List` operations. The plugin service also exposes health so discovery can be
distinguished from readiness.

The host converts the remote description into ordinary SDK proxy
implementations and stages them through the same registrar used by local
plugins. The runtime does not contain a separate remote-plugin registry path.

RPC requirements:

- request context and cancellation propagation;
- explicit insecure development transport (`grpc://`);
- TLS by default for `grpcs://`;
- OpenTelemetry gRPC client/server stats handlers;
- request size limits;
- per-hook timeout support;
- no secret values in plugin catalog or logs;
- W3C/OpenTelemetry context propagation across every RPC;
- graceful lease renewal and expiry for standalone plugins.

Remote plugins cannot call arbitrary in-process Go interfaces. Cross-plugin
communication uses serializable capabilities. Host-callback capability calls
from a remote plugin are outside API v1 and require a later bidirectional
protocol extension.

## CLI and configuration

Cobra owns the command and flag contract. Viper resolves configuration through
Cobra-bound flags:

```text
CLI flag > AGENTM_* environment > config file > built-in default
```

The CLI uses `PluginRegistry` exactly like an embedded application. It does not
construct or access the runtime's private registries.

Planned stable command tree:

```text
ag run
ag config show
ag config path
ag plugin list
ag plugin discover
ag plugin inspect
ag trajectory list
ag trajectory show
ag trajectory rollback
ag version
```

Business output goes to stdout. Diagnostics and logs go to stderr.

## End-to-end acceptance

Completion requires end-to-end tests that:

1. starts a real gRPC plugin server on a loopback TCP listener;
2. registers local and gRPC plugin entries in `PluginRegistry`;
3. discovers and resolves those entries;
4. dynamically mounts a scripted provider, a real tool, and the remote policy;
5. drives model -> remote `before_tool` hook -> blocked tool result -> model;
6. invokes a remote capability and verifies shared remote state;
7. unmounts the remote plugin;
8. drives another turn and verifies the tool now executes;
9. verifies generation changes and lifecycle/OTel evidence;
10. uses the public SDK only, not private runtime fields.

The complete acceptance suite also:

11. starts a standalone plugin process and registers it through a lease;
12. proves lease renewal and expiry under concurrent discovery;
13. propagates cancellation and OTel context across the process boundary;
14. records a multi-turn trajectory with provider, hook, and tool entries;
15. kills/reopens the host and restores from the last committed turn;
16. rolls back to an earlier turn without deleting the abandoned branch;
17. races concurrent sessions with mount/unmount and passes `go test -race`;
18. drives Cobra CLI commands as an external process and verifies stdout,
    stderr, exit codes, JSON output, and persistent trajectory behavior;
19. executes real confined file and bash tools through the public SDK.

Shape-only tests and tests that only check getters do not satisfy this
acceptance criterion.

## Explicit non-goals for API version 1

- Go `.so` loading;
- WASM execution;
- local process supervision for `exec://`;
- plugin-originated asynchronous event streams;
- remote-to-host capability callbacks;
- TUI and gateway protocols;
- multi-agent orchestration.

These may be added through new drivers, capabilities, or protocol versions
without changing the core plugin ownership and event-effect model.
