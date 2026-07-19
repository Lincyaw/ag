# Pluggable SDK design

Status: active implementation contract

API version: 1

Go baseline: 1.26

## Product definition

`ag` is a protocol-first, SDK-hosted agent runtime. The executable in `cmd/`
is one presenter and composition example, not the product boundary. The
gateway is another peer presenter built from the public SDK and durable state
ports.

The kernel owns only mechanisms that cannot be moved into a plugin:

- immutable registry snapshots and generation publication;
- transactional plugin mount and ownership-based unmount;
- deterministic hook dispatch;
- the recursive provider/tool/agent invocation loop;
- structured sibling concurrency and workflow dependency scheduling;
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
        |             |
        v             v
   sdk/runtime       sdk contracts and ports
        |             ^
        +-------------+
        |
        v
 sdk/storage (optional reference implementations)

pluginrpc -> sdk contracts and ports
standalone plugins -> versioned plugin protocol
```

The package layout makes those dependency layers visible:

```text
sdk/*.go                         shared domain language and ports
sdk/runtime/                     public agent-execution facade
sdk/runtime/internal/durability/ checkpoint and restore domain rules
sdk/storage/                     durability infrastructure adapters
registry/                        plugin discovery bounded context
gateway/                         user-session bounded context
pluginrpc/                       protobuf transport adapters
plugins/                         optional resource implementations
```

See [architecture.md](architecture.md) for the context map, application entry
points, and dependency rules.

Presenters compose `sdk/runtime` with SDK ports. Plugins implement the
versioned protocol. In-process Go plugins may use public SDK interfaces as a
convenience, but must never import `sdk/runtime` or another plugin package.
The execution engine depends on `TrajectoryStore`, `OperationStore`,
`DeliveryStore`, `StateBackend`, and optionally `AtomicStateBackend`; it does
not depend on file store types.

## State backend bootstrap boundary

Trajectory, operation, and delivery persistence are SDK ports. Their concrete
shape is not assumed to be a file:

```go
type StateBackend interface {
    Trajectories() TrajectoryStore
    Operations() OperationStore
    Deliveries(name string) (DeliveryStore, error)
    Capabilities() StorageCapabilities
    Namespace() string
    Prune(context.Context, RetentionPolicy) (PruneResult, error)
    Health(context.Context) error
    Close(context.Context) error
}

type AtomicStateBackend interface {
    StateBackend
    AppendTrajectory(context.Context, TrajectoryAppendCommit) (TrajectoryAppendResult, error)
    StartExecution(context.Context, ExecutionStartCommit) (ExecutionMutationResult, error)
    CommitExecution(context.Context, ExecutionMutationCommit) (ExecutionMutationResult, error)
    CancelExecution(context.Context, ExecutionCancelCommit) (ExecutionCancelResult, error)
}

type StorageDriver interface {
    Scheme() string
    Open(context.Context, *url.URL) (StateBackend, error)
}
```

Applications can register `postgres://`, `s3://`, network, or other drivers
with `StorageRegistry` without changing the runtime. `memory://`, `file://`,
`duckdb://`, `postgres://`, and `postgresql://` are built-in drivers in
`sdk/storage`.
Storage drivers may implement different locking, indexing, and transaction
strategies, but they share the same aggregate model rules for trajectory forks,
operation leases/completion, and delivery enqueue/lease/ack/retry/dead-letter
transitions. A driver should persist the result of those rules, not redefine
them in backend-specific mutation code.

Storage is a bootstrap extension rather than an ordinary runtime plugin. The
runtime needs its source of truth before it can recover operations, load a
trajectory, deliver plugin events, or mount plugins. Making storage depend on
the plugin composition it must restore would create a boot cycle.

`StorageCapabilities` explicitly reports durability, multi-process safety,
cross-store atomicity, fencing, pagination, maintenance, namespace isolation,
and encryption-at-rest. Callers must not infer these properties from a URI
scheme. `NewRuntime` treats capabilities as a startup contract: it requires
operation fencing and named delivery queues, and `AtomicState` must match the
`AtomicStateBackend` interface in both directions. Built-in memory and file
backends do not claim cross-store atomicity or encryption at rest.

The built-in file backend is a local/reference implementation. It uses
cross-process file locks where the platform supports them, atomic file
replacement, restrictive permissions, namespace partitions, pagination, and
retention cleanup. It still rewrites whole JSON state files and should be treated
as a development, debugging, and compatibility backend. High-frequency
subscriber delivery, long-running agents, and gateway deployments should use an
indexed database-backed driver instead of relying on file state growth. New CLI
state directories default to DuckDB; explicit `file://` URIs and legacy file
state directories remain supported for compatibility. If the default
`agent-state.duckdb` file already exists in a state directory, it takes
precedence over legacy file-state markers in the same directory.

The built-in DuckDB backend stores trajectory metadata, execution cursors,
immutable entries, operation state, and named delivery queues in normalized
tables. Subscriber outbox enqueue, lease, ack, retry, purge, operation submit,
claim, renew, complete, recovery, and invocation-root lookup are incremental SQL
mutations or indexed queries rather than whole-file rewrites. `BeginExecution`
and `CommitExecution` are ACID SQL transactions, while execution, provider,
tool, correlation, kind, time, operation invocation, and delivery scheduling
fields are native indexed columns. `TrajectoryAnalyzer.AnalyzeEntries` exposes
bounded fixed-field extraction without parsing payload JSON. DuckDB reports
`AtomicState=true` for one process: trajectory append, execution start,
execution commit, execution cancellation, and subscriber outbox enqueue share
one DuckDB transaction.

DuckDB is intended for one read-write agent host process with concurrent
in-process readers. It reports `MultiProcessSafe=false`: multiple independent
writer processes must not open the same native DuckDB file. A distributed
deployment still needs a network database or service-backed storage driver.

The built-in PostgreSQL backend stores trajectories, operations, and named
delivery queues in one database and reports `AtomicState=true`. It implements
`AppendTrajectory`, `StartExecution`, `CommitExecution`, and
`CancelExecution`, so trajectory appends, execution acceptance, execution
progress, cancellation completion, and host outbox projection can share a
database transaction when the event contract allows planning subscriber
deliveries before commit.

`NewRuntime` requires a `StateBackend`; selecting memory, file, or an external
driver is an application composition decision. Hosts with a startup context use
`NewRuntimeContext` so construction-time storage health checks are cancellable.
A successful construction transfers ownership to the runtime by default. If
construction fails, ownership remains with the caller. `StorageBorrowed` keeps
lifecycle ownership with the embedding application after successful
construction.

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

`pluginrpc.NewServer` receives its `OperationStore` and inbox `DeliveryStore`
explicitly. The standalone plugin host opens both from its configured
`StateBackend`; tests may choose memory stores. The RPC adapter does not select
or import a concrete storage implementation.

`PluginDriver` resolves one URI scheme into a `Source`. The initial drivers are:

- direct in-process `Source` registrations;
- `grpc://` for an insecure development endpoint;
- `grpcs://` for a TLS remote endpoint.

Future `exec://` and `wasm://` support is added by registering another driver,
not by changing `Runtime`.

`Source.Open` returns a `Connection`. A connection exposes the same logical
`Plugin` contract regardless of whether it is an in-process object or an RPC
proxy. A successful open transfers one connection to the caller, which must
close it. `sdk.Local(plugin)` wraps one existing plugin instance and is
therefore a one-shot source with an idempotent connection close; create a new
plugin instance for another mount. Transport-backed sources may create a fresh
connection on each open.

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
    Name          string
    Version       string
    Description   string
    APIVersion    int // legacy exact version
    MinAPIVersion int
    MaxAPIVersion int
    Requires      []string
    Conflicts     []string
    Registers     []string
}
```

A plugin can declare a compatible API range. `APIVersion` remains the legacy
exact-version form. Mount succeeds only when the current SDK API is inside the
declared range.

Contribution resource IDs are stable strings:

```text
provider:<name>
tool:<name>
agent:<name>        # same-process extension
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
- `Agent`: a declarative same-process child-agent environment;
- `Hook`: synchronous policy effect required by the current boundary;
- `Subscriber`: passive asynchronous event consumer;
- `Capability`: generic JSON request/response operation;
- `EventContract`: custom event declaration.

Provider and Tool are specialized capabilities because the agent loop needs
their typed contracts directly.

`AgentRegistrar` is an optional local registrar extension rather than part of
the cross-process `Registrar` contract. A Go plugin registers an agent with
`sdk.RegisterAgent(registrar, spec)`. This succeeds for an in-process
runtime staging registrar and fails explicitly for an RPC registrar. Agent
callbacks are intentionally same-process in API v1; providers and tools used by
that agent may still be local or RPC resources already mounted in the inherited
snapshot.

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
zero uses the runtime default (one second unless configured). Hooks on events
that allow patches, blocking, or actions default to `fail_closed`, so a broken
permission or mutation policy cannot silently disappear or block the runtime
indefinitely.

Passive observation is not a Hook. A `Subscriber` receives an immutable event
from a durable delivery queue through its inbox. Subscriber execution is
asynchronous and cannot delay the producer. The durable enqueue is part of
publishing the event: an enqueue failure is returned instead of being logged
and lost. Subscribers cannot patch, block, or choose an action. The
OpenTelemetry projection is the first built-in subscriber plugin.

Each delivery has a stable delivery ID, event ID, subscription ID, attempt,
and timestamp. The queue deduplicates enqueue by delivery ID, then processes
the record at least once. If a receiver succeeds but acknowledgement fails,
the same delivery may invoke it again; subscriber effects must therefore be
idempotent. Retries use bounded exponential backoff and eventually reach a
dead-letter state. Ordering is preserved per trajectory, not globally.

Deduplication is strict, not lossy: an existing delivery ID is accepted as a
duplicate only when the target, subscription, resource revision, partition, and
event identity, including payload, match. Reusing a delivery ID for different
event content is a conflict so atomic outbox projection cannot silently drop a
real event.

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

Go plugins may implement a small synchronous convenience interface. During
mount, the runtime decorates every sync-only provider, tool, and capability as
an asynchronous resource backed by `OperationStore`; the loop never invokes
those methods directly. Native async plugins retain their own Submit/Poll/Cancel
implementation. Short control hooks are the only synchronous plugin path.

The host records the provider/tool request and terminal result in trajectory.
The operation idempotency key is derived from the trajectory execution ID and
logical step (provider turn or tool call), so it remains stable when recovery
creates a new immutable attempt entry. Intermediate operation state lives in
`OperationStore`, so a restarted local or standalone worker can re-run a
non-terminal operation when it is submitted again. Poll remains the portable
baseline; a streaming `Watch` optimization may reduce latency without changing
this contract. Cancellation is best-effort once an external side effect has
started.

Workers claim operations with a time-bounded lease and unique fencing token,
renew the lease while working, and must present the token to complete. A worker
whose lease expired cannot overwrite a result committed by its replacement.
If a claimed operation no longer matches the worker's resource revision, the
worker fails it terminally under the same fence instead of leaving it running
until lease expiry. This fences stale state writes; it does not make an
external side effect exactly-once.
If worker cancellation interrupts the claim before completion, including during
resource validation, the runner releases the claim through detached
finalization so recovery can re-evaluate the record instead of recording
shutdown as an operation failure.
`OperationStore` intentionally exposes narrow domain commands rather than a raw
state-transition CAS method; generic transition helpers stay inside storage
model code where they cannot bypass claim, cancellation, and stale-resource
rules.
Awaiting an operation preserves non-success terminal states as structured SDK
errors: callers should use `errors.Is(err, sdk.ErrOperationFailed)` or
`errors.Is(err, sdk.ErrOperationCancelled)` for control flow, and
`errors.As(err, &terminalErr)` when they need the terminal
operation snapshot. Presentation and gateway code must not parse terminal
operation error strings.
Recovery scheduling consumes the store's non-terminal operation view and turns
each record into a worker recovery candidate. Pending operations and running
operations without an active lease can start immediately; running operations
with a still-valid lease carry a delay and are re-read before recovery claims
them. Presenters and RPC servers should not scan all operations and rediscover
terminal-state or lease-expiry rules outside the store and worker scheduler.
Delayed recovery is owned by the operation host lifecycle: server shutdown
cancels it, but startup or request context cancellation does not.

Operation IDs and idempotency keys are distinct. Retrying Submit with the same
idempotency key and resource revision returns the original operation; if that
operation is not terminal, Submit is also an idempotent scheduling hint and the
worker may attempt to claim it again. Resource revision includes plugin version
and the resource specification, so a new plugin version cannot inherit an
incompatible old result. Receivers may execute a command more than once after a
crash unless the concrete effect is idempotent or uses a plugin-owned
transaction. The SDK therefore guarantees at-least-once execution, not
exactly-once effects.

Cancel is a control request for the operation target. It validates operation
kind and resource, but it does not require the caller's currently mounted
resource revision to match the record being cancelled. Polling and execution do
validate the resource revision because they consume revision-specific state.
This split lets stale work be stopped or surfaced after a plugin upgrade
without allowing old commands to run against a new implementation.

## Invocation graph and structured concurrency

Every provider, tool, agent, workflow, and capability operation may carry an
`Invocation` envelope:

```go
type Invocation struct {
    ID              string
    RootID          string
    ParentID        string
    GroupID         string
    SessionID       string
    TargetSessionID string
    ExecutionID     string
    Dependencies    []string
    Ordinal         uint32
}
```

`ID` is stable across retries. `ParentID` is causal ownership,
`Dependencies` are explicit DAG edges, and `GroupID` identifies siblings
submitted as one structured-concurrency group. `SessionID`/`ExecutionID`
identify the trajectory execution that issued the call; an agent invocation
also records its child in `TargetSessionID`. The envelope is persisted in
`OperationRecord`, propagated through `OperationRequest`, and available to
in-process plugin code through `sdk.InvocationFromContext`. The existing RPC
operation request carries the same optional envelope so a remote provider/tool
store does not lose graph lineage; this is metadata propagation, not an agent
host callback.

When one model response contains multiple tool calls, the runtime prepares all
calls against the same immutable snapshot, submits every sibling, awaits them
concurrently, and finalizes hooks, trajectory entries, tool messages, and
returned results in the model's original order. Tool execution therefore does
not serialize independent calls, while nondeterministic completion timing does
not change the next model request. Caller cancellation is propagated to every
outstanding sibling. A tool failure is a model-visible result and does not by
itself erase successful sibling results.

An in-process tool may call `sdk.InvokeAgent(ctx, request)`. Registered
`AgentSpec` values are declarative: provider and tool visibility inherit from
the caller by default, while explicit provider/tool choices must exist in the
inherited snapshot and a tool allowlist can only narrow it. `MaxTurns` also
inherits by default and may only be reduced. In the serialized contract,
`tools: null` means inheritance while `tools: []` explicitly exposes no tools.
`AgentSessionNew` starts an empty child context;
`AgentSessionFork` creates a copy-on-write trajectory at the issuing tool-call
entry and seeds the child with the parent messages. If the inherited parent
branch ends with assistant tool calls whose results are not part of the fork
base, the projection closes them with placeholder tool results before appending
the child prompt. `AgentSessionResume` opens an existing child trajectory and
appends a new prompt as a new agent operation; it does not replay the original
creation/fork invocation.
Created child trajectories record `parent_session_id`, `origin_invocation_id`,
and `origin_mode` in the trajectory environment, and resume validates those
lineage fields before continuing.

`sdk.ExecuteWorkflow` schedules a declarative DAG of agent nodes. All ready
nodes in a wave run concurrently; a dependent wave starts only after its
predecessors join. Agent operations are children of the workflow invocation,
carry predecessor invocation IDs as dependency edges, and join into stable
declaration order. `IncludeDependencyOutputs` explicitly appends predecessor
outputs to a reducer prompt. Failure or cancellation cancels the outstanding
wave. A swarm is the zero-dependency fanout case; fanout/reduce is the same
model with a dependent reducer node.

`sdk.LoadInvocationGraph` reconstructs the durable causal graph through the
narrow `InvocationGraphStore` read port, which built-in backends satisfy with
`OperationStore.ListByInvocationRoot`. It then projects a rooted set of
`InvocationNode` values with parent/child edges, dependency edges, and
graph-order operations. Presenters may render the graph differently, but they
must not scan all operations, sort by wall-clock timing, or reinterpret raw
metadata to rebuild DAG shape. Trajectory stores remain responsible for
per-agent state and copy-on-write history; operation stores remain responsible
for invocation lifecycle, idempotency, leases, graph lookup, and recovery.
These are linked records, not competing representations of the same state.

## Inbox and outbox

Both host and standalone plugin expose the same logical queues:

```text
host outbox --event/command--> plugin inbox
host inbox  <--state/result--- plugin outbox
```

Inbox and outbox are topology roles, not two persistence interfaces. Both use
the neutral `DeliveryStore` port and are opened as different named queues
(`host-outbox` and `plugin-inbox`) from `StateBackend`. This intentionally
shares queue mechanics without accidentally sharing queue contents.

An outbox record is appended before publication. An inbox record is persisted
and deduplicated before acknowledgement. Dispatch workers lease records under a
subscriber timeout shorter than the lease, retry expired leases, and move
repeatedly failing records to a dead-letter state. Delivery leases are not
heartbeat-renewed during handler execution; the timeout-below-lease invariant is
the concurrency fence. Delivery identity includes the target plugin version and
resource revision, so stale work is dead-lettered instead of being delivered to
a newly mounted implementation. Queue storage remains replaceable by an
external broker-backed `StateBackend`.
Lifecycle drains read the store's non-terminal delivery view; runtime code does
not scan delivered/dead-letter history and rederive queue state at shutdown.
When worker cancellation aborts a leased delivery, the worker releases the
delivery back to pending through detached finalization. Graceful shutdown
therefore does not require waiting for lease expiry before another host can
deliver the same record.

Backends with `AtomicState=false` do not offer one transaction spanning
trajectory state and the delivery queue. Runtime-owned execution events therefore
treat subscriber enqueue failure as delivery degradation: the error is recorded
and logged, while hook policy and trajectory mutation semantics remain the
execution gate. Explicit application `Runtime.Emit` keeps enqueue errors visible
to its caller because that call's purpose is event publication.

## Trajectory contract

A trajectory is the durable, append-only record needed to reconstruct agent
state. It is distinct from telemetry.

```go
type TrajectoryEntry struct {
    ID             string
    TrajectoryID   string
    ParentID       string
    Ordinal        uint64
    Depth          uint64
    Kind           TrajectoryKind
    Timestamp      time.Time
    Generation     uint64
    Fields         TrajectoryEntryFields
    PayloadVersion uint32
    Payload        json.RawMessage
}

type TrajectoryStore interface {
    Create(...)
    Append(... expectedHead ...)
    BeginExecution(... expectedHead ...)
    ClaimExecution(... owner, now, ttl ...)
    RenewExecution(... executionID, token, now, ttl ...)
    CommitExecution(... expectedHead, token, entries, state ...)
    CancelExecution(context.Context, TrajectoryExecutionCancelCommit) (TrajectoryExecutionCancelResult, error)
    ListRecoverable(... now ...)
    LoadMetadata(...)
    LoadEntry(...)
    LoadBranch(...)
    FindLatest(...)
    Load(...)
    List(...)
    ListPage(...)
    Delete(...)
}
```

Entries are immutable and form a graph through `ParentID`; each trajectory has
one active head and one cached checkpoint reference. Append inserts only new
entries and uses compare-and-swap on the expected head so two writers cannot
silently overwrite each other. The store assigns each entry its origin
trajectory, origin-local ordinal, and graph depth.

`TrajectoryEntryFields` is a fixed typed projection for fields used by indexes
and analysis: execution ID, stable operation key, turn, correlation ID,
provider, model, tool name/call ID, finish reason, token counts, error flag,
action kind, and cause code. Stores should map these to native columns and
indexes. `Payload` contains kind-specific detail; it is not the storage unit
for the trajectory itself, and analysis should not need to parse it for the
fixed fields above.

A schema-v2 fork starts with its head pointing at the selected source entry.
The fork owns no copied entries until its first append; that append names the
source entry as its parent. Source entries remain immutable and shared.
Backends should use the shared trajectory model preparation rule for fork
initialization instead of re-deriving head, checkpoint, and inherited-entry
counts independently.
Deleting a trajectory that still has a live fork is rejected. `Load` remains a
compatibility operation that materializes the inherited prefix plus locally
owned history; runtime recovery uses metadata, entry, branch, and latest-kind
point reads instead.

Every user message, provider response, tool result, policy decision, and
terminal cause is recorded. A clean turn ends with a committed checkpoint.
Restore loads the cached committed checkpoint directly; incomplete tail entries
remain observable but are not replayed. Legacy schema-v1 stores may derive the
checkpoint by walking the active branch when loading metadata.

Starting a prompt is a short store transaction: it appends the user-message
entry and creates a pending `TrajectoryExecution` together. When a backend
advertises `AtomicState`, the runtime uses `StartExecution` so the
`trajectory_appended` subscriber delivery boundary for that user-message entry
can be attached to the accepted execution mutation. A worker claims the execution
with a renewable lease and fencing token. While claimed, unfenced `Append` calls
are rejected; the worker uses `CommitExecution` to atomically append entries,
advance head/checkpoint pointers, and optionally transition the execution to a
terminal state. Hosts run from the accepted execution input recorded in that
user-message entry, so delayed same-process hosting and crash recovery share the
same base messages.

`Session.Prompt` is a synchronous presenter convenience over the same durable
contract: it accepts the prompt as a `PromptSubmission` and immediately hosts
that accepted execution. It must not reconstruct input from mutable session
state or bypass the lifecycle used by `SubmitPrompt`, gateway execution
hosting, and recovery.

Cancellation is also an execution lifecycle transition, not a gateway-local
presentation concern. `TrajectoryStore.CancelExecution` accepts a
`TrajectoryExecutionCancelCommit`, so a runtime-owned cancellation can commit
the cancelled state and terminal/restore trajectory entries through the same
aggregate mutation on every backend. `FenceExecutionCancellation` is the
weaker lifecycle-only form: it passes no completion entries and only prevents
further renew/commit calls for the active execution. When a backend advertises
`AtomicState`, runtime uses state `CancelExecution` so the cancelled state,
terminal/restore entries, trajectory events, and `agent_end` subscriber
deliveries are committed together. Repeating the same cancellation is idempotent
and must not enqueue another completion event. Runtime-owned restore and
rollback entries use `AppendTrajectory` on atomic backends, and
failure/cancellation unwind uses the execution commit boundary, so trajectory
event subscriber delivery follows the mutation rather than a later best-effort
dispatch.
If resume finds the active head already restores the target checkpoint, it
loads session state without publishing a synthetic trajectory event.

After a process crash, pending executions and running executions with expired
leases appear in the execution lifecycle's recovery-candidate view. Once the
same plugin composition is mounted, `Runtime.RecoverExecutions` claims those
candidates. It resumes after the latest checkpoint belonging to that execution,
or restarts the incomplete first turn from its durable input entry. Stable
operation keys prevent a new attempt entry from accidentally becoming a new
provider/tool command.

During graceful runtime shutdown, `Runtime.RequestClose` cancels the execution
context and the claimed execution is fenced back to `pending` before plugin and
storage cleanup completes. `Runtime.Close` performs the same request and then
waits for cleanup. A replacement runtime can therefore recover the execution
immediately, without waiting for the old lease to expire. A hard process
termination cannot perform that handoff and instead uses the expired-lease path.

The portable baseline covers trajectory state, not external side effects or the
separate delivery store. Provider/tool effects and subscriber delivery remain
at-least-once. A backend that advertises `AtomicState` may additionally couple
runtime-owned trajectory append, execution start, execution progress, and
cancellation mutations with host outbox projection when the event contract
keeps subscriber delivery stable before dispatch; the built-in file backend does
not.

Operation results must outlive every non-terminal trajectory execution that
can reference them. The bundled state backend rejects operation pruning while
one of its trajectories is pending or running. Independently deployed plugins
must configure their operation retention horizon to satisfy the same invariant.

Rollback never deletes history. It appends a rollback entry whose parent is
the selected committed entry and moves the active head atomically. Fork creates
a new trajectory with lineage back to the source trajectory and entry.

Rollback restores agent/runtime state only. External effects such as file
writes or network calls require an explicit compensating capability from the
owning plugin; the kernel never pretends those effects were undone.

Trajectory and entry payloads carry explicit schema versions. A trajectory also
captures the runtime version, SDK API version, requested provider, system and
composition digests, mounted plugin versions, and resource specifications.
Each execution input is recorded as a typed `user_message` payload that carries
the accepted base messages, submitted message, and environment snapshot.
Recovery uses that payload before falling back to checkpoint projection, so a
fork created after a parent checkpoint still resumes from the state that was
actually accepted for the child execution.
Child trajectories additionally capture their parent session, origin
invocation, and new/fork mode without overloading copy-on-write `ParentID`.
`ResumeExact` (the default) restores checkpoint state and rejects a changed
composition; `ResumeCurrent` deliberately reuses recorded messages with the
current configuration. Legacy trajectories without an environment snapshot
remain readable but cannot provide the exact-composition guarantee.

`TrajectoryStore` is the runtime dependency and is fully pluggable. The SDK
ships memory, file, and DuckDB implementations under `sdk/storage`; other
database, object-store, and network implementations satisfy the same
interface. CLI trajectory commands open a `StateBackend` and use
`backend.Trajectories()` rather than constructing a concrete store.

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
default. Otherwise any `Stop` is an execution-control fence and wins over
`Inject`; among `Stop` actions the last one wins. With no `Stop`, all `Inject`
messages are concatenated in hook order, then any `Step`, then the default
action.

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

Stable command tree:

```text
ag run
ag config show
ag config path
ag plugin list
ag plugin discover
ag plugin inspect <name-or-uri>
ag trajectory list
ag trajectory show <id> [--head <entry-id>]
ag trajectory rollback <id> <checkpoint-id>
ag invocation show <root-invocation-id>
ag state inspect
ag state prune --before <RFC3339-or-duration>
ag version
```

Business output goes to stdout. Diagnostics and logs go to stderr.

The repository also builds `agentm-plugin-file` and `agentm-plugin-bash` as
standalone Cobra processes. Each owns durable operation/inbox state, exposes
gRPC health, optionally serves TLS, and may register/renew/unregister a lease.
`examples/python-plugin` independently implements the wire protocol without
importing the Go SDK.

## End-to-end acceptance

The end-to-end suite verifies that:

1. starts a real gRPC plugin server on a loopback TCP listener;
2. registers local and gRPC plugin entries in `PluginRegistry`;
3. discovers and resolves those entries;
4. drives model -> remote hook -> remote tool -> model and delivers a terminal
   event through the remote inbox;
5. races operation cancellation against completion;
6. verifies generation changes and lifecycle/OTel evidence;
7. uses the public SDK and public protobuf protocol at process boundaries.

The complete acceptance suite also:

11. starts a standalone plugin process and registers it through a lease;
12. proves lease renewal and expiry under concurrent discovery;
13. propagates cancellation and OTel transport context across the process boundary;
14. records a multi-turn trajectory with provider, hook, and tool entries;
15. reopens persistent stores and restores from the last committed turn;
16. rolls back to an earlier turn without deleting the abandoned branch;
17. races concurrent sessions with mount/unmount and passes `go test -race`;
18. drives Cobra CLI commands and verifies stdout, stderr, exit codes, JSON
    output, and persistent trajectory behavior;
19. builds and starts real file/bash child processes, then executes tools via
    protobuf Submit/Poll and checks durable operation state.
20. generates Python stubs from the public proto, starts a real Python plugin
    process, mounts it through its renewable lease, and runs provider -> hook ->
    tool -> provider plus capability, subscriber, idempotency, Poll, and Cancel.

Shape-only tests and tests that only check getters do not satisfy this
acceptance criterion.

## Explicit non-goals for API version 1

- Go `.so` loading;
- WASM execution;
- local process supervision for `exec://`;
- plugin-originated asynchronous event streams;
- remote-to-host capability callbacks;
- TUI and gateway protocols;

These may be added through new drivers, capabilities, or protocol versions
without changing the core plugin ownership and event-effect model.
