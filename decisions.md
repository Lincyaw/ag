# Decisions

## 2026-07-17

- **Protocol-first plugins** (L2: repository goal and AgentM reference). A
  plugin depends on the versioned wire contract, not on the Go SDK. In-process
  Go plugins are an optimization of the same logical contract.
- **Registry discovery has one wire representation and explicit layers** (L2:
  existing boundaries). Wire list pages expose `PluginInstance` only.
  `registry.Directory` owns leases and replicas; a `PluginDriver` may project
  directory results into generic references without owning registry state.
- **Discovery is not execution** (L4, flagged). Plugins may self-register with
  a lease and become discoverable, but mounting remains an explicit
  composition-policy decision.
- **Trajectory is the recovery source of truth** (L4, flagged). Durable,
  append-only trajectory entries drive restore and rollback. OpenTelemetry is
  a correlated projection and may never be the only recovery record.
- **Trajectory persistence is an immutable graph, not a document** (L4,
  flagged). Stores append fixed-envelope entries and move a head with CAS.
  Metadata, entry, branch, and latest-kind reads are the recovery path;
  whole-trajectory `Load` is a materialized compatibility view.
- **Indexed trajectory fields are typed columns** (L4, flagged). Fields needed
  for filtering and analysis live in `TrajectoryEntryFields`; kind-specific
  detail may remain in a versioned payload. A database backend maps the typed
  fields to native columns and indexes instead of extracting them from JSON.
- **Forks use copy-on-write lineage** (L4, flagged). A schema-v2 fork initially
  points its head at the selected source entry and owns no copied prefix.
  Entries record their origin trajectory, origin-local ordinal, and graph
  depth. A referenced source cannot be deleted before its forks.
- **Rollback preserves history** (L2: AgentM session-tree precedent). Rolling
  back creates a new head whose parent is the selected committed entry; it
  never deletes entries and does not implicitly compensate external effects.
- **One kernel** (L2: active design contract). The legacy `agent` package is
  removed after the SDK path owns CLI and plugin execution; no compatibility
  runtime is kept before the first stable release.
- **Cobra owns the CLI contract** (L2: user direction). Viper may resolve
  files and environment values only through Cobra-bound flags; the standard
  library `flag` package is not a second command surface.
- **Semantic observability is an event projection** (L2: user direction).
  The loop emits complete, correlated lifecycle events but does not construct
  run, provider, tool, or trajectory spans itself. An optional OpenTelemetry
  plugin subscribes to those events and projects traces, metrics, and logs.
  Mechanism-level instrumentation remains with the mechanism: RPC telemetry
  stays in the transport adapter and hook-dispatch telemetry stays in the
  dispatcher. Durable trajectory writes remain a kernel correctness concern.
- **One plugin contract, two initial deployment modes** (L2: user direction).
  An in-process plugin and a remote RPC plugin both enter through
  `Source.Open -> Connection.Install`; deployment mode must not fork Runtime
  registration, ownership, dispatch, or unmount semantics.
- **Asynchronous by default, synchronous only for control** (L2: user
  direction). Passive subscribers consume durable outbox deliveries and never
  block the loop. Only hooks whose Patch/Block/Action result is required at the
  current boundary execute synchronously, with a short explicit deadline.
- **Long work is an operation, not a long RPC** (L2: user direction).
  Provider and tool adapters submit idempotent operations and expose Poll/Watch
  state. Local implementations may complete immediately, but remote execution
  must survive disconnects and process restarts without holding one RPC open.
- **Plugin operation panics fail the operation** (L2: deployment-mode parity).
  In-process and RPC workers recover plugin panics at the executor boundary so
  one faulty provider, tool, or capability cannot terminate its host process.
- **Inbox/outbox delivery is at-least-once** (L4, flagged). Every command and
  event has a stable delivery/idempotency key; queues deduplicate enqueue, but
  a receiver may run again if acknowledgement fails. Subscriber effects must
  be idempotent; the system does not claim exactly-once external effects.
- **Trajectory execution is the durable orchestration cursor** (L4, flagged).
  Beginning a prompt atomically appends its input and creates a pending
  execution. Workers claim executions with renewable leases and fencing tokens;
  entry/checkpoint appends and terminal execution state commit in one
  trajectory-store transaction. Graceful shutdown returns claimed work to
  pending immediately; crashes are recovered after lease expiry. After
  composition is mounted, a stateless worker calls `RecoverExecutions` to claim
  pending or expired work.
- **Logical operation keys survive trajectory retries** (L4, flagged).
  Provider/tool operation keys derive from execution ID plus logical step,
  rather than from an attempt entry ID. Recovery may retain multiple immutable
  attempt entries while resubmitting the same idempotent operation key.
- **Invocation is the recursive execution primitive** (L2: user direction).
  Provider, tool, agent, and workflow calls are durable invocation nodes.
  Parent edges express causal ownership, dependency edges express DAG order,
  group IDs express concurrently submitted siblings, and operation state owns
  retry/cancellation. Trajectory remains the per-agent context and state
  branch; it is not overloaded as the scheduler.
- **Same-turn tool calls use structured concurrency** (L2: user direction).
  The runtime prepares and submits every sibling, awaits them concurrently,
  then commits hooks, trajectory results, and tool messages in original model
  order. Completion timing cannot reorder the next provider request.
- **Subagents are same-process declarative resources** (L2: user direction).
  `AgentRegistrar` is an optional local registrar extension. An agent inherits
  the current immutable provider/tool snapshot by default and may attenuate
  tools through an allowlist and its turn budget may not exceed the parent's.
  The wire contract preserves `nil` (inherit) versus empty (no tools). Agent
  invocation is available to plugin code only through a context-scoped narrow
  invoker; RPC host callbacks are not implied.
- **New and fork are trajectory initialization policies** (L2: user
  direction). Both execute through the same AgentInvocation path. Fork adds
  copy-on-write content lineage; new starts empty. Child environment metadata
  records parent session, origin invocation, and mode in either case.
- **Swarm and workflow share one DAG scheduler** (L2: user direction).
  A swarm is a dependency-free wave. Workflows add explicit predecessor edges
  and stable joins; fanout/reduce is a fanout wave followed by a reducer.
- **Operation retention follows execution liveness** (L4, flagged). Terminal
  operation results cannot be pruned while a trajectory execution that may
  reuse them is pending or running. A shared backend enforces this
  conservatively; independently deployed plugins must retain results for at
  least the trajectory recovery horizon.
- **Runtime storage is explicit** (L4, flagged). `NewRuntime` requires one
  `StateBackend`; memory/file selection and driver registration belong to the
  application composition root. The runtime no longer accepts parallel
  trajectory, operation, and delivery store configuration.
- **Storage ownership transfers on successful construction** (L2: Go
  constructor convention). A failed `NewRuntime` leaves the backend with the
  caller. A successful runtime closes it unless `StorageBorrowed` is selected.
- **DeliveryStore is the persistence term** (L2: active architecture
  contract). Inbox and outbox describe topology roles and named queues; they
  are not separate store types or compatibility aliases.
- **Runtime does not re-export SDK contracts** (L4, flagged). Public runtime
  APIs refer to `sdk` types directly; the runtime package owns execution
  concepts such as `Runtime`, `Session`, and their configuration, not a mirror
  of plugin, event, operation, delivery, or storage contracts.
- **RPC server persistence ports are explicit** (L4, flagged).
  `pluginrpc.NewServer` requires its operation and inbox delivery stores; test
  helpers and application composition roots choose concrete implementations.
  The transport adapter does not import `sdk/storage` or create hidden memory
  state.
- **Runtime shutdown is monotonic and retryable** (L4, flagged). The first
  `Close` seals the runtime and starts cleanup exactly once. A caller context
  limits only how long that call waits; cleanup continues, and later calls join
  the same completion result. Background-task registration happens before the
  shutdown gate opens, so no `WaitGroup.Add` can race with shutdown waiting.
- **File confinement is capability-based** (L4, flagged). File tools perform
  open, stat, walk, and rename operations through Go's `os.Root`; a validated
  absolute path is not reused after the filesystem can change. Writes use one
  bounded lock rather than retaining a lock for every path ever observed.
- **Saved client configuration is constructor-owned** (L2: Go constructor
  convention). RPC sources and drivers normalize and snapshot mutable client
  configuration when constructed, so later caller mutations cannot change an
  already configured plugin boundary.
- **Discovery exposes ambiguous names** (L4, flagged). `PluginRegistry`
  returns every matching descriptor in deterministic source order instead of
  silently keeping whichever same-name result a map happened to visit first.
  Callers that require one executable source must resolve the ambiguity
  explicitly.
- **Runtime does not expose persistence stores** (L4, flagged). Runtime APIs
  operate on execution concepts; diagnostics and maintenance retain their
  `StateBackend` at the composition root instead of using Runtime as a service
  locator for trajectory, operation, or delivery stores.
- **RPC server owns plugin lifecycle after construction** (L4, flagged).
  Standalone shutdown unregisters discovery, stops accepting RPCs, closes the
  adapter and its plugin once, then closes storage. The same ordered cleanup
  runs for startup failures after any resource has been acquired.
- **Delivery workers remain deployment-local** (L4, flagged). Runtime outbox
  workers own snapshot leases and mounted-plugin identity, while RPC inbox
  workers own one remote plugin manifest. They share the `DeliveryStore`
  contract, not a callback-driven worker abstraction; the small retry-loop
  overlap is cheaper than hiding these distinct lifecycle boundaries.
- **Trajectory encoding helpers are runtime-private** (L4, flagged).
  Trajectory JSON fields and payload versions are durable contracts, but the
  Go structs used to assemble checkpoint, provider-request, and decision
  payloads are not runtime APIs. Existing SDK event payloads are reused when
  their serialized shape already matches.
- **Pagination strategy belongs to storage implementations** (L4, flagged).
  The SDK defines `PageRequest` and size limits, but does not export the
  in-memory `PageWindow` algorithm. Built-in memory/file stores share that
  helper privately; database and network backends should paginate natively.
- **Named backends enter through the storage driver** (L4, flagged).
  `NewMemoryStateBackend`, `NewFileStateBackend`, and
  `NewDuckDBStateBackend` remain default-namespace constructors; named
  namespaces use validated `memory://`, `file://`, or `duckdb://` URIs. The
  CLI follows the same URI path as custom backend composition.
- **DuckDB is the local analytical trajectory backend** (L4, flagged).
  Trajectory metadata, execution cursors, and fixed entry fields use normalized
  tables and ACID transactions. DuckDB is a single read-write-process backend,
  so it does not advertise multi-process safety. Operations and deliveries
  remain file sidecars and cross-store atomicity remains false.
- **Lease management does not expose its writable registry** (L4, flagged).
  Composition roots retain the `PluginRegistry` they inject into a
  `LeaseRegistry`; lease consumers cannot bypass expiry and ownership rules
  through a service-locator accessor.
- **Unrecoverable operations fail visibly** (L4, flagged). When a standalone
  plugin restarts with a different resource revision, its unfinished stored
  operations transition to `failed`; recovery never leaves work permanently
  non-terminal by silently skipping it.
- **Mutable plugin values cross as snapshots** (L4, flagged). Registry
  queries/references, hook events, and hook effects are copied at the host
  boundary. Plugins communicate changes through declared results such as
  `Effect.Patch`, not by retaining or mutating host-owned maps and slices.
- **Plugin driver sets register atomically** (L4, flagged).
  `PluginRegistry.RegisterDrivers` validates and conflict-checks a complete
  set before mutation; adapters cannot leave one scheme installed when
  another scheme in the same set fails.
- **File-store process locks are bounded and shared** (L4, flagged). All file
  stores acquire one of a fixed set of process-local lock stripes before the
  platform file lock. This preserves same-process and multi-process
  serialization without retaining one mutex forever for every opened path.
- **Protocol factories hide concrete adapters** (L4, flagged). RPC factories
  return generated service interfaces when callers only need to register the
  service. The package does not export concrete server implementations or thin
  forwarding wrappers around generated gRPC registration functions.
- **Lease lifecycle belongs to the deployment host** (L4, flagged).
  `RegistryClient` exposes protocol CRUD only. Registration readiness, renewal,
  cancellation, and best-effort unregistration are one ordered host lifecycle,
  not a second convenience loop hidden inside the transport client.
- **Reference stores expose ports, not concrete types** (L4, flagged).
  Memory/file store constructors return SDK store interfaces; their concrete
  structs and filesystem-directory details remain private. Applications choose
  implementations without coupling to reference implementation internals.
  Those interface-returning constructors are also the compile-time conformance
  boundary; a parallel assertion registry is redundant.
- **Validation mechanics stay with their owner** (L4, flagged). The SDK
  exposes resource contracts and naming rules, not generic sorting and
  deduplication helpers used only by runtime staging. Small validation
  transformations remain next to the invariant they enforce.
- **Resource identity has one SDK vocabulary** (L4, flagged). Every plugin
  resource revision uses `ResourceRevision` and the same manifest/kind/name/spec
  digest. Operation storage only declares persistence ports; it does not own
  plugin resource identity or expose operation-specific forwarding helpers.
  The hash implementation stays with that one concept rather than creating a
  generic SDK digest layer.
- **Telemetry setup publishes global state only after success** (L4, flagged).
  Exporters and providers are constructed and registered for cleanup locally;
  process-global OpenTelemetry providers change only after every enabled
  signal has initialized. Failed setup returns both its cause and cleanup
  errors without leaving a partially installed provider behind.
- **Runtime mounts resolved sources** (L4, flagged). Plugin registries own
  name, URI, driver, and discovery resolution; Runtime owns connection,
  validation, resource publication, and mount lifecycle. Composition roots
  explicitly resolve a source before calling the single `Runtime.Mount`
  primitive.
- **Event contracts expose authority, not runtime policy** (L4, flagged).
  `MutableFields`, `AllowBlock`, and `AllowAction` declare permitted effects;
  Runtime derives the default hook failure policy directly from those fields.
  Its built-in authority table lives in the runtime catalog; the SDK exports
  event names and payloads, not host policy helpers such as
  `BuiltinEventContracts` or `EventContract.Active`.
- **Telemetry shutdown is idempotent** (L2: resource-owner convention).
  Enabled signal providers close once in reverse construction order; repeated
  callers receive the same joined result instead of re-closing one-shot OTel
  exporters.
- **RPC boundary cleanup errors remain visible** (L2: Go error convention).
  Failed remote connection construction joins its cause with connection
  cleanup; short-lived registry and inspection clients join operation and
  close results instead of discarding close failures.
- **Lease cleanup is conditional on registration ownership** (L4, flagged).
  Plugin registry entries carry an internal owner token. Lease expiry removes
  only the entry created by that lease, so a composition root may replace a
  name without a stale lease later deleting the replacement.
- **Local plugin sources are one-shot** (L4, flagged). `Local(plugin)` wraps
  one existing instance rather than a factory. Its single successful
  connection owns that instance and closes it idempotently; another mount
  requires another plugin instance. Transport sources may remain reusable
  because each open creates an independent connection.
- **RPC source and driver factories expose SDK ports** (L4, flagged).
  `NewSource` and `NewDriver` return `sdk.Source` and `sdk.PluginDriver`;
  transport-specific concrete adapters remain private because they add no
  public behavior beyond those contracts.
- **Session rollback consumes its commit result** (L4, flagged). The internal
  rollback operation validates and decodes the target checkpoint before
  appending, then returns the committed head and checkpoint. Session state is
  updated from that result without a fallible post-commit reload.
- **File-store mutation results imply publication** (L4, flagged). File
  adapters mutate an isolated in-memory snapshot and publish it atomically.
  If publication fails, record, lease, created, and removal-count results are
  zeroed so callers cannot observe an uncommitted proposal as usable state.
- **File-store directories are explicit** (L2: constructor convention). All
  built-in file constructors share one path preparation rule: trim, reject an
  empty value, resolve an absolute path, and create it with private
  permissions. An omitted path never silently selects the process cwd.
- **Persisted snapshots are validated on read** (L2: storage boundary
  convention). File stores reject decoded operation, delivery, and trajectory
  state that violates the in-memory domain invariants instead of importing
  corruption into a trusted store.
- **Execution interruption preserves ownership semantics** (L2: runtime state
  convention). Caller cancellation is a terminal cancelled execution; runtime
  shutdown returns an interrupted execution to pending so another runtime may
  recover it immediately.
- **Trajectory identity is validated at the storage boundary** (L2: storage
  boundary convention). Every store validates trajectory IDs consistently,
  while common trajectory validation owns fork parent and parent-entry
  identity for both newly created and persisted data.
- **Durable execution control is part of the trajectory port** (L4, flagged).
  Every `TrajectoryStore` fences cancellation durably alongside begin, claim,
  renew, and commit. Backends may optionally expose analysis, but a gateway
  never discovers missing execution control through a runtime type assertion.
- **Runtime owns checkpoint payload interpretation** (L2: layer boundary).
  Gateways load durable results through Runtime instead of mirroring the
  private checkpoint JSON schema.
- **Gateway terminal results have one durable source** (L2: state ownership).
  Gateway workers retain only active execution handles. Terminal results are
  decoded from trajectory checkpoints through Runtime; the gateway does not
  keep an unbounded process-local shadow cache.
- **Driver catalogs outlive opened resources** (L2: composition boundary).
  Runtime builders register plugin URI drivers once, and state factories
  capture storage drivers once. Concurrent sessions share resolution, while
  each runtime connection and state backend remains independently owned.
- **Etcd lease credentials have one durable owner** (L2: state ownership).
  The lease index stores its token and renewal TTL; the instance record stores
  discovery state, epoch, and removal intent. Etcd's native lease binding
  associates both keys, so application JSON does not duplicate lease IDs or
  secrets.
- **Gateway recovery is part of the execution port** (L2: durable boundary).
  Every execution backend implements startup recovery alongside submit, poll,
  and cancel. The service does not silently disable recovery through an
  optional capability assertion.
- **Invocation roots are part of a child trajectory's recovery environment**
  (L2: durable state ownership). Agent trajectories persist both their origin
  invocation and its root. Recovery does not scan operation history whose
  retention may be shorter than the trajectory it must resume.
- **Declarative agents register specs, not behaviorless wrappers** (L2:
  resource ownership). Agent resources carry policy only; staging clones one
  `AgentSpec` and snapshots attach its plugin owner. Provider/tool interfaces
  remain reserved for resources that execute behavior.
- **Optional storage ports resolve at composition time** (L2: dependency
  ownership). `NewRuntime` validates an advertised atomic-state capability once
  and retains the resulting `AtomicStateBackend` port. Execution commits do not
  repeatedly query backend metadata or rediscover the same interface.
