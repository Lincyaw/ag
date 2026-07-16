# Decisions

## 2026-07-17

- **Protocol-first plugins** (L2: repository goal and AgentM reference). A
  plugin depends on the versioned wire contract, not on the Go SDK. In-process
  Go plugins are an optimization of the same logical contract.
- **Discovery is not execution** (L4, flagged). Plugins may self-register with
  a lease and become discoverable, but mounting remains an explicit
  composition-policy decision.
- **Trajectory is the recovery source of truth** (L4, flagged). Durable,
  append-only trajectory entries drive restore and rollback. OpenTelemetry is
  a correlated projection and may never be the only recovery record.
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
- **Inbox/outbox delivery is at-least-once** (L4, flagged). Every command and
  event has a stable delivery/idempotency key; receivers durably deduplicate
  and acknowledge. The system does not claim exactly-once external effects.
