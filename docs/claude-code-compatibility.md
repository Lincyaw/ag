# Claude Code Compatibility Map

This note maps the Claude Code message, fork, resume, and queue patterns to the
SDK runtime model. The target is compatibility at the abstraction boundary, not
line-by-line emulation of Claude Code's JSONL or UI implementation.

## Compatibility Snapshot

| Claude Code abstraction | SDK kernel owner | Status | Notes |
| --- | --- | --- | --- |
| Parent-pointer transcript tree, rewind, leaf resume | Trajectory | Covered | The SDK uses immutable trajectory entries, `ParentID`, branch projection, and checkpoint resume rather than caller-local message slices. |
| Sidechains and subagents | Trajectory + invocation | Covered with a different physical layout | Claude Code stores sidechains in one JSONL file with `agentId`; the SDK stores child trajectories and uses invocation ancestry for orchestration. |
| Fork child context and resume | Trajectory + agent session mode | Covered for state semantics | Fork/resume are trajectory operations. Exact Claude Code cache-safe fork metadata is still separate. |
| Queue priority `now` / `next` / `later` | Context injection | Covered | The SDK queue is durable and target-addressed instead of a process singleton. |
| Submit interrupt and tool interrupt behavior | Context injection + operation cancellation + tool spec | Mostly covered | `ContextInjectionNow` can interrupt provider and cancellable tool waits; `ToolSpec.InterruptBehavior` models `block` vs `cancel`. Exact interrupt wording is presenter policy. |
| Permission rejection | Events/effects + tool error payload | Covered for semantics | `PermissionRejection` and `DenyToolPermission` standardize model-visible denial text and structured `ToolErrorPermissionDenied`. |
| Hook blocking/context/permission outcomes | Events/effects + context injection | Mostly covered | Blocking, patching, action selection, and injected context are SDK concepts. Claude Code attachment names remain presenter/plugin formatting. |
| Task notifications | Context injection | Partially covered | `ContextInjectionTaskNotification` can target sessions/executions; portable XML/schema fields are not first-class SDK types. |
| Inter-agent and channel messages | Context injection + invocation | Partially covered | The routing carrier exists through `TargetSessionID`, `TargetExecutionID`, mode, origin, and attributes. Swarm/channel schemas remain external. |
| Local command caveats and slash command output | Context injection | Partially covered | `ContextInjectionLocalCommand`, `IsMeta`, `Origin`, and attributes can carry it. The SDK should not bake Claude Code XML strings into providers. |
| Hidden attachments, reminders, skill deltas, mode deltas | Context injection | Partially covered | The carrier exists, but typed SDK payloads are not defined for every Claude Code attachment kind. Add only those that need cross-presenter semantics. |
| Query loop terminal/continue decisions | Runtime action/cause reducer | Mostly covered | `ActionStep`, `ActionInject`, `ActionStop`, causes, and max-turn handling cover the core state machine. Claude Code's exact reason taxonomy is not all public SDK state. |
| Stop hooks | Events/effects | Partially covered | A hook can stop, block, or inject context through the event contract. A named stop-hook phase and background stop tasks are not first-class. |
| Token budget continuation | Runtime policy + context injection | Not yet first-class | It can be approximated by injected nudges, but there is no SDK budget controller or durable budget state. |
| Auto-compact and reactive compact | Trajectory/checkpoint policy | Not yet first-class | Checkpoints and branch projection can host compacted state, but there is no SDK compact scheduler, compact boundary entry, or retry policy. |
| Content replacement state for large tool results | Checkpoint/resume metadata | Not yet covered | This should be durable checkpoint/resume state so fork/resume stay prompt-cache stable. |
| Streaming provider deltas and early tool execution | Provider completion + tool scheduler | Not yet covered | The public provider contract still returns a completed `ModelResponse`. `sdk.OperationWatcher` gives async resources an event-driven operation progress path and `provider_outcome` exposes coarse live provider observations, but content deltas/tool-use-ready events still need a true provider outcome stream. |
| Recursive fork ban | Runtime policy | Covered by configuration | The SDK can represent nested forks by default, and `AgentForkPolicyDenyNested` lets Claude-compatible hosts reject fork-child fork requests without weakening trajectory storage. |

## Covered By The SDK Kernel

### Trajectory tree, fork, and resume

Claude Code persists a parent-pointer transcript tree and projects one leaf
back into a linear model request. The SDK equivalent is the trajectory
aggregate:

- `TrajectoryEntry.ParentID` is the parent pointer.
- `TrajectoryStore.LoadBranch` and the shared trajectory model own root-to-head
  projection, cycle detection, and unknown-entry handling.
- Forked agent sessions are copy-on-write trajectories with `ParentID` and
  `ParentEntryID`; they inherit no copied entries until their first append.
- `LoadSessionResumeBase` resumes from checkpoint state, terminal execution
  base state, or the fork anchor, depending on the trajectory metadata.

This means fork and resume should remain trajectory operations. Gateway,
presenter, and agent orchestration code must not rebuild resumed messages from
caller-local session slices.

### Sidechains and subagents

Claude Code stores subagent sidechains in the same JSONL file with `agentId`.
The SDK models the same ownership with separate child trajectories plus an
`Invocation` envelope:

- `OperationRecord.Invocation` carries parent/root/session/dependency shape.
- `sdk.LoadInvocationGraph` is the read boundary for agent and workflow graphs.
- Child agent sessions keep trajectory ancestry for state and invocation ancestry
  for orchestration.

This avoids using trajectory lineage as a workflow scheduler while still
preserving sidechain-style replay.

### Queue priority and synthetic context

Claude Code uses a process queue with `now`, `next`, and `later`. The SDK
equivalent is durable context injection:

- `ContextInjection.Priority` defines `now`, `next`, and `later`.
- `ContextInjection.Mode` covers prompt, hook, permission, task notification,
  inter-agent, local command, and system payloads.
- `ContextInjectionStore` is durable and target-addressed by session/execution.
- `ExecutionControl.EnqueueContextInjectionView` is the presenter/gateway
  boundary for enqueue-plus-read-after-control.

The carrier is unified, but the SDK deliberately keeps provider messages simple:
providers receive only `sdk.Message` values, not Claude-Code-specific attachment
types.

### Interrupt and tool behavior

Claude Code distinguishes blocking tools from cancellable tools during submit
interrupt. The SDK equivalent is:

- `ToolSpec.InterruptBehavior`: `block` or `cancel`.
- `ToolSpec.Concurrency`: `exclusive` or `parallel`.
- context injection priority `now`, which can interrupt provider waits and
  cancellable tool waits without losing the durable queued payload.
- operation cancellation and terminal classification through
  `ErrOperationCancelled` / `OperationTerminalError`.

Parallel tool calls are stored as operation/invocation graph state and projected
back to the model in stable tool-call order. The SDK does not need Claude
Code's multi-parent `tool_result` transcript trick for this.

### Hooks and stop/continue policy

Claude Code has blocking hooks, context hooks, stop hooks, and permission
decisions. The SDK equivalent is the event/effect contract:

- mutable event fields are explicitly declared by `EventContract`.
- hooks can `Patch`, `Block`, or return an `Action`.
- actions are reduced by the runtime into `Step`, `Stop`, or `Inject`.
- subscribers are durable asynchronous observers and cannot mutate execution.

This is already a cleaner kernel-level abstraction than injecting every hook
outcome as an ad hoc message.

## Partial Compatibility

### Permission and rejection policy

The SDK has the mechanics to express permission rejection:

- `before_tool` hooks can block or patch a tool call.
- `tool_error` can produce an `is_error` tool result.
- `ContextInjectionModePermission` can inject model-visible permission context.
- `PermissionRejection` and `DenyToolPermission` give hooks and gateways one
  standard way to produce foreground, subagent, or policy-neutral denial text
  with `ToolErrorPermissionDenied` as the structured error kind.

Exact Claude Code UI wording remains a presenter concern, but the control
semantics no longer require every gateway/plugin to invent its own rejection
strings.

### Query loop terminal and continuation state

Claude Code names many terminal and continuation reasons in `query.ts`. The SDK
does not need to expose the same strings to be compatible with the abstraction.
The kernel equivalent is:

- `ActionStep` to continue after model-visible tool results or injected context;
- `ActionInject` to add model-visible context and continue;
- `ActionStop` plus `Cause` to terminate with a durable reason;
- `EventDecide`, `EventTurnEnd`, and `EventAgentEnd` for policy and
  presentation;
- trajectory terminal and checkpoint payloads for resume/recovery.

This covers the loop shape. Exact Claude Code reason labels such as
`stop_hook_blocking`, `token_budget_continuation`, or
`reactive_compact_retry` should become SDK causes only if other presenters need
to coordinate around them.

### Task notifications, inter-agent messages, and local commands

The SDK can carry these as context injections targeted at a session/execution,
but it does not define Claude Code's XML schemas as first-class SDK payloads.
That is acceptable if presenters own formatting. It becomes a gap if plugins
need portable task-notification or teammate-message semantics across presenters.

The same rule applies to local command caveats and hidden attachment messages.
`ContextInjectionMode`, `Origin`, `IsMeta`, `Attributes`, and target fields are
the carrier. Claude Code-specific XML tags and attachment names should remain
outside the provider contract unless the SDK needs to make their meaning
portable.

### Stop hooks and plan-mode policy

The SDK event/effect model can represent Claude Code stop-hook outcomes:
blocking errors are injected context, `preventContinuation` is an `ActionStop`,
and continuation is an `ActionStep` or `ActionInject`. Plan-mode rejection can
also be expressed as a context injection plus a non-final stop/action policy.

What is not yet first-class is a named stop-hook phase with built-in background
jobs such as prompt suggestions, memory extraction, or job classification. Those
should stay plugin/presenter concerns until more than one host needs the same
durable semantics.

### Fork cache safety

Forked sessions project from the durable parent branch and close inherited
unresolved tool calls with a placeholder. This preserves the important
byte-stable prefix property at the message level.

The SDK does not yet expose Claude Code-style cache-safe parameter metadata or a
content replacement state. If prompt-cache hit rate becomes a public guarantee,
that state should live in checkpoint/resume payloads, not in gateway memory.

## Not Yet Covered

### Streaming provider deltas and early tool execution

Claude Code can start tool execution while the assistant stream is still
arriving, as soon as a `tool_use` block is observed. The current SDK provider
contract returns a completed `ModelResponse` before tool preparation begins.

Async operations cover long-running provider calls and tools, but they do not
expose a provider-delta stream or a tool-use-ready event. `provider_outcome`
currently reports terminal provider completion/failure as a live observation,
not as durable resume state and not as a token/content stream. Its `sequence`
field is the ordering slot for future provider deltas. Full compatibility with
Claude Code's streaming executor requires a new provider streaming abstraction,
with durable ordering rules for partial assistant content and early tool-call
preparation.

The non-streaming runtime path now separates provider terminal completion from
the trajectory response append. That keeps today's completed `ModelResponse`
contract intact while giving streaming a single internal replacement point:
future stream deltas and tool-ready signals should produce ordered provider
outcomes, and the trajectory projection should remain the durable source for
fork/resume.

Async resources can also implement `sdk.OperationWatcher`, so provider, tool,
and capability waits no longer have to rely only on fixed-interval polling.
That is an operation-level event-driven boundary; it does not yet expose
assistant content deltas or early `tool_use` readiness.

### Tool-result replacement state

Claude Code tracks large tool results and replaces repeated results with compact
summaries. The SDK has checkpoint continuation and branch projection, but no
portable `ContentReplacementState` equivalent. This should be modeled as
checkpoint/resume metadata if needed, not as provider-specific message surgery.

### Compact and context-collapse policy

Claude Code's auto-compact, reactive compact, and context-collapse paths are
runtime policies that mutate the model-visible prefix while preserving durable
transcript history. The SDK has checkpoints and branch projection, but no
first-class compact boundary entry, compact scheduler, retry circuit breaker, or
replacement-state projection.

If this becomes a compatibility requirement, the compact state should belong to
trajectory/checkpoint durability. Gateway memory must not decide which branch
prefix is visible after resume.

### Token budget continuation

Claude Code can inject nudge messages until an output-token target is met. The
SDK can approximate this today with context injection and `ActionStep`, but it
does not own a durable token-budget controller. A real SDK abstraction would
need budget state in the execution checkpoint and a policy reducer that decides
whether to continue, stop, or escalate output limits.

### Recursive fork policy

Claude Code forbids fork children from forking again. The SDK trajectory model
can represent nested forks, which remains the default general-purpose SDK
behavior. Hosts that need exact Claude Code compatibility can configure
`AgentForkPolicyDenyNested`; the runtime then rejects fork-child fork requests
while leaving the trajectory model capable of representing nested forks for
other hosts.

## Boundary Rule

When a Claude Code feature looks like a workaround in gateway or presenter code,
first ask which SDK kernel owns it:

- branch/history semantics belong to trajectory;
- long-running work belongs to operation;
- nested execution shape belongs to invocation;
- model-visible external events belong to context injection;
- policy gates belong to events/effects;
- asynchronous observations belong to delivery.

If a feature cannot be placed in one of those kernels, it is probably a missing
SDK abstraction rather than a gateway feature.
