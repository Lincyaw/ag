# Claude Code Compatibility Map

This note maps the Claude Code message, fork, resume, and queue patterns to the
SDK runtime model. The target is compatibility at the abstraction boundary, not
line-by-line emulation of Claude Code's JSONL or UI implementation.

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

### Task notifications and inter-agent messages

The SDK can carry these as context injections targeted at a session/execution,
but it does not define Claude Code's XML schemas as first-class SDK payloads.
That is acceptable if presenters own formatting. It becomes a gap if plugins
need portable task-notification or teammate-message semantics across presenters.

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
expose a provider-delta stream or a tool-use-ready event. Full compatibility
with Claude Code's streaming executor requires a new provider streaming
abstraction, with durable ordering rules for partial assistant content and
early tool-call preparation.

### Tool-result replacement state

Claude Code tracks large tool results and replaces repeated results with compact
summaries. The SDK has checkpoint compaction and branch projection, but no
portable `ContentReplacementState` equivalent. This should be modeled as
checkpoint/resume metadata if needed, not as provider-specific message surgery.

### Recursive fork policy

Claude Code forbids fork children from forking again. The SDK trajectory model
can represent nested forks. This is more general, but exact compatibility needs
an explicit policy decision: either keep nested forks as an SDK extension, or
add a runtime policy guard for Claude-Code-compatible agents.

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
