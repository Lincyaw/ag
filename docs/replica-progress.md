# Claude Code Replica Progress

North star: reproduce Claude Code's terminal interaction logic and visible
behavior completely, while keeping the TUI responsible only for interaction
and presentation and the gateway responsible for durable background agents.

## Current status

The autonomous harness can now use `ag` itself as the coding agent. One durable
trajectory survives accepted builds, auto-managed gateway restarts, and context
compaction. It is registered in the normal manager and remains visible to
`ag trajectory list` and `ag run <trajectory-id>`. Each iteration must leave a
structured handoff below before the loop will consider its code.

The self-hosted trajectory now uses the same URI-selected GORM state path as
the Agent Manager. SQLite and PostgreSQL share one persistence implementation;
the manager configuration is authoritative, with PostgreSQL recommended for a
persistent or shared deployment rather than a second loop-private backend.
Gateway sessions, queued inputs, interactions, reconnect events, and runtime
trajectory state also share that URI. Legacy JSON files are migration inputs,
not a second live control plane.

Trajectory checkpoints and provider-request audit entries are now compact
branch projections rather than full conversation copies. Conversation views
and restart recovery inspect the branch index first and hydrate only the latest
compatible snapshot plus later message deltas. On the long-running replica
trajectory this reduced a real `trajectory show` to about 53 MiB of client RSS
and a restart recovery from roughly 1.2 GiB peak gateway RSS to about 130 MiB,
without deleting the durable trajectory.

The harness also supplies a dedicated coding system prompt to emphasize focused
implementation, tests, and the progress contract. Write capability itself is
controlled by the file plugin profile: the self-hosted trajectory exposes both
`write_file` and `edit_file` with `workspace.enable_write=true`.
The loop does not set `max_turns`; runtime value `0` is the normal unlimited
mode. Wall-clock timeout, cancellation, and semantic terminal causes remain the
outer safety boundaries.

The comparison score is a regression gate, not an agent reward or a claim of
functional completeness. It is the mean of the fixed scenario comparison
scores produced by `replica-lab`; each scenario combines terminal-cell and
rendered-pixel similarity. A candidate is accepted only when every scenario is
non-regressing and the suite mean improves. Interaction coverage and explicit
state-transition tests remain authoritative for behavior that a screenshot
cannot observe.

The fixed comparison suite currently establishes these first deterministic
surfaces:

| Surface | Scenario | Status |
|---|---|---|
| Cold launch and idle prompt | `launch` | measured |
| Prompt editing | `editor` | measured |
| Slash-command completion | `slash` | measured |

Coverage still needs to grow toward submission/streaming, tool calls,
cancellation, queued input, background/detach/reattach, `ask_user`, permission
and diff flows, scrolling, resize behavior, session lifecycle, errors, and
terminal capability variations. Scenario definitions live under
`tools/replica/scenarios`; a run snapshots them before capturing Claude so an
iteration cannot accidentally compare different input scripts.

## Iteration handoff contract

Every candidate entry appended by the self-hosted agent records:

- the observed Claude/ag mismatch and chosen scope;
- files and interaction state transitions changed;
- tests and visual evidence inspected;
- remaining risk and the next highest-value target.

The loop appends the authoritative accepted/rejected result, score, trajectory
ID, and artifact path. Rejected code is discarded only inside the disposable
worktree, but its attempt remains in the run artifacts and results TSV.

## Iterations
