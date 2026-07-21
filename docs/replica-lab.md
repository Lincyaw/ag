# TUI Replica Lab

The replica lab treats Claude Code as a fixed black-box reference and `ag run`
as the candidate. It captures both in isolated tmux pseudo-terminals with the
same rows, columns, `TERM`, timing, and scripted input.

The candidate wrapper exercises the same production cold-start path as users:
`ag run` starts its private local manager automatically and the wrapper only
cleans that disposable process up after capture.

Every capture contains:

- `screen.txt`: normalized visible terminal cells;
- `screen.ansi`: the styled tmux pane;
- `screen.html`: a deterministic Menlo rendering of the ANSI state;
- `screen.png`: a headless-Chrome raster when Chrome is available;
- `capture.json`: dimensions, command, and capture metadata.

`replica-lab compare` produces cell similarity, pixel similarity, a combined
score, and a red pixel-diff image. The two metrics are kept separate because a
font rasterization change must not hide a real terminal-cell regression.
Captures can use `--wait-for TEXT` to gate the snapshot on visible readiness;
this avoids treating gateway or Claude startup latency as a visual difference.

## One comparison

```sh
go build -o /tmp/replica-lab ./cmd/replica-lab
go build -o /tmp/ag ./cmd/ag
/tmp/replica-lab capture --command claude --out /tmp/reference \
  --wait-for Claude --settle 2s
/tmp/replica-lab capture \
  --command "tools/replica/run-ag.sh /tmp/ag" \
  --out /tmp/candidate --wait-for ag --settle 2s
/tmp/replica-lab compare \
  --reference /tmp/reference --candidate /tmp/candidate --out /tmp/report
```

Capture configuration can also be supplied as JSON with `--config`. Its
`actions` array accepts literal `text`, tmux `keys` such as `Enter` or `C-c`,
and a `wait_ms` stabilization delay. This is the deterministic scenario format
for launch, multiline editing, slash commands, scrolling, cancellation,
queued input, and `ask_user` interactions.

For small scenarios, repeat `--action` with the same JSON shape; order is
preserved:

```sh
/tmp/replica-lab capture --command claude --out /tmp/claude-editor \
  --wait-for Claude \
  --action '{"text":"Explain this repository","wait_ms":500}'
```

## Continuous improvement loop

`tools/replica/loop.sh` creates a dedicated git branch and disposable worktree.
It never edits or resets the caller's worktree. The caller's tracked changes and
non-ignored untracked files are copied into that worktree and committed as the
private starting checkpoint, so an uncommitted gateway or TUI change is not
silently omitted from the experiment.

By default the coding agent is `ag` itself:

```text
build ag → create trajectory in the normal manager → edit/test/measure
    ▲                                                   │
    └──── rebuild → controlled gateway restart → resume ┘
```

The first iteration records the trajectory ID. Accepted iterations install the
already captured candidate binary atomically and send `SIGTERM` only to the PID
whose ready record belongs to the effective auto-managed gateway directory. The
next invocation starts that new binary, recovers the same trajectory, and
continues with `ag run <trajectory-id>`. The loop does not create a second
private manager and does not stop the shared manager when the loop exits, so the
self-hosted trajectory is visible through the normal `ag trajectory list` and
attach flow.

The self-hosted trajectory enables the existing `auto-compact` hook with a low
threshold, so resumed provider calls compact prior conversation. Before an
iteration can be measured, the agent must create an in-progress entry in
`docs/replica-progress.md` before broad exploration and complete it before
returning. That file is the durable handoff across compaction and process
restart. A provider, hook, execution, cancellation, or turn-limit cause is a
failed iteration even when the CLI transport itself exited successfully. The
loop then appends the authoritative keep/discard result. A copy is always
available as `progress.md` under the run artifacts.

Scenario input is data-driven under `tools/replica/scenarios`. At run start the
corpus is frozen into the artifact directory and used for both Claude and ag.
Each iteration runs the configured tests, captures every fixed scenario, and
commits code only when no scenario regresses and the mean suite score strictly
improves. Failed tests and non-improving changes are saved as rejected patches;
the code is reset only inside the disposable worktree, while the rejection is
still committed to the progress log and appended to `results.tsv`.

The loop is infinite when `REPLICA_MAX_ITERATIONS=0` (the default):

```sh
REPLICA_REFERENCE_COMMAND='claude' \
REPLICA_MAX_ITERATIONS=0 \
tools/replica/loop.sh
```

The self-hosted agent uses the normal OpenAI-compatible ag configuration,
credentials, gateway directory, and state backend. Configure
`state.backend_uri` on that manager before starting the loop; PostgreSQL is
recommended for persistent/shared deployments, while the normal local SQLite
backend also preserves restart state. Useful controls include:

- `REPLICA_TEST_COMMAND` — authoritative test command for each iteration;
- `REPLICA_AGENT_TIMEOUT` — one coding turn timeout, default `45m`;
- `REPLICA_AGENT_SYSTEM` — self-hosted coding system prompt; the default
  emphasizes focused implementation, tests, and the progress contract;
- `REPLICA_COMPACT_TRIGGER_TOKENS` — auto-compact threshold, default `1` so
  every eligible resumed turn crosses the compact boundary;
- `REPLICA_PROGRESS_DOCUMENT` — repository-relative handoff path;
- `REPLICA_SCENARIOS_DIR` — source scenario corpus;
- `REPLICA_ARTIFACTS` / `REPLICA_WORKTREES` / `REPLICA_RUN_ID` — explicit
  isolated locations and identifier, useful for CI and controlled smoke tests;
- `REPLICA_AGENT_COMMAND` — optional legacy external-agent override. It receives
  `REPLICA_PROMPT`, `REPLICA_REPORT`, `REPLICA_WORKTREE`, and
  `REPLICA_PROGRESS`; self-restart and same-trajectory guarantees apply only to
  the default self-hosted mode.

Accepted changes stay on the printed `replica-lab/...` branch for human review
or cherry-picking. `state.json` records the live trajectory, binary, score,
report, worktree, and next iteration. Artifacts and worktrees live under ignored
`.replica-artifacts/` and `.replica-worktrees/` directories.
