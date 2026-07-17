# CLI contract

`ag` serves humans and programs through one command tree and two explicit
representations:

- `text` is the default. It presents answers, summaries, aligned tables, and
  copyable trajectory/session identifiers.
- `json` is enabled only with `-o json` or `--output json`. It preserves stable
  field names and complete payloads for scripts, agents, and CI.

The CLI never changes representation merely because stdout is redirected. A
program that requires JSON must request it explicitly.

## Output boundary

Business results are written to stdout. Runtime logs are appended to
`~/.ag/logs/ag.log` by default, while command errors are written to stderr.
Consequently, this is safe:

```bash
ag trajectory list -o json | jq '.[].id'
```

On success, `-o json` emits one JSON document to stdout. On failure, stdout is
empty and stderr ends with this error document:

```json
{
  "error": {
    "type": "usage",
    "message": "--prompt is required",
    "exit_code": 2,
    "retryable": false,
    "suggestion": "Run 'ag --help' or 'ag <command> --help' for valid arguments."
  }
}
```

Runtime logs do not precede the error on stderr unless console logging was
explicitly enabled.

Human progress is separate from runtime logs. In text mode, `--progress auto`
opens a transient inline Bubble Tea dashboard on stderr when stderr is a
terminal. The dashboard shows the task, current phase, turn, tool progress, and
high-level activity while the run is active, then clears before the final answer
is written to stdout. The default Overview intentionally hides low-level
provider details, message counts, checksums, and raw tool output. Timeline and
Details retain those traces for inspection. The renderer adapts to any tool by
summarizing its JSON arguments and textual result; it does not require the CLI
to know plugin-specific tool names.

When stdin is also a terminal, the dashboard is interactive: `tab` switches
Overview / Timeline / Details, `j` / `k` or arrow keys move between events,
`f` toggles following the newest event, `?` shows key help, `q` hides the
dashboard without stopping the run, and `ctrl+c` cancels the run.

Use `--progress plain` for append-only progress lines, `--progress always` to
force progress even when stderr is not a terminal, `--progress tui` to prefer
the inline terminal panel, or `--progress never` for fully quiet text output.
`--color auto` colors human progress only on a terminal; use `--color always`
or `--color never` to override that. JSON mode never emits progress records.

`ag registry serve -o json` is a long-running exception: it emits one complete
ready document after the listener and backend are ready, then keeps stdout
open until shutdown.

Use `--log-file <path>` or `[logging].file` to change the append-only log
destination. Use `--log-console` or `[logging].console = true` to additionally
copy runtime logs to stderr for interactive debugging. Console logging never
replaces the file destination.

OpenAI-compatible credentials are read from `[openai].api_key` in the config
file, with `OPENAI_API_KEY` and `AGENTM_OPENAI_API_KEY` kept as compatibility
aliases. There is no API-key CLI flag. `ag config show` reports only whether the
key is set; it never prints the key value.

## JSON result shapes

Existing JSON fields are a compatibility contract. Additive fields may appear
in minor releases; fields are not renamed or removed without a major release.

| Command | JSON document |
|---|---|
| `ag run` | `{"session_id": string, "result": Result}` |
| `ag config show` | `{"file": string, "config": Config}` |
| `ag config path` | `{"path": string}` |
| `ag plugin list` | `PluginDescriptor[]` |
| `ag plugin discover` | `PluginDiscovery[]` (includes the existing descriptor fields plus namespace, instance, version, lease times, revision, and epoch) |
| `ag plugin inspect` | `Manifest` |
| `ag registry serve` | `{"uri": string, "listen": string, "backend": string, "capabilities": RegistryCapabilities, "pid": number}` |
| `ag trajectory list` | `TrajectorySummary[]` |
| `ag trajectory show` | `Trajectory` |
| `ag trajectory rollback` | `{"trajectory_id": string, "head": string, "checkpoint_id": string}` |
| `ag trajectory rollback --dry-run` | `{"trajectory_id": string, "current_head": string, "checkpoint_id": string, "dry_run": true}` |
| `ag invocation show` | `InvocationGraph` |
| `ag state inspect` | `{"backend": string, "namespace": string, "capabilities": StorageCapabilities}` |
| `ag state prune` | `{"operations": number, "deliveries": number, "trajectories": number}` |
| `ag state prune --dry-run` | `{"cutoff": string, "dry_run": true}` |
| `ag version` | `{"version": string}` |
| `ag --dump-schema` | `CommandSchema` |

`trajectory show` intentionally keeps complete entry payloads in JSON. The
default text renderer summarizes high-value fields such as turn, provider,
tool name, and tool result status.

## Exit status

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | runtime or unknown failure |
| 2 | invalid command arguments |
| 130 | interrupted by the user or cancellation signal |

## Examples

Human-oriented defaults:

```bash
ag run -p "Summarize this repository"
ag trajectory list
ag trajectory show <session-id>
ag invocation show <root-invocation-id>
ag config show
ag plugin discover
ag registry serve
ag state prune --before 720h --dry-run
```

Program-oriented equivalents:

```bash
ag run -p "Summarize this repository" -o json
ag trajectory list -o json
ag trajectory show <session-id> -o json
ag invocation show <root-invocation-id> -o json
ag config show -o json
ag plugin discover -o json
ag registry serve -o json
ag --dump-schema
```

`-o` is a global flag, so it may appear before or after the subcommand.
`ag --version` is equivalent to `ag version`; both honor `-o json`.

Plugin selection is explicit:

```bash
ag run --plugin name=grpc://host:port -p "Use this endpoint"
ag run --plugin name@instance-id -p "Use this discovered instance"
```

A plain discovered name succeeds only when one active instance exists. See
[registry.md](registry.md) for backend and cursor semantics.
