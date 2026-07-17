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

Business results are written to stdout. Logs, warnings, progress, and errors
are written to stderr. Consequently, this is safe:

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

Runtime logs may precede the error on stderr; stderr is a diagnostic stream,
not the success data channel.

`ag registry serve -o json` is a long-running exception: it emits one complete
ready document after the listener and backend are ready, then keeps stdout
open until shutdown. Runtime logs remain on stderr.

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
| `ag state inspect` | `{"backend": string, "namespace": string, "capabilities": StorageCapabilities}` |
| `ag state prune` | `{"operations": number, "deliveries": number, "trajectories": number}` |
| `ag version` | `{"version": string}` |

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
ag config show
ag plugin discover
ag registry serve
```

Program-oriented equivalents:

```bash
ag run -p "Summarize this repository" -o json
ag trajectory list -o json
ag trajectory show <session-id> -o json
ag config show -o json
ag plugin discover -o json
ag registry serve -o json
```

`-o` is a global flag, so it may appear before or after the subcommand.

Plugin selection is explicit:

```bash
ag run --plugin name=grpc://host:port -p "Use this endpoint"
ag run --plugin name@instance-id -p "Use this discovered instance"
```

A plain discovered name succeeds only when one active instance exists. See
[registry.md](registry.md) for backend and cursor semantics.
