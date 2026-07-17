# File plugin

The file plugin exposes root-confined text operations designed for agent use.
It does not execute shell commands and never accepts absolute paths.

Read-only mode registers:

- `read_file`: reads a bounded, numbered line range and returns the complete
  file's SHA-256 revision.
- `list_files`: lists one directory level.
- `search_files`: searches UTF-8 files using literal text or Go regular
  expressions, with optional `*`, `**`, and `?` file globs.

Enabling writes also registers:

- `write_file`: creates a file atomically. Replacing an existing file requires
  its current `expected_sha256`.
- `edit_file`: atomically replaces one exact text occurrence, every exact
  occurrence, or an inclusive 1-based line range. It always requires
  `expected_sha256`.

The revision is an explicit optimistic-concurrency token. An agent should read,
copy the returned `sha256`, and use it in the next edit or overwrite. Calls in a
plugin process are serialized per target path; if another call changes the file
first, the write is rejected and the agent must read again. Expected input
errors are returned as tool errors so the agent can correct its call without
terminating the surrounding loop.

The plugin rejects root traversal, escaping symbolic links, non-UTF-8 content,
oversized files, and attempts to replace a symbolic link. Writes use a
same-directory temporary file, `fsync`, rename, directory `fsync`, and read-back
verification.
