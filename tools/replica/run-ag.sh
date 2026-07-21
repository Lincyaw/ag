#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: run-ag.sh /absolute/path/to/ag" >&2
  exit 2
fi

candidate_binary=$1
replica_state=$(mktemp -d "${TMPDIR:-/tmp}/ag-replica.XXXXXX")
gateway_directory="$replica_state/gateway"
manager_ready="$gateway_directory/managed/ready.json"

cleanup() {
  if [[ -f "$manager_ready" ]]; then
    manager_pid=$(sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' "$manager_ready" | head -1)
    if [[ -n "$manager_pid" ]]; then
      kill "$manager_pid" 2>/dev/null || true
    fi
  fi
  rm -rf "$replica_state"
}
trap cleanup EXIT INT TERM

AGENTM_GATEWAY_DIRECTORY="$gateway_directory" \
AGENTM_REGISTRY_BACKEND_URI="file://$replica_state/registry" \
"$candidate_binary" --otel=false run \
  --registry-uri ""
