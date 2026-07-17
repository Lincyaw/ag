#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository=$(CDPATH= cd -- "$script_dir/../.." && pwd)
stubs="$script_dir/.generated"

mkdir -p "$stubs"

AGENTM_PYTHON_STUBS="$stubs" \
UV_PROJECT_ENVIRONMENT="$script_dir/.venv" \
uv run --project "$script_dir" --frozen \
	python -m grpc_tools.protoc \
	-I "$repository/pluginrpc/v1" \
	--python_out "$stubs" \
	--grpc_python_out "$stubs" \
	"$repository/pluginrpc/v1/plugin.proto"

exec env \
	AGENTM_PYTHON_STUBS="$stubs" \
	UV_PROJECT_ENVIRONMENT="$script_dir/.venv" \
	uv run --project "$script_dir" --frozen \
	python "$script_dir/plugin.py" \
	--listen "${AGENTM_PYTHON_LISTEN:-127.0.0.1:9003}" \
	--events-file "${AGENTM_PYTHON_EVENTS_FILE:-$script_dir/.events.jsonl}" \
	"$@"
