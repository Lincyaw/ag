# Python plugin example

This process implements `pluginrpc/v1/plugin.proto` directly. It does not
import the Go SDK or any AgentM runtime package.

Generate Python stubs and start it from this directory:

```bash
mkdir -p .generated
uv run python -m grpc_tools.protoc \
  -I ../../pluginrpc/v1 \
  --python_out .generated \
  --grpc_python_out .generated \
  ../../pluginrpc/v1/plugin.proto

AGENTM_PYTHON_STUBS=.generated uv run python plugin.py \
  --listen 127.0.0.1:9003 \
  --events-file .events.jsonl
```

The process prints one ready JSON record on stdout. Mount the returned URI
explicitly:

```bash
bin/ag run \
  --openai=false --file=false \
  --plugin python-e2e=grpc://127.0.0.1:9003 \
  --provider python-model \
  --prompt "exercise the Python provider and tool"
```

Pass `--registry-uri grpc://host:port --lease-ttl-ms 3000` to register,
periodically renew, and unregister a discovery lease.
