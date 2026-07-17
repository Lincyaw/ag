# Python plugin example

This process implements `pluginrpc/v1/plugin.proto` directly. It does not
import the Go SDK or any AgentM runtime package.

Generate Python stubs and start it locally from this directory:

```bash
./run-local.sh
```

The script keeps its virtual environment and generated protobuf stubs in this
directory, then listens on `grpc://127.0.0.1:9003`. The process prints one
ready JSON record on stdout. Mount the URI explicitly:

```bash
bin/ag run \
  --openai=false --file=false \
  --plugin python-e2e=grpc://127.0.0.1:9003 \
  --provider python-model \
  --prompt "exercise the Python provider and tool"
```

Pass `--registry-uri grpc://host:port --lease-ttl-ms 3000` to register,
periodically renew, and unregister a discovery lease.
