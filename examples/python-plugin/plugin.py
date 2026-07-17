#!/usr/bin/env python3
"""Independent Python implementation of the AgentM plugin wire protocol."""

from __future__ import annotations

import argparse
from concurrent import futures
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time
from urllib.parse import urlparse
import uuid

stub_directory = os.environ.get("AGENTM_PYTHON_STUBS", "")
if not stub_directory:
    raise RuntimeError("AGENTM_PYTHON_STUBS must point to generated Python stubs")
sys.path.insert(0, stub_directory)

import grpc  # noqa: E402
from google.protobuf import json_format  # noqa: E402
from google.protobuf.struct_pb2 import Struct  # noqa: E402
import plugin_pb2 as protocol  # noqa: E402
import plugin_pb2_grpc as services  # noqa: E402


PLUGIN_NAME = "python-e2e"
PROVIDER_NAME = "python-model"
ECHO_TOOL = "python-echo"
SLOW_TOOL = "python-slow"
CAPABILITY_NAME = "python-state"
HOOK_NAME = "python-system"
SUBSCRIBER_NAME = "python-agent-end"


def now_millis() -> int:
    return int(time.time() * 1000)


def to_struct(value: dict[str, object]) -> Struct:
    return json_format.ParseDict(value, Struct())


def from_struct(value: Struct) -> dict[str, object]:
    return json_format.MessageToDict(value, preserving_proto_field_name=True)


def clone_operation(operation: protocol.Operation) -> protocol.Operation:
    result = protocol.Operation()
    result.CopyFrom(operation)
    return result


def manifest() -> protocol.Manifest:
    return protocol.Manifest(
        name=PLUGIN_NAME,
        version="1.0.0",
        description="independent Python provider, tools, capability, hook, and subscriber",
        api_version=1,
        registers=[
            f"provider:{PROVIDER_NAME}",
            f"tool:{ECHO_TOOL}",
            f"tool:{SLOW_TOOL}",
            f"capability:{CAPABILITY_NAME}",
            f"hook:{HOOK_NAME}",
            f"subscriber:{SUBSCRIBER_NAME}",
        ],
    )


@dataclass
class OperationRecord:
    kind: int
    resource: str
    operation: protocol.Operation
    cancelled: threading.Event = field(default_factory=threading.Event)


class PluginService(services.PluginServiceServicer):
    def __init__(self, events_file: str) -> None:
        self._events_file = events_file
        self._lock = threading.RLock()
        self._operations: dict[str, OperationRecord] = {}
        self._idempotency: dict[tuple[int, str, str], str] = {}
        self._executor = futures.ThreadPoolExecutor(
            max_workers=8,
            thread_name_prefix="python-plugin-operation",
        )

    def Describe(self, _request, _context):
        object_schema = to_struct(
            {
                "type": "object",
                "additionalProperties": False,
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            }
        )
        return protocol.DescribeResponse(
            manifest=manifest(),
            providers=[protocol.ProviderSpec(name=PROVIDER_NAME, model="python-v1")],
            tools=[
                protocol.ToolSpec(
                    name=ECHO_TOOL,
                    description="echo a value from a Python worker",
                    parameters=object_schema,
                ),
                protocol.ToolSpec(
                    name=SLOW_TOOL,
                    description="wait until completion or cancellation",
                    parameters=to_struct({"type": "object"}),
                ),
            ],
            hooks=[
                protocol.HookSpec(
                    name=HOOK_NAME,
                    event="before_agent_start",
                    priority=100,
                    failure_policy=protocol.FAILURE_POLICY_FAIL_CLOSED,
                    timeout_millis=1000,
                )
            ],
            subscribers=[
                protocol.SubscriberSpec(
                    name=SUBSCRIBER_NAME,
                    events=["agent_end"],
                    timeout_millis=1000,
                )
            ],
            capabilities=[
                protocol.CapabilitySpec(
                    name=CAPABILITY_NAME,
                    description="return Python-owned serializable state",
                    input_schema=to_struct({"type": "object"}),
                    output_schema=to_struct({"type": "object"}),
                )
            ],
        )

    def SubmitOperation(self, request, context):
        if request.request is None or not request.request.idempotency_key:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "operation request and idempotency key are required",
            )
        if not self._supports(request.kind, request.resource):
            context.abort(grpc.StatusCode.NOT_FOUND, "operation resource not found")

        key = (
            request.kind,
            request.resource,
            request.request.idempotency_key,
        )
        with self._lock:
            existing_id = self._idempotency.get(key)
            if existing_id:
                return protocol.SubmitOperationResponse(
                    operation=clone_operation(self._operations[existing_id].operation)
                )
            operation_id = uuid.uuid4().hex
            timestamp = now_millis()
            operation = protocol.Operation(
                id=operation_id,
                idempotency_key=request.request.idempotency_key,
                state=protocol.OPERATION_STATE_RUNNING,
                revision=1,
                submitted_unix_milli=timestamp,
                updated_unix_milli=timestamp,
            )
            record = OperationRecord(
                kind=request.kind,
                resource=request.resource,
                operation=operation,
            )
            self._operations[operation_id] = record
            self._idempotency[key] = operation_id
            operation_input = from_struct(request.request.input)
            self._executor.submit(
                self._execute,
                operation_id,
                operation_input,
            )
            return protocol.SubmitOperationResponse(
                operation=clone_operation(operation)
            )

    def PollOperation(self, request, context):
        record = self._record(request.id, request.kind, request.resource, context)
        with self._lock:
            return protocol.PollOperationResponse(
                operation=clone_operation(record.operation)
            )

    def CancelOperation(self, request, context):
        record = self._record(request.id, request.kind, request.resource, context)
        with self._lock:
            if record.operation.state in (
                protocol.OPERATION_STATE_PENDING,
                protocol.OPERATION_STATE_RUNNING,
            ):
                record.cancelled.set()
                record.operation.state = protocol.OPERATION_STATE_CANCELLED
                record.operation.revision += 1
                record.operation.updated_unix_milli = now_millis()
            return protocol.CancelOperationResponse(
                operation=clone_operation(record.operation)
            )

    def HandleHook(self, request, context):
        if request.hook != HOOK_NAME:
            context.abort(grpc.StatusCode.NOT_FOUND, "hook not found")
        if request.event.name != "before_agent_start":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unexpected hook event")
        return protocol.HandleHookResponse(
            effect=protocol.Effect(
                patch=to_struct({"system": "system-from-python-hook"})
            )
        )

    def Deliver(self, request, context):
        if request.delivery.subscription != SUBSCRIBER_NAME:
            context.abort(grpc.StatusCode.NOT_FOUND, "subscriber not found")
        if self._events_file:
            path = Path(self._events_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = json_format.MessageToDict(
                request.delivery,
                preserving_proto_field_name=True,
            )
            with self._lock:
                with path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(payload, sort_keys=True) + "\n")
        return protocol.DeliverResponse(accepted=True)

    def close(self) -> None:
        with self._lock:
            for record in self._operations.values():
                record.cancelled.set()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _record(self, operation_id, kind, resource, context) -> OperationRecord:
        with self._lock:
            record = self._operations.get(operation_id)
            if record is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "operation not found")
            if record.kind != kind or record.resource != resource:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "operation kind or resource mismatch",
                )
            return record

    @staticmethod
    def _supports(kind: int, resource: str) -> bool:
        return (
            (kind == protocol.OPERATION_KIND_PROVIDER and resource == PROVIDER_NAME)
            or (
                kind == protocol.OPERATION_KIND_TOOL
                and resource in (ECHO_TOOL, SLOW_TOOL)
            )
            or (
                kind == protocol.OPERATION_KIND_CAPABILITY
                and resource == CAPABILITY_NAME
            )
        )

    def _execute(
        self,
        operation_id: str,
        operation_input: dict[str, object],
    ) -> None:
        try:
            time.sleep(0.03)
            with self._lock:
                record = self._operations[operation_id]
                kind = record.kind
                resource = record.resource
            if resource == SLOW_TOOL:
                if record.cancelled.wait(5):
                    return
                output = {
                    "content": "slow Python operation completed",
                    "is_error": False,
                }
            elif kind == protocol.OPERATION_KIND_PROVIDER:
                output = self._complete_model(operation_input)
            elif kind == protocol.OPERATION_KIND_TOOL:
                output = {
                    "content": f"python:{operation_input.get('value', '')}",
                    "is_error": False,
                }
            elif kind == protocol.OPERATION_KIND_CAPABILITY:
                output = {
                    "language": "python",
                    "input": operation_input,
                }
            else:
                raise ValueError("unsupported operation")
            self._succeed(operation_id, output)
        except Exception as error:
            self._fail(operation_id, str(error))

    @staticmethod
    def _complete_model(
        operation_input: dict[str, object],
    ) -> dict[str, object]:
        messages = operation_input.get("messages", [])
        if not isinstance(messages, list):
            raise ValueError("provider messages must be a list")
        system_messages = [
            message
            for message in messages
            if isinstance(message, dict) and message.get("role") == "system"
        ]
        if not system_messages or system_messages[0].get("content") != (
            "system-from-python-hook"
        ):
            raise ValueError("Python provider did not observe the remote hook patch")

        tool_messages = [
            message
            for message in messages
            if isinstance(message, dict) and message.get("role") == "tool"
        ]
        if not tool_messages:
            return {
                "tool_calls": [
                    {
                        "id": "python-tool-call",
                        "name": ECHO_TOOL,
                        "arguments": {"value": "from-python-provider"},
                    }
                ],
                "model": "python-v1",
                "finish_reason": "tool_calls",
                "usage": {"input_tokens": 3, "output_tokens": 2},
            }
        return {
            "content": (
                "python-session-complete:" + str(tool_messages[-1].get("content", ""))
            ),
            "model": "python-v1",
            "finish_reason": "stop",
            "usage": {"input_tokens": 5, "output_tokens": 3},
        }

    def _succeed(
        self,
        operation_id: str,
        output: dict[str, object],
    ) -> None:
        with self._lock:
            record = self._operations[operation_id]
            if record.operation.state == protocol.OPERATION_STATE_CANCELLED:
                return
            record.operation.output.CopyFrom(to_struct(output))
            record.operation.state = protocol.OPERATION_STATE_SUCCEEDED
            record.operation.revision += 1
            record.operation.updated_unix_milli = now_millis()

    def _fail(self, operation_id: str, message: str) -> None:
        with self._lock:
            record = self._operations[operation_id]
            if record.operation.state == protocol.OPERATION_STATE_CANCELLED:
                return
            record.operation.error = message
            record.operation.state = protocol.OPERATION_STATE_FAILED
            record.operation.revision += 1
            record.operation.updated_unix_milli = now_millis()


def registry_address(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "grpc" or not parsed.netloc:
        raise ValueError("the Python example registry URI must use grpc://")
    return parsed.netloc


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1:0")
    parser.add_argument("--events-file", default="")
    parser.add_argument("--registry-uri", default="")
    parser.add_argument("--lease-ttl-ms", type=int, default=3000)
    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    if arguments.lease_ttl_ms < 30:
        raise ValueError("--lease-ttl-ms must be at least 30")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())

    service = PluginService(arguments.events_file)
    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=8,
            thread_name_prefix="python-plugin-rpc",
        )
    )
    services.add_PluginServiceServicer_to_server(service, server)
    port = server.add_insecure_port(arguments.listen)
    if port == 0:
        raise RuntimeError(f"cannot listen on {arguments.listen}")
    server.start()

    host = arguments.listen.rsplit(":", 1)[0]
    if host in ("", "0.0.0.0"):
        host = "127.0.0.1"
    uri = f"grpc://{host}:{port}"

    registry_channel = None
    registry = None
    lease_id = ""
    try:
        if arguments.registry_uri:
            registry_channel = grpc.insecure_channel(
                registry_address(arguments.registry_uri)
            )
            grpc.channel_ready_future(registry_channel).result(timeout=5)
            registry = services.RegistryServiceStub(registry_channel)
            response = registry.Register(
                protocol.RegisterRequest(
                    registration=protocol.Registration(
                        name=PLUGIN_NAME,
                        uri=uri,
                        manifest=manifest(),
                    ),
                    ttl_millis=arguments.lease_ttl_ms,
                ),
                timeout=5,
            )
            lease_id = response.lease.id

        print(
            json.dumps(
                {
                    "name": PLUGIN_NAME,
                    "uri": uri,
                    "pid": os.getpid(),
                },
                sort_keys=True,
            ),
            flush=True,
        )

        renewal_interval = arguments.lease_ttl_ms / 3000
        while not stop.wait(renewal_interval):
            if registry is not None:
                registry.Renew(
                    protocol.RenewRequest(
                        id=lease_id,
                        ttl_millis=arguments.lease_ttl_ms,
                    ),
                    timeout=5,
                )
    finally:
        if registry is not None and lease_id:
            try:
                registry.Unregister(
                    protocol.UnregisterRequest(id=lease_id),
                    timeout=2,
                )
            except grpc.RpcError:
                pass
        if registry_channel is not None:
            registry_channel.close()
        server.stop(grace=1).wait()
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
