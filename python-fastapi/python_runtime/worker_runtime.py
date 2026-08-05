from __future__ import annotations

import argparse
import asyncio
import json
import os
import struct
from pathlib import Path
from typing import Any

from app.main import app
from python_runtime.asgi_adapter import AsgiLifespan, invoke_http

PROTOCOL_VERSION = 1


async def read_frame(reader: asyncio.StreamReader, max_frame_bytes: int) -> tuple[dict[str, Any], bytes]:
    length_bytes = await reader.readexactly(4)
    payload_length = struct.unpack(">I", length_bytes)[0]
    if payload_length <= 4 or payload_length > max_frame_bytes:
        raise ValueError(f"Invalid protocol frame length: {payload_length}")
    payload = await reader.readexactly(payload_length)
    metadata_length = struct.unpack(">I", payload[:4])[0]
    if metadata_length <= 0 or metadata_length > payload_length - 4:
        raise ValueError(f"Invalid protocol metadata length: {metadata_length}")
    metadata = json.loads(payload[4 : 4 + metadata_length].decode("utf-8"))
    if metadata.get("protocolVersion") != PROTOCOL_VERSION:
        raise ValueError(f"Unsupported protocol version: {metadata.get('protocolVersion')}")
    if not isinstance(metadata.get("type"), str):
        raise ValueError("Protocol message type is required")
    return metadata, payload[4 + metadata_length :]


async def write_frame(
    writer: asyncio.StreamWriter,
    metadata: dict[str, Any],
    body: bytes,
    max_frame_bytes: int,
) -> None:
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    payload_length = 4 + len(metadata_bytes) + len(body)
    if payload_length <= 4 or payload_length > max_frame_bytes:
        raise ValueError(f"Protocol frame size is outside the configured limit: {payload_length}")
    writer.write(struct.pack(">II", payload_length, len(metadata_bytes)))
    writer.write(metadata_bytes)
    writer.write(body)
    await writer.drain()


async def run_worker(
    socket_path: Path,
    max_frame_bytes: int,
    max_in_flight_per_worker: int,
) -> None:
    lifespan = AsgiLifespan(app)
    await lifespan.startup()
    stop_event = asyncio.Event()
    shutdown_completed = False
    client_connected = False

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal client_connected, shutdown_completed
        if client_connected:
            writer.close()
            await writer.wait_closed()
            return
        client_connected = True
        active_tasks: dict[str, asyncio.Task[None]] = {}
        response_write_lock = asyncio.Lock()

        async def write_serialized(metadata: dict[str, Any], body: bytes) -> None:
            async with response_write_lock:
                await write_frame(writer, metadata, body, max_frame_bytes)

        async def execute_request(
            request_id: str,
            metadata: dict[str, Any],
            body: bytes,
        ) -> None:
            try:
                try:
                    status, headers, response_body = await invoke_http(app, metadata, body)
                    response_metadata = {
                        "protocolVersion": PROTOCOL_VERSION,
                        "type": "response",
                        "requestId": request_id,
                        "status": status,
                        "headers": headers,
                    }
                except Exception as exception:
                    response_body = b""
                    response_metadata = {
                        "protocolVersion": PROTOCOL_VERSION,
                        "type": "error",
                        "requestId": request_id,
                        "code": "ASGI_EXECUTION_FAILED",
                        "message": str(exception),
                    }
                await write_serialized(response_metadata, response_body)
            finally:
                active_tasks.pop(request_id, None)

        await write_serialized(
            {
                "protocolVersion": PROTOCOL_VERSION,
                "type": "ready",
                "workerPid": os.getpid(),
                "maxInFlightPerWorker": max_in_flight_per_worker,
            },
            b"",
        )
        try:
            while True:
                metadata, body = await read_frame(reader, max_frame_bytes)
                message_type = metadata["type"]
                if message_type == "shutdown":
                    if active_tasks:
                        await asyncio.gather(*list(active_tasks.values()), return_exceptions=True)
                    await lifespan.shutdown()
                    shutdown_completed = True
                    await write_serialized(
                        {"protocolVersion": PROTOCOL_VERSION, "type": "shutdown_ack"},
                        b"",
                    )
                    stop_event.set()
                    return
                if message_type != "request":
                    raise ValueError(f"Unexpected protocol message: {message_type}")
                request_id = metadata.get("requestId")
                if not isinstance(request_id, str) or not request_id:
                    raise ValueError("Request ID is required")
                if request_id in active_tasks:
                    raise ValueError(f"Duplicate active request ID: {request_id}")
                if len(active_tasks) >= max_in_flight_per_worker:
                    await write_serialized(
                        {
                            "protocolVersion": PROTOCOL_VERSION,
                            "type": "error",
                            "requestId": request_id,
                            "code": "CAPACITY_EXCEEDED",
                            "message": "Managed Python worker capacity exceeded",
                        },
                        b"",
                    )
                    continue
                task = asyncio.create_task(
                    execute_request(request_id, metadata, body),
                    name=f"managed-python-request-{request_id}",
                )
                task.add_done_callback(
                    lambda completed: writer.close()
                    if not completed.cancelled() and completed.exception() is not None
                    else None
                )
                active_tasks[request_id] = task
        except asyncio.IncompleteReadError:
            stop_event.set()
        finally:
            remaining_tasks = list(active_tasks.values())
            for task in remaining_tasks:
                task.cancel()
            if remaining_tasks:
                await asyncio.gather(*remaining_tasks, return_exceptions=True)
            stop_event.set()
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass

    socket_path.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(handle_client, path=socket_path)
    try:
        async with server:
            await stop_event.wait()
    finally:
        server.close()
        await server.wait_closed()
        try:
            if not shutdown_completed:
                await lifespan.shutdown()
        finally:
            socket_path.unlink(missing_ok=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Managed Python Runtime worker")
    parser.add_argument("--socket-path", type=Path, required=True)
    parser.add_argument("--max-frame-bytes", type=int, required=True)
    parser.add_argument("--max-in-flight-per-worker", type=int, required=True)
    arguments = parser.parse_args()
    if arguments.max_frame_bytes < 1024:
        parser.error("--max-frame-bytes must be at least 1024")
    if not 1 <= arguments.max_in_flight_per_worker <= 64:
        parser.error("--max-in-flight-per-worker must be between 1 and 64")
    return arguments


def main() -> None:
    arguments = parse_arguments()
    asyncio.run(
        run_worker(
            arguments.socket_path,
            arguments.max_frame_bytes,
            arguments.max_in_flight_per_worker,
        )
    )


if __name__ == "__main__":
    main()
