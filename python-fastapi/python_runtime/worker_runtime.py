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


async def run_worker(socket_path: Path, max_frame_bytes: int) -> None:
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
        await write_frame(
            writer,
            {"protocolVersion": PROTOCOL_VERSION, "type": "ready", "workerPid": os.getpid()},
            b"",
            max_frame_bytes,
        )
        try:
            while True:
                metadata, body = await read_frame(reader, max_frame_bytes)
                message_type = metadata["type"]
                if message_type == "shutdown":
                    await lifespan.shutdown()
                    shutdown_completed = True
                    await write_frame(
                        writer,
                        {"protocolVersion": PROTOCOL_VERSION, "type": "shutdown_ack"},
                        b"",
                        max_frame_bytes,
                    )
                    stop_event.set()
                    return
                if message_type != "request":
                    raise ValueError(f"Unexpected protocol message: {message_type}")
                request_id = metadata.get("requestId")
                try:
                    status, headers, response_body = await invoke_http(app, metadata, body)
                    await write_frame(
                        writer,
                        {
                            "protocolVersion": PROTOCOL_VERSION,
                            "type": "response",
                            "requestId": request_id,
                            "status": status,
                            "headers": headers,
                        },
                        response_body,
                        max_frame_bytes,
                    )
                except Exception as exception:
                    await write_frame(
                        writer,
                        {
                            "protocolVersion": PROTOCOL_VERSION,
                            "type": "error",
                            "requestId": request_id,
                            "code": "ASGI_EXECUTION_FAILED",
                            "message": str(exception),
                        },
                        b"",
                        max_frame_bytes,
                    )
        except asyncio.IncompleteReadError:
            stop_event.set()
        finally:
            writer.close()
            await writer.wait_closed()

    socket_path.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(handle_client, path=socket_path)
    try:
        async with server:
            await stop_event.wait()
    finally:
        server.close()
        await server.wait_closed()
        if not shutdown_completed:
            await lifespan.shutdown()
        socket_path.unlink(missing_ok=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Managed Python Runtime worker")
    parser.add_argument("--socket-path", type=Path, required=True)
    parser.add_argument("--max-frame-bytes", type=int, required=True)
    arguments = parser.parse_args()
    if arguments.max_frame_bytes < 1024:
        parser.error("--max-frame-bytes must be at least 1024")
    return arguments


def main() -> None:
    arguments = parse_arguments()
    asyncio.run(run_worker(arguments.socket_path, arguments.max_frame_bytes))


if __name__ == "__main__":
    main()
