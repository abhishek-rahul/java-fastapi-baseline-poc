from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

AsgiApplication = Callable[
    [dict[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]


class AsgiLifespan:
    def __init__(self, application: AsgiApplication) -> None:
        self._application = application
        self._incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self._outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self._task: asyncio.Task[None] | None = None

    async def startup(self) -> None:
        self._task = asyncio.create_task(
            self._application(self._scope(), self._incoming.get, self._outgoing.put),
            name="managed-python-lifespan",
        )
        await self._incoming.put({"type": "lifespan.startup"})
        message = await self._outgoing.get()
        if message["type"] == "lifespan.startup.failed":
            raise RuntimeError(f"ASGI lifespan startup failed: {message.get('message', '')}")
        if message["type"] != "lifespan.startup.complete":
            raise RuntimeError(f"Unexpected ASGI lifespan startup message: {message['type']}")

    async def shutdown(self) -> None:
        if self._task is None:
            return
        await self._incoming.put({"type": "lifespan.shutdown"})
        message = await self._outgoing.get()
        if message["type"] == "lifespan.shutdown.failed":
            raise RuntimeError(f"ASGI lifespan shutdown failed: {message.get('message', '')}")
        if message["type"] != "lifespan.shutdown.complete":
            raise RuntimeError(f"Unexpected ASGI lifespan shutdown message: {message['type']}")
        await self._task
        self._task = None

    @staticmethod
    def _scope() -> dict[str, Any]:
        return {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": {},
        }


async def invoke_http(
    application: AsgiApplication,
    metadata: dict[str, Any],
    body: bytes,
) -> tuple[int, list[list[str]], bytes]:
    request_sent = False
    response_status: int | None = None
    response_headers: list[list[str]] = []
    response_body = bytearray()

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        nonlocal response_status, response_headers
        message_type = message["type"]
        if message_type == "http.response.start":
            if response_status is not None:
                raise RuntimeError("ASGI application sent response start more than once")
            response_status = int(message["status"])
            response_headers = [
                [name.decode("latin-1"), value.decode("latin-1")]
                for name, value in message.get("headers", [])
            ]
            return
        if message_type == "http.response.body":
            if response_status is None:
                raise RuntimeError("ASGI application sent response body before response start")
            response_body.extend(message.get("body", b""))
            return
        raise RuntimeError(f"Unsupported ASGI response message: {message_type}")

    raw_headers = metadata.get("headers", [])
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": metadata["method"],
        "scheme": "http",
        "path": metadata["path"],
        "raw_path": metadata["path"].encode("utf-8"),
        "query_string": metadata.get("queryString", "").encode("ascii"),
        "root_path": "",
        "headers": [
            (name.lower().encode("latin-1"), value.encode("latin-1"))
            for name, value in raw_headers
        ],
        "client": None,
        "server": None,
        "state": {},
    }
    await application(scope, receive, send)
    if response_status is None:
        raise RuntimeError("ASGI application completed without a response")
    return response_status, response_headers, bytes(response_body)
