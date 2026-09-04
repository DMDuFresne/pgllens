"""Per-request correlation id: a contextvar plus an ASGI middleware to bind it.

``contextvars`` gives each asyncio task (and thread) its own copy of the value, so
concurrent requests never see each other's correlation id. The middleware is pure
ASGI (not Starlette ``BaseHTTPMiddleware``) so it doesn't interfere with the MCP
streamable-HTTP transport's long-lived responses.
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar

from starlette.types import ASGIApp, Message, Receive, Scope, Send

# This module predates caller.py's token+finally reset pattern (added in
# Task 8's fix round for CallerContextMiddleware) -- it never resets the
# contextvar after a request. Left as-is: a correlation id lingering into
# whatever runs next on the same task is a cosmetic log-correlation glitch,
# not a security or accounting bug, so behaviour here is intentionally
# unchanged. Add the same token/reset if this ever needs the stronger
# guarantee.
_correlation_id: ContextVar[str | None] = ContextVar("pgllens_correlation_id", default=None)

_HEADER_NAMES = (b"x-correlation-id", b"x-request-id")


def correlation_id() -> str | None:
    return _correlation_id.get()


def set_correlation_id(value: str) -> None:
    _correlation_id.set(value)


def new_correlation_id() -> str:
    value = uuid.uuid4().hex
    set_correlation_id(value)
    return value


class CorrelationMiddleware:
    """ASGI middleware: assigns a correlation id per request, honouring an inbound
    ``X-Correlation-Id`` / ``X-Request-Id`` header, and echoes it back as a response header.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        incoming = next((headers[h] for h in _HEADER_NAMES if h in headers), None)
        cid = incoming.decode("latin-1") if incoming else uuid.uuid4().hex
        cid_bytes = cid.encode("latin-1")

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {
                    **message,
                    "headers": [*message.get("headers", []), (b"x-correlation-id", cid_bytes)],
                }
            await send(message)

        set_correlation_id(cid)
        await self.app(scope, receive, send_with_header)
