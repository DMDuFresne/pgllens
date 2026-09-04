"""Inbound MCP tools/call rate limit (pure ASGI).

Security controls: path scoping, the Content-Length/running-total body-size guard, per-client
AND per-peer-IP ceilings, and eviction of stale buckets. Metrics/telemetry
hooks are still dropped -- pgllens's obs modules don't expose the same
counters (record_inbound_rate_limit_rejection / record_tool_invocation)
today. It lives here, not in oauth/, because it throttles tool *calls*, not
logins -- unrelated to oauth/ratelimit.py's RateLimiter, which throttles
OAuth login/register attempts.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from pgllens.caller import Caller, reset_caller, set_caller
from pgllens.limits import InMemoryLimitStore, LimitStore
from pgllens.oauth.clientip import client_ip
from pgllens.oauth.okta import SCOPE_ADMIN, SCOPE_READ
from pgllens.obs import metrics

logger = logging.getLogger("pgllens")

# Counters live behind the LimitStore seam (limits.py): InMemoryLimitStore by
# default, RedisLimitStore when REDIS_URL is set -- see limits.py for why.

# 1 MiB -- pgllens has no settings
# field for this yet (nothing has asked for one); add one if an operator
# ever needs a different ceiling.
_DEFAULT_MAX_REQUEST_BYTES: int = 1 * 1024 * 1024


def _tool_names(body: bytes) -> list[str]:
    """Extract the tool name of every ``tools/call`` in the (possibly batched)
    body. Returns one entry per tools/call request; a call whose name is
    missing/unparsable is reported as ``"unknown"``. Non-tools/call items are
    skipped. Never raises."""
    try:
        parsed = json.loads(body)
    except Exception:  # noqa: BLE001 -- malformed body must pass through uncounted
        return []
    items = parsed if isinstance(parsed, list) else [parsed]
    names: list[str] = []
    for it in items:
        if not isinstance(it, dict) or it.get("method") != "tools/call":
            continue
        params = it.get("params")
        name = params.get("name") if isinstance(params, dict) else None
        names.append(str(name) if isinstance(name, str) and name else "unknown")
    return names


class InboundToolRateLimitMiddleware:
    """Throttle ``tools/call`` invocations per client per minute.

    Only inspects/counts POSTs to ``protected_path`` -- every other path (and
    method) passes straight through uncounted, so an unauthenticated endpoint
    (e.g. ``/oauth/register``) can never consume the tool-call budget. Keyed
    on the authenticated client id (``scope["pgllens.client_id"]``, set by
    BearerAuthMiddleware) when present, else the peer address from
    ``oauth/clientip.py`` (honouring ``trust_proxy_headers``). A second,
    per-peer-IP ceiling is enforced alongside the per-client one: a single
    source IP can never exceed ``per_minute`` calls/window in total no matter
    how many client ids it registers (defeats DCR identity-rotation dilution
    -- pgllens's ``/oauth/register`` is open and successful registrations are
    deliberately not throttled). ``tools/list``, ``initialize``, ``ping`` and
    notifications always pass through uncounted.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        protected_path: str,
        per_minute: int,
        trust_proxy_headers: bool,
        peer_limit: int | None = None,
        max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES,
        window_s: float = 60.0,
        now: Callable[[], float] = time.monotonic,
        store: LimitStore | None = None,
    ) -> None:
        self.app = app
        self.protected_path = protected_path
        self.per_minute = per_minute
        self.trust_proxy_headers = trust_proxy_headers
        # Per-peer-IP ceiling defaults to the per-client limit -- see the
        # class docstring for why this must not be looser than per_minute.
        self.peer_limit = peer_limit if peer_limit is not None else per_minute
        self.max_request_bytes = max_request_bytes
        self.window_s = window_s
        self._now = now
        self._store = store if store is not None else InMemoryLimitStore(now=now)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            self.per_minute <= 0
            or scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != self.protected_path
        ):
            await self.app(scope, receive, send)
            return

        # Cheap rejection before buffering: a Content-Length that already
        # exceeds the ceiling never needs its body read at all.
        headers: dict[bytes, bytes] = dict(scope.get("headers", []))
        cl_raw = headers.get(b"content-length")
        if cl_raw is not None:
            try:
                content_length = int(cl_raw)
            except ValueError:
                content_length = 0
            if content_length > self.max_request_bytes:
                await _reject_413(send)
                return

        # Buffer body with a running-total guard, so a client lying about (or
        # omitting) Content-Length can't force an unbounded buffer either.
        body = b""
        more = True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)
            if len(body) > self.max_request_bytes:
                await _reject_413(send)
                return

        tool_names = _tool_names(body)
        if tool_names:
            peer = client_ip(Request(scope), self.trust_proxy_headers)
            client_id = str(scope.get("pgllens.client_id") or peer)
            for _ in tool_names:
                if not await self._allow(client_id, peer):
                    logger.warning(
                        "tool-call rate limit exceeded",
                        extra={"event": "rate_limit.rejected", "client_id": client_id},
                    )
                    metrics.record_limit_rejection("calls")
                    await self._reject(send)
                    return

        sent = False

        async def replay() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            # After the buffered body is delivered, delegate to the original
            # receive so real client events (notably http.disconnect) still
            # reach the app -- fabricating one here would abort a long-lived
            # streamable-HTTP response mid-stream.
            return await receive()

        await self.app(scope, replay, send)

    async def _allow(self, client_id: str, peer: str) -> bool:
        # The store returns the post-increment total, so both counters are
        # charged and then compared. A request rejected by the peer ceiling
        # therefore also consumed one unit of the client budget -- accepted:
        # both keys belong to the same rejected caller, and with a shared store
        # a read-then-write check-first would be a race across replicas.
        client_total = await self._store.incr(
            f"calls:client:{client_id}", 1, self.window_s
        )
        peer_total = await self._store.incr(f"calls:peer:{peer}", 1, self.window_s)
        if self.per_minute > 0 and client_total > self.per_minute:
            return False
        return not (self.peer_limit > 0 and peer_total > self.peer_limit)

    async def _reject(self, send: Send) -> None:
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32000,
                "message": (
                    "Rate limit exceeded: too many tool invocations in the window. "
                    "Slow down and retry shortly."
                ),
            },
        }).encode()
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [
                (b"content-type", b"application/json"),
                (b"retry-after", str(int(self.window_s)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})

async def _reject_413(send: Send) -> None:
    """Reject with 413 Payload Too Large -- no internal detail leaked.

    Shared by InboundToolRateLimitMiddleware and ScopeEnforcementMiddleware:
    an oversize body must be rejected the same way at both guards, not parsed
    truncated (which would silently degrade the scope gate to "no tool name
    found" -- see ScopeEnforcementMiddleware.__call__).
    """
    body = b"Payload Too Large"
    await send({
        "type": "http.response.start",
        "status": 413,
        "headers": [
            (b"content-type", b"text/plain"),
            (b"content-length", str(len(body)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body})


class BodySizeLimitMiddleware:
    """App-wide request-body ceiling (audit M2). Counts, never buffers.

    The /mcp middlewares above buffer because they parse tool names; this one
    only enforces a ceiling, so it wraps `receive` with a running total. On
    overrun it 413s (when the app has not started responding), then feeds the
    app `http.disconnect` so it unwinds without ever seeing the rest. That feed
    is real -- a real route reading the body (e.g. Starlette's `request.form()`)
    raises `ClientDisconnect` on it, not a clean return. Once the 413 is on the
    wire that's expected, not an error: the exception is swallowed so a
    security rejection doesn't get logged as an unhandled server error.
    Installed unconditionally: the cap must exist even when the rate limiter
    is disabled, and must cover /oauth/* form/json parsing.
    """

    def __init__(self, app: ASGIApp, *,
                 max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES) -> None:
        self.app = app
        self.max_request_bytes = max_request_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers: dict[bytes, bytes] = dict(scope.get("headers", []))
        cl_raw = headers.get(b"content-length")
        if cl_raw is not None:
            try:
                # Deliberately simple: redundant with InboundToolRateLimitMiddleware's
                # own Content-Length pre-check on /mcp. Cheap and harmless to run
                # twice, and this one is what covers every other path.
                if int(cl_raw) > self.max_request_bytes:
                    await _reject_413(send)
                    return
            except ValueError:
                pass  # unparseable header -- the running total below still guards

        total = 0
        tripped = False
        response_started = False

        async def guarded_receive() -> Message:
            nonlocal total, tripped
            if tripped:
                return {"type": "http.disconnect"}
            message = await receive()
            if message["type"] == "http.request":
                total += len(message.get("body", b""))
                if total > self.max_request_bytes:
                    tripped = True
                    if not response_started:
                        await _reject_413(send)
                    return {"type": "http.disconnect"}
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                if tripped:
                    return  # 413 already sent; drop the app's late response
                response_started = True
            elif tripped:
                return
            await send(message)

        try:
            await self.app(scope, guarded_receive, guarded_send)
        except Exception:
            # The 413 was already sent; the app choked on the http.disconnect
            # we fed it in response (e.g. Starlette's request.form() raises
            # ClientDisconnect). That unwind IS the design -- don't let it
            # surface as an unhandled-exception traceback for a rejection we
            # already handled correctly. If we never tripped, this is a real
            # app error and must propagate.
            if not tripped:
                raise


# The three tools that return SQL text other people wrote. Everything else is
# the client's own read-only view of the schema it was granted.
ADMIN_TOOLS: frozenset[str] = frozenset(
    {"get_active_sessions", "get_blocking", "get_query_store"}
)


class ScopeEnforcementMiddleware:
    """Map OAuth scopes onto tool groups (okta mode only).

    `pgllens.read` grants the query/discovery/diagnostic tools; `pgllens.admin`
    additionally grants ADMIN_TOOLS and implies read (a token holding only
    admin is not a client error worth a 403 on `query`). A `tools/call` for a
    tool the token has no scope for is 403, not 401, because the token IS
    valid, it is just not entitled.

    Deliberately simple: this buffers the request body a second time (the rate
    limiter already buffered it). Both are capped at 1 MiB, so the ceiling is one
    extra 1 MiB copy per request; merge the two into one body-reading middleware
    only if that ever shows up in a profile.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        protected_path: str,
        admin_tools: frozenset[str] = ADMIN_TOOLS,
        max_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES,
    ) -> None:
        self.app = app
        self.protected_path = protected_path
        self.admin_tools = admin_tools
        self.max_request_bytes = max_request_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != self.protected_path
        ):
            await self.app(scope, receive, send)
            return

        body = b""
        more = True
        while more:
            message = await receive()
            body += message.get("body", b"")
            more = message.get("more_body", False)
            if len(body) > self.max_request_bytes:
                # Mirror InboundToolRateLimitMiddleware's guard: reject outright
                # rather than parse a truncated buffer. Parsing a truncated body
                # can yield an empty/garbled tool list, which _tool_names()
                # reports as "unknown" -- not in ADMIN_TOOLS, so the scope check
                # below would silently pass an oversize request through with no
                # enforcement at all (see final-review.md Minor 1).
                await _reject_413(send)
                return

        granted = scope.get("pgllens.scopes") or frozenset()
        assert isinstance(granted, frozenset)
        for name in _tool_names(body):
            required = SCOPE_ADMIN if name in self.admin_tools else SCOPE_READ
            if required in granted:
                continue
            # admin implies read; read never implies admin.
            if required == SCOPE_READ and SCOPE_ADMIN in granted:
                continue
            logger.warning(
                "tool call rejected: missing scope",
                extra={"event": "scope.rejected", "tool": name, "required": required},
            )
            metrics.record_limit_rejection("scope")
            await self._reject_403(send, name, required)
            return

        sent = False

        async def replay() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)

    @staticmethod
    async def _reject_403(send: Send, tool: str, required: str) -> None:
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32001,
                "message": (
                    f"Tool {tool!r} requires the {required} scope, which this "
                    f"token does not carry."
                ),
            },
        }).encode()
        await send({
            "type": "http.response.start",
            "status": 403,
            "headers": [(b"content-type", b"application/json")],
        })
        await send({"type": "http.response.body", "body": body})


class CallerContextMiddleware:
    """Publish the authenticated identity into the contextvar for this task.

    Added AFTER the bearer middlewares in server.py so it runs after them at
    request time and sees the scope keys they stamp. In `none` mode there is no
    identity, and the caller is the peer address with client_id "anonymous".
    """

    def __init__(self, app: ASGIApp, *, trust_proxy_headers: bool) -> None:
        self.app = app
        self.trust_proxy_headers = trust_proxy_headers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        ip = client_ip(Request(scope), self.trust_proxy_headers)
        scopes = scope.get("pgllens.scopes") or frozenset()
        token = set_caller(Caller(
            client_id=str(scope.get("pgllens.client_id") or "anonymous"),
            sub=scope.get("pgllens.sub"),
            ip=ip,
            scopes=scopes if isinstance(scopes, frozenset) else frozenset(),
        ))
        try:
            await self.app(scope, receive, send)
        finally:
            # Restore whatever was set before this request -- a contextvar
            # normally scopes itself to the task automatically, but MCP's
            # streamable-HTTP transport can reuse a task across multiple
            # logical calls, so an explicit reset is what actually bounds
            # this identity to the request that set it.
            reset_caller(token)


class ConcurrencyLimitMiddleware:
    """Cap in-flight tool calls per client, so one client cannot occupy the
    whole psycopg pool.

    Uses `LimitStore.acquire`/`release` -- a real gauge, not the windowed
    `incr` counter (see limits.py's `LimitStore` docstring for why the two
    must not share a code path: `incr`'s window TTL is sized for a rate-limit
    window, not for however long a single query takes to run, and reusing it
    here let a slow query outlive its own gauge key).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        protected_path: str,
        max_concurrent: int,
        store: LimitStore,
        trust_proxy_headers: bool,
    ) -> None:
        self.app = app
        self.protected_path = protected_path
        self.max_concurrent = max_concurrent
        self.store = store
        self.trust_proxy_headers = trust_proxy_headers

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            self.max_concurrent <= 0
            or scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != self.protected_path
        ):
            await self.app(scope, receive, send)
            return

        peer = client_ip(Request(scope), self.trust_proxy_headers)
        # Same identity rule as the cost budget (tools/query.py): the
        # authenticated client id when present, else the peer IP -- never the
        # literal string "anonymous", which would merge every unauthenticated
        # caller's concurrency onto one shared slot.
        key = f"conc:{scope.get('pgllens.client_id') or peer}"
        granted = await self.store.acquire(key, self.max_concurrent)
        if not granted:
            await self.store.release(key)
            metrics.record_limit_rejection("concurrency")
            logger.warning(
                "concurrency cap exceeded",
                extra={"event": "rate_limit.concurrency", "client_id": key},
            )
            await self._reject_429(send)
            return
        try:
            await self.app(scope, receive, send)
        finally:
            await self.store.release(key)

    @staticmethod
    async def _reject_429(send: Send) -> None:
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32000,
                "message": (
                    "Concurrency limit exceeded: too many in-flight tool calls "
                    "for this client. Retry when your outstanding calls finish."
                ),
            },
        }).encode()
        await send({
            "type": "http.response.start",
            "status": 429,
            "headers": [(b"content-type", b"application/json"), (b"retry-after", b"1")],
        })
        await send({"type": "http.response.body", "body": body})
