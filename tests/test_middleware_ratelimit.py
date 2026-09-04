import json

import httpx
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from pgllens.middleware import InboundToolRateLimitMiddleware


def _app(per_minute, **kw):
    async def mcp(request):
        return JSONResponse({"ok": True})

    async def register(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[
        Route("/mcp", mcp, methods=["POST"]),
        Route("/oauth/register", register, methods=["POST"]),
    ])
    app.add_middleware(InboundToolRateLimitMiddleware, protected_path="/mcp",
                       per_minute=per_minute, trust_proxy_headers=False, **kw)
    return app


def _call():
    return {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query", "arguments": {"sql": "SELECT 1"}}}


async def _post(app, body):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post("/mcp", json=body)


async def test_calls_under_the_limit_pass():
    app = _app(per_minute=3)
    for _ in range(3):
        assert (await _post(app, _call())).status_code == 200


async def test_call_over_the_limit_is_rejected_with_429():
    app = _app(per_minute=2)
    for _ in range(2):
        await _post(app, _call())
    r = await _post(app, _call())
    assert r.status_code == 429
    assert "Retry-After" in r.headers


async def test_non_tool_calls_are_not_counted():
    # tools/list, initialize and notifications must never be throttled --
    # a client that cannot list tools looks like a broken server.
    app = _app(per_minute=1)
    listing = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    for _ in range(5):
        assert (await _post(app, listing)).status_code == 200


async def test_limit_of_zero_disables_the_middleware():
    app = _app(per_minute=0)
    for _ in range(50):
        assert (await _post(app, _call())).status_code == 200


async def test_malformed_body_is_passed_through_not_counted():
    # The middleware must never be the thing that rejects bad JSON -- that is
    # the MCP layer's job, and a parse failure here would mask the real error.
    transport = httpx.ASGITransport(app=_app(per_minute=1))
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/mcp", content=b"{not json")
    assert r.status_code == 200


async def test_body_is_still_readable_downstream():
    # The middleware buffers the request body to inspect it; if it fails to
    # replay the receive channel, every tool call downstream hangs or sees b"".
    seen = {}

    async def mcp(request):
        seen["body"] = await request.body()
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    app.add_middleware(InboundToolRateLimitMiddleware, protected_path="/mcp",
                       per_minute=10, trust_proxy_headers=False)
    await _post(app, _call())
    assert json.loads(seen["body"])["method"] == "tools/call"


async def test_unprotected_path_is_never_counted():
    # /oauth/register is unauthenticated; a tools/call-shaped body posted there
    # must not consume the /mcp tool-call budget, and must not be throttled at
    # all -- only protected_path is inspected.
    app = _app(per_minute=1)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        for _ in range(5):
            r = await c.post("/oauth/register", json=_call())
            assert r.status_code == 200
    # the /mcp budget is still untouched
    assert (await _post(app, _call())).status_code == 200


async def test_oversize_content_length_is_rejected_with_413_before_buffering():
    app = _app(per_minute=10, max_request_bytes=1000)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/mcp", content=b"x" * 2000,
                         headers={"content-length": "2000"})
    assert r.status_code == 413


async def test_oversize_body_without_content_length_is_rejected_with_413():
    # A client lying about (or omitting) Content-Length must still be caught by
    # the running-total guard while buffering, not just the cheap pre-check.
    app = _app(per_minute=10, max_request_bytes=1000)

    async def chunks():
        for _ in range(5):
            yield b"x" * 500

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/mcp", content=chunks())
    assert r.status_code == 413


async def _downstream_ok(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b""})


async def _raw_call(mw, *, client_id=None, peer="9.9.9.9", body=None):
    """Drive one request straight through the ASGI middleware with a hand-built
    scope -- lets tests set scope["pgllens.client_id"] directly (normally
    stamped by BearerAuthMiddleware) and control the peer address, neither of
    which httpx's ASGITransport exposes per-call."""
    body = body if body is not None else json.dumps(_call()).encode()
    scope = {"type": "http", "method": "POST", "path": "/mcp", "headers": [],
              "client": (peer, 1234)}
    if client_id is not None:
        scope["pgllens.client_id"] = client_id
    sent = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    await mw(scope, receive, send)
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


async def test_per_peer_ip_ceiling_limits_total_across_client_ids():
    # DCR is open (see oauth/store.py's ClientStore) -- without a peer ceiling,
    # one IP could register unboundedly many client_ids and multiply its
    # effective budget by peer_limit-less per-client keying alone.
    mw = InboundToolRateLimitMiddleware(_downstream_ok, protected_path="/mcp",
                                        per_minute=100, trust_proxy_headers=False,
                                        peer_limit=2)
    assert await _raw_call(mw, client_id="client-a") == 200
    assert await _raw_call(mw, client_id="client-b") == 200
    # each client_id has its own 100/min budget, but the shared peer is capped at 2
    assert await _raw_call(mw, client_id="client-c") == 429


async def test_stale_buckets_are_evicted_not_retained_forever():
    t = [0.0]
    mw = InboundToolRateLimitMiddleware(_downstream_ok, protected_path="/mcp",
                                        per_minute=1, trust_proxy_headers=False,
                                        now=lambda: t[0])
    for i in range(50):
        t[0] = i * 1000.0  # each call is its own client_id, far outside the 60s window
        assert await _raw_call(mw, client_id=f"client-{i}") == 200
    # a flood of one-off keys must not retain state forever -- only the most
    # recent call's bucket(s) should still be present (mirrors
    # tests/test_oauth_ratelimit.py::test_elapsed_lockout_entries_are_evicted_not_just_ignored).
    # Counters now live in the default store's single dict (limits.py), keyed
    # "calls:client:*"/"calls:peer:*" -- one surviving entry per namespace
    # (client_id churns every call, peer is constant), not the two separate
    # dicts this test pinned before the LimitStore seam existed.
    assert len(mw._store._buckets) == 2


async def test_window_rollover_resets_the_budget():
    t = [0.0]
    mw = InboundToolRateLimitMiddleware(_downstream_ok, protected_path="/mcp",
                                        per_minute=2, trust_proxy_headers=False,
                                        window_s=60.0, now=lambda: t[0])
    t[0] = 0.0
    assert await _raw_call(mw, client_id="same-client") == 200
    assert await _raw_call(mw, client_id="same-client") == 200
    t[0] = 30.0  # still inside the window -- budget exhausted
    assert await _raw_call(mw, client_id="same-client") == 429
    t[0] = 60.1  # window has rolled over -- budget resets
    assert await _raw_call(mw, client_id="same-client") == 200
    assert await _raw_call(mw, client_id="same-client") == 200
    t[0] = 60.2
    assert await _raw_call(mw, client_id="same-client") == 429
