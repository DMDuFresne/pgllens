"""BodySizeLimitMiddleware: app-wide body ceiling, no buffering."""

import httpx

from pgllens.middleware import BodySizeLimitMiddleware


def _echo_app():
    async def app(scope, receive, send):
        assert scope["type"] == "http"
        body = b""
        while True:
            msg = await receive()
            if msg["type"] == "http.disconnect":
                return  # client gone -- unwind without responding
            body += msg.get("body", b"")
            if not msg.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"%d" % len(body)})
    return app


def _client(max_bytes):
    mw = BodySizeLimitMiddleware(_echo_app(), max_request_bytes=max_bytes)
    transport = httpx.ASGITransport(app=mw)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_body_within_cap_passes_through():
    async with _client(max_bytes=100) as c:
        r = await c.post("/anything", content=b"x" * 100)
    assert r.status_code == 200
    assert r.text == "100"


async def test_oversize_body_is_rejected_413():
    async with _client(max_bytes=100) as c:
        r = await c.post("/oauth/token", content=b"x" * 101)
    assert r.status_code == 413


async def test_content_length_lie_is_caught_by_running_total():
    # httpx sets Content-Length honestly; hand-roll the ASGI messages instead.
    mw = BodySizeLimitMiddleware(_echo_app(), max_request_bytes=10)
    sent = []

    async def send(msg):
        sent.append(msg)

    chunks = [
        {"type": "http.request", "body": b"x" * 8, "more_body": True},
        {"type": "http.request", "body": b"x" * 8, "more_body": False},
    ]

    async def receive():
        return chunks.pop(0) if chunks else {"type": "http.disconnect"}

    scope = {"type": "http", "method": "POST", "path": "/mcp",
             "headers": [(b"content-length", b"5")]}  # lies
    await mw(scope, receive, send)
    assert sent[0]["status"] == 413


async def test_get_requests_pass_untouched():
    async with _client(max_bytes=1) as c:
        r = await c.get("/health-ish")
    assert r.status_code == 200
