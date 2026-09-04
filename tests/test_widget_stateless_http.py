"""get_erd_widget over the production transport: stateless streamable HTTP.

This is the transport a client-capability gate would break on -- in
stateless mode the SDK hands every request `client_capabilities=None`, so a
client-capability gate can never pass. The in-memory wire tests are stateful
(initialize -> tools/call) and never caught it; this one POSTs a tools/call
with NO prior initialize, exactly as a stateless host does.
"""

from __future__ import annotations

import json

import httpx

from pgllens.config import Settings
from pgllens.database.pool import Db
from pgllens.server import create_mcp
from tests.test_tools_erd import make_intro


def _settings() -> Settings:
    return Settings(_env_file=None, database_url="postgresql://u:p@localhost:5432/flux",
                    exposed_schemas="public")


def _result(r: httpx.Response) -> dict:
    body = r.text
    if r.headers["content-type"].startswith("text/event-stream"):
        body = next(line[len("data:"):] for line in body.splitlines() if line.startswith("data:"))
    return json.loads(body)["result"]


async def test_stateless_http_widget_call_carries_structured_content_and_resource_uri():
    settings = _settings()
    # Mirrors build_app(): create_mcp + streamable_http_app(stateless_http=True). build_app
    # constructs its own Introspector, so it is bypassed here to inject the mocked intro.
    app = create_mcp(settings, Db(settings), intro=make_intro()).streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                          base_url="http://localhost:3000") as c,
    ):
        r = await c.post(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream",
                     "Content-Type": "application/json"},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                  "params": {"name": "get_erd_widget", "arguments": {}}},
        )
    assert r.status_code == 200, r.text
    result = _result(r)
    assert result.get("isError") is not True, result
    assert result["structuredContent"]
    assert result["_meta"]["ui"]["resourceUri"].startswith("pgllens://view/erd/")


async def test_stateless_http_resources_list_exposes_the_widget_view():
    settings = _settings()
    app = create_mcp(settings, Db(settings), intro=make_intro()).streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                          base_url="http://localhost:3000") as c,
    ):
        headers = {"Accept": "application/json, text/event-stream",
                   "Content-Type": "application/json"}
        listed = await c.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "resources/list", "params": {}})
        read = await c.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 2, "method": "resources/read",
            "params": {"uri": "ui://pgllens/erd-widget"}})
    assert listed.status_code == 200, listed.text
    resources = {r["uri"]: r for r in _result(listed)["resources"]}
    assert resources["ui://pgllens/erd-widget"]["mimeType"] == "text/html;profile=mcp-app"
    assert read.status_code == 200, read.text
    content = _result(read)["contents"][0]
    assert content["mimeType"] == "text/html;profile=mcp-app"
    assert "erd-data" in content["text"]
