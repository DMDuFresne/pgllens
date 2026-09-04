"""Wire-protocol verification: `build_app()`'s real ASGI app, driven over
`httpx.ASGITransport` through a real `initialize` -> `tools/list` ->
`tools/call` sequence -- no mocked transport, no subprocess. Every other test
in this repo either calls tool coroutines directly (bypassing MCP entirely)
or hits `/mcp` with a single canned request; this file is the one place that
plays a real client's full sequence against the real app, then asserts the
OAuth discovery documents are served when OAuth is enabled and 404 otherwise.

Skips cleanly (via `tests/integration/conftest.py`'s `dsn` fixture) when no
real, reachable PostgreSQL is configured.

MCPServer's DNS-rebinding protection (see `mcp.server.transport_security`)
only allows `Host` headers matching `127.0.0.1:*`/`localhost:*`/`[::1]:*` --
the base_url below must carry an explicit port for that wildcard match to hit.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from pgllens.config import Settings
from pgllens.server import build_app

pytestmark = pytest.mark.integration

BASE_URL = "http://127.0.0.1:3000"
_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def _parse(response: httpx.Response) -> dict:
    """`/mcp` replies as an SSE stream (one `data:` line) even for a single
    JSON-RPC response -- stateless_http's transport still frames it that way."""
    for line in response.text.splitlines():
        if line.startswith("data:"):
            return json.loads(line[len("data:"):].strip())
    return json.loads(response.text)


async def _rpc(client: httpx.AsyncClient, method: str, params: dict | None = None,
                id_: int | None = 1) -> dict:
    body: dict[str, object] = {"jsonrpc": "2.0", "method": method}
    if id_ is not None:
        body["id"] = id_
    if params is not None:
        body["params"] = params
    r = await client.post("/mcp", json=body, headers=_HEADERS)
    r.raise_for_status()
    return _parse(r) if id_ is not None else {}


async def _initialized_client(client: httpx.AsyncClient) -> None:
    await _rpc(client, "initialize", {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "pgllens-integration-test", "version": "0"},
    })
    await _rpc(client, "notifications/initialized", id_=None)


@asynccontextmanager
async def _wire(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """Lifespan + client in one block per test. Not a yielding fixture:
    pytest-asyncio tears async fixtures down in a different task than setup,
    which trips anyio's cancel-scope check inside the MCP lifespan."""
    app = build_app(settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=BASE_URL) as c,
    ):
        yield c


async def test_initialize_handshake_reports_server_identity(settings: Settings):
    async with _wire(settings) as wire_client:
        result = (await _rpc(wire_client, "initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pgllens-integration-test", "version": "0"},
        }))["result"]
    assert result["serverInfo"]["name"] == "pgllens"
    assert result["protocolVersion"]  # non-empty: a version was actually negotiated


async def test_tools_list_discovers_all_tools_with_readonly_annotations(settings: Settings):
    async with _wire(settings) as wire_client:
        await _initialized_client(wire_client)
        tools = (await _rpc(wire_client, "tools/list", id_=2))["result"]["tools"]

    # 30 tools via register_all + get_erd_widget, bound through the Apps
    # extension (see tools/erd.py) rather than the uniform register_all pass.
    assert len(tools) == 31, f"registry drifted: found {sorted(t['name'] for t in tools)}"
    for tool in tools:
        assert tool.get("description", "").strip(), f"{tool['name']}: empty description"
        assert tool.get("inputSchema"), f"{tool['name']}: empty inputSchema"
        assert tool.get("annotations", {}).get("readOnlyHint") is True, (
            f"{tool['name']}: readOnlyHint is not true"
        )


async def test_tool_call_succeeds_against_the_live_database(settings: Settings):
    async with _wire(settings) as wire_client:
        await _initialized_client(wire_client)
        result = (await _rpc(wire_client, "tools/call", {
            "name": "list_tables", "arguments": {},
        }, id_=2))["result"]
    assert result.get("isError") is not True
    text = result["content"][0]["text"]
    assert isinstance(text, str) and text.strip()


async def test_oauth_discovery_documents_404_when_oauth_is_disabled(settings: Settings):
    # default: mcp_auth_mode="none", oauth=False
    async with _wire(settings) as c:
        pr = await c.get("/.well-known/oauth-protected-resource")
        as_ = await c.get("/.well-known/oauth-authorization-server")
    assert pr.status_code == 404
    assert as_.status_code == 404


async def test_oauth_discovery_documents_served_when_oauth_is_enabled(settings: Settings):
    # Rebuilt via the constructor (not model_copy(update=...), which bypasses
    # validation) so mcp_auth_password is coerced to SecretStr like real config.
    oauth_settings = Settings(
        _env_file=None, database_url=settings.database_url,
        exposed_schemas=settings.exposed_schemas,
        mcp_auth_mode="password", mcp_auth_password="letmein",
    )
    async with _wire(oauth_settings) as c:
        pr = await c.get("/.well-known/oauth-protected-resource")
        as_ = await c.get("/.well-known/oauth-authorization-server")
    assert pr.status_code == 200
    assert as_.status_code == 200
    body = as_.json()
    assert body["authorization_endpoint"].endswith("/oauth/authorize")
    assert body["token_endpoint"].endswith("/oauth/token")
