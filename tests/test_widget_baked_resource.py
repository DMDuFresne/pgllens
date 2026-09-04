"""Widget result path: get_erd_widget's result carries a result-level
`_meta.ui.resourceUri` pointing at a per-call, data-baked HTML resource, readable
via the server's resource layer -- see erd.py's module docstring for why there is
no client-capability gate (stateless HTTP never has client capabilities).

The first test drives the real wire (initialize -> tools/call -> resources/read)
over `mcp.client._memory.InMemoryTransport`, per the task brief's "gold standard".
The rest exercise `erd.py`'s pieces directly (unit-level, no live DB, matching the
rest of this test file's siblings).
"""

from __future__ import annotations

from mcp.client._memory import InMemoryTransport
from mcp.client.session import ClientSession
from mcp.server.apps import APP_MIME_TYPE
from mcp.types import Implementation

from pgllens.config import Settings
from pgllens.database.pool import Db
from pgllens.server import create_mcp
from pgllens.tools import erd as mod
from tests.test_tools_erd import build, build_plain, make_intro


def _settings() -> Settings:
    return Settings(_env_file=None, database_url="postgresql://u:p@localhost:5432/flux",
                     exposed_schemas="public")


async def test_wire_widget_call_carries_a_baked_resource_uri_readable_over_the_wire():
    mod._baked_erd_resources.clear()
    settings = _settings()
    server = create_mcp(settings, Db(settings), intro=make_intro())

    # No Apps extension declared on purpose: production runs stateless HTTP, where
    # the SDK hands every request a connection with client_capabilities=None, so
    # the widget must ship structuredContent + resourceUri with NO negotiation.
    async with InMemoryTransport(server) as (read, write), ClientSession(
        read, write,
        client_info=Implementation(name="stateless-shaped-test", version="0"),
    ) as session:
        await session.initialize()
        result = await session.call_tool("get_erd_widget", {})

        assert result.structured_content is not None
        meta = result.meta or {}
        uri = meta["ui"]["resourceUri"]
        assert uri.startswith("pgllens://view/erd/")

        read_result = await session.read_resource(uri)
        content = read_result.contents[0]
        assert content.mime_type == APP_MIME_TYPE
        assert "authors" in content.text or "Customers" in content.text


async def test_wire_get_erd_has_no_widget_binding_and_get_erd_widget_does():
    """Bug 11 (owner retest, Claude Desktop): the host flagged format="text",
    format="mermaid", and even the format="png" rejection as widget renders,
    because it reads the STATIC per-tool _meta.ui.resourceUri that
    apps.tool(resource_uri=...) stamps on the tools/list entry -- present for
    every call regardless of format, and spec-required for the negotiated
    widget path, so it cannot be withheld from the widget tool. The fix is the
    split: get_erd is a plain @mcp.tool with no ui binding at all, and only
    get_erd_widget carries it. This test drives the real wire to pin both
    halves, plus the per-call guarantee that no get_erd result carries a
    result-level _meta or anything beyond the SDK's own `{"result": <str>}`
    wrapper (what EVERY `-> str` tool in this server returns -- it carries no
    ui binding, so it is not a widget signal).
    """
    settings = _settings()
    server = create_mcp(settings, Db(settings), intro=make_intro())

    async with InMemoryTransport(server) as (read, write), ClientSession(
        read, write,
        client_info=Implementation(name="wire-format-test", version="0"),
    ) as session:
        await session.initialize()

        tools = {t.name: t for t in (await session.list_tools()).tools}
        # The whole point of the split: no ui.resourceUri on get_erd's entry...
        assert "resourceUri" not in ((tools["get_erd"].meta or {}).get("ui") or {})
        # ...and the spec-required static binding still on the widget tool's.
        assert (tools["get_erd_widget"].meta or {})["ui"]["resourceUri"] == (
            "ui://pgllens/erd-widget")

        for arguments in (
            {"format": "text"},
            {"format": "mermaid"},
            {"format": "widget"},  # rejected, points at get_erd_widget
            {"format": "png"},  # rejected, error text only
        ):
            result = await session.call_tool("get_erd", arguments)
            assert result.meta is None, arguments
            assert set(result.structured_content or {}) <= {"result"}, arguments


async def test_baking_a_widget_result_registers_a_readable_resource():
    # Unit-level equivalent of the wire test above, without a live transport:
    # exercises _bake_erd_resource directly and reads it back through the
    # module's own baked-resource store (what the registered template reads from).
    mod._baked_erd_resources.clear()
    out = await build()()
    uri = out.meta["ui"]["resourceUri"]
    assert uri.startswith("pgllens://view/erd/")
    resource_id = uri.rsplit("/", 1)[-1]
    html = mod._baked_erd_resources[resource_id]
    assert "Customers" in html
    assert "erd-data" in html


async def test_rejected_widget_results_carry_no_structured_content_or_meta():
    out = await build()(depth=9)
    assert out.meta is None and out.structured_content is None


async def test_get_erd_results_are_plain_strings_with_nowhere_to_hide_meta():
    # The structural half of the split: get_erd returns str, so there is no
    # CallToolResult for structuredContent or _meta to ride on at all.
    get_erd = build_plain()
    for kwargs in ({}, {"format": "text"}, {"format": "widget"}, {"format": "png"}):
        assert isinstance(await get_erd(**kwargs), str), kwargs


async def test_baked_resources_are_capped_and_evict_oldest_first():
    mod._baked_erd_resources.clear()
    get_erd = build(make_intro())
    uris = []
    for _ in range(mod._ERD_BAKED_CAP + 1):
        out = await get_erd()
        uris.append(out.meta["ui"]["resourceUri"])

    assert len(mod._baked_erd_resources) == mod._ERD_BAKED_CAP
    first_id = uris[0].rsplit("/", 1)[-1]
    last_id = uris[-1].rsplit("/", 1)[-1]
    assert first_id not in mod._baked_erd_resources
    assert last_id in mod._baked_erd_resources
