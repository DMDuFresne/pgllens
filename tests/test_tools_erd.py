from unittest.mock import AsyncMock, MagicMock

from mcp.server.apps import Apps

from pgllens.database.introspect import Column, ForeignKey, Table, TableNotFoundError
from pgllens.erd.model import Erd
from pgllens.llens_style import lint
from pgllens.tools import erd as mod
from pgllens.tools._util import respond
from tests.conftest import make_registered

CUSTOMERS = Table("dbo", "Customers", "r", "People who buy things",
                   [Column("Id", "int", False, None, None, 1),
                    Column("Name", "text", False, None, None, 2)],
                   ["Id"], 50)
ORDERS = Table("dbo", "Orders", "r", None,
                [Column("Id", "int", False, None, None, 1),
                 Column("CustomerId", "int", False, None, None, 2),
                 Column("Amount (USD)", "decimal", True, None, None, 3)],
                ["Id"], 900)
ORDER_LINES = Table("dbo", "OrderLines", "r", None,
                     [Column("Id", "int", False, None, None, 1),
                      Column("OrderId", "int", False, None, None, 2)],
                     ["Id"], 4200)
LONELY = Table("dbo", "Lonely", "r", None, [Column("Id", "int", False, None, None, 1)], ["Id"], 1)
VORDERS = Table("rpt", "vOrders", "v", "Reporting view", [], [], 0)
WIDGET = Table("dbo", "Widget", "r", None,
               [Column("Id", "int", False, None, None, 1),
                Column("FactId", "int", False, None, None, 2)],
               ["Id"], 5)
FACT = Table("rpt", "Fact", "r", None, [Column("Id", "int", False, None, None, 1)], ["Id"], 10)

TABLES = [CUSTOMERS, ORDERS, ORDER_LINES, LONELY, VORDERS, WIDGET, FACT]

FKS = [
    ForeignKey("FK_Orders_Customers", "dbo", "Orders", ["CustomerId"], "dbo", "Customers", ["Id"]),
    ForeignKey("FK_Lines_Orders", "dbo", "OrderLines", ["OrderId"], "dbo", "Orders", ["Id"]),
    # cross-schema, isolated from the Orders/Customers/OrderLines cluster above
    # so tests asserting exact neighbour sets for those stay unaffected.
    ForeignKey("FK_Widget_Fact", "dbo", "Widget", ["FactId"], "rpt", "Fact", ["Id"]),
]


def make_intro():
    """A fake matching pgllens.database.introspect.Introspector's real surface
    (tables()/foreign_keys()/table()) -- erd.py's _IntroAdapter is what reshapes
    this into the QueryResult-based interface erd.model.build_erd expects."""
    intro = MagicMock()
    intro.tables = AsyncMock(return_value=TABLES)
    intro.foreign_keys = AsyncMock(return_value=FKS)

    async def table(name, schema=None):
        needle = name.lower()
        for t in TABLES:
            if t.name.lower() == needle and (schema is None or t.schema.lower() == schema.lower()):
                return t
        raise TableNotFoundError(f"Table {name!r} not found.")

    intro.table = AsyncMock(side_effect=table)
    return intro


def build(intro=None):
    """register_apps() binds get_erd_widget on a real Apps() instance (apps.tool()
    returns the undecorated function itself, same as FakeMCP's tool() elsewhere),
    so the test harness pulls it back out via apps.tools() rather than a fake
    registry."""
    apps = Apps()
    db, settings = MagicMock(), MagicMock()
    mod.register_apps(apps, db, settings, intro or make_intro())
    return apps.tools()[0].fn


def build_plain(intro=None):
    """The plain-text half of the split: get_erd, registered through the uniform
    register(mcp, db, settings, intro, caps) pass like every other tool module.
    Returns a str, never a CallToolResult."""
    mcp, _db, _intro = make_registered(mod, intro or make_intro())
    return mcp.tools["get_erd"]


def _text(result) -> str:
    return result.content[0].text


# --- get_erd_widget (the MCP Apps half) --------------------------------------

async def test_widget_always_emits_structured_content_and_a_resource_uri():
    # No request context at all -- the same signal production gives every call:
    # stateless HTTP builds each connection with client_capabilities=None, so
    # any negotiation gate here would degrade the widget to text on a real
    # deployment (which is exactly what happened; see erd.py's module docstring).
    out = await build()()
    assert out.structured_content["nodes"] and out.structured_content["edges"]
    assert {n["table"] for n in out.structured_content["nodes"]} >= {"Customers", "Orders"}
    assert out.meta["ui"]["resourceUri"].startswith("pgllens://view/erd/")
    assert "tables · " in _text(out) and "relationships" in _text(out)


async def test_widget_structured_content_carries_the_server_computed_mermaid():
    """Regression: the widget's "Copy Mermaid" button preferred DATA.mermaid but
    that key was never populated, so every real call fell to a JS reimplementation
    that quoted attribute names. structuredContent must carry the same text
    to_mermaid() produces -- one generator, not two -- with valid, unquoted
    `type name PK` attribute syntax."""
    from pgllens.erd.model import to_mermaid

    out = await build()(tables=["Orders"])
    mermaid = out.structured_content["mermaid"]
    assert isinstance(mermaid, str) and mermaid
    assert '"Id"' not in mermaid and '"CustomerId"' not in mermaid
    assert "int Id PK" in mermaid

    erd = await mod.build_erd(mod._IntroAdapter(make_intro()), None, tables=["Orders"])
    assert mermaid == to_mermaid(erd)


async def test_get_erd_widget_is_bound_to_the_erd_view_resource():
    apps = Apps()
    db, settings = MagicMock(), MagicMock()
    mod.register_apps(apps, db, settings, make_intro())
    binding = apps.tools()[0]
    assert binding.fn.__name__ == "get_erd_widget"
    assert binding.meta["ui"]["resourceUri"] == "ui://pgllens/erd-widget"
    resource = apps.resources()[0].resource
    assert resource.uri == "ui://pgllens/erd-widget"
    assert resource.mime_type == "text/html;profile=mcp-app"


async def test_widget_result_carries_correctly_keyed_structured_content():
    # Regression for the structured_content= (snake_case, silently swallowed
    # by pydantic's extra="allow") vs structuredContent (real field) typo:
    # assert the data survives to the wire shape a host actually reads.
    out = await build()()
    wire = out.model_dump(mode="json", by_alias=True)
    assert wire["structuredContent"] is not None
    assert wire["structuredContent"]["nodes"] and wire["structuredContent"]["edges"]
    assert out.structured_content is not None


async def test_widget_caption_does_not_assert_the_host_rendered_it():
    out = await build()()
    text = _text(out)
    assert "Interactive ERD diagram attached" not in text
    assert "Interactive diagram sent as structured content" in text
    assert 'format="mermaid"' in text and 'format="text"' in text


def test_widget_takes_no_format_argument():
    # The whole point of the split: format= lives on get_erd only, so the widget
    # tool's static _meta.ui.resourceUri can never ride a plain-text answer.
    import inspect
    params = inspect.signature(build()).parameters
    assert "format" not in params
    assert "depth" in params


async def test_widget_max_nodes_and_depth_out_of_range_are_rejected():
    for out in (await build()(max_nodes=0), await build()(max_nodes=9999),
                await build()(depth=0), await build()(depth=4)):
        text = _text(out)
        assert text.startswith("## pgllens · get_erd_widget · error")
        assert "- code: `ARG_OUT_OF_RANGE`" in text
        assert out.structured_content is None and out.meta is None


# --- get_erd (the plain-text half) -------------------------------------------

async def test_get_erd_returns_a_plain_string_mermaid_diagram_by_default():
    out = await build_plain()()
    assert isinstance(out, str)
    assert "```mermaid" in out and "erDiagram" in out


async def test_get_erd_text_format_lists_relationships_without_a_diagram():
    out = await build_plain()(format="text")
    assert "```mermaid" not in out
    assert "Orders" in out and "### relationships" in out


async def test_get_erd_rejects_widget_format_and_names_the_widget_tool():
    out = await build_plain()(format="widget")
    assert out.startswith("## pgllens · get_erd · error")
    assert "- code: `FORMAT_UNKNOWN`" in out
    assert "get_erd_widget" in out


async def test_get_erd_invalid_format_lists_only_mermaid_and_text():
    out = await build_plain()(format="nonsense")
    assert out.startswith("## pgllens · get_erd · error")
    assert "- code: `FORMAT_UNKNOWN`" in out
    assert "mermaid" in out and "text" in out
    assert "widget" not in out


async def test_get_erd_text_format_with_include_columns_lists_column_names_and_types():
    assert "`CustomerId` fk" in await build_plain()(format="text", include_columns=True)


async def test_get_erd_text_format_without_include_columns_omits_column_detail():
    assert "`CustomerId`" not in await build_plain()(format="text", include_columns=False)


async def test_get_erd_max_nodes_out_of_range_is_rejected():
    assert "- code: `ARG_OUT_OF_RANGE`" in await build_plain()(max_nodes=0)
    assert "- code: `ARG_OUT_OF_RANGE`" in await build_plain()(max_nodes=9999)


async def test_get_erd_depth_out_of_range_is_rejected():
    assert "- code: `ARG_OUT_OF_RANGE`" in await build_plain()(depth=0)
    assert "- code: `ARG_OUT_OF_RANGE`" in await build_plain()(depth=4)


async def test_get_erd_depth_threads_through_to_neighbour_expansion():
    # OrderLines -> Orders -> Customers: depth=1 stops at Orders, depth=2 reaches
    # Customers, and neither pulls in the unrelated Widget/Fact cluster.
    one = await build_plain()(tables=["OrderLines"], format="text", depth=1)
    two = await build_plain()(tables=["OrderLines"], format="text", depth=2)
    assert "Orders" in one and "Customers" not in one
    assert "Customers" in two
    assert "Widget" not in two and "Fact" not in two


async def test_get_erd_tables_accepts_a_comma_separated_string():
    out = await build_plain()(tables="Orders, Customers", format="text")
    assert "Orders" in out and "Customers" in out


async def test_get_erd_unknown_table_name_is_surfaced_as_a_warning():
    # audit #10: an unknown name in `tables` used to be silently dropped.
    out = await build_plain()(tables=["Orders", "Nonexistent"], format="text")
    assert "ignored unknown table: Nonexistent" in out
    assert "Orders" in out


async def test_get_erd_truncation_note_is_surfaced_to_the_model():
    out = (await build_plain()(max_nodes=2, format="text")).lower()
    assert "truncat" in out or "narrow" in out


async def test_get_erd_schema_filter_excludes_views_but_keeps_tables():
    # audit #12: a view has no FKs and must never appear in the ERD, filtered
    # or not -- Fact (a real rpt table) still shows under the schema filter.
    out = await build_plain()(schema="rpt", format="text")
    assert "vOrders" not in out
    assert "Fact" in out
    assert "Orders" not in out.replace("Widget", "")  # Widget is the one dbo leak-in (below)


async def test_get_erd_schema_filter_keeps_cross_schema_fk_target_as_external_stub():
    # audit #9: schema="dbo" would naively drop rpt.Fact, the target of
    # dbo.Widget's FK -- the edge (and a stub node for Fact) must survive,
    # tagged with the same marker idiom get_erd already uses for "related".
    out = await build_plain()(schema="dbo", format="text")
    assert "Widget" in out
    assert "Fact" in out and "| external |" in out


def test_widget_resource_is_registered_as_an_mcp_app():
    from pgllens.widgets.render import load_widget_html
    html = load_widget_html()
    assert "erd-data" in html and "GENERATED FILE" in html[:400]


def test_render_erd_view_bakes_data_into_the_block():
    from pgllens.widgets.render import render_erd_view
    html = render_erd_view({"database": "Sales", "nodes": [], "edges": [],
                            "truncated": False})
    assert '"database": "Sales"' in html or '"database":"Sales"' in html
    # and the baked document is still self-contained
    assert "fetch(" not in html


def test_render_erd_view_escapes_a_script_close_in_data():
    from pgllens.widgets.render import render_erd_view
    evil = {"database": "</script><script>alert(1)</script>", "nodes": [],
            "edges": [], "truncated": False}
    html = render_erd_view(evil)
    assert "<script>alert(1)</script>" not in html


async def test_wire_get_erd_has_no_widget_binding_and_get_erd_widget_does():
    """The host reads the STATIC per-tool _meta.ui.resourceUri that
    apps.tool(resource_uri=...) stamps on the tools/list entry. get_erd is a
    plain @mcp.tool with no ui binding; only get_erd_widget carries it. Drives
    the real wire (initialize -> tools/list -> tools/call) to pin both halves,
    plus that no result of either tool carries a result-level _meta."""
    from mcp.client._memory import InMemoryTransport
    from mcp.client.session import ClientSession
    from mcp.types import Implementation

    from pgllens.config import Settings
    from pgllens.database.pool import Db
    from pgllens.server import create_mcp

    settings = Settings(_env_file=None, database_url="postgresql://u:p@localhost:5432/flux",
                        exposed_schemas="public")
    server = create_mcp(settings, Db(settings), intro=make_intro())

    async with InMemoryTransport(server) as (read, write), ClientSession(
        read, write, client_info=Implementation(name="wire-format-test", version="0"),
    ) as session:
        await session.initialize()
        tools = {t.name: t for t in (await session.list_tools()).tools}
        assert "resourceUri" not in ((tools["get_erd"].meta or {}).get("ui") or {})
        assert (tools["get_erd_widget"].meta or {})["ui"]["resourceUri"] == "ui://pgllens/erd-widget"

        widget = await session.call_tool("get_erd_widget", {})
        assert widget.structured_content is not None
        assert widget.meta["ui"]["resourceUri"].startswith("pgllens://view/erd/")

        for arguments in ({"format": "text"}, {"format": "mermaid"},
                          {"format": "widget"}, {"format": "png"}):
            result = await session.call_tool("get_erd", arguments)
            assert result.meta is None, arguments
            assert set(result.structured_content or {}) <= {"result"}, arguments


# --- _one_sentence / erd_response caveat sanitizing --------------------------

def test_one_sentence_collapses_internal_sentence_breaks():
    assert mod._one_sentence("Columns omitted. Narrow with schema.") == "Columns omitted; Narrow with schema."
    assert mod._one_sentence("Already one.") == "Already one."
    assert mod._one_sentence("  no period  ") == "no period."


def test_erd_response_sanitizes_a_truncated_note_into_one_sentence():
    # A real erd.note (build_erd's own truncation message) already contains its
    # own ". " sentence break -- erd_response must run it through _one_sentence
    # before wrapping it in a Caveat, or the Caveat validator raises.
    erd = Erd(database=None, nodes=[], edges=[], truncated=True,
             note="Showing 60 of 200 tables. Raise max_nodes.")
    out = respond(mod.erd_response("get_erd", erd, None, None, mermaid=False, widget=False))
    assert lint(out) == []
    assert "; Raise" in out
