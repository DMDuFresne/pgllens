"""Entity-relationship diagrams: `get_erd` (mermaid/text) and `get_erd_widget`.

TWO tools, deliberately. `@apps.tool(resource_uri=...)` stamps a static,
per-TOOL `_meta.ui.resourceUri` on the tool's tools/list entry -- the MCP Apps
spec's required negotiation binding, present for every call regardless of
arguments. A host reads that binding as "this tool renders a widget", so it
cannot live on a tool that also serves plain mermaid/text (Claude.ai flagged
every format="text"/"mermaid" call as a widget render; see docs/tools.md's ERD
section). The fix is the split:

* `get_erd` -- a plain `@mcp.tool` registered through the uniform
  `register()`/`_MODULES` pass, returning `str` via the shared `tool_errors`
  decorator. No Apps binding, no ui resource, no result-level `_meta`. Its structured
  output is only the SDK's `{"result": "<markdown>"}` wrapper, the same one
  every `-> str` tool in this server returns; it carries no ui binding and is
  not a widget signal.
* `get_erd_widget` -- the MCP Apps path, registered on the SDK's `Apps`
  extension (`mcp.server.apps`, SEP-1865:
  https://modelcontextprotocol.io/specification/draft/extensions/apps) via
  `register_apps()`. Only this tool carries the resourceUri binding; it still
  goes through `tool_errors`, which wraps its errors in a `CallToolResult`
  because the function is annotated to return one.

Read-only: `_IntroAdapter` reads only from Introspector.tables()/foreign_keys()/
table() -- the same cached, safety-checked introspection every other discovery
tool uses -- and reshapes it into the QueryResult-based interface erd.model
.build_erd (ported verbatim from MSSQL in Task 1) expects. Both tools always
return a markdown summary (table/edge counts, the truncation note, the Mermaid
text for format="mermaid") so a host that can't render widgets still gets full
value -- neither returns an empty body on the assumption the widget will show.

`get_erd_widget` is annotated `-> CallToolResult`: the SDK treats that as "no
advertised outputSchema, pass results through"
(mcp.server.mcpserver.utilities.func_metadata). Every successful call attaches
structuredContent AND a result-level `_meta.ui.resourceUri` pointing at a
per-call, data-baked HTML resource (see `_ERD_BAKED_CAP`), exactly the
reference MCP Apps widget shape that claude.ai renders. There is
deliberately NO `client_supports_apps(ctx)` gate: production serves streamable
HTTP with `stateless_http=True`, and in that mode the SDK builds every
request's connection with `client_capabilities=None`
(streamable_http_manager.py, `Connection.from_envelope(pv, None, None)`), so
the gate could never pass on a real deployment -- the widget degraded to text
on every call while the in-memory (stateful) wire tests kept passing. A host
that doesn't render MCP Apps still gets the markdown summary in `content`.
"""

from __future__ import annotations

import json
import logging
import secrets
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from mcp.server.apps import Apps
from mcp.server.mcpserver.exceptions import ResourceNotFoundError
from mcp.types import CallToolResult, TextContent

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.database.introspect import TableDetail, TableNotFoundError
from pgllens.erd.model import build_erd, to_dict, to_erd_out, to_mermaid
from pgllens.llens_style import (
    Block,
    Call,
    Caveat,
    Code,
    ErrorCode,
    Response,
    Section,
    Table,
    hint_for,
    nof,
)
from pgllens.llens_style import estimate as fmt_estimate
from pgllens.tools._util import SERVER, ToolError, check_range, respond, tool_errors
from pgllens.widgets.render import load_widget_html, render_erd_view

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

logger = logging.getLogger("pgllens")

# Hosts cache tool->resource bindings by URI, so this URI belongs to get_erd_widget alone; the
# plain get_erd tool has no ui binding.
_ERD_VIEW_URI = "ui://pgllens/erd-widget"

# Deliberately simple: a flat byte budget rather than a per-field estimate. If a
# 400KB with-columns payload proves too tight/loose for real schemas, tune this
# constant; the fallback (drop columns, note it) is the actual guard.
_MAX_PAYLOAD_BYTES = 400_000

# Pre-spec/legacy host path: the host resources/read's the URI straight from the
# tool result's _meta.ui.resourceUri (result-level _meta.ui.resourceUri -> a
# per-call, data-baked, self-contained HTML resource).
_ERD_BAKED_URI_TEMPLATE = "pgllens://view/erd/{resource_id}"
_ERD_BAKED_CAP = 128
# Deliberately simple: plain dict used as an insertion-ordered FIFO, not a real
# LRU. The cap is PROCESS-WIDE, shared across every concurrently connected
# client/session, not per caller. Baked HTML is small (tens of KB), so 128 of
# them is trivial memory and makes "some other client's 128 widget calls evict
# mine before my host fetches it" practically unreachable for any sane
# deployment. Upgrade to per-session keying (or a TTL) if a deployment ever runs
# >128 in-flight widget calls across ALL connected hosts at once.
_baked_erd_resources: OrderedDict[str, str] = OrderedDict()


def _bake_erd_resource(data: dict[str, Any]) -> str:
    """Render `data` into a self-contained HTML doc, store it under a fresh
    id (evicting the oldest entry once _ERD_BAKED_CAP is exceeded), and
    return its pgllens:// URI."""
    resource_id = secrets.token_urlsafe(9)
    _baked_erd_resources[resource_id] = render_erd_view(data)
    while len(_baked_erd_resources) > _ERD_BAKED_CAP:
        _baked_erd_resources.popitem(last=False)
    return _ERD_BAKED_URI_TEMPLATE.format(resource_id=resource_id)


def register_erd_resource_template(mcp: MCPServer) -> None:
    """Register the pgllens://view/erd/{resource_id} template resource.

    A plain resource template on `mcp` itself, so it is called from
    server.py's create_mcp() AFTER `MCPServer(...)` exists -- unlike the
    Apps-bound static `ui://pgllens/erd-widget` view, which is consumed inside
    `MCPServer.__init__`.
    """

    @mcp.resource(
        _ERD_BAKED_URI_TEMPLATE,
        name="erd-view-baked",
        mime_type="text/html;profile=mcp-app",
        description=(
            "Per-call, data-baked ERD diagram: get_erd_widget's result points here "
            "via _meta.ui.resourceUri (legacy/pre-spec host path). The static MCP "
            "Apps view is ui://pgllens/erd-widget."
        ),
    )
    def erd_view_baked(resource_id: str) -> str:
        html = _baked_erd_resources.get(resource_id)
        if html is None:
            raise ResourceNotFoundError(f"Unknown or evicted ERD resource: {resource_id!r}")
        logger.info("erd baked resource read: %s", resource_id)
        return html

_COLUMN_FIELDS = ["column", "type", "max_length", "precision", "scale",
                   "is_nullable", "is_identity", "is_computed", "default", "description"]
_FK_FIELDS = ["fk", "schema", "table", "column", "ref_schema", "ref_table", "ref_column", "not_null"]


class _IntroAdapter:
    """Adapts pgllens.database.introspect.Introspector (tables()/foreign_keys(),
    both loaded in one cached round trip) to the QueryResult-shaped
    list_tables/relationships/describe_table interface erd.model.build_erd
    expects -- that interface was ported verbatim from MSSQL's multi-database
    Introspector in Task 1 and is left unchanged; this adapter is the only new
    code, so build_erd's graph/truncation/mermaid logic is exercised unmodified.
    The `database` parameter build_erd passes through is always ignored: PgLLens
    is a single-database lens (DATABASE_URL); scoping is by schema (EXPOSED_SCHEMAS).
    """

    def __init__(self, intro: Introspector) -> None:
        self._intro = intro

    async def list_tables(self, database: str | None = None) -> QueryResult:
        tables = await self._intro.tables()
        rows: list[tuple[object, ...]] = [
            (t.schema, t.name, "VIEW" if t.kind in ("v", "m") else "USER_TABLE",
             str(t.row_estimate), t.comment)
            for t in tables
        ]
        return QueryResult(["schema", "object", "type", "rows", "description"], rows, False)

    async def relationships(self, database: str | None = None) -> QueryResult:
        return QueryResult(_FK_FIELDS, _fk_rows(await self._intro.foreign_keys()), False)

    async def describe_table(self, database: str | None, schema: str, table: str) -> TableDetail:
        try:
            t = await self._intro.table(table, schema)
        except TableNotFoundError:
            return {
                "columns": QueryResult(_COLUMN_FIELDS, [], False),
                "primary_key": QueryResult(["column"], [], False),
                "foreign_keys": QueryResult(_FK_FIELDS, [], False),
                "description": None,
            }
        column_rows: list[tuple[object, ...]] = [
            (c.name, c.data_type, 0, 0, 0, c.nullable, False, False, c.default, c.comment)
            for c in t.columns
        ]
        fks = await self._intro.foreign_keys()
        return {
            "columns": QueryResult(_COLUMN_FIELDS, column_rows, False),
            "primary_key": QueryResult(["column"], [(c,) for c in t.primary_key], False),
            "foreign_keys": QueryResult(_FK_FIELDS, _fk_rows(fks), False),
            "description": t.comment,
        }


def _fk_rows(fks: list[Any]) -> list[tuple[object, ...]]:
    return [
        (fk.constraint, fk.from_schema, fk.from_table, from_col,
         fk.to_schema, fk.to_table, to_col, not_null)
        for fk in fks
        # fk.from_not_null may be shorter (empty, for tests/callers that build
        # ForeignKey without it) -- fall back to nullable rather than crash.
        for from_col, to_col, not_null in zip(
            fk.from_columns, fk.to_columns,
            fk.from_not_null or [False] * len(fk.from_columns),
            strict=True,
        )
    ]


def _parse_tables(tables: list[str] | str | None) -> list[str] | None:
    if tables is None:
        return None
    if isinstance(tables, str):
        return [t.strip() for t in tables.split(",") if t.strip()]
    return list(tables)


PLANE = "catalog"


def _erd_scope(schema: str | None, tables: list[str] | None) -> str | None:
    """Never the raw `tables` list -- it's user-supplied text, not a header-safe
    scope value (see llens_style.model.Response.__post_init__'s scope guard).
    A `tables`-scoped call falls back to no scope; `schema` alone is safe."""
    if tables:
        return None
    return schema


def _one_sentence(text: str) -> str:
    """Collapse dynamic (`erd.note`/`erd.warnings`/`payload_note`) text -- which
    may already contain its own ". " sentence breaks -- into the single sentence
    Caveat requires, joining the breaks with "; " instead of dropping them."""
    return text.strip().rstrip(".").replace(". ", "; ") + "."


def erd_response(tool: str, erd: Any, scope: str | None, payload_note: str | None,
                 *, mermaid: bool, widget: bool) -> Response:
    n_tables, n_edges = len(erd.nodes), len(erd.edges)
    caveats: list[Block] = []
    if erd.truncated and erd.note:
        caveats.append(Caveat(_one_sentence(erd.note)))
    caveats += [Caveat(_one_sentence(w)) for w in erd.warnings]
    if payload_note:
        caveats.append(Caveat(_one_sentence(payload_note)))
    if widget:
        caveats.append(Caveat('Interactive diagram sent as structured content; if not visible, '
                              'call get_erd with format="mermaid" or format="text".'))
    sections: list[Section]
    if mermaid:
        sections = [Section(None, (Code("mermaid", to_mermaid(erd)), *caveats))]
    else:
        trows = tuple((f"`{n.schema}.{n.table}`", n.kind,
                       "" if n.rows is None else fmt_estimate(n.rows),
                       "related" if n.related else ("external" if n.external else ""),
                       ", ".join(f"`{c.name}`" + (" pk" if c.is_pk else "") + (" fk" if c.is_fk else "")
                                for c in n.columns))
                      for n in erd.nodes)
        erows = tuple((f"`{e.from_schema}.{e.from_table}.{e.from_column}`",
                       f"`{e.to_schema}.{e.to_table}.{e.to_column}`", f"`{e.constraint}`")
                      for e in erd.edges)
        sections = [
            Section("tables", (Table(("table", "kind", "rows (estimate)", "tag", "columns"), trows),
                               *caveats)),
            Section("relationships", (Table(("from", "to", "constraint"), erows),)),
        ]
    first = erd.nodes[0] if erd.nodes else None
    nxt: tuple[Call, ...] = ()
    if first:
        nxt = (Call("describe_table", {"table": f"{first.schema}.{first.table}"}),
               Call("get_relationships", {"table": f"{first.schema}.{first.table}"}))
    return Response(SERVER, tool, scope, PLANE, tuple(sections),
                    tally=(nof(n_tables, "table"), nof(n_edges, "relationship"),
                           *(("truncated",) if erd.truncated else ())),
                    next=nxt)


async def _build_guarded(
    adapter: _IntroAdapter,
    schema: str | None,
    table_list: list[str] | None,
    include_columns: bool,
    max_nodes: int,
    depth: int,
) -> tuple[Any, dict[str, Any], str | None]:
    """build_erd + the payload-size guard both tools share.

    Guards the widget payload (to_dict, as consumed over the ui:// bridge)
    against a size the host would reject,
    falling back to a columns-less rebuild rather than an oversized payload
    Returns (erd, data, note);
    get_erd ignores `data` but keeps the same fallback so its mermaid/text
    output is identical to what the pre-split tool produced.
    """
    erd = await build_erd(
        adapter, None, schema=schema, tables=table_list,
        include_columns=include_columns, max_nodes=max_nodes, depth=depth,
    )
    # Deliberately simple: get_erd pays for a to_dict + json.dumps it never sends,
    # purely so its mermaid/text bytes stay identical to the pre-split tool's
    # (which sized the widget payload before choosing a format). Drop the guard
    # from the get_erd path if that byte-parity ever stops mattering.
    data = to_dict(erd)
    if include_columns and len(json.dumps(data).encode("utf-8")) > _MAX_PAYLOAD_BYTES:
        erd = await build_erd(
            adapter, None, schema=schema, tables=table_list,
            include_columns=False, max_nodes=max_nodes, depth=depth,
        )
        return erd, to_dict(erd), (
            "Columns omitted: the full payload exceeded the size budget; "
            "narrow with `schema` or `tables` for column detail."
        )
    return erd, data, None


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    """Register get_erd -- the plain-text ERD, no MCP Apps binding.

    Deliberately a `@mcp.tool` rather than an `@apps.tool`: the Apps decorator's
    static per-tool `_meta.ui.resourceUri` is what a host reads as
    "widget-capable", and it must not ride a mermaid/text answer (module
    docstring). get_erd_widget in `register_apps` is the widget half.
    """
    adapter = _IntroAdapter(intro)

    @mcp.tool(annotations=read_only("Get ERD"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_erd(
        schema: str | None = None,
        tables: list[str] | str | None = None,
        include_columns: bool = True,
        max_nodes: int = 60,
        format: str = "mermaid",
        depth: int = 1,
    ) -> str:
        """Entity-relationship diagram. Defaults to all exposed schemas (or a
        subset of tables).

        format="mermaid" (default) returns a Mermaid erDiagram code block.
        format="text" lists tables and relationships as markdown, no diagram.
        `tables` (a list, or a comma-separated string) selects a subset and
        pulls in their FK neighbours; `schema` filters to one schema; with
        neither, every exposed table is shown, truncated to `max_nodes`
        (1-200) by FK degree. `depth` (1-3) is how many rounds of FK neighbours
        to pull in around `tables` (1 = immediate neighbours only); it has no
        effect when `tables` is not given. For an interactive diagram on a host
        that renders MCP Apps widgets, call get_erd_widget instead.
        """
        if format == "widget":
            raise ToolError(ErrorCode.FORMAT_UNKNOWN, 'format="widget" moved to the get_erd_widget tool.',
                            "Call get_erd_widget(), or pass format=\"mermaid\" or format=\"text\".")
        if format not in ("mermaid", "text"):
            raise ToolError(ErrorCode.FORMAT_UNKNOWN, f"Unknown format {format!r}.",
                            hint_for(ErrorCode.FORMAT_UNKNOWN, valid='"mermaid", "text"'))
        check_range("max_nodes", max_nodes, 1, 200)
        check_range("depth", depth, 1, 3)
        table_list = _parse_tables(tables)
        erd_, _data, payload_note = await _build_guarded(
            adapter, schema, table_list, include_columns, max_nodes, depth,
        )
        return respond(erd_response("get_erd", erd_, _erd_scope(schema, table_list), payload_note,
                                    mermaid=(format == "mermaid"), widget=False))


def register_apps(apps: Apps, db: Db, settings: Settings, intro: Introspector) -> None:
    adapter = _IntroAdapter(intro)

    # apps.tool(resource_uri=...) stamps a static, per-TOOL _meta.ui.resourceUri
    # on this tool's tools/list entry -- the MCP Apps spec's required negotiation
    # binding, present on every call regardless of arguments. That is exactly why
    # the plain mermaid/text answer lives in a SEPARATE tool (get_erd, see
    # register() above): a host reads this binding as "widget-capable", and it
    # cannot be removed here without breaking the widget for hosts that do
    # negotiate Apps correctly.
    @apps.tool(
        resource_uri=_ERD_VIEW_URI,
        annotations=read_only("Get ERD Widget"),
        visibility=MODEL_ONLY,
    )
    @tool_errors
    async def get_erd_widget(
        schema: str | None = None,
        tables: list[str] | str | None = None,
        include_columns: bool = True,
        max_nodes: int = 60,
        depth: int = 1,
    ) -> CallToolResult:
        """Interactive entity-relationship diagram, for hosts that render MCP Apps.
        Defaults to all exposed schemas.

        Shows a capable host the interactive diagram (pan/zoom, drag, search,
        drill-down, copy-as-Mermaid); every host also gets a markdown summary.
        For plain Mermaid or a text listing, call get_erd instead. `tables` (a list, or a comma-separated
        string) selects a subset and pulls in their FK neighbours; `schema`
        filters to one schema; with neither, every exposed table is shown,
        truncated to `max_nodes` (1-200) by FK degree. `depth` (1-3) is how
        many rounds of FK neighbours to pull in around `tables` (1 = immediate
        neighbours only); it has no effect when `tables` is not given.
        """
        check_range("max_nodes", max_nodes, 1, 200)
        check_range("depth", depth, 1, 3)
        table_list = _parse_tables(tables)
        erd_, _data, payload_note = await _build_guarded(
            adapter, schema, table_list, include_columns, max_nodes, depth,
        )
        # Same generator get_erd's format="mermaid" path uses (to_mermaid) --
        # attaching it here is what makes the widget's "Copy Mermaid" button
        # read DATA.mermaid instead of falling back to its own JS reimplementation.
        structured = to_erd_out(erd_, mermaid=to_mermaid(erd_)).model_dump(mode="json", by_alias=True)
        text = respond(erd_response("get_erd_widget", erd_, _erd_scope(schema, table_list), payload_note,
                                    mermaid=False, widget=True))
        # Result-level _meta.ui.resourceUri (see module docstring): stamped on every
        # successful result alongside structuredContent; tool_errors's own error
        # CallToolResult carries neither, and get_erd (a plain string) carries neither.
        return CallToolResult(
            content=[TextContent(type="text", text=text)],
            structuredContent=structured,
            meta={"ui": {"resourceUri": _bake_erd_resource(structured)}},
        )

    # No `permissions=` declaration: the reference widget shape declares none and
    # the "Copy Mermaid" button already falls back to selectable text when the
    # host blocks the clipboard (copyMermaid() in the template).
    apps.add_html_resource(
        _ERD_VIEW_URI,
        load_widget_html(),
        description=(
            "MCP Apps view template for get_erd_widget: the self-contained ERD renderer "
            "with an empty data block -- the host delivers each call's diagram data "
            "over the ui bridge (ui/notifications/tool-result, "
            "params.structuredContent). Static file, baked once at server startup; "
            "no per-call database access."
        ),
        prefers_border=True,
    )
