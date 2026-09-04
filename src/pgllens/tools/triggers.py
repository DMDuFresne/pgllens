"""User-defined triggers per table, with the CREATE TRIGGER text and the
function each one calls. Internal (FK-enforcement) triggers are excluded.
New SQL against pg_trigger."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.llens_style import Block, Call, Caveat, Response, Section, Table, nof
from pgllens.tools._util import SERVER, resolve_table, respond, tool_errors

PLANE = "catalog"

_TRIGGERS_SQL = """
    SELECT n.nspname AS schema, cl.relname AS table, t.tgname AS trigger,
           CASE t.tgenabled WHEN 'D' THEN 'disabled' WHEN 'O' THEN 'enabled'
                            WHEN 'R' THEN 'replica only' WHEN 'A' THEN 'always'
                            ELSE t.tgenabled::text END AS enabled,
           pg_get_triggerdef(t.oid, true) AS definition,
           pn.nspname || '.' || p.proname AS function
    FROM pg_trigger t
    JOIN pg_class cl ON t.tgrelid = cl.oid
    JOIN pg_namespace n ON cl.relnamespace = n.oid
    JOIN pg_proc p ON t.tgfoid = p.oid
    JOIN pg_namespace pn ON p.pronamespace = pn.oid
    WHERE NOT t.tgisinternal AND n.nspname = ANY(%s)
"""

_ORDER_BY = " ORDER BY n.nspname, cl.relname, t.tgname"

# Catalog metadata rows are small and must not be silently cut at the 200-row
# query cap -- mirrors introspect._CATALOG_ROWS / discovery._CATALOG_ROWS.
_CATALOG_ROWS = 5000


def format_triggers(result: QueryResult, scope: str | None) -> Response:
    rows = tuple((f"`{s}`", f"`{t}`", f"`{n}`", str(e), f"`{d}`", f"`{f}`")
                 for s, t, n, e, d, f in result.rows)
    blocks: list[Block] = [
        Table(("schema", "table", "trigger", "enabled", "definition", "function"), rows)]
    tally = [nof(len(rows), "trigger")]
    if result.truncated:
        tally.append(f"truncated at {_CATALOG_ROWS} rows")
        blocks.append(Caveat("Narrow with `table` or `schema` to see the rest."))
    first = result.rows[0] if result.rows else None
    nxt: tuple[Call, ...] = ()
    if first:
        full = str(first[5])
        if "." in full:
            fschema, fname = full.split(".", 1)
            nxt = (Call("get_function_source", {"function": fname, "schema": fschema}),)
        else:
            nxt = (Call("get_function_source", {"function": full}),)
    return Response(SERVER, "get_triggers", scope, PLANE, (Section(None, tuple(blocks)),),
                     tally=tuple(tally), next=nxt)


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Triggers"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_triggers(table: str | None = None, schema: str | None = None) -> str:
        """User-defined triggers per table (FK-enforcement internal triggers
        excluded), with the full CREATE TRIGGER text and the function each one
        calls. Defaults to all exposed schemas."""
        sql = _TRIGGERS_SQL
        params: tuple[object, ...] = (list(settings.exposed_schemas),)
        scope: str | None = None
        if table is not None:
            t = await resolve_table(db, intro, table, schema)
            sql += " AND (n.nspname, cl.relname) = (%s, %s)"
            params += (t.schema, t.name)
            scope = t.qualified
        elif schema is not None:
            scope = db.resolve_schema(schema)
            sql += " AND n.nspname = %s"
            params += (scope,)
        result = await db.run_system(sql + _ORDER_BY, params, max_rows=_CATALOG_ROWS)
        return respond(format_triggers(result, scope))
