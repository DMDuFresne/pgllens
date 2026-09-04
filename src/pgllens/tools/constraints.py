"""Table constraints: CHECK/UNIQUE/EXCLUDE/PRIMARY KEY/FOREIGN KEY with their
full pg_get_constraintdef text. describe_table only shows the primary key, so
this is the one place a model can see the rest. New SQL against pg_constraint."""

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

_CONSTRAINTS_SQL = """
    SELECT n.nspname AS schema, cl.relname AS table, con.conname AS name,
           CASE con.contype WHEN 'c' THEN 'CHECK' WHEN 'u' THEN 'UNIQUE' WHEN 'x' THEN 'EXCLUDE'
                            WHEN 'p' THEN 'PRIMARY KEY' WHEN 'f' THEN 'FOREIGN KEY'
                            ELSE con.contype::text END AS type,
           pg_get_constraintdef(con.oid, true) AS definition,
           -- pg_get_constraintdef drops the schema from a REFERENCES target on the
           -- search_path, which is ambiguous for cross-schema FKs; name it explicitly.
           CASE WHEN con.contype = 'f' THEN tn.nspname || '.' || tc.relname END AS references,
           con.convalidated AS validated
    FROM pg_constraint con
    JOIN pg_class cl ON con.conrelid = cl.oid
    JOIN pg_namespace n ON cl.relnamespace = n.oid
    LEFT JOIN pg_class tc ON con.confrelid = tc.oid
    LEFT JOIN pg_namespace tn ON tc.relnamespace = tn.oid
    WHERE n.nspname = ANY(%s) AND con.contype IN ('c','u','x','p','f')
"""

_ORDER_BY = " ORDER BY n.nspname, cl.relname, con.contype, con.conname"

# Catalog metadata rows are small and must not be silently cut at the 200-row
# query cap. Lower than introspect/discovery's 100_000 on purpose: this one is
# a user-facing table (one row per constraint) and 5000 rows is already past
# useful, so it truncates with a narrow-your-scope hint instead.
_CATALOG_ROWS = 5000


def format_constraints(result: QueryResult, scope: str | None) -> Response:
    rows = tuple((f"`{s}`", f"`{t}`", f"`{n}`", str(ct).lower(), f"`{d}`", f"`{r}`" if r else "",
                  "yes" if v else "NOT VALID") for s, t, n, ct, d, r, v in result.rows)
    blocks: list[Block] = [
        Table(("schema", "table", "constraint", "type", "definition", "references", "validated"), rows)]
    tally = [nof(len(rows), "constraint")]
    if result.truncated:
        tally.append(f"truncated at {_CATALOG_ROWS} rows")
        blocks.append(Caveat("Narrow with `table` or `schema` to see the rest."))
    first_fk = next((r for r in result.rows if r[3] == "FOREIGN KEY"), None)
    return Response(
        SERVER, "get_constraints", scope, PLANE, (Section(None, tuple(blocks)),),
        tally=tuple(tally),
        next=(Call("get_relationships", {"table": f"{first_fk[0]}.{first_fk[1]}"}),) if first_fk else (),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Constraints"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_constraints(table: str | None = None, schema: str | None = None) -> str:
        """CHECK, UNIQUE, EXCLUDE, PRIMARY KEY and FOREIGN KEY constraints with
        their full definitions; describe_table shows only the PK. Defaults to
        all exposed schemas. FOREIGN KEY rows also name the referenced table as
        schema.table in `references`, since the definition text omits the
        schema for targets on the search_path."""
        sql = _CONSTRAINTS_SQL
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
        return respond(format_constraints(result, scope))
