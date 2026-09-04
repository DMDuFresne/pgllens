"""View/function introspection: definitions and source, lifted from
TS/tools/get-view-definition.ts, list-functions.ts, get-function-source.ts."""

from __future__ import annotations

import dataclasses
import difflib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.database.introspect import FunctionNotFoundError, TableNotFoundError
from pgllens.llens_style import (
    Block,
    Bullet,
    Bullets,
    Call,
    Caveat,
    Code,
    Response,
    Section,
    Table,
    nof,
)
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "catalog"
_CATALOG_ROWS = 5000
_MAX_SOURCE_CHARS = 100_000

_VIEW_SQL = """
    SELECT c.relkind, pg_get_viewdef(c.oid, true) AS definition,
           obj_description(c.oid, 'pg_class') AS comment
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = %s AND c.relname = %s AND c.relkind IN ('v', 'm')
"""

# Used only when schema is unqualified -- searches every exposed schema in one
# query (mirrors how describe_table/Introspector.table resolve an unqualified
# table name across EXPOSED_SCHEMAS) instead of assuming the default schema.
_VIEW_ANY_SCHEMA_SQL = """
    SELECT n.nspname, c.relkind, pg_get_viewdef(c.oid, true) AS definition,
           obj_description(c.oid, 'pg_class') AS comment
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = ANY(%s) AND c.relname = %s AND c.relkind IN ('v', 'm')
"""

_VIEW_COLUMNS_SQL = """
    SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS data_type,
           col_description(a.attrelid, a.attnum) AS comment
    FROM pg_attribute a
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
"""

_FUNCTIONS_SQL = """
    SELECT n.nspname AS schema, p.proname AS name,
           pg_get_function_arguments(p.oid) AS arguments,
           pg_get_function_result(p.oid) AS return_type,
           p.provolatile AS volatility, obj_description(p.oid, 'pg_proc') AS comment
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE p.prokind = 'f' AND n.nspname = ANY(%s)
"""

_FUNCTION_SOURCE_SQL = """
    SELECT p.oid, pg_get_functiondef(p.oid) AS full_definition, p.prosrc AS source,
           pg_get_function_result(p.oid) AS return_type,
           pg_get_function_arguments(p.oid) AS arguments,
           l.lanname AS language, p.provolatile AS volatility,
           p.prosecdef AS security_definer, p.proisstrict AS is_strict,
           p.prokind AS kind, obj_description(p.oid, 'pg_proc') AS comment
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    JOIN pg_language l ON p.prolang = l.oid
    WHERE n.nspname = %s AND p.proname = %s
    ORDER BY pg_get_function_arguments(p.oid)
"""

_FUNCTION_NAMES_SQL = """
    SELECT p.proname
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = %s
"""

_VOLATILITY = {"v": "VOLATILE", "s": "STABLE", "i": "IMMUTABLE"}
_KIND_LABEL = {"f": "Function", "p": "Procedure", "a": "Aggregate", "w": "Window Function"}


def format_view_definition(schema: str, view: str, view_row: tuple[object, ...],
                            columns: QueryResult, also_in: list[str]) -> Response:
    relkind, definition, comment = view_row
    facts = Bullets((
        Bullet("kind", "materialized view" if relkind == "m" else "view"),
        *((Bullet("comment", str(comment), is_code=False),) if comment else ()),
    ))
    cols = Table(("column", "type", "comment"),
                 tuple((f"`{n}`", f"`{t}`", str(c) if c else "") for n, t, c in columns.rows))
    blocks: list[Block] = [cols]
    if also_in:
        blocks.append(Caveat(f"Also defined in: {', '.join(also_in)}."))
    return Response(
        SERVER, "get_view_definition", f"{schema}.{view}", PLANE,
        (Section("identity", (facts,)), Section("columns", tuple(blocks)),
         Section("definition", (Code("sql", str(definition) if definition else "-- not available"),))),
        tally=(nof(len(columns.rows), "column"),),
        next=(Call("get_sample_data", {"table": f"{schema}.{view}", "limit": 5}),
              Call("explain_query", {"sql": f"SELECT * FROM {schema}.{view} LIMIT 100"})),
    )


def format_functions(result: QueryResult, scope: str | None) -> Response:
    rows = tuple((f"`{s}`", f"`{n}`", f"`{a or ''}`", f"`{r}`", _VOLATILITY.get(str(v), str(v)).lower(),
                  str(c).split("\n")[0] if c else "") for s, n, a, r, v, c in result.rows)
    first = result.rows[0] if result.rows else None
    return Response(
        SERVER, "list_functions", scope, PLANE,
        (Section(None, (Table(("schema", "function", "arguments", "returns", "volatility", "comment"), rows),)),),
        tally=(nof(len(rows), "function"),
               f"{sum(1 for r in result.rows if r[4] == 'v')} volatile"),
        next=(Call("get_function_source", {"function": str(first[1]), "schema": str(first[0])}),) if first else (),
    )


def format_function_source(schema: str, function: str, result: QueryResult) -> Response:
    sections: list[Section] = []
    for i, row in enumerate(result.rows, 1):
        (_oid, full_definition, source, return_type, arguments, language,
         volatility, security_definer, is_strict, kind, comment) = row
        facts = Bullets((
            Bullet("kind", _KIND_LABEL.get(str(kind), str(kind)).lower()),
            Bullet("arguments", str(arguments) if arguments else "none"),
            Bullet("returns", str(return_type)),
            Bullet("language", str(language)),
            Bullet("volatility", _VOLATILITY.get(str(volatility), str(volatility)).lower()),
            Bullet("security", "definer" if security_definer else "invoker"),
            Bullet("strict", "yes" if is_strict else "no"),
            *((Bullet("comment", str(comment), is_code=False),) if comment else ()),
        ))
        body = str(full_definition) if full_definition else f"-- source body\n{source}"
        heading = f"overload {i}" if len(result.rows) > 1 else "source"
        sections.append(Section(heading, (facts, Code("sql", body))))
    if len(sections) == 1:
        sections = [Section(None, sections[0].blocks)]
    return Response(
        SERVER, "get_function_source", f"{schema}.{function}", PLANE, tuple(sections),
        tally=(nof(len(result.rows), "overload"),),
        next=(Call("list_functions", {"schema": schema}),),
    )


def _truncate_sources(resp: Response) -> Response:
    """Cap the total size of every Code block's source text at
    _MAX_SOURCE_CHARS, splitting the budget evenly across overloads. Only the
    Code blocks are unbounded (facts/headings are small and fixed), so that's
    the only thing measured and cut."""
    n_codes = sum(1 for s in resp.sections for b in s.blocks if isinstance(b, Code))
    total = sum(len(b.text) for s in resp.sections for b in s.blocks if isinstance(b, Code))
    if total <= _MAX_SOURCE_CHARS:
        return resp
    per = _MAX_SOURCE_CHARS // max(1, n_codes)
    new_sections = tuple(
        dataclasses.replace(s, blocks=tuple(
            dataclasses.replace(b, text=b.text[:per]) if isinstance(b, Code) else b
            for b in s.blocks))
        for s in resp.sections
    )
    return dataclasses.replace(
        resp, sections=new_sections, tally=(*resp.tally, "source truncated at 100,000 chars"))


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get View Definition"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_view_definition(view: str, schema: str | None = None) -> str:
        """Full SQL source and column listing for a view or materialized view.
        Defaults to all exposed schemas (like describe_table) rather than
        assuming the default one; if the view exists in more than one, the
        default schema wins and the rest are named for disambiguation."""
        also_in: list[str] = []
        if schema is not None:
            schema_name = db.resolve_schema(schema)
            view_result = await db.run_system(_VIEW_SQL, (schema_name, view))
            if not view_result.rows:
                raise TableNotFoundError(f"View '{view}' not found in the exposed schemas.")
            row = view_result.rows[0]
        else:
            found = await db.run_system(_VIEW_ANY_SCHEMA_SQL, (list(settings.exposed_schemas), view))
            if not found.rows:
                raise TableNotFoundError(f"View '{view}' not found in the exposed schemas.")
            # Tie-break in exposed-schema order, not catalog-scan order (the SQL has
            # no ORDER BY): index() is safe because every nspname came back from an
            # n.nspname = ANY(%s) filter, so it is by construction in the allowlist.
            schemas = sorted({str(r[0]) for r in found.rows}, key=settings.exposed_schemas.index)
            schema_name = (settings.default_schema if settings.default_schema in schemas
                            else schemas[0])
            row = next(r for r in found.rows if r[0] == schema_name)[1:]
            also_in = [s for s in schemas if s != schema_name]
        columns = await db.run_system(_VIEW_COLUMNS_SQL, (schema_name, view))
        return respond(format_view_definition(schema_name, view, row, columns, also_in))

    @mcp.tool(annotations=read_only("List Functions"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def list_functions(schema: str | None = None) -> str:
        """List functions (name, arguments, return type, volatility, comment).
        Defaults to all exposed schemas; pass `schema` to narrow to one."""
        sql = _FUNCTIONS_SQL
        params: tuple[object, ...] = (list(settings.exposed_schemas),)
        resolved = None
        if schema is not None:
            resolved = db.resolve_schema(schema)
            sql += " AND n.nspname = %s"
            params = (list(settings.exposed_schemas), resolved)
        sql += " ORDER BY n.nspname, p.proname"
        result = await db.run_system(sql, params)
        return respond(format_functions(result, resolved))

    @mcp.tool(annotations=read_only("Get Function Source"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_function_source(function: str, schema: str | None = None) -> str:
        """Full function/procedure definition, including all overloads.
        Defaults to the server's default schema."""
        schema_name = db.resolve_schema(schema)
        result = await db.run_system(_FUNCTION_SOURCE_SQL, (schema_name, function))
        if not result.rows:
            names_result = await db.run_system(_FUNCTION_NAMES_SQL, (schema_name,))
            names = [str(r[0]) for r in names_result.rows]
            near = difflib.get_close_matches(function, names, n=3)
            hint = f" Did you mean: {', '.join(near)}?" if near else ""
            raise FunctionNotFoundError(
                f"Function {function!r} not found in schema {schema_name!r}.{hint}"
            )
        return respond(_truncate_sources(format_function_source(schema_name, function, result)))
