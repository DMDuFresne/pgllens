"""Schema discovery tools: tables, columns, samples, schema cache refresh."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_AND_APP, MODEL_ONLY, read_only, visibility
from pgllens.database.format import matches_redacted, redact, table_from
from pgllens.database.introspect import Table as Table_
from pgllens.llens_style import Block, Bullet, Bullets, Call, Caveat, Response, Section, Table, nof
from pgllens.llens_style import estimate as fmt_estimate
from pgllens.tools._util import (
    SERVER,
    _quote_ident,
    check_range,
    resolve_table,
    respond,
    tool_errors,
)

_MAX_SAMPLE = 1000

PLANE_CATALOG = "catalog"
PLANE_MIXED = "catalog+stats"
PLANE_QUERY = "query"

_KIND = {"r": "table", "v": "view", "m": "materialized view", "p": "partitioned table",
         "f": "foreign table"}

# Most-common-values from the planner's own sample: no table scan, and pg_stats
# already hides columns the role can't SELECT. most_common_vals is anyarray, so
# it needs the ::text::text[] double cast to come back as a uniform text array.
# An inheritance/partition parent has TWO rows per attname -- inherited = false
# (its own rows) and true (the whole tree). Both are kept and ordered so the
# own-rows row is seen first and wins; filtering inherited = false instead would
# blank out declarative partitioned parents, which only ever have the true row.
_STATS_SQL = """
    SELECT attname, n_distinct, most_common_vals::text::text[] AS mcv, inherited
    FROM pg_stats
    WHERE schemaname = %s AND tablename = %s
    ORDER BY attname, inherited
"""
# Two rows per column on a partitioned parent, and a table can be wide -- don't
# let the user-facing MAX_ROWS (200 by default) silently drop columns.
_CATALOG_ROWS = 5000
# Cap both the "is this low cardinality?" test and the rendered list -- a column
# with hundreds of distinct values is noise, and the cell is a token budget.
_MAX_VALUES = 20


def _values_cell(prefix: str, values: list[str]) -> str:
    shown = ", ".join(values[:_MAX_VALUES])
    more = ", …" if len(values) > _MAX_VALUES else ""
    return f"{prefix}: {shown}{more}"


def format_columns(table: Table_, values: dict[str, str] | None = None) -> Table:
    """Column list as a style Table: name, type, nullability, default, PK
    marker, comment, values. One row per column, ordered by ordinal (already
    the order Introspector returns them in). `values` maps column name to the
    pre-rendered enum/sample-values cell; callers that don't have one get an
    empty column, which keeps this function pure."""
    values = values or {}
    rows = []
    for c in table.columns:
        default = c.default or ""
        if c.is_identity == "a":
            default = "identity (always)"
        elif c.is_identity == "d":
            default = "identity (by default)"
        rows.append((f"`{c.name}`", f"`{c.data_type}`", "yes" if c.nullable else "no",
                     f"`{default}`" if default else "", "✓" if c.name in table.primary_key else "",
                     c.comment or "", values.get(c.name, "")))
    return Table(("column", "type", "null", "default", "pk", "comment", "values"), tuple(rows))


def format_table_list(tables: list[Table_]) -> Table:
    """Table list as a style Table: schema, name, kind, row estimate, comment."""
    rows = tuple((f"`{t.schema}`", f"`{t.name}`", _KIND[t.kind], fmt_estimate(t.row_estimate),
                  t.comment or "") for t in tables)
    return Table(("schema", "table", "kind", "rows (estimate)", "comment"), rows)


def describe_response(t: Table_, values: dict[str, str], has_stats: bool) -> Response:
    blocks: list[Block] = [format_columns(t, values)]
    if t.kind in ("r", "m", "p") and t.columns and not has_stats:
        blocks.append(Caveat("No planner statistics yet for this table; run ANALYZE for "
                             "distinct-value samples."))
    facts = Bullets((
        Bullet("kind", _KIND[t.kind]),
        Bullet("primary key", ", ".join(t.primary_key) if t.primary_key else "none"),
        Bullet("rows", fmt_estimate(t.row_estimate), qualifier="estimate"),
        *((Bullet("comment", t.comment, is_code=False),) if t.comment else ()),
    ))
    return Response(
        SERVER, "describe_table", t.qualified, PLANE_MIXED,
        (Section("identity", (facts,)), Section("columns", tuple(blocks))),
        tally=(nof(len(t.columns), "column"), f"{fmt_estimate(t.row_estimate)} rows (estimate)"),
        next=(Call("get_sample_data", {"table": t.qualified, "limit": 5}),
              Call("get_relationships", {"table": t.qualified}),
              Call("get_constraints", {"table": t.qualified})),
    )


def sample_sql(table: Table_, limit: int) -> tuple[str, tuple[object, ...]]:
    if not 1 <= limit <= _MAX_SAMPLE:
        raise ValueError(f"limit must be between 1 and {_MAX_SAMPLE} (got {limit})")
    ident = f"{_quote_ident(table.schema)}.{_quote_ident(table.name)}"
    return f"SELECT * FROM {ident} LIMIT %s", (limit,)


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("List Tables"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def list_tables(schema: str | None = None) -> str:
        """List tables and views with kind, row estimate, and comment.
        Defaults to all exposed schemas; pass `schema` to filter to one."""
        tables = await intro.tables()
        resolved = db.resolve_schema(schema) if schema is not None else None
        if resolved is not None:
            tables = [t for t in tables if t.schema == resolved]
        tables.sort(key=lambda t: (-t.row_estimate, t.schema, t.name))
        n_tables = sum(t.kind in ("r", "p", "f") for t in tables)
        n_views = len(tables) - n_tables
        first = tables[0].qualified if tables else None
        return respond(Response(
            SERVER, "list_tables", resolved, PLANE_MIXED,
            (Section(None, (format_table_list(tables),)),),
            tally=(nof(n_tables, "table"), nof(n_views, "view"),
                   f"{fmt_estimate(sum(t.row_estimate for t in tables))} rows (estimate)"),
            next=tuple(c for c in (
                Call("describe_table", {"table": first}) if first else None,
                Call("get_erd", {"schema": resolved}) if resolved else Call("get_erd"),
            ) if c),
        ))

    @mcp.tool(annotations=read_only("Describe Table"), meta=visibility(*MODEL_AND_APP))
    @tool_errors
    async def describe_table(table: str, schema: str | None = None) -> str:
        """Describe a table's columns (type, nullability, PK, default, comment,
        values) and its primary key. Defaults to all exposed schemas; `schema`
        narrows it. A name that matches nothing in the exposed schemas
        reports "not found" (with a suggestion) rather than an empty description.

        The `values` column shows an enum column's labels (read from the catalog,
        so always complete and always available), and for a non-enum column with
        at most 20 distinct values, a sample of its most common values. Those
        samples come from PLANNER STATISTICS: approximate, only as fresh as the
        last ANALYZE, and absent for a never-analyzed table. A column whose
        sampled values are all distinct (every row unique, as in a name or
        email column on a small table) has no most-common-values list, so it
        shows no sample even when its distinct count is under 20. Use
        `get_table_stats` when you need exact counts. A column matching
        REDACT_COLUMNS shows no sampled values at all (enum labels are type
        metadata and still show)."""
        t = await resolve_table(db, intro, table, schema)
        stats = await db.run_system(_STATS_SQL, (t.schema, t.name), max_rows=_CATALOG_ROWS)
        values: dict[str, str] = {}
        for stat_row in stats.rows:
            attname, n_distinct, mcv, _inherited = cast(
                "tuple[str, float | None, list[str] | None, bool]", stat_row
            )
            if not mcv or n_distinct is None:
                continue
            # n_distinct < 0 is a fraction of the row count (Postgres stores it
            # negative once distinct values exceed ~10% of rows) -- resolve it
            # against row_estimate before gating, or the sample feature almost
            # never fires on a small table.
            distinct_count = n_distinct if n_distinct >= 0 else abs(n_distinct) * t.row_estimate
            if not (1 <= distinct_count <= _MAX_VALUES):
                continue
            # These are stored data values, not metadata, so they go through the
            # same REDACT_COLUMNS matcher get_sample_data/query use. A match
            # renders NOTHING rather than a [masked] marker: the cell is a
            # sample, and "no sample" is the honest rendering of one we won't show.
            if matches_redacted(attname, settings.redact_columns):
                continue
            values.setdefault(attname, _values_cell("values", mcv))  # own rows win
        for c in t.columns:
            labels = await intro.enum_labels(c.data_type, c.type_schema)
            if labels:
                values[c.name] = _values_cell("enum", labels)   # beats any sample
        return respond(describe_response(t, values, bool(stats.rows)))

    @mcp.tool(annotations=read_only("Schema Overview"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def schema_overview(schema: str | None = None) -> str:
        """Summarize the schema: per-schema table/view counts and total row
        estimate, as a quick orientation. Defaults to all exposed schemas;
        pass `schema` to limit to one."""
        tables = await intro.tables()
        resolved = db.resolve_schema(schema) if schema is not None else None
        if resolved is not None:
            tables = [t for t in tables if t.schema == resolved]
        per: dict[str, list[int]] = {}   # schema -> [tables, views, rows]
        for t in tables:
            slot = per.setdefault(t.schema, [0, 0, 0])
            slot[0 if t.kind in ("r", "p", "f") else 1] += 1
            slot[2] += t.row_estimate
        ordered = sorted(per.items(), key=lambda kv: (-kv[1][2], kv[0]))
        rows = tuple((f"`{s}`", str(a), str(b), fmt_estimate(r)) for s, (a, b, r) in ordered)
        largest = ordered[0][0] if ordered else None
        return respond(Response(
            SERVER, "schema_overview", resolved, PLANE_MIXED,
            (Section(None, (Table(("schema", "tables", "views", "rows (estimate)"), rows),)),),
            tally=(nof(len(per), "schema"), nof(len(tables), "object"),
                   f"{fmt_estimate(sum(t.row_estimate for t in tables))} rows (estimate)"),
            next=(Call("list_tables", {"schema": largest}),
                  Call("get_relationships", {"schema": largest})) if largest else (),
        ))

    @mcp.tool(annotations=read_only("Search Columns"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def search_columns(pattern: str, schema: str | None = None) -> str:
        """Search for columns by name (case-insensitive substring match)
        across every exposed schema. Defaults to all exposed schemas; pass
        `schema` to limit to one."""
        matches = await intro.search_columns(pattern)
        resolved = db.resolve_schema(schema) if schema is not None else None
        if resolved is not None:
            matches = [m for m in matches if m[0] == resolved]
        rows = tuple((f"`{s}`", f"`{t}`", f"`{c}`", f"`{d}`") for s, t, c, d in matches)
        first = f"{matches[0][0]}.{matches[0][1]}" if matches else None
        return respond(Response(
            SERVER, "search_columns", None, PLANE_CATALOG,
            (Section(None, (Table(("schema", "table", "column", "type"), rows),)),),
            tally=(f"{nof(len(rows), 'column')} match `{pattern}`",),
            next=(Call("describe_table", {"table": first}),) if first else (),
        ))

    @mcp.tool(annotations=read_only("Get Sample Data"), meta=visibility(*MODEL_AND_APP))
    @tool_errors
    async def get_sample_data(table: str, limit: int = 10, schema: str | None = None) -> str:
        """Return up to `limit` (1-1000) sample rows from a table, unfiltered.
        Defaults to all exposed schemas. Values outside 1-1000 are rejected,
        not clamped. If REDACT_COLUMNS is
        configured, matching columns render as `[masked]` -- this is
        best-effort DISPLAY MASKING by column name, not a security boundary
        (see the `query` tool's docstring). For a real, unbypassable
        guarantee, use column-level `REVOKE SELECT (col)` on the pgllens role
        instead -- note that this makes `SELECT *`-shaped tools like this one
        error on that table (see docs/DEPLOY.md's Database role section)."""
        t = await resolve_table(db, intro, table, schema)
        check_range("limit", limit, 1, _MAX_SAMPLE)
        sql, params = sample_sql(t, limit)
        result = redact(await db.run_system(sql, params), settings.redact_columns)
        masked = [c for c in result.columns if matches_redacted(c, settings.redact_columns)]
        return respond(Response(
            SERVER, "get_sample_data", t.qualified, PLANE_QUERY,
            (Section(None, (table_from(result),)),),
            tally=(f"{nof(len(result.rows), 'row')} of {limit} requested",
                   *((f"masked: {', '.join(masked)}",) if masked else ())),
            next=(Call("describe_table", {"table": t.qualified}),
                  Call("get_table_stats", {"table": t.qualified})),
        ))

    @mcp.tool(annotations=read_only("Refresh Schema Cache"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def refresh_schema() -> str:
        """Force a re-read of the database catalog, replacing the cached
        schema metadata. Returns the number of tables/views now cached."""
        count = await intro.refresh()
        return respond(Response(
            SERVER, "refresh_schema", None, PLANE_CATALOG,
            (Section(None, (Bullets((Bullet("cached objects", str(count)),)),)),),
        ))
