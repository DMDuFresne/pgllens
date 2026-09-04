"""Index health diagnostics: unused, invalid, and duplicate indexes, with
size. New SQL (not lifted from TS -- this tool exists in MsSQLLens and not in
PgLLens), written against pg_stat_user_indexes and pg_index. Point-in-time
reads -- never cached."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.llens_style import Block, Call, Caveat, Response, Section, Table, iso, nof
from pgllens.llens_style import count as fmt_count
from pgllens.llens_style import size as fmt_size
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "stats"

_INDEX_HEALTH_SQL = """
    SELECT n.nspname AS schema, t.relname AS table, i.relname AS index,
           s.idx_scan, pg_relation_size(s.indexrelid) AS index_size,
           NOT ix.indisvalid AS is_invalid, ix.indkey::text AS indkey, ix.indrelid,
           pg_get_expr(ix.indpred, ix.indrelid) AS indpred,
           pg_get_expr(ix.indexprs, ix.indrelid) AS indexprs,
           con.contype AS constraint_type
    FROM pg_stat_user_indexes s
    JOIN pg_index ix ON ix.indexrelid = s.indexrelid
    JOIN pg_class i ON i.oid = s.indexrelid
    JOIN pg_class t ON t.oid = s.relid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    LEFT JOIN pg_constraint con
           ON con.conindid = ix.indexrelid AND con.contype IN ('p', 'u')
    WHERE n.nspname = %s
    ORDER BY s.idx_scan ASC, index_size DESC
"""

# Leading-column-only: a composite index (a, b) covers a FK on (a), so this
# only misses FKs whose covering index exists but doesn't lead with the FK
# column, the standard "is there any index usable for this join" heuristic.
# Deliberately simple: leading-column match ceiling; add full multi-column FK
# matching (conkey vs. indkey prefix) if single-column FKs turn out not to be
# enough.
_FK_COVERAGE_SQL = """
    SELECT n.nspname AS schema, t.relname AS table, a.attname AS column,
           con.conname AS constraint, con.conrelid AS conrelid,
           con.conkey[1] AS leading_attnum
    FROM pg_constraint con
    JOIN pg_class t ON t.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = con.conkey[1]
    JOIN pg_class ft ON ft.oid = con.confrelid
    JOIN pg_namespace fn ON fn.oid = ft.relnamespace
    WHERE con.contype = 'f' AND n.nspname = %s AND fn.nspname = ANY(%s)
"""

# How long idx_scan has been accumulating. pg_stat_get_db_stat_reset_time is
# NULL until the first pg_stat_reset(); then the counters date from postmaster
# start. Days are computed server-side so the formatter needs no clock.
_STATS_WINDOW_SQL = """
    SELECT coalesce(pg_stat_get_db_stat_reset_time(d.oid), pg_postmaster_start_time()) AS since,
           pg_stat_get_db_stat_reset_time(d.oid) IS NULL AS from_postmaster,
           extract(epoch FROM now() - coalesce(pg_stat_get_db_stat_reset_time(d.oid),
                                               pg_postmaster_start_time())) / 86400.0 AS days
    FROM pg_database d
    WHERE d.datname = current_database()
"""

# Under a week of counters, "0 scans" says more about the window than the index.
STATS_WINDOW_SHORT_DAYS = 7


def format_index_health(
    result: QueryResult, fk_coverage: QueryResult | None, scope: str, others: list[str],
    window: QueryResult | None = None,
) -> Response:
    """Duplicate detection compares indrelid + the full indkey column list + the
    partial-index predicate + the expression list: an exact
    same-columns-same-order-same-predicate match only, not prefix subsumption
    (an index on (a) is not flagged as redundant with one on (a, b)).
    Deliberately simple: exact-match ceiling; add prefix subsumption if that
    turns out to matter in practice."""
    idx = {name: i for i, name in enumerate(result.columns)}
    rows: list[tuple[str, ...]] = []
    unused: list[str] = []
    invalid: list[str] = []
    by_key: dict[tuple[object, object, object, object], list[str]] = {}
    leading_columns: set[tuple[object, str]] = set()  # (indrelid, leading attnum)

    for row in result.rows:
        schema = row[idx["schema"]]
        table = row[idx["table"]]
        index = row[idx["index"]]
        scans = cast("int", row[idx["idx_scan"]])
        size = row[idx["index_size"]]
        is_invalid = row[idx["is_invalid"]]
        indkey = row[idx["indkey"]]
        indrelid = row[idx["indrelid"]]
        constraint_type = row[idx["constraint_type"]]
        indpred = row[idx["indpred"]]
        indexprs = row[idx["indexprs"]]
        full_name = f"`{schema}.{table}.{index}`"
        if constraint_type in ("p", "u"):
            full_name += " (constraint-backed)"

        rows.append((f"`{schema}`", f"`{table}`", f"`{index}`", fmt_count(scans),
                     fmt_size(int(cast("int | str", size)))))
        # A PK/UNIQUE-backed index exists to enforce the constraint, not for
        # query performance -- 0 scans there is not a candidate for dropping.
        if scans == 0 and constraint_type not in ("p", "u"):
            unused.append(full_name)
        if is_invalid:
            invalid.append(full_name)
        # (indrelid, indkey) as a dict key relies on both surviving the driver
        # round-trip as stable, hashable, equal-if-equal raw types (psycopg
        # returns indrelid as int and indkey::text as str -- fine today, but
        # a future row-factory or type-adapter change could break equality).
        by_key.setdefault((indrelid, indkey, indpred, indexprs), []).append(full_name)
        leading_attnum = str(cast("str", indkey)).split()[0] if indkey else None
        # An invalid index (failed CREATE INDEX CONCURRENTLY) doesn't actually
        # cover anything -- Postgres won't plan against it -- so it must not
        # count as FK coverage.
        if leading_attnum and not is_invalid:
            leading_columns.add((indrelid, leading_attnum))

    duplicates = [names for names in by_key.values() if len(names) > 1]

    uncovered_fks: list[str] = []
    if fk_coverage is not None and fk_coverage.rows:
        fk_idx = {name: i for i, name in enumerate(fk_coverage.columns)}
        for row in fk_coverage.rows:
            conrelid = row[fk_idx["conrelid"]]
            leading_attnum = str(row[fk_idx["leading_attnum"]])
            if (conrelid, leading_attnum) not in leading_columns:
                uncovered_fks.append(
                    f"`{row[fk_idx['table']]}.{row[fk_idx['column']]}` "
                    f"({row[fk_idx['constraint']]})")

    window_caveats: list[Caveat] = []
    short_window = False
    days = 0.0
    if window is not None and window.rows:
        w_idx = {name: i for i, name in enumerate(window.columns)}
        since = window.rows[0][w_idx["since"]]
        from_pm = bool(window.rows[0][w_idx["from_postmaster"]])
        days = float(cast("float | str", window.rows[0][w_idx["days"]]))
        since_str = iso(since) if isinstance(since, datetime) else str(since)
        origin = " (server start; statistics never reset)" if from_pm else ""
        window_caveats.append(Caveat(
            f"Scan counts accumulated since {since_str}{origin}: {days:.0f} days."))
        short_window = days < STATS_WINDOW_SHORT_DAYS

    sections = [Section("indexes", (
        Table(("schema", "table", "index", "scans", "size"), tuple(rows)),
        *window_caveats,
        *([Caveat(f"Scope is {scope}; also exposed: {', '.join(others)}.")] if others else []),
    ))]
    for heading, items in (("unused", unused), ("invalid", invalid),
                           ("fks without index", uncovered_fks)):
        if items:
            col = "foreign key" if heading == "fks without index" else "index"
            blocks: tuple[Block, ...] = (Table((col,), tuple((i,) for i in items)),)
            if heading == "unused" and short_window:
                blocks += (Caveat(
                    f"Only {days:.1f} days of statistics -- 0 scans is not evidence the index "
                    "is unneeded, do not drop on this window alone."),)
            sections.append(Section(heading, blocks))
    if duplicates:
        sections.append(Section("duplicates", (
            Table(("indexes",), tuple((", ".join(dup),) for dup in duplicates)),)))
    if len(sections) == 1:
        sections = [Section(None, sections[0].blocks)]
    return Response(
        SERVER, "get_index_health", scope, PLANE, tuple(sections),
        tally=(nof(len(rows), "index", "indexes"), f"{len(unused)} unused",
               f"{len(invalid)} invalid", nof(len(duplicates), "duplicate set"),
               nof(len(uncovered_fks), "uncovered fk")),
        next=(Call("get_table_health", {"schema": scope}), Call("get_space_usage", {"schema": scope})),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Index Health"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_index_health(schema: str | None = None) -> str:
        """Report index health for a schema: every index with scan count and
        size, plus call-outs for unused indexes (idx_scan = 0, excluding
        PK/UNIQUE-backed indexes, which exist to enforce a constraint rather
        than for query performance), invalid indexes (a failed CREATE INDEX
        CONCURRENTLY left behind), duplicate indexes (same table, identical
        column list, predicate, and expressions -- two partial indexes with
        different WHERE clauses are not duplicates), and foreign keys with no
        covering index on their leading column. Defaults to the server's
        default schema (a caveat names the others); pass `schema` to scope
        to another exposed schema. A caveat under the table states since when
        scan counts have accumulated (the database stats reset time, or
        server start); under 7 days the unused section warns not to drop on
        that evidence alone."""
        resolved = db.resolve_schema(schema)
        result = await db.run_system(_INDEX_HEALTH_SQL, (resolved,))
        # Referenced end filtered to the exposed schemas to match introspection
        # (schemas enter by allowlist only): an FK into an unexposed schema is not listed by
        # get_relationships or find_path, so flagging it here would make the
        # three tools disagree about which relationships exist.
        fk_coverage = await db.run_system(
            _FK_COVERAGE_SQL, (resolved, settings.exposed_schemas))
        window = await db.run_system(_STATS_WINDOW_SQL)
        others = [s for s in settings.exposed_schemas if s != resolved] if schema is None else []
        return respond(format_index_health(result, fk_coverage, resolved, others, window=window))
