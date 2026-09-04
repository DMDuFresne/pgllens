"""Table health, table stats, and space usage. get_table_health and
get_table_stats' SQL is lifted from TS/tools/get-table-health.ts and
TS/tools/get-table-stats.ts (renderSummary's pg_stat_user_tables query, and
the UNION ALL per-column null/distinct batch respectively) -- get_space_usage
is new SQL, written against pg_total_relation_size/pg_relation_size/
pg_indexes_size/pg_database_size (this tool does not exist in MsSQLLens: SQL
Server sizes files, not relations, so space.py's shape does not carry over).
Point-in-time reads -- never cached."""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Column, Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_AND_APP, MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.llens_style import Block, Call, Caveat, Response, Section, Table, iso, nof
from pgllens.llens_style import count as fmt_count
from pgllens.llens_style import estimate as fmt_estimate
from pgllens.llens_style import size as fmt_size
from pgllens.tools._util import SERVER, _quote_ident, resolve_table, respond, tool_errors

PLANE_STATS = "stats"
PLANE_MIXED = "catalog+stats"

# Bloat is the classic catalog estimate (check_postgres lineage): expected pages
# = reltuples x (24-byte tuple header + sum of pg_stats avg_width weighted by
# non-null fraction) / usable page bytes, compared with relpages. It is an
# estimate; pgstattuple measured mode is v3. NULL when the table has never
# been analyzed (reltuples < 0 on PG14+, or no pg_stats rows).
# Deliberately simple: ignores fillfactor, alignment padding and the null bitmap;
# the 20% / 10 MB flag floor absorbs that error on any table big enough to matter.
# Also deliberately simple: per-table autovacuum_freeze_max_age reloptions are
# ignored in favour of the global GUC; parse c.reloptions if a deployment tunes
# it per table.
_TABLE_HEALTH_SQL = """
    SELECT n.nspname AS schema, c.relname AS table,
           c.reltuples::bigint AS n_live_tup, s.n_dead_tup,
           CASE WHEN (s.n_live_tup + s.n_dead_tup) > 0
                THEN round(100.0 * s.n_dead_tup / (s.n_live_tup + s.n_dead_tup), 1)
                ELSE 0 END AS dead_pct,
           s.last_autovacuum, {ins_since_vacuum},
           CASE WHEN c.relfrozenxid <> '0'::xid THEN age(c.relfrozenxid) END AS xid_age,
           current_setting('autovacuum_freeze_max_age')::bigint AS freeze_max_age,
           (SELECT age(d.datfrozenxid) FROM pg_database d
             WHERE d.datname = current_database()) AS db_xid_age,
           b.bloat_pct, b.bloat_bytes
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN LATERAL (
        SELECT CASE WHEN c.relpages > 0 AND e.expected IS NOT NULL
                    THEN round(100.0 * greatest(c.relpages - e.expected, 0) / c.relpages, 1)
               END AS bloat_pct,
               CASE WHEN e.expected IS NOT NULL
                    THEN (greatest(c.relpages - e.expected, 0)
                          * current_setting('block_size')::bigint)::bigint
               END AS bloat_bytes
        FROM (
            SELECT CASE WHEN c.reltuples >= 0 AND st.datawidth IS NOT NULL
                        THEN ceil((c.reltuples::numeric * (24 + st.datawidth::numeric))
                                  / (current_setting('block_size')::numeric - 24))
                   END AS expected
            FROM (SELECT sum((1 - null_frac) * avg_width) AS datawidth
                    FROM pg_stats
                   WHERE schemaname = n.nspname AND tablename = c.relname) st
        ) e
    ) b ON true
    WHERE n.nspname = %s
    ORDER BY s.n_dead_tup DESC, s.n_live_tup DESC
"""


def _table_health_sql(major: int) -> str:
    # Deliberately simple: n_ins_since_vacuum is PG13+ only (README's supported
    # floor is PG12). On PG12 select a literal NULL under the same alias so
    # format_table_health's existing "is not None" check quietly degrades the
    # never-vacuumed gate to n_dead_tup alone, no downstream branching needed.
    col = "s.n_ins_since_vacuum" if major >= 13 else "NULL::bigint"
    return _TABLE_HEALTH_SQL.format(ins_since_vacuum=f"{col} AS n_ins_since_vacuum")

# Deliberately simple: 1000 inserts-since-vacuum is a fixed ceiling, not scaled
# to table size. Revisit with a ratio (e.g. vs. n_live_tup) if small high-churn
# tables start slipping through or large tables start false-flagging.
_INS_SINCE_VACUUM_THRESHOLD = 1000

XID_WARN_FRACTION = 0.75           # of autovacuum_freeze_max_age
XID_WRAPAROUND = 2**31             # the ceiling age(datfrozenxid) must never reach
BLOAT_WARN_PCT = 20.0
BLOAT_WARN_BYTES = 10 * 1024 * 1024

_SPACE_USAGE_SQL = """
    SELECT n.nspname AS schema, c.relname AS table,
           pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
           pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
           pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
           pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
    ORDER BY total_bytes DESC
"""

_DATABASE_SIZE_SQL = "SELECT pg_size_pretty(pg_database_size(current_database()))"

# Sequences in the scoped schema with how much of their range is spent. The
# owning table comes from pg_depend (deptype 'a' = OWNED BY / identity, 'i' =
# internal). A sequence never nextval'd has NULL last_value and is skipped.
_SEQUENCES_SQL = """
    SELECT s.sequencename AS sequence, own.relname AS owned_by,
           s.last_value, s.max_value,
           round(100.0 * (s.last_value::numeric - s.min_value)
                 / greatest(s.max_value::numeric - s.min_value, 1), 1) AS pct_used
    FROM pg_sequences s
    JOIN pg_namespace n ON n.nspname = s.schemaname
    JOIN pg_class sc ON sc.relname = s.sequencename AND sc.relnamespace = n.oid
    LEFT JOIN pg_depend dp ON dp.objid = sc.oid
                          AND dp.classid = 'pg_class'::regclass
                          AND dp.refclassid = 'pg_class'::regclass
                          AND dp.deptype IN ('a', 'i')
    LEFT JOIN pg_class own ON own.oid = dp.refobjid
    WHERE s.schemaname = %s AND s.last_value IS NOT NULL
    ORDER BY pct_used DESC, s.sequencename
"""

SEQ_LIST_PCT = 50.0
SEQ_WARN_PCT = 80.0


def format_table_health(result: QueryResult, scope: str, others: list[str],
                        sequences: QueryResult | None = None) -> Response:
    idx = {name: i for i, name in enumerate(result.columns)}
    rows: list[tuple[str, ...]] = []
    attention: list[tuple[str, str]] = []
    db_xid_age: int | None = None
    for row in result.rows:
        table = row[idx["table"]]
        n_live = cast("int", row[idx["n_live_tup"]])
        n_dead = row[idx["n_dead_tup"]]
        dead_pct = row[idx["dead_pct"]]
        last_auto = row[idx["last_autovacuum"]]
        n_ins_since_vacuum = row[idx["n_ins_since_vacuum"]]
        raw_xid_age = row[idx["xid_age"]]
        xid_age = int(cast("int", raw_xid_age)) if raw_xid_age is not None else None
        freeze_max_age = int(cast("int", row[idx["freeze_max_age"]]))
        db_xid_age = int(cast("int", row[idx["db_xid_age"]]))
        bloat_pct = row[idx["bloat_pct"]]
        bloat_bytes = row[idx["bloat_bytes"]]
        # NULL last_autovacuum means the table has never been autovacuumed --
        # must read as "never", not as the literal NULL sentinel.
        last_str = iso(last_auto) if isinstance(last_auto, datetime) else "never"
        bloat_str = f"{float(cast('float | str', bloat_pct)):.1f}%" if bloat_pct is not None else "n/a"
        xid_age_str = fmt_count(xid_age) if xid_age is not None else "-"
        rows.append((f"`{table}`", fmt_estimate(n_live), fmt_count(cast("int", n_dead)),
                     f"{dead_pct}%", bloat_str, xid_age_str, last_str))
        if dead_pct is not None and float(cast("float | str", dead_pct)) > 5:
            attention.append((f"`{table}`", f"{dead_pct}% dead tuples"))
        elif last_auto is None and (
            (n_dead is not None and cast("int", n_dead) > 0)
            or (n_ins_since_vacuum is not None
                and cast("int", n_ins_since_vacuum) > _INS_SINCE_VACUUM_THRESHOLD)
        ):
            # A tiny, quiet, never-autovacuumed table (no dead tuples, few
            # inserts) is not a health problem -- only flag "never vacuumed"
            # once there's actual churn autovacuum should have caught.
            attention.append((f"`{table}`", "never vacuumed"))
        if xid_age is not None and freeze_max_age > 0 and xid_age > XID_WARN_FRACTION * freeze_max_age:
            pct = round(100 * xid_age / freeze_max_age)
            attention.append((f"`{table}`",
                              f"xid age {fmt_count(xid_age)} ({pct}% of autovacuum_freeze_max_age)"))
        if (bloat_pct is not None and bloat_bytes is not None
                and float(cast("float | str", bloat_pct)) > BLOAT_WARN_PCT
                and int(cast("int", bloat_bytes)) > BLOAT_WARN_BYTES):
            attention.append((f"`{table}`", (
                f"{float(cast('float | str', bloat_pct)):.1f}% estimated bloat "
                f"({fmt_size(int(cast('int', bloat_bytes)))})")))

    blocks: list[Block] = [
        Table(("table", "rows (estimate)", "dead", "dead %", "bloat est.", "xid age",
               "last autovacuum"), tuple(rows))]
    if db_xid_age is not None:
        blocks.append(Caveat(
            f"database xid age {fmt_count(db_xid_age)} "
            f"({round(100 * db_xid_age / XID_WRAPAROUND)}% of the 2^31 wraparound ceiling)."))
    if others:
        blocks.append(Caveat(f"Scope is {scope}; also exposed: {', '.join(others)}."))

    seq_rows: list[tuple[str, ...]] = []
    if sequences is not None and sequences.rows:
        s_idx = {name: i for i, name in enumerate(sequences.columns)}
        for srow in sequences.rows:
            seq_pct = float(cast("float | str", srow[s_idx["pct_used"]]))
            if seq_pct <= SEQ_LIST_PCT:
                continue
            name = str(srow[s_idx["sequence"]])
            owner = srow[s_idx["owned_by"]]
            seq_rows.append((f"`{name}`", f"`{owner}`" if owner else "-",
                             fmt_count(int(cast("int", srow[s_idx["last_value"]]))),
                             fmt_count(int(cast("int", srow[s_idx["max_value"]]))),
                             f"{seq_pct:.1f}%"))
            if seq_pct > SEQ_WARN_PCT:
                attention.append((f"`{owner}`" if owner else "-",
                                  f"sequence {name} {seq_pct:.1f}% used"))

    sections = [Section("tables", tuple(blocks))]
    if seq_rows:
        sections.append(Section("sequences near limit", (
            Table(("sequence", "owned by", "last value", "max", "% used"), tuple(seq_rows)),)))
    if attention:
        sections.append(Section("needs attention", (Table(("table", "why"), tuple(attention)),)))
    if len(sections) == 1:
        sections = [Section(None, tuple(blocks))]
    table_attention = [t for t, _ in attention if t != "-"]
    worst = (table_attention[0].strip("`") if table_attention
             else (str(result.rows[0][idx["table"]]) if result.rows else None))
    return Response(
        SERVER, "get_table_health", scope, PLANE_MIXED, tuple(sections),
        tally=(nof(len(rows), "table"), f"{len({t for t, _ in attention})} need attention"),
        next=((Call("get_table_stats", {"table": f"{scope}.{worst}"}),
               Call("get_index_health", {"schema": scope})) if worst else ()),
    )


# The exact base-type spellings format_type emits for the types that have a
# min/max aggregate. Exact match, not prefix: `daterange`, `datemultirange` or
# a user type named `timeslot` share a prefix with these but have no min()/max().
_STATS_ELIGIBLE_TYPES = frozenset({
    "smallint", "integer", "bigint", "numeric", "real", "double precision",
    "date", "timestamp without time zone", "timestamp with time zone",
    "time without time zone", "time with time zone", "interval", "money",
})


def _is_stats_eligible(data_type: str) -> bool:
    """True for the numeric/date/time/money types min/max is meaningful for.
    A `(p, s)` modifier is stripped first, so `numeric(12,4)` and
    `timestamp(3) with time zone` qualify; `interval day to second` does via
    the `interval ` prefix (the only eligible type with trailing words).
    Arrays (`integer[]`) never do -- min/max over an array isn't a scalar
    comparison."""
    dt = re.sub(r"\s+", " ", re.sub(r"\([^)]*\)", "", data_type.lower())).strip()
    return dt in _STATS_ELIGIBLE_TYPES or dt.startswith("interval ")


def _minmax_select(c: Column) -> str:
    ident = _quote_ident(c.name)
    if _is_stats_eligible(c.data_type):
        return f"min({ident})::text AS min_text, max({ident})::text AS max_text"
    # Aliased even here: the batch query's outer SELECT reads the UNION ALL's
    # columns by these names, and an unaliased `NULL::text` is named `text`.
    return "NULL::text AS min_text, NULL::text AS max_text"


def format_table_stats(
    qualified_name: str, total_rows: int,
    columns: list[tuple[str, str, int | None, int | None, str, str]],
) -> Response:
    """`columns` is (name, data_type, null_count, distinct_count, min_text,
    max_text) per column, in table-column order. `total_rows` drives the null
    percentage; a 0-row table lists every column with zero stats rather than
    dividing by zero. A column whose stats couldn't be computed at all (see
    `_column_stats`'s per-column fallback) carries `None` for both counts and
    renders as N/A. min/max are blank for columns that aren't numeric,
    date/time, or money."""
    rows = tuple(
        (f"`{name}`", f"`{dtype}`", "n/a" if nc is None else fmt_count(nc),
         "n/a" if nc is None else (f"{round(nc * 100 / total_rows, 1)}%" if total_rows else "0%"),
         "n/a" if dc is None else fmt_count(dc), mn, mx)
        for name, dtype, nc, dc, mn, mx in columns)
    return Response(
        SERVER, "get_table_stats", qualified_name, PLANE_STATS,
        (Section(None, (Table(("column", "type", "nulls", "null %", "distinct", "min", "max"),
                              rows),
                        Caveat("Counts are exact; this tool scans the table."))),),
        tally=(nof(total_rows, "row"), nof(len(rows), "column")),
        next=(Call("get_sample_data", {"table": qualified_name, "limit": 5}),
              Call("get_index_health", {"schema": qualified_name.split(".")[0]})),
    )


async def _column_stats(
    db: Db, ident: str, columns: list[Column],
) -> list[tuple[str, str, int | None, int | None, str, str]]:
    """Null/distinct/min/max stats per column. Tries one batched UNION ALL
    query first; a single incompatible column (COUNT(DISTINCT json_col) has no
    equality operator, same failure mode as xml) fails that whole query, so
    on any error this falls back to one query per column -- isolating the
    failure to just the columns that actually can't support it, which are
    reported as N/A rather than sinking the entire tool. min/max are computed
    only for numeric, date/time, and money columns (`_is_stats_eligible`);
    every other column selects `NULL::text, NULL::text` so the row shape stays
    uniform. Ported from TS/tools/get-table-stats.ts's
    batchColumnStats/perColumnStats two-tier fallback."""
    try:
        selects = "\nUNION ALL\n".join(
            f"SELECT {i} AS ord, count(*) - count({_quote_ident(c.name)}) AS null_count, "
            f"count(DISTINCT {_quote_ident(c.name)}) AS distinct_count, "
            f"{_minmax_select(c)} FROM {ident}"
            for i, c in enumerate(columns)
        )
        result = await db.run_system(
            f"SELECT ord, null_count, distinct_count, min_text, max_text "
            f"FROM ({selects}) s ORDER BY ord")
        return [(c.name, c.data_type, int(cast("int", result.rows[i][1])),
                 int(cast("int", result.rows[i][2])),
                 cast("str | None", result.rows[i][3]) or "",
                 cast("str | None", result.rows[i][4]) or "")
                for i, c in enumerate(columns)]
    except Exception:  # noqa: BLE001 -- batch query intentionally has no fixed
        # exception type to catch (driver errors vary by incompatible type);
        # per-column retry below isolates which column(s) actually fail.
        stats: list[tuple[str, str, int | None, int | None, str, str]] = []
        for c in columns:
            try:
                r = await db.run_system(
                    f"SELECT count(*) - count({_quote_ident(c.name)}), "
                    f"count(DISTINCT {_quote_ident(c.name)}), "
                    f"{_minmax_select(c)} FROM {ident}")
                stats.append((c.name, c.data_type, int(cast("int", r.rows[0][0])),
                             int(cast("int", r.rows[0][1])),
                             cast("str | None", r.rows[0][2]) or "",
                             cast("str | None", r.rows[0][3]) or ""))
            except Exception:  # noqa: BLE001 -- this column's type doesn't support
                # COUNT(DISTINCT) (e.g. json/xml); report N/A, don't fail the tool.
                stats.append((c.name, c.data_type, None, None, "", ""))
        return stats


def format_space_usage(
    result: QueryResult, database_size: str, scope: str, others: list[str],
) -> Response:
    idx = {name: i for i, name in enumerate(result.columns)}
    rows = tuple(
        (f"`{r[idx['schema']]}`", f"`{r[idx['table']]}`", str(r[idx["total_size"]]),
         str(r[idx["table_size"]]), str(r[idx["index_size"]]))
        for r in result.rows)
    blocks: list[Block] = [Table(("schema", "table", "total", "table", "indexes"), rows)]
    if others:
        blocks.append(Caveat(f"Scope is {scope}; also exposed: {', '.join(others)}."))
    return Response(
        SERVER, "get_space_usage", scope, PLANE_STATS, (Section(None, tuple(blocks)),),
        tally=(nof(len(rows), "table"), f"database {database_size}"),
        next=(Call("get_table_health", {"schema": scope}), Call("get_index_health", {"schema": scope})),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Table Health"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_table_health(schema: str | None = None) -> str:
        """Report vacuum/bloat health for every table in a schema: live/dead
        tuple counts (live count is pg_class.reltuples, refreshed by ANALYZE --
        the same source as list_tables; dead-tuple figures come from the stats
        collector), dead-tuple percentage, and when autovacuum last ran (a NULL reads as
        "never"), the table's transaction-ID age (age(relfrozenxid)), and a
        catalog bloat estimate (relpages vs. the pages reltuples and pg_stats
        widths predict; n/a until analyzed). Flags tables over 5% dead tuples, or never vacuumed with
        actual churn (dead tuples present, or over 1000 inserts since the
        last vacuum), under "needs attention" -- a tiny quiet table that's
        simply never needed a vacuum isn't flagged. Also flagged: xid age
        above 75% of autovacuum_freeze_max_age (wraparound risk), and
        estimated bloat above 20% when the wasted space exceeds 10 MB. A
        caveat states the database-wide xid age against the 2^31 wraparound
        ceiling. A "sequences near limit" section lists sequences in the
        schema past 50% of their range (owning table via pg_depend); past 80%
        they are flagged under needs attention. Defaults to the server's
        default schema (a caveat names the others); pass `schema` to
        scope to another exposed schema."""
        assert caps is not None  # register_all always constructs a real Capabilities
        resolved = db.resolve_schema(schema)
        major, _minor = await caps.server_version()
        result = await db.run_system(_table_health_sql(major), (resolved,))
        sequences = await db.run_system(_SEQUENCES_SQL, (resolved,))
        others = [s for s in settings.exposed_schemas if s != resolved] if schema is None else []
        return respond(format_table_health(result, resolved, others, sequences=sequences))

    @mcp.tool(annotations=read_only("Get Table Stats"), meta=visibility(*MODEL_AND_APP))
    @tool_errors
    async def get_table_stats(table: str, schema: str | None = None) -> str:
        """Report row count and per-column null count/percentage/distinct
        count, plus min/max for numeric, date/time and money columns.
        Defaults to all exposed schemas; `schema` narrows the table lookup.
        Batched into one UNION ALL query where possible. A table with 0 rows lists every column with
        zero stats rather than dividing by zero. If the batch query fails
        (e.g. a json/xml column COUNT(DISTINCT) can't support), falls back to
        one query per column, reporting N/A only for the column(s) that
        actually can't be computed."""
        t = await resolve_table(db, intro, table, schema)
        ident = f"{_quote_ident(t.schema)}.{_quote_ident(t.name)}"

        count_result = await db.run_system(f"SELECT count(*) FROM {ident}")
        total_rows = int(cast("int", count_result.rows[0][0]))

        columns: list[tuple[str, str, int | None, int | None, str, str]]
        if total_rows == 0 or not t.columns:
            columns = [(c.name, c.data_type, 0, 0, "", "") for c in t.columns]
        else:
            columns = await _column_stats(db, ident, t.columns)
        return respond(format_table_stats(t.qualified, total_rows, columns))

    @mcp.tool(annotations=read_only("Get Space Usage"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_space_usage(schema: str | None = None) -> str:
        """Report space usage for a schema: total/table/index size per table
        (via pg_total_relation_size/pg_relation_size/pg_indexes_size,
        formatted with pg_size_pretty) plus the overall database size.
        Defaults to the server's default schema (a caveat names the
        others); pass `schema` to scope to another exposed schema."""
        resolved = db.resolve_schema(schema)
        result = await db.run_system(_SPACE_USAGE_SQL, (resolved,))
        db_size_result = await db.run_system(_DATABASE_SIZE_SQL)
        database_size = str(db_size_result.rows[0][0])
        others = [s for s in settings.exposed_schemas if s != resolved] if schema is None else []
        return respond(format_space_usage(result, database_size, resolved, others))
