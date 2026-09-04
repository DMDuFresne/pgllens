"""TimescaleDB hypertable listing, lifted from TS/tools/list-hypertables.ts:
hypertables + dimensions, policies/jobs, continuous aggregates, and chunk
statistics (with the chunk_size-less fallback for older TimescaleDB builds).

Gated on the `timescaledb` extension via `requires_extension`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.capability import requires_extension
from pgllens.database.format import QueryResult
from pgllens.llens_style import (
    Block,
    Bullet,
    Bullets,
    Call,
    Code,
    Response,
    Section,
    Table,
    nof,
    size,
)
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "catalog"

_HYPERTABLES_SQL = """
    SELECT h.hypertable_schema, h.hypertable_name, h.compression_enabled,
           h.num_dimensions, d.column_name AS time_column,
           d.time_interval::text AS time_interval
    FROM timescaledb_information.hypertables h
    LEFT JOIN timescaledb_information.dimensions d
      ON h.hypertable_schema = d.hypertable_schema
     AND h.hypertable_name = d.hypertable_name
     AND d.dimension_number = 1
    ORDER BY h.hypertable_schema, h.hypertable_name
"""

_JOBS_SQL = """
    SELECT j.hypertable_schema, j.hypertable_name, j.job_id,
           j.application_name AS job_type, j.schedule_interval::text AS schedule,
           j.config::text AS config, j.next_start::text AS next_start
    FROM timescaledb_information.jobs j
    ORDER BY j.hypertable_schema, j.hypertable_name, j.application_name
"""

_CONTINUOUS_AGGREGATES_SQL = """
    SELECT ca.materialization_hypertable_schema, ca.materialization_hypertable_name,
           ca.view_schema, ca.view_name, ca.view_definition
    FROM timescaledb_information.continuous_aggregates ca
    ORDER BY ca.view_schema, ca.view_name
"""

_CHUNK_STATS_SQL = """
    SELECT ch.hypertable_schema, ch.hypertable_name, count(*) AS chunk_count,
           min(ch.range_start)::text AS range_start, max(ch.range_end)::text AS range_end,
           sum(ch.chunk_size)::bigint AS total_bytes,
           sum(ch.compressed_chunk_size)::bigint AS compressed_bytes
    FROM (
        SELECT ch.hypertable_schema, ch.hypertable_name, ch.range_start, ch.range_end,
               COALESCE(chs.total_bytes, 0) AS chunk_size,
               COALESCE(chs.compressed_total_bytes, 0) AS compressed_chunk_size
        FROM timescaledb_information.chunks ch
        LEFT JOIN timescaledb_information.chunk_size chs
          ON ch.chunk_schema = chs.chunk_schema AND ch.chunk_name = chs.chunk_name
    ) ch
    GROUP BY ch.hypertable_schema, ch.hypertable_name
    ORDER BY ch.hypertable_schema, ch.hypertable_name
"""

# Fallback when chunk_size isn't available (older TimescaleDB): counts and
# date range only, no byte totals.
_CHUNK_STATS_SIMPLE_SQL = """
    SELECT ch.hypertable_schema, ch.hypertable_name, count(*) AS chunk_count,
           min(ch.range_start)::text AS range_start, max(ch.range_end)::text AS range_end
    FROM timescaledb_information.chunks ch
    GROUP BY ch.hypertable_schema, ch.hypertable_name
    ORDER BY ch.hypertable_schema, ch.hypertable_name
"""


def format_hypertables(result: QueryResult) -> Table:
    rows = tuple((f"`{s}.{n}`", f"`{tc}`" if tc else "", str(ti) if ti else "",
                 "yes" if comp else "no") for s, n, comp, _d, tc, ti in result.rows)
    return Table(("hypertable", "time column", "chunk interval", "compression"), rows)


def format_jobs(result: QueryResult) -> Table:
    rows = []
    for _s, name, _id, job_type, schedule, config, next_start in result.rows:
        cfg = str(config) if config else ""
        if len(cfg) > 60:
            cfg = cfg[:57] + "…"
        rows.append((f"`{name}`", str(job_type), str(schedule) if schedule else "",
                     f"`{cfg}`" if cfg else "", str(next_start)[:19] if next_start else ""))
    return Table(("hypertable", "job type", "schedule", "config", "next run"), tuple(rows))


def format_continuous_aggregates(result: QueryResult) -> tuple[Block, ...]:
    blocks: list[Block] = []
    for ms, mn, vs, vn, definition in result.rows:
        blocks.append(Bullets((Bullet("view", f"{vs}.{vn}"), Bullet("materialization", f"{ms}.{mn}"))))
        if definition:
            blocks.append(Code("sql", str(definition)))
    return tuple(blocks)


def format_chunk_stats(result: QueryResult) -> Table:
    idx = {name: i for i, name in enumerate(result.columns)}
    has_sizes = "total_bytes" in idx
    headers = ["hypertable", "chunks", "range"] + (["total", "compressed", "ratio"]
                                                    if has_sizes else [])
    rows = []
    for row in result.rows:
        rs, re_ = row[idx["range_start"]], row[idx["range_end"]]
        rng = f"{str(rs)[:10]} → {str(re_)[:10]}" if rs and re_ else ""
        cells = [f"`{row[idx['hypertable_name']]}`", str(row[idx["chunk_count"]]), rng]
        if has_sizes:
            total = int(cast("int | None", row[idx["total_bytes"]]) or 0)
            comp = int(cast("int | None", row[idx["compressed_bytes"]]) or 0)
            cells += [size(total), size(comp) if comp else "",
                      f"{total / comp:.1f}x" if total and comp else ""]
        rows.append(tuple(cells))
    return Table(tuple(headers), tuple(rows))


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    assert caps is not None  # register_all always constructs a real Capabilities
    @mcp.tool(annotations=read_only("List Hypertables"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    @requires_extension("timescaledb", caps)
    async def list_hypertables() -> str:
        """List TimescaleDB hypertables (time column, chunk interval,
        compression), their policies/jobs, continuous aggregates, and chunk
        statistics. Needs the `timescaledb` extension."""
        result = await db.run_system(_HYPERTABLES_SQL)

        jobs = 0
        caggs = 0
        extra: list[Section] = []

        try:
            jobs_result = await db.run_system(_JOBS_SQL)
            if jobs_result.rows:
                extra.append(Section("jobs", (format_jobs(jobs_result),)))
                jobs = len(jobs_result.rows)
        except Exception:  # noqa: BLE001, S110 -- jobs view may not exist in older versions
            pass

        try:
            caggs_result = await db.run_system(_CONTINUOUS_AGGREGATES_SQL)
            cagg_blocks = format_continuous_aggregates(caggs_result)
            if cagg_blocks:
                extra.append(Section("continuous aggregates", cagg_blocks))
                caggs = len(caggs_result.rows)
        except Exception:  # noqa: BLE001, S110 -- continuous_aggregates view may differ across versions
            pass

        try:
            chunks = await db.run_system(_CHUNK_STATS_SQL)
        except Exception:  # noqa: BLE001 -- chunk_size view not available in all versions
            try:
                chunks = await db.run_system(_CHUNK_STATS_SIMPLE_SQL)
            except Exception:  # noqa: BLE001 -- silently skip chunk stats if unavailable
                chunks = None
        if chunks is not None and chunks.rows:
            extra.append(Section("chunks", (format_chunk_stats(chunks),)))

        if extra:
            sections: tuple[Section, ...] = (
                Section("hypertables", (format_hypertables(result),)), *extra)
        else:
            sections = (Section(None, (format_hypertables(result),)),)

        first_schema = result.rows[0][0] if result.rows else None
        return respond(Response(
            SERVER, "list_hypertables", None, PLANE, sections,
            tally=(nof(len(result.rows), "hypertable"), nof(jobs, "job"),
                   nof(caggs, "continuous aggregate")),
            next=(Call("get_table_health", {"schema": first_schema}),) if first_schema else (),
        ))
