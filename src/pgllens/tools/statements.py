"""Query Store equivalent: pg_stat_statements top consumers. New SQL (not
lifted from TS -- MsSQLLens has Query Store, PgLLens does not). Gated on the
`pg_stat_statements` extension via `requires_extension`.

`order_by` is the one place in the whole port where a free-text argument is
destined for an ORDER BY clause. An ORDER BY column cannot be a bound
parameter, so ORDER_COLUMNS is the whole defence: the argument selects a key
into a fixed dict, it is never concatenated into SQL text."""

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
from pgllens.database.capability import requires_extension
from pgllens.database.format import QueryResult
from pgllens.llens_style import (
    Block,
    Call,
    Caveat,
    ErrorCode,
    Response,
    Section,
    Table,
    hint_for,
    iso,
    nof,
)
from pgllens.llens_style import count as fmt_count
from pgllens.tools._util import SERVER, ToolError, _quote_ident, check_range, respond, tool_errors

PLANE = "stats"

EXT = "pg_stat_statements"

ORDER_COLUMNS: dict[str, str] = {
    "total_time": "total_exec_time DESC",
    "mean_time": "mean_exec_time DESC",
    "calls": "calls DESC",
    "rows": "rows DESC",
}


def order_clause(order_by: str) -> str:
    """Map an argument to a fixed ORDER BY fragment.

    An ORDER BY column cannot be a bound parameter, so this allowlist is the
    whole defence: the argument selects a key, it is never concatenated.
    """
    try:
        return ORDER_COLUMNS[order_by]
    except KeyError:
        raise ValueError(
            f"order_by must be one of: {', '.join(sorted(ORDER_COLUMNS))} (got {order_by!r})"
        ) from None


# {schema} is the extension's own schema (Capabilities.extension_schema), quoted:
# the pgllens role's search_path is deliberately just the exposed schema plus
# pg_catalog, so an unqualified `pg_stat_statements` would not resolve.
_QUERY_STORE_SQL = """
    SELECT queryid, calls, total_exec_time, mean_exec_time, rows,
           shared_blks_hit, shared_blks_read, left(query, 121) AS query
    FROM {schema}.pg_stat_statements
    {where}
    ORDER BY {order}
    LIMIT %s
"""

_STATS_RESET_SQL = "SELECT stats_reset FROM {schema}.pg_stat_statements_info"

# Gated on the EXTENSION version, not the server's: pg_stat_statements_info
# arrived in 1.9 (shipped with PG14) and stats_since in 1.11 (PG17), but a
# pg_upgrade'd cluster keeps the old extversion until ALTER EXTENSION UPDATE --
# a PG16 server with extversion 1.8 has no _info view, and querying it would
# turn the whole tool into a database error.
_INFO_VERSION = (1, 9)
_SINCE_VERSION = (1, 11)


async def _stats_reset_value(db: Db, schema: str, version: tuple[int, ...]) -> str:
    """`pg_stat_statements_info.stats_reset` needs extension 1.9+ (PG14). A NULL
    means the extension has never been reset since it was installed. `schema`
    is the extension's schema, already quoted."""
    if version < _INFO_VERSION:
        return "unknown (pg_stat_statements_info needs pg_stat_statements 1.9+, PostgreSQL 14+)"
    result = await db.run_system(_STATS_RESET_SQL.format(schema=schema))
    if not result.rows:
        # pg_stat_statements_info can come back empty (denied read, or a build
        # without the view) -- report unknown rather than IndexError.
        return "unknown"
    stats_reset = result.rows[0][0]
    if stats_reset is None:
        return "never reset (since extension install)"
    if isinstance(stats_reset, datetime):
        return iso(stats_reset)
    return str(stats_reset)


def format_query_store(
    result: QueryResult, since_reset: str, filtered_since: str | None,
    since_unsupported: bool, order_by: str,
) -> Response:
    rows = []
    for queryid, calls, total_time, mean_time, n_rows, blks_hit, blks_read, query in result.rows:
        # Deliberately simple: the 121st char signals truncation; render the first
        # 120 + "…" if cut.
        q = cast("str", query or "")
        rows.append((f"`{queryid}`", fmt_count(cast("int", calls)),
                     f"{cast('float', total_time):.1f}", f"{cast('float', mean_time):.1f}",
                     fmt_count(cast("int", n_rows)), fmt_count(cast("int", blks_hit)),
                     fmt_count(cast("int", blks_read)),
                     f"`{q[:120] + '…' if len(q) > 120 else q}`"))
    blocks: list[Block] = [Table(
        ("queryid", "calls", "total ms", "mean ms", "rows", "blks hit", "blks read", "query"),
        tuple(rows))]
    blocks.append(Caveat(f"Statistics since reset: {since_reset}."))
    if since_unsupported:
        blocks.append(Caveat(
            "since= needs pg_stat_statements 1.11+ (PostgreSQL 17); showing all entries."))
    tally = [nof(len(rows), "statement"), f"ordered by {order_by}"]
    if filtered_since:
        tally.append(f"since {filtered_since}")
    top_q = cast("str", result.rows[0][7] or "") if result.rows else ""
    return Response(
        SERVER, "get_query_store", None, PLANE, (Section(None, tuple(blocks)),),
        tally=tuple(tally),
        next=(Call("explain_query", {"sql": top_q}),) if top_q and len(top_q) <= 120 else (),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    assert caps is not None  # register_all always constructs a real Capabilities
    @mcp.tool(annotations=read_only("Get Query Store"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    @requires_extension(EXT, caps)
    async def get_query_store(
        order_by: str = "total_time", limit: int = 20, since: str | None = None
    ) -> str:
        """Report the top statements tracked by pg_stat_statements: call count,
        total/mean execution time, rows produced, and shared-buffer hit/read
        counts. Stats are cumulative since the extension was last reset -- a
        caveat names when that was (`pg_stat_statements_info.stats_reset`,
        extension 1.9+/PG14+; NULL there means never reset since install). A seed/load script
        run once shows up as a high `calls` entry with an old `stats_since`,
        not as ongoing traffic. `order_by` is one of "total_time", "mean_time",
        "calls", "rows" (default "total_time"); `limit` caps rows returned
        (1-100). `since` is an ISO-8601 timestamp filtering to statements first
        seen at or after it (`stats_since`, extension 1.11+/PG17+ only); on
        older versions it is ignored and a caveat explains why. Needs the `pg_stat_statements`
        extension."""
        check_range("limit", limit, 1, 100)
        try:
            order = order_clause(order_by)
        except ValueError as e:
            raise ToolError(
                ErrorCode.FORMAT_UNKNOWN, str(e),
                hint_for(ErrorCode.FORMAT_UNKNOWN, valid=", ".join(sorted(ORDER_COLUMNS))),
            ) from None
        if since is not None:
            try:
                datetime.fromisoformat(since)
            except ValueError:
                raise ToolError(
                    ErrorCode.ARG_OUT_OF_RANGE,
                    f"`since` is not an ISO-8601 timestamp: {since!r}.",
                    "Pass e.g. `2026-01-01T00:00:00Z`.",
                ) from None
        schema = _quote_ident(await caps.extension_schema(EXT) or "public")
        version = await caps.extension_version(EXT)
        since_reset = await _stats_reset_value(db, schema, version)
        since_unsupported = False
        filtered_since: str | None = None
        if since is not None and version >= _SINCE_VERSION:
            sql = _QUERY_STORE_SQL.format(schema=schema, order=order,
                                          where="WHERE stats_since >= %s::timestamptz")
            params: tuple[object, ...] = (since, limit)
            filtered_since = since
        else:
            sql = _QUERY_STORE_SQL.format(schema=schema, order=order, where="")
            params = (limit,)
            if since is not None:
                since_unsupported = True
        result = await db.run_system(sql, params)
        return respond(format_query_store(result, since_reset, filtered_since,
                                          since_unsupported, order_by))
