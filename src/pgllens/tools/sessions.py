"""Live-session diagnostics: who's connected, who's blocking whom, what the
engine is waiting on. New SQL (not lifted from TS -- these tools exist in
MsSQLLens and not in PgLLens), written against pg_stat_activity. Point-in-time
reads -- never cached."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult, table_from
from pgllens.llens_style import Call, Caveat, Response, Section, nof
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "stats"

# pid = pg_backend_pid() excludes this tool's own connection; idle sessions
# are excluded unless include_idle is true, and background workers
# (autovacuum, walsender, etc.) are excluded unless include_background is
# true -- both bound as real parameters rather than branching the SQL text.
_ACTIVE_SESSIONS_SQL = """
    SELECT pid, usename, application_name, client_addr, state, backend_type,
           wait_event_type, wait_event, now() - query_start AS duration, query
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
      AND application_name IS DISTINCT FROM 'pgllens'
      AND (%s OR state IS DISTINCT FROM 'idle')
      AND (%s OR backend_type = 'client backend')
    ORDER BY duration DESC NULLS LAST
"""

_BLOCKING_SQL = """
    SELECT a.pid AS blocked_pid, a.query AS blocked_query,
           b.pid AS blocker_pid, bl.query AS blocker_query,
           now() - a.query_start AS waiting_duration
    FROM pg_stat_activity a
    JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS b(pid) ON true
    JOIN pg_stat_activity bl ON bl.pid = b.pid
    ORDER BY waiting_duration DESC
"""

# A sample of right now, not a cumulative counter: unlike SQL Server's
# sys.dm_os_wait_stats (which accumulates since the last service restart),
# pg_stat_activity only shows what each backend is waiting on at this instant.
# Tracking wait history over time needs the pg_wait_sampling extension, which
# is not assumed to be installed here.
_WAIT_STATS_SQL = """
    SELECT wait_event_type, wait_event, count(*) AS sessions
    FROM pg_stat_activity
    WHERE wait_event_type IS NOT NULL
      AND (%s OR backend_type = 'client backend')
    GROUP BY wait_event_type, wait_event
    ORDER BY sessions DESC
"""


def format_active_sessions(result: QueryResult, include_idle: bool) -> Response:
    table = table_from(result, columns=[
        "pid", "user", "application", "client", "state", "backend", "wait type", "wait event",
        "duration", "query"])
    n = len(result.rows)
    return Response(
        SERVER, "get_active_sessions", None, PLANE,
        (Section(None, (table,)),),
        tally=(nof(n, "session"),
               "idle included" if include_idle else "idle excluded"),
        next=(Call("get_blocking"), Call("get_wait_stats")),
    )


def format_blocking(result: QueryResult) -> Response:
    table = table_from(result, columns=[
        "blocked pid", "blocked query", "blocker pid", "blocker query", "waiting"])
    n = len(result.rows)
    return Response(
        SERVER, "get_blocking", None, PLANE,
        (Section(None, (table,)),),
        tally=(nof(n, "blocked session"),),
        next=(Call("get_active_sessions", {"include_idle": True}),) if n else (),
    )


def format_wait_stats(result: QueryResult) -> Response:
    table = table_from(result, columns=["wait type", "wait event", "sessions"])
    n = len(result.rows)
    return Response(
        SERVER, "get_wait_stats", None, PLANE,
        (Section(None, (
            table,
            Caveat("Point-in-time sample of pg_stat_activity, not a cumulative counter."),
        )),),
        tally=(nof(n, "wait event"),),
        next=(Call("get_active_sessions"), Call("get_blocking")),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Active Sessions"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_active_sessions(include_idle: bool = False,
                                   include_background: bool = False) -> str:
        """List currently connected sessions from pg_stat_activity: pid, user,
        application, client address, state, backend type, wait info,
        running-query duration, and the query text. Always excludes this
        tool's own connection (pid = pg_backend_pid()) and the server's own
        pooled connections (application_name = 'pgllens'); excludes idle
        sessions unless `include_idle` is true; excludes non-client backends
        (autovacuum, walsender, background workers, etc.) unless
        `include_background` is true."""
        result = await db.run_system(_ACTIVE_SESSIONS_SQL, (include_idle, include_background))
        return respond(format_active_sessions(result, include_idle))

    @mcp.tool(annotations=read_only("Get Blocking"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_blocking() -> str:
        """Show sessions currently blocked on a lock, paired with the session(s)
        blocking them (via pg_blocking_pids), each blocked/blocker query text,
        and how long the blocked session has been waiting. When nothing is
        blocked, the tally reads `0 blocked sessions`."""
        result = await db.run_system(_BLOCKING_SQL)
        return respond(format_blocking(result))

    @mcp.tool(annotations=read_only("Get Wait Stats"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_wait_stats(include_background: bool = False) -> str:
        """Report what active sessions are waiting on right now, grouped by
        wait_event_type/wait_event with a count. Excludes non-client backends
        (autovacuum, walsender, background workers, etc.) unless
        `include_background` is true. This is a point-in-time
        sample of pg_stat_activity, not a cumulative counter like SQL Server's
        dm_os_wait_stats -- history across time would need the
        pg_wait_sampling extension, which is not assumed to be installed."""
        result = await db.run_system(_WAIT_STATS_SQL, (include_background,))
        return respond(format_wait_stats(result))
