"""server_info: version, uptime, connection counts, and notable settings.

Net-new (no TS equivalent). Modeled on MsSQLLens's server_info.py shape:
identity (version()), runtime (pg_stat_database for connections/uptime), and
a curated slice of pg_settings -- not the full ~300-row GUC dump.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.llens_style import Bullet, Bullets, Call, Response, Section, Table, iso, nof
from pgllens.llens_style import duration as fmt_duration
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "catalog"

_VERSION_SQL = "SELECT version()"

_SETTINGS_NAMES = (
    "max_connections", "shared_buffers", "work_mem", "maintenance_work_mem",
    "effective_cache_size", "random_page_cost", "max_worker_processes",
    "wal_level",
)

_SETTINGS_SQL = """
    SELECT name, setting, unit, short_desc
    FROM pg_settings
    WHERE name = ANY(%s)
    ORDER BY name
"""

_RUNTIME_SQL = """
    SELECT pg_postmaster_start_time() AS start_time,
           now() - pg_postmaster_start_time() AS uptime,
           (SELECT sum(numbackends) FROM pg_stat_database) AS connections
"""


def format_server_info(version_row: tuple[object, ...], runtime_row: tuple[object, ...],
                        settings: QueryResult) -> Response:
    start_time, uptime, connections = runtime_row
    version = str(version_row[0]).split(" on ")[0]          # "PostgreSQL 16.3"
    identity = Bullets((
        Bullet("version", version),
        Bullet("started", iso(start_time) if isinstance(start_time, datetime) else str(start_time)),
        Bullet("uptime", fmt_duration(uptime.total_seconds())
               if isinstance(uptime, timedelta) else str(uptime)),
        Bullet("connections", str(connections)),
    ))
    rows = tuple((f"`{n}`", str(v), str(u) if u else "", str(d) if d else "")
                 for n, v, u, d in settings.rows)
    config = Table(("setting", "value", "unit", "description"), rows)
    return Response(
        SERVER, "server_info", None, PLANE,
        (Section("identity", (identity,)), Section("configuration", (config,))),
        tally=(nof(cast("int", connections), "connection"), version,
               nof(len(rows), "setting shown", "settings shown")),
        next=(Call("list_extensions"), Call("schema_overview")),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Server Info"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def server_info() -> str:
        """Report the PostgreSQL server's version, uptime, total connection
        count, and a curated slice of notable settings (memory, workers,
        planner cost, WAL level). Instance-scoped -- takes no arguments."""
        version_result = await db.run_system(_VERSION_SQL)
        runtime_result = await db.run_system(_RUNTIME_SQL)
        settings_result = await db.run_system(_SETTINGS_SQL, (list(_SETTINGS_NAMES),))
        return respond(format_server_info(version_result.rows[0], runtime_result.rows[0],
                                           settings_result))
