"""Validate that a query is read-only and plans successfully, without running it."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.safety import assert_read_only
from pgllens.llens_style import Bullet, Bullets, Call, Response, Section
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "query"


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Validate Query"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def validate_query(sql: str) -> str:
        """Check a query is read-only and parses/plans, without running it."""
        assert_read_only(sql)
        await db.run_system(f"EXPLAIN (FORMAT JSON) {sql}")
        return respond(Response(
            SERVER, "validate_query", None, PLANE,
            (Section(None, (Bullets((
                Bullet("read_only", "yes"),
                Bullet("plans", "yes"),
            )),)),),
            tally=("read-only", "plans"),
            next=(Call("explain_query", {"sql": sql}), Call("query", {"sql": sql, "limit": 20})),
        ))
