"""Tool registration. Each tool module exposes register(mcp, db, settings, intro, caps)."""

from __future__ import annotations

from types import ModuleType
from typing import TYPE_CHECKING

from pgllens.tools import (
    catalog,
    constraints,
    discovery,
    erd,
    explain,
    health,
    hypertables,
    indexes,
    modules,
    ontology,
    query,
    relationships,
    server_info,
    sessions,
    statements,
    triggers,
    validate,
)

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

_MODULES: list[ModuleType] = [
    query,
    validate,
    explain,
    discovery,
    relationships,
    erd,
    modules,
    constraints,
    triggers,
    catalog,
    hypertables,
    server_info,
    sessions,
    indexes,
    statements,
    health,
    ontology,
]


def register_all(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
                  caps: Capabilities | None) -> None:
    for mod in _MODULES:
        mod.register(mcp, db, settings, intro, caps)
