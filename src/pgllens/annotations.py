"""MCP tool-annotation and tool-metadata conventions.

Every tool declares ``annotations`` (title + behavior hints). Clients treat these as
*untrusted hints* — the structural read-only lock (safety.py, Phase 1) is the real
guarantee — but the Claude Connector Directory *requires* title + readOnlyHint on
every tool, so they are mandatory.
"""

from __future__ import annotations

from typing import Literal

from mcp.types import ToolAnnotations

# Typed as the SDK's own Literal (not plain str) so apps.tool(visibility=MODEL_ONLY)
# type-checks against Apps' `Sequence[Literal['model', 'app']] | None` parameter.
MODEL_ONLY: tuple[Literal["model"]] = ("model",)
MODEL_AND_APP: tuple[Literal["model"], Literal["app"]] = ("model", "app")
_VALID_MODES = {"model", "app"}


def visibility(*modes: str) -> dict[str, dict[str, list[str]]]:
    """`_meta.ui.visibility` for a tool registration.

    The MCP Apps spec defaults an omitted `visibility` to `["model","app"]` --
    every tool callable from a view. Stamping this explicitly on every tool
    registration is what makes the allowlist (get_sample_data, describe_table,
    get_table_stats) safe by construction rather than "safe until someone adds
    a tool and forgets the default is wide open".
    """
    unknown = set(modes) - _VALID_MODES
    if unknown:
        raise ValueError(f"unknown visibility mode(s): {sorted(unknown)}")
    return {"ui": {"visibility": list(modes)}}


def read_only(title: str) -> ToolAnnotations:
    """Standard read tool: read-only, idempotent, closed-world.

    openWorldHint=False: PgLLens tools operate against ONE configured PostgreSQL
    database (`DATABASE_URL`) and its allowlisted schemas (`EXPOSED_SCHEMAS`) -- a closed, bound
    world, not an open/unbounded set of external resources.
    """
    return ToolAnnotations(
        title=title,
        read_only_hint=True,
        idempotent_hint=True,
        open_world_hint=False,
    )


# No hand-rolled ui_resource()/_meta.ui helper here: MCP Apps (SEP-1865 --
# https://modelcontextprotocol.io/specification/draft/extensions/apps) is a real,
# negotiated server extension (`mcp.server.apps.Apps`), not just a `_meta` key a
# tool can stamp on itself. `tools/erd.py` registers get_erd on an `Apps` instance
# via `@apps.tool(resource_uri=...)`, which both stamps `_meta.ui.resourceUri` AND
# advertises the extension in `ServerCapabilities.extensions` -- a hand-stamped
# meta key with no negotiation gives a capable host no signal to ever render the
# widget, which is exactly the bug an earlier version of this file shipped.
