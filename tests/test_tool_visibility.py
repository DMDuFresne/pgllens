"""Explicit MCP Apps tool-visibility allowlist.

The MCP Apps spec defaults an omitted `visibility` to ["model","app"] -- every
tool silently callable from inside a rendered view. This pins the allowlist
(get_sample_data, describe_table, get_table_stats) both in source (cheap,
fragile grep) and over the real registered tool objects (the actual guard).
"""

import re
from pathlib import Path

import pytest

APP_CALLABLE = {"get_sample_data", "describe_table", "get_table_stats"}
TOOLS_DIR = Path("src/pgllens/tools")


def _decorator_blocks(src: str) -> dict[str, str]:
    """Map each registered tool name to its contiguous run of `@...` decorator
    lines immediately above `async def name(`.

    A fixed-size character lookback (the MSSQL original) bleeds into the
    PRECEDING tool's decorator once a tool's body is shorter than the window --
    exactly what happened here with pgllens's shorter `get_sample_data` body
    bleeding its MODEL_AND_APP into the next tool's (false-positive) block.
    Anchoring on the actual decorator-line run is precise regardless of body
    length.
    """
    # Non-greedy from the outer decorator (@mcp.tool(...)/@apps.tool(...), which
    # may itself span multiple lines) through to the `async def` it decorates --
    # tolerant of multi-line decorator calls (e.g. erd.py's get_erd), unlike a
    # simple "one line starting with @" match.
    # Anchored to a real decorator LINE (`(?m)^[ \t]*@`): unanchored, the regex
    # also matched the literal `@apps.tool(resource_uri=...)` prose inside
    # erd.py's module docstring, so get_erd's "block" became the whole file
    # prefix -- which contains the word "visibility" (the import line) and thus
    # passed even with `meta=visibility(...)` deleted from the decorator.
    pattern = re.compile(
        r"(?m)^([ \t]*@(?:mcp|apps)\.tool\([\s\S]*?)\n[ \t]*async def (\w+)\((?!self)"
    )
    return {name: block for block, name in pattern.findall(src)}


def registered_tool_names(src: str) -> set[str]:
    """Names actually decorated with @mcp.tool(...)/@apps.tool(...) -- see
    `_decorator_blocks`. A bare "async def NAME(" regex (the MSSQL original)
    over-matches: pgllens's tools dir has module-level async helpers
    (explain.py's `estimated_cost`) and adapter class methods (erd.py's
    `_IntroAdapter.list_tables`/`describe_table`/`relationships`, reshaping
    Introspector into the QueryResult-based interface erd.model.build_erd
    expects) that share names with real tools or look tool-like but are
    never registered as MCP tools at all."""
    return set(_decorator_blocks(src).keys())


def test_decorator_parser_ignores_prose_that_mentions_a_decorator():
    """Regression: unanchored, the pattern matched `@apps.tool(resource_uri=...)`
    written inside erd.py's module DOCSTRING, so the tool's "decorator block"
    swallowed the whole file prefix -- including the `visibility` import -- and
    the source-level checks below passed even with `meta=` deleted."""
    src = '''"""Docs mentioning @apps.tool(resource_uri=...) in prose."""

    @mcp.tool(annotations=read_only("Decoy"))
    async def decoy() -> str:
        ...
'''
    assert _decorator_blocks(src) == {
        "decoy": '    @mcp.tool(annotations=read_only("Decoy"))'}


def test_visibility_helper_shape():
    from pgllens.annotations import MODEL_AND_APP, MODEL_ONLY, visibility
    assert visibility(*MODEL_ONLY) == {"ui": {"visibility": ["model"]}}
    assert visibility(*MODEL_AND_APP) == {"ui": {"visibility": ["model", "app"]}}


def test_visibility_rejects_unknown_mode():
    from pgllens.annotations import visibility
    with pytest.raises(ValueError):
        visibility("model", "wizard")


def test_every_tool_declares_visibility_explicitly():
    """The spec default is ["model","app"]; relying on it silently exposes every
    tool to the view. Each registration must say what it means."""
    src = "\n".join(p.read_text(encoding="utf-8") for p in TOOLS_DIR.glob("*.py"))
    blocks = _decorator_blocks(src)
    for name in registered_tool_names(src):
        assert "visibility" in blocks.get(name, ""), f"{name} does not declare _meta.ui.visibility"


def test_only_the_allowlist_is_app_callable():
    src = "\n".join(p.read_text(encoding="utf-8") for p in TOOLS_DIR.glob("*.py"))
    blocks = _decorator_blocks(src)
    for name in registered_tool_names(src):
        app_callable = "MODEL_AND_APP" in blocks.get(name, "")
        assert app_callable == (name in APP_CALLABLE), (
            f"{name}: app-callable={app_callable} but allowlist membership="
            f"{name in APP_CALLABLE}")


def test_query_is_never_app_callable():
    """Arbitrary SQL from a view widens the surface for no benefit."""
    src = (TOOLS_DIR / "query.py").read_text(encoding="utf-8")
    assert "MODEL_ONLY" in src and "MODEL_AND_APP" not in src


async def test_runtime_registry_matches_allowlist_exactly():
    """Source-grepping is fragile (it trusts a naming convention). This reads
    the actual `_meta.ui.visibility` off the real registered tool objects --
    the metadata a connecting client would actually see -- for every tool
    across both the plain MCPServer registry and the Apps extension (get_erd)."""
    from pgllens.config import Settings
    from pgllens.database.pool import Db
    from pgllens.server import create_mcp

    settings = Settings(_env_file=None, database_url="postgresql://u:p@localhost:5432/flux",
                        exposed_schemas="public")
    db = Db(settings)
    server = create_mcp(settings, db, intro=None)
    tools = await server.list_tools()

    app_callable = set()
    for t in tools:
        visibility = ((t.meta or {}).get("ui") or {}).get("visibility") or []
        assert "model" in visibility, f"{t.name}: not model-visible ({visibility!r})"
        if "app" in visibility:
            app_callable.add(t.name)

    assert app_callable == APP_CALLABLE
