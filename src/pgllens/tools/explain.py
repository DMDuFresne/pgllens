"""EXPLAIN a query, and the cost gate that reads its estimated cost."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.safety import assert_read_only
from pgllens.llens_style import Block, Bullet, Bullets, Call, Caveat, Code, Response, Section
from pgllens.llens_style import estimate as fmt_estimate
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "query"


def _top(plan: object) -> tuple[float, float] | None:
    """`(Total Cost, Plan Rows)` of the top plan node of an already-fetched
    `EXPLAIN (FORMAT JSON)` payload. None on any shape this can't parse."""
    try:
        # EXPLAIN (FORMAT JSON)'s payload shape is genuinely dynamic (an
        # arbitrary, driver-dependent plan tree) -- Any is the honest type
        # here, not a typing gap; the try/except is what actually guards
        # against a shape this can't parse.
        parsed: Any = json.loads(plan) if isinstance(plan, str) else plan
        top = parsed[0]["Plan"]
        # Only Total Cost is required: a plan that carries a cost but no
        # Plan Rows must still gate on cost rather than failing the gate open.
        # 0.0 rows can never trip query's row governor.
        return float(top["Total Cost"]), float(top.get("Plan Rows", 0.0))
    # ValueError covers json.JSONDecodeError (a subclass) and a non-numeric
    # cost/rows value in one arm.
    except (KeyError, IndexError, TypeError, ValueError):
        return None


async def plan_estimate(db: Db, sql: str) -> tuple[float, float] | None:
    """`(Total Cost, Plan Rows)` of the top plan node for `sql`, via
    `EXPLAIN (FORMAT JSON)`.

    Never runs ANALYZE -- this only plans the query, it never executes it.
    Returns None on any plan shape it can't parse rather than raising, since
    callers treat "no estimate available" as "don't gate" (documented fail-open
    for both the cost gate and query's row governor).
    """
    # run_system bypasses assert_read_only (it's meant for our own parameterised
    # catalog SQL) -- but this f-string embeds the caller's raw SQL, so the gate
    # has to run here, first, or a stacked "SELECT 1; SET ..." statement reaches
    # the server unvalidated. See database/safety.py.
    sql = assert_read_only(sql)
    result = await db.run_system(f"EXPLAIN (FORMAT JSON) {sql}")
    if not result.rows:
        return None
    return _top(result.rows[0][0])


async def estimated_cost(db: Db, sql: str) -> float | None:
    """Total cost of the top plan node for `sql`; None when unparseable."""
    estimate = await plan_estimate(db, sql)
    return None if estimate is None else estimate[0]


def format_plan(plan: object) -> str:
    """Render an `EXPLAIN (FORMAT JSON)` payload as an indented plan tree, one
    line per node: node type, relation, cost, rows. Two-space indent per depth,
    matching the old TS server's plan formatting."""
    parsed: Any = json.loads(plan) if isinstance(plan, str) else plan

    lines: list[str] = []

    def walk(node: dict[str, Any], depth: int) -> None:
        node_type = node.get("Node Type", "?")
        relation = node.get("Relation Name") or node.get("Function Name")
        cost = node.get("Total Cost")
        rows = node.get("Plan Rows")
        label = node_type
        if relation:
            label += f" on {relation}"
        details = []
        if cost is not None:
            details.append(f"cost={cost:g}")
        if rows is not None:
            details.append(f"rows={rows}")
        actual_time = node.get("Actual Total Time")
        actual_rows = node.get("Actual Rows")
        actual_loops = node.get("Actual Loops")
        if actual_time is not None:
            actual = [f"actual={actual_time:g}ms"]
            if actual_rows is not None:
                actual.append(f"rows={actual_rows}")
            if actual_loops is not None:
                actual.append(f"loops={actual_loops}")
            details.append(" ".join(actual))
        suffix = f" ({', '.join(details)})" if details else ""
        lines.append("  " * depth + label + suffix)
        indent = "  " * (depth + 1)
        for key in ("Strategy", "Partial Mode", "Join Type", "Index Name", "Index Cond", "Filter"):
            value = node.get(key)
            if value is not None:
                lines.append(f"{indent}{key}: {value}")
        for child in node.get("Plans", []):
            walk(child, depth + 1)

    for entry in parsed:
        walk(entry["Plan"], 0)
        planning_time = entry.get("Planning Time")
        execution_time = entry.get("Execution Time")
        if planning_time is not None:
            lines.append(f"Planning Time: {planning_time:g}ms")
        if execution_time is not None:
            lines.append(f"Execution Time: {execution_time:g}ms")

    return "\n".join(lines)


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Explain Query"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def explain_query(sql: str, analyze: bool = False) -> str:
        """Show the query plan for a read-only SQL SELECT.

        `analyze=False` (default) only plans the query -- it is never run.
        `analyze=True` runs `EXPLAIN (ANALYZE)`, which EXECUTES the query to
        measure real timings; still safe because the query must first pass
        assert_read_only and the session is read-only, but it is opt-in and
        does real work against the database."""
        assert_read_only(sql)
        mode = "ANALYZE, FORMAT JSON" if analyze else "FORMAT JSON"
        result = await db.run_system(f"EXPLAIN ({mode}) {sql}")
        plan = result.rows[0][0]
        est = _top(plan)
        facts = Bullets((
            Bullet("mode", "analyze" if analyze else "plan only"),
            *((Bullet("estimated rows", fmt_estimate(est[1]), qualifier="estimate"),
               Bullet("estimated cost", f"{est[0]:g}")) if est else ()),
        ))
        blocks: list[Block] = [facts, Code("text", format_plan(plan))]
        if analyze:
            blocks.append(Caveat("ANALYZE executed the query to measure real timings."))
        return respond(Response(SERVER, "explain_query", None, PLANE, (Section(None, tuple(blocks)),),
                                tally=("1 plan",), next=(Call("query", {"sql": sql, "limit": 20}),)))
