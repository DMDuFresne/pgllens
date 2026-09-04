"""Execute read-only SELECT queries."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.caller import caller
from pgllens.database.format import redact, table_from
from pgllens.database.safety import scrub
from pgllens.llens_style import (
    Block,
    Bullet,
    Bullets,
    Call,
    Caveat,
    ErrorCode,
    Response,
    Section,
    nof,
)
from pgllens.llens_style import count as fmt_count
from pgllens.llens_style import estimate as fmt_estimate
from pgllens.obs import metrics
from pgllens.tools._util import SERVER, ToolError, check_range, respond, tool_errors

logger = logging.getLogger("pgllens")

PLANE = "query"


def build_paged_sql(sql: str, page: int, max_rows: int,
                    always_limit: bool = False) -> str:
    """Page 1 is the query untouched. Later pages append LIMIT/OFFSET.

    `always_limit` (the caller passed an explicit `limit`) also appends a bare
    LIMIT on page 1, so a `limit=20` call really returns 20 rows.

    Raises ValueError for an out-of-range page or a missing ORDER BY -- OFFSET
    paging is only deterministic over an explicit order.
    """
    if not 1 <= page <= 10000:
        raise ValueError(f"page must be between 1 and 10000 (got {page})")
    if page == 1 and not always_limit:
        return sql
    # Match against scrubbed SQL with balanced parenthesised groups removed, so
    # 'limit exceeded' literals, "limit" identifiers, -- limit comments and a
    # subquery/CTE LIMIT don't false-reject; only a top-level LIMIT/FETCH, the
    # one a second appended clause would actually collide with, matches.
    # Deliberately simple: paren-stripping, not a parse; good enough because only
    # the top-level clause can ever be un-parenthesised.
    top_level = scrub(sql)
    while True:
        stripped = re.sub(r"\([^()]*\)", "", top_level)
        if stripped == top_level:
            break
        top_level = stripped
    if re.search(r"\b(limit|fetch\s+(first|next))\b", top_level, re.IGNORECASE):
        raise ValueError(
            "the query already has its own LIMIT/FETCH clause, and `limit`/`page` "
            "would append a second one (PostgreSQL rejects multiple LIMIT clauses). "
            "Remove it from the SQL, or drop the `limit`/`page` arguments."
        )
    # Same scrubbed, paren-stripped text as the LIMIT guard: an ORDER BY that
    # only appears in a comment, a literal or a subquery does not make OFFSET
    # paging deterministic, so it must not satisfy this check either.
    if page > 1 and "order by" not in top_level.lower():
        raise ValueError(
            "page > 1 requires an ORDER BY in the query — OFFSET pagination is "
            "only deterministic over an explicit order. Add ORDER BY (ideally on "
            "a unique column) and retry."
        )
    offset = (page - 1) * max_rows
    # +1 row so the existing truncation detection doubles as has-more.
    # Appended on its OWN LINE: a same-line append lets a trailing -- comment in
    # the query swallow the LIMIT clause, silently returning page 1 as page N.
    clause = f"LIMIT {max_rows + 1}" + (f" OFFSET {offset}" if offset else "")
    return f"{sql.rstrip().rstrip(';')}\n{clause}"


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Run Query"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def query(sql: str, page: int = 1, limit: int | None = None,
                    explain_first: bool = False,
                    max_estimated_rows: int | None = None) -> str:
        """Execute a read-only SQL SELECT. The exposed-schema allowlist scopes the
        catalog and discovery tools; this tool can read any relation the
        connection role can SELECT (for example pg_catalog or pg_stat_statements),
        and the read-only guarantee comes from the connection itself
        (default_transaction_read_only=on) plus the SQL gate.
        Results return as a markdown table, capped at the server's row limit
        (200 rows by default).

        Any single cell longer than 2,000 characters is cut with a marker that
        states its full length; read a long value in slices with
        `substr(col::text, 1, 2000)`, `substr(col::text, 2001, 2000)`, and so on.

        `limit` sets the page size for this call: 1 to the server maximum,
        which the rejection message names when you exceed it. Out-of-range
        values are rejected, never clamped. Omit it to use the server maximum.

        `page` (1-10000) fetches later pages of a large result: the query must
        contain an ORDER BY, and each page holds `limit` rows -- pass the SAME
        `limit` on every page of a run, or the offset shifts and rows are
        skipped. Values outside 1-10000 are rejected, not clamped.

        `explain_first=True` plans the query before running it and prefixes the
        result with the planner's estimated row count and cost, so you can see
        what the query was going to do. It never runs ANALYZE.

        `max_estimated_rows` is a pre-execution governor: it implies
        `explain_first`, and if the planner estimates more rows than this, the
        query is REFUSED without ever touching the data. Use it when you are
        unsure how selective a WHERE clause is. A plan the server cannot parse
        never refuses.

        If REDACT_COLUMNS is configured, matching output columns render as
        `[masked]` -- this is best-effort DISPLAY MASKING by output-column
        name, not a security boundary: `upper(api_token)` or `api_token AS x`
        bypasses it and returns cleartext. For a real, unbypassable guarantee,
        use column-level `REVOKE SELECT (col)` on the pgllens role instead
        (see docs/DEPLOY.md's Database role section)."""
        if limit is not None:
            check_range("limit", limit, 1, settings.max_rows)
        if max_estimated_rows is not None:
            check_range("max_estimated_rows", max_estimated_rows, 1, 2_000_000_000)
        page_size = limit if limit is not None else settings.max_rows
        try:
            run_sql = build_paged_sql(sql, page, page_size, always_limit=limit is not None)
        except ValueError as e:
            raise ToolError(ErrorCode.QUERY_REJECTED, f"{e}".rstrip(".") + ".",
                            "Adjust `page`/`limit` or the SQL as the message says.") from None
        facts: list[Bullet] = [Bullet("page", str(page)), Bullet("page size", str(page_size))]
        if explain_first or max_estimated_rows is not None:
            from pgllens.tools.explain import plan_estimate

            # The UNPAGED sql on purpose: the governor is about total work, and
            # the paged form's LIMIT would make every estimate pass.
            est = await plan_estimate(db, sql)
            if est is None:
                facts.append(Bullet("plan estimate", "unavailable"))
            else:
                plan_cost, rows = est
                if max_estimated_rows is not None and rows > max_estimated_rows:
                    raise ToolError(
                        ErrorCode.QUERY_REJECTED,
                        f"Planner estimates {fmt_count(int(rows))} rows, above "
                        f"`max_estimated_rows`={fmt_count(max_estimated_rows)}.",
                        "Narrow with WHERE, add LIMIT, or raise `max_estimated_rows`.")
                facts += [Bullet("estimated rows", fmt_estimate(rows), qualifier="estimate"),
                         Bullet("estimated cost", f"{plan_cost:g}")]
        if settings.max_estimated_cost or settings.tool_cost_budget_per_minute:
            from pgllens.limits import charge_cost
            from pgllens.tools.explain import estimated_cost

            cost = await estimated_cost(db, run_sql)
            if cost is not None:
                if settings.max_estimated_cost and cost > settings.max_estimated_cost:
                    raise ToolError(
                        ErrorCode.QUERY_REJECTED,
                        f"Rejected by the cost gate: estimated cost {cost:g} exceeds "
                        f"MAX_ESTIMATED_COST ({settings.max_estimated_cost:g}).",
                        "Inspect it with explain_query, add WHERE filters or LIMIT, or "
                        "raise MAX_ESTIMATED_COST.")
                # Same identity rule as ConcurrencyLimitMiddleware: the
                # authenticated client id when present, else the peer IP --
                # never the literal "anonymous", which would pool every
                # unauthenticated caller's spend onto one shared budget.
                c = caller()
                client_key = c.client_id if c.client_id != "anonymous" else c.ip
                if not await charge_cost(client_key, cost):
                    metrics.record_limit_rejection("cost")
                    raise ToolError(
                        ErrorCode.QUERY_REJECTED,
                        "Rejected by the cost budget: this client has spent its "
                        "query-cost allowance for the current minute.",
                        "Wait for the window to roll over, or make the query cheaper.")
            else:
                # Audit L5: documented fail-open, but never silent -- an
                # unparseable EXPLAIN shape must be visible in the logs.
                logger.warning(
                    "cost gate skipped: EXPLAIN cost unparseable for this query",
                    extra={"event": "cost_gate.skipped"},
                )
        # Deliberately simple: redaction matches on output column names only.
        # MsSQLLens's alias-proof redaction relies on
        # dm_exec_describe_first_result_set, which has no Postgres equivalent.
        # Upgrade path: a pg_query source-column probe if alias-hiding redaction
        # bypass becomes a real concern. This is display masking, not a security
        # boundary (see format.redact()'s docstring); the real, unbypassable
        # control is column-level `REVOKE SELECT (col)` on the role
        # (docs/DEPLOY.md's Database role section).
        result = redact(await db.run_readonly(run_sql, limit), settings.redact_columns)
        blocks: list[Block] = [Bullets(tuple(facts)), table_from(result)]
        tally = [nof(len(result.rows), "row")]
        nxt: list[Call] = []
        if result.truncated:
            tally.append("more rows exist")
            # page > 1 requires an ORDER BY (build_paged_sql rejects it otherwise),
            # so only suggest requesting the next page when one is actually there
            # to find -- an ORDER BY-less query would just have that suggestion
            # rejected by the same validator.
            if "order by" in sql.lower():
                # Carry the limit into the suggestion: page N's OFFSET is
                # page_size-relative, so query(sql, page=2) without the same
                # limit would offset by settings.max_rows and skip rows.
                kw: dict[str, object] = {"sql": sql, "page": page + 1}
                if limit is not None:
                    kw["limit"] = limit
                nxt.append(Call("query", kw))
            else:
                blocks.append(Caveat(
                    "Add an ORDER BY to page through the rest, or add LIMIT/WHERE."))
        elif page > 1:
            tally.append("last page")
        nxt.append(Call("explain_query", {"sql": sql}))
        return respond(Response(SERVER, "query", None, PLANE, (Section(None, tuple(blocks)),),
                                tally=tuple(tally), next=tuple(nxt[:3])))
