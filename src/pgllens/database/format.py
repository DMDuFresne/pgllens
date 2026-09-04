"""Rows -> markdown tables."""

from __future__ import annotations

import fnmatch
from collections.abc import Sequence
from dataclasses import dataclass

from pgllens.llens_style import Table


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[object, ...]]
    truncated: bool


# One cap for every row-returning tool: query, get_sample_data, get_table_stats
# all render through table_from -> cell. A 1536-dim vector is ~20 KB of text
# per row; at the 200-row default that is megabytes into the client context.
# The marker always states the true length, so nothing is silently hidden, and
# names the escape hatch (slice the value with substr via the query tool).
# Deliberately simple: fixed cap, no Settings entry; make it configurable only
# if a real deployment asks.
MAX_CELL_CHARS = 2000


def cell(v: object) -> str:
    """NULL/true/false plus str(v), capped at MAX_CELL_CHARS -- no markdown
    escaping. table_from feeds this straight into a style Table, whose renderer
    (llens_style.render) does its own pipe/newline escaping at render time."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    s = str(v)
    if len(s) <= MAX_CELL_CHARS:
        return s
    return (f"{s[:MAX_CELL_CHARS]} … [truncated: {len(s):,} chars total; read in slices "
            f"with substr(col::text, 1, {MAX_CELL_CHARS}) via the query tool]")


def table_from(result: QueryResult, columns: Sequence[str] | None = None) -> Table:
    """A style Table from a QueryResult: every cell formatted by `cell`, so
    NULL/true/false have one implementation. `columns` overrides the header
    labels (same length) for human-friendly names. No markdown escaping here
    -- llens_style.render escapes when it renders the Table."""
    headers = tuple(columns) if columns is not None else tuple(result.columns)
    return Table(headers, tuple(tuple(cell(v) for v in row) for row in result.rows))


# Display-mask token, not a secrecy guarantee -- see redact()'s docstring.
REDACTED = "[masked]"


def matches_redacted(name: str, patterns: list[str]) -> bool:
    """True when a column NAME matches any REDACT_COLUMNS pattern (`%` = any
    run, case-insensitive; `_` is LITERAL, not LIKE's one-char wildcard --
    column names are underscore-heavy, so `%_ssn` must mean "ends in _ssn", not
    "any char then ssn", or classname/businessname mask). The name-matching half
    of redact(), lifted out so a tool that prints stored values WITHOUT building
    a QueryResult -- describe_table's sampled `values` column -- masks by exactly
    the same rule instead of growing a second, drifting matcher."""
    return any(
        fnmatch.fnmatchcase(name.lower(), p.lower().replace("[", "[[]").replace("?", "[?]")
                            .replace("*", "[*]").replace("%", "*"))
        for p in patterns
    )


def redact(result: QueryResult, patterns: list[str],
           source_columns: list[str | None] | None = None) -> QueryResult:
    """Best-effort DISPLAY MASKING, not a security boundary: mask every value
    in columns whose NAME matches any pattern (% = any run, _ literal;
    case-insensitive -- see matches_redacted). When `source_columns` is given
    (per-output-column source column name from dm_exec_describe_first_result_set
    browse mode, index-aligned), it is checked too, so `SELECT Password AS p`
    still matches %password%. Pure; returns the input object untouched when
    nothing matches.

    Deliberately simple: this is a name match on the OUTPUT column, nothing
    more; it has no idea what expression produced that value.
    `SELECT upper(api_token)` or `SELECT api_token AS x` sails through in
    cleartext because there's no SQL parser resolving output columns back to
    source columns (Postgres has no dm_exec_describe_first_result_set
    equivalent to lean on, unlike MsSQLLens). Treat REDACT_COLUMNS as a UX
    nicety for accidental exposure, not a secrecy guarantee. The real,
    Postgres-enforced control is column-level `REVOKE SELECT (col) FROM
    pgllens` on the role; see docs/DEPLOY.md's Database role section.
    """
    if not patterns:
        return result

    def _matches(i: int, name: str) -> bool:
        src = source_columns[i] if source_columns and i < len(source_columns) else None
        candidates = [name, src] if src else [name]
        return any(matches_redacted(c, patterns) for c in candidates)

    hit = [_matches(i, c) for i, c in enumerate(result.columns)]
    if not any(hit):
        return result
    rows = [tuple(REDACTED if h else v for v, h in zip(r, hit)) for r in result.rows]
    return QueryResult(result.columns, rows, result.truncated)
