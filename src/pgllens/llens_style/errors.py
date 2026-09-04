"""Error-code registry (the style guide's RS-1 §6). Every code carries a
hint template; an error cannot ship without a hint."""

from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    QUERY_REJECTED = "QUERY_REJECTED"
    SCHEMA_UNKNOWN = "SCHEMA_UNKNOWN"
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    FUNCTION_NOT_FOUND = "FUNCTION_NOT_FOUND"
    EXTENSION_MISSING = "EXTENSION_MISSING"
    ARG_OUT_OF_RANGE = "ARG_OUT_OF_RANGE"
    FORMAT_UNKNOWN = "FORMAT_UNKNOWN"
    TIMEOUT = "TIMEOUT"
    DB_ERROR = "DB_ERROR"


HINTS: dict[ErrorCode, str] = {
    ErrorCode.QUERY_REJECTED: (
        "Submit a single `SELECT`/`WITH`/`TABLE`/`VALUES` statement with no "
        "write, DDL, or side-effecting call."),
    ErrorCode.SCHEMA_UNKNOWN: (
        "Pass one of the exposed schemas; call schema_overview() to list them."),
    ErrorCode.TABLE_NOT_FOUND: (
        "Call search_columns(pattern={name!r}) or list_tables() to locate it."),
    ErrorCode.FUNCTION_NOT_FOUND: (
        "Call list_functions(schema={schema!r}) to see what is exposed."),
    ErrorCode.EXTENSION_MISSING: (
        "Run `CREATE EXTENSION {extension};` as a superuser, then retry."),
    ErrorCode.ARG_OUT_OF_RANGE: "Pass `{arg}` between {lo} and {hi}.",
    ErrorCode.FORMAT_UNKNOWN: "Pass one of: {valid}.",
    ErrorCode.TIMEOUT: (
        "Narrow the query or raise `QUERY_TIMEOUT_MS`; retry after {retry_after}."),
    ErrorCode.DB_ERROR: (
        "Check the message against docs/runbook.md; the server logged request "
        "{request_id}."),
}


def hint_for(code: ErrorCode, **kw: object) -> str:
    """Fill the hint template for `code`. A missing kwarg raises KeyError:
    that is a programming error in the caller, not a runtime path."""
    return HINTS[code].format(**kw)
