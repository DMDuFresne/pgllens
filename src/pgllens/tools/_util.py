"""Shared tool helpers."""

from __future__ import annotations

import functools
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypeVar, cast

from mcp.types import CallToolResult, TextContent

from pgllens.caller import caller
from pgllens.database import pool
from pgllens.database.capability import ExtensionMissingError
from pgllens.database.introspect import FunctionNotFoundError, TableNotFoundError
from pgllens.database.pool import UnknownSchemaError
from pgllens.database.safety import UnsafeQueryError
from pgllens.llens_style import Error, ErrorCode, Response, hint_for, render, render_error
from pgllens.obs import audit as audit_mod
from pgllens.obs import metrics, telemetry
from pgllens.obs.correlation import correlation_id, new_correlation_id

if TYPE_CHECKING:
    from pgllens.database.introspect import Introspector, Table
    from pgllens.database.pool import Db

logger = logging.getLogger("pgllens")

SERVER = "pgllens"

# Every tool name wrapped by tool_errors. metrics.preregister_tools() reads this
# at startup, which is why tool_errors -- not each tool module -- owns it.
_TOOL_NAMES: set[str] = set()
_TOOLS_PKG = "pgllens.tools."


def register_tool_name(name: str, module: str) -> None:
    """Record a real tool name. Decorating a throwaway function with
    tool_errors outside pgllens.tools (tests do, a lot) must not enter the
    registry: test_tool_registry.py pins it equal to the MCP tool list."""
    if module.startswith(_TOOLS_PKG):
        _TOOL_NAMES.add(name)


def registered_tool_names() -> frozenset[str]:
    return frozenset(_TOOL_NAMES)


class ToolError(Exception):
    """A tool-specific rejection with a fixed code, message and hint. Raised
    inside a tool body; tool_errors renders it. Prefer the typed subclasses
    and the shared exception map; use this directly only for a code the
    registry already has and no exception type covers (FORMAT_UNKNOWN, a
    paging rule under QUERY_REJECTED)."""

    def __init__(self, code: ErrorCode, message: str, hint: str,
                 retry_after: str | None = None) -> None:
        super().__init__(message)
        self.code, self.message, self.hint, self.retry_after = code, message, hint, retry_after


class ArgOutOfRangeError(ToolError):
    def __init__(self, name: str, value: object, lo: int, hi: int) -> None:
        super().__init__(
            ErrorCode.ARG_OUT_OF_RANGE,
            f"`{name}` must be between {lo} and {hi} (got {value}).",
            hint_for(ErrorCode.ARG_OUT_OF_RANGE, arg=name, lo=lo, hi=hi),
        )


def check_range(name: str, value: int, lo: int, hi: int) -> None:
    """Values outside the range are always rejected, never clamped."""
    if not lo <= value <= hi:
        raise ArgOutOfRangeError(name, value, lo, hi)


def _now() -> datetime:
    return datetime.now(UTC)


def _request_id() -> str:
    """The HTTP correlation id when the request carried one (obs/correlation.py);
    a fresh id under stdio. Either way the same id lands in the JSON log line."""
    return correlation_id() or new_correlation_id()


def respond(r: Response) -> str:
    return render(r, now=_now(), request_id=_request_id())


def fail(tool: str, code: ErrorCode, message: str, hint: str,
         retry_after: str | None = None) -> str:
    return render_error(Error(SERVER, tool, code, message, hint, retry_after),
                        now=_now(), request_id=_request_id())


# ErrorCode -> metrics outcome label. Lives next to the exception map so the two
# cannot drift; the label set is fixed (see obs/metrics.py cardinality comment).
OUTCOME: dict[ErrorCode, str] = {
    ErrorCode.QUERY_REJECTED: "rejected",
    ErrorCode.ARG_OUT_OF_RANGE: "rejected",
    ErrorCode.FORMAT_UNKNOWN: "rejected",
    ErrorCode.SCHEMA_UNKNOWN: "unknown_schema",
    ErrorCode.TABLE_NOT_FOUND: "not_found",
    ErrorCode.FUNCTION_NOT_FOUND: "not_found",
    ErrorCode.EXTENSION_MISSING: "unavailable",
    ErrorCode.TIMEOUT: "db_error",
    ErrorCode.DB_ERROR: "db_error",
}

_TIMEOUT_MARKERS = ("statement timeout", "canceling statement", "pool timeout", "timed out")


async def resolve_table(db: Db, intro: Introspector, table: str, schema: str | None) -> Table:
    """The one table lookup every table-taking tool goes through. `schema` is
    resolved with db.resolve_schema FIRST so an unexposed schema raises
    UnknownSchemaError -- the same error and audit outcome list_tables gives
    for the identical mistake -- instead of intro.table's unrelated
    TableNotFoundError with a misleading "did you mean" hint."""
    if schema is not None:
        schema = db.resolve_schema(schema)
    return await intro.table(table, schema)


def _quote_ident(name: str) -> str:
    """Postgres identifier quoting -- the sanctioned SQL-injection exception for
    the whole codebase: identifiers cannot be bound as parameters, so every
    table/schema/column name interpolated into SQL goes through here instead.
    Only ever applied to a name that has already round-tripped through
    Introspector (i.e. it names a real object in an exposed schema) -- never to
    raw user input. Doubling internal quotes is what makes a hostile relname inert.
    """
    return '"' + name.replace('"', '""') + '"'


def clean_db_error(e: BaseException) -> str:
    """Reduce a psycopg error to its first sentence. psycopg's str() is the
    server message followed by a LINE n:/caret position block and often a
    HINT: -- all noise once the model already has the SQL it sent."""
    first = str(e).split("\n", 1)[0].strip()
    return first or str(e)


def _args_hash(args: tuple[object, ...], kwargs: dict[str, object]) -> str:
    """A stable fingerprint of a tool call's arguments.

    The arguments themselves are the client's data (the SQL text above all) and
    never belong in the audit trail. The hash answers the question the trail is
    actually for -- "is this the same call as the one at 03:14?" -- without
    storing any of it. Truncated to 16 hex chars: still 64 bits, and the trail
    stays readable.
    """
    blob = json.dumps([args, kwargs], sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _to_error(tool: str, e: BaseException, request_id: str) -> Error:
    """The single exception -> Error map for every tool."""
    if isinstance(e, ToolError):
        return Error(SERVER, tool, e.code, e.message, e.hint, e.retry_after)
    if isinstance(e, UnsafeQueryError):
        msg = str(e) or type(e).__name__
        return Error(SERVER, tool, ErrorCode.QUERY_REJECTED, f"{msg}.".replace("..", "."),
                     hint_for(ErrorCode.QUERY_REJECTED))
    if isinstance(e, UnknownSchemaError):
        return Error(SERVER, tool, ErrorCode.SCHEMA_UNKNOWN, str(e) or type(e).__name__,
                     hint_for(ErrorCode.SCHEMA_UNKNOWN))
    if isinstance(e, TableNotFoundError):
        msg = str(e) or type(e).__name__
        name = msg.split("'")[1] if "'" in msg else "table"
        return Error(SERVER, tool, ErrorCode.TABLE_NOT_FOUND, msg,
                     hint_for(ErrorCode.TABLE_NOT_FOUND, name=name))
    if isinstance(e, FunctionNotFoundError):
        msg = str(e) or type(e).__name__
        schema = msg.rsplit("'", 2)[1] if msg.count("'") >= 4 else "public"
        return Error(SERVER, tool, ErrorCode.FUNCTION_NOT_FOUND, msg,
                     hint_for(ErrorCode.FUNCTION_NOT_FOUND, schema=schema))
    if isinstance(e, ExtensionMissingError):
        return Error(SERVER, tool, ErrorCode.EXTENSION_MISSING, f"{e or type(e).__name__}.",
                     hint_for(ErrorCode.EXTENSION_MISSING, extension=e.extension))
    first = clean_db_error(e) or type(e).__name__
    if any(m in first.lower() for m in _TIMEOUT_MARKERS):
        return Error(SERVER, tool, ErrorCode.TIMEOUT, first,
                     hint_for(ErrorCode.TIMEOUT, retry_after="30s"), retry_after="30s")
    return Error(SERVER, tool, ErrorCode.DB_ERROR, first,
                 hint_for(ErrorCode.DB_ERROR, request_id=f"`{request_id}`"))


_T = TypeVar("_T")


def tool_errors(fn: Callable[..., Awaitable[_T]]) -> Callable[..., Awaitable[_T]]:
    """Convert expected failures into the fixed LLens error envelope instead of
    a traceback.

    This is also the single instrumentation point for every ``@mcp.tool``: one
    call records both a metric and an audit line, so no individual tool module
    needs to know about either. ``outcome`` is one of a small fixed set ("ok",
    "rejected", "unknown_schema", "not_found", "unavailable", "db_error")
    drawn from ``OUTCOME`` -- metrics.record_tool_call's `tool`/`outcome`
    labels are drawn only from this enum and the tool's own __name__, never
    from schema names or SQL text (see the cardinality comment in
    obs/metrics.py). It is also the identity-stamping point: the audit line
    carries the authenticated subject, client id, and source IP from
    ``caller()``, plus a hash of the tool's arguments and the row count the
    call produced -- never the raw arguments themselves.
    """
    returns_result = fn.__annotations__.get("return") in ("CallToolResult", CallToolResult)
    register_tool_name(fn.__name__, fn.__module__)

    @functools.wraps(fn)
    async def wrapper(*args: object, **kwargs: object) -> object:
        start = time.monotonic()
        outcome = "ok"
        pool.reset_rows()
        try:
            return await fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 -- tool boundary: every failure becomes the
            # fixed error envelope, never a traceback.
            rid = _request_id()
            err = _to_error(fn.__name__, e, rid)
            outcome = OUTCOME[err.code]
            if outcome == "db_error":
                logger.warning("tool %s failed: %s", fn.__name__, e)
            text = render_error(err, now=_now(), request_id=rid)
            if returns_result:
                return CallToolResult(content=[TextContent(type="text", text=text)])
            return text
        finally:
            duration_s = time.monotonic() - start
            who = caller()
            trace_id = telemetry.current_trace_id()
            metrics.record_tool_call(fn.__name__, outcome, duration_s, trace_id=trace_id)
            # Only present when there is a real sampled trace to join to -- a
            # null trace_id on every line is noise the log backend still indexes.
            extra = {"trace_id": trace_id} if trace_id else {}
            audit_mod.audit(
                "tool_call",
                tool=fn.__name__,
                outcome=outcome,
                duration_ms=round(duration_s * 1000),
                sub=who.sub,
                client_id=who.client_id,
                ip=who.ip,
                args_hash=_args_hash(args, kwargs),
                rows=pool.rows_returned(),
                **extra,
            )

    return cast("Callable[..., Awaitable[_T]]", wrapper)
