"""Connection handling and query execution (psycopg 3, async pool)."""

from __future__ import annotations

import asyncio
import logging
import time
from contextvars import ContextVar
from typing import Any

from psycopg_pool import AsyncConnectionPool

from pgllens.config import Settings
from pgllens.database.format import QueryResult
from pgllens.database.safety import assert_read_only
from pgllens.obs import metrics

# Rows the database actually returned for the current tool call. A contextvar,
# not a Db attribute: one Db is shared by every concurrent request, so an
# attribute would attribute one client's row count to another's audit record.
_rows_returned: ContextVar[int] = ContextVar("pgllens_rows_returned", default=0)


def record_rows(n: int) -> None:
    _rows_returned.set(_rows_returned.get() + n)


def rows_returned() -> int:
    return _rows_returned.get()


def reset_rows() -> None:
    _rows_returned.set(0)


logger = logging.getLogger("pgllens")


async def _reset_connection(conn: Any) -> None:
    """Scrub session state when a connection returns to the pool.

    DISCARD ALL releases advisory locks (pg_try_advisory_lock survives
    rollback -- session-scoped), drops prepared statements/temp state, and
    RESETs to the connection's startup values -- which INCLUDE the
    default_transaction_read_only=on baked into the DSN options, so the
    read-only guarantee is never weakened by this, only reasserted.

    DISCARD ALL refuses to run inside a transaction block, and a
    non-autocommit connection's execute() implicitly opens one -- so flip
    autocommit for the single statement.
    """
    await conn.set_autocommit(True)
    try:
        await conn.execute("DISCARD ALL")
    finally:
        await conn.set_autocommit(False)


class UnknownSchemaError(ValueError):
    """Schema not in EXPOSED_SCHEMAS."""


def rows_from_cursor(cur: Any, max_rows: int) -> QueryResult:
    """Cursor -> QueryResult.

    SECURITY / CORRECTNESS: columns is a positional list and rows are tuples --
    never a dict. psycopg's dict_row row factory would collapse duplicate output
    names (SELECT a.created_at, b.created_at) to a single key, silently losing a
    value with no error. That is exactly the bug the TypeScript server shipped
    and the reason its users were told to alias every column in a join. Do not
    "simplify" this to a row factory.
    """
    if cur.description is None:
        return QueryResult(columns=[], rows=[], truncated=False)
    columns = [d.name for d in cur.description]
    fetched = cur.fetchmany(max_rows + 1)
    truncated = len(fetched) > max_rows
    result = QueryResult(columns=columns,
                         rows=[tuple(r) for r in fetched[:max_rows]],
                         truncated=truncated)
    record_rows(len(result.rows))
    return result


class _Prefetched:
    """Sync cursor facade over rows already awaited from psycopg's AsyncCursor,
    so rows_from_cursor keeps one signature for both the tests and live use."""

    def __init__(self, description: Any, rows: list[tuple[object, ...]]) -> None:
        self.description = description
        self._rows = rows

    def fetchmany(self, n: int) -> list[tuple[object, ...]]:
        return self._rows[:n]


class Db:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: AsyncConnectionPool | None = None

    async def open(self) -> None:
        if self._pool is None:
            # open=False + explicit open() so construction never blocks an event
            # loop that has not started yet (build_app runs at import time in tests).
            self._pool = AsyncConnectionPool(self._settings.conninfo(), min_size=1,
                                             max_size=self._settings.db_pool_max_size,
                                             open=False, reset=_reset_connection,
                                             # DISCARD ALL in _reset_connection deallocates
                                             # server-side prepared statements psycopg's
                                             # per-connection cache still references --
                                             # disable auto-prepare so the two never conflict.
                                             kwargs={"prepare_threshold": None})
            await self._pool.open()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    def resolve_schema(self, schema: str | None) -> str:
        if schema is None:
            return self._settings.default_schema
        for name in self._settings.exposed_schemas:
            if name.lower() == schema.lower():
                return name
        raise UnknownSchemaError(
            f"Schema {schema!r} is not exposed. "
            f"Available: {', '.join(self._settings.exposed_schemas)}"
        )

    async def _execute(self, sql: str, params: tuple[object, ...],
                       max_rows: int) -> QueryResult:
        await self.open()
        assert self._pool is not None
        start = time.monotonic()
        try:
            async with self._pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(sql, params or None)
                fetched = ([] if cur.description is None
                           else await cur.fetchmany(max_rows + 1))
                result = rows_from_cursor(_Prefetched(cur.description, fetched),
                                          max_rows)
        except Exception:
            metrics.record_query_duration("error", time.monotonic() - start)
            raise
        metrics.record_query_duration("ok", time.monotonic() - start)
        return result

    async def run_readonly(self, sql: str, max_rows: int | None = None) -> QueryResult:
        """`max_rows` narrows the row cap for this call only (query's `limit`);
        None keeps the configured server maximum. It can only ever tighten the
        cap -- callers validate against settings.max_rows before passing it."""
        sql = assert_read_only(sql)  # execute the exact string that was validated
        return await self._execute(sql, (), max_rows or self._settings.max_rows)

    async def run_system(self, sql: str, params: tuple[object, ...] = (),
                         max_rows: int | None = None) -> QueryResult:
        """Internal catalog queries. Not gated by assert_read_only -- these are
        our own parameterised SQL, never user input."""
        return await self._execute(sql, params, max_rows or self._settings.max_rows)

    async def ping(self, timeout: float = 2.0) -> bool:
        """Cheap DB-reachability check for /health. False on any failure
        (timeout, refused connection, auth error, ...) -- /health must never
        raise, it must report unhealthy."""
        try:
            async with asyncio.timeout(timeout):
                await self._execute("SELECT 1", (), 1)
            return True
        except Exception:  # noqa: BLE001 -- /health must report unhealthy, never raise
            return False
