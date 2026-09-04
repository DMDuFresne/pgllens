"""Fixtures for the opt-in live-PostgreSQL integration suite.

`dsn` calls `pytest.skip()` itself, per test, rather than skipping the whole
module at collection/import time -- a plain `uv run pytest tests/integration`
on a machine with no Postgres (or `PGLLENS_TEST_DSN` unset) stays green with
clean skips, never a collection error.

No seed.sql: every fixture/test here works against whatever the configured
database already contains (catalog queries, `sample_table`/`sample_view`/
`sample_function` discovery) rather than assuming specific seeded rows --
see task-14-brief.md's "prefer tests that run against ANY database" guidance.
`PGLLENS_TEST_SCHEMAS` (comma-separated, default `public`) picks the exposed schemas.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from pgllens.config import Settings
from pgllens.database.capability import Capabilities
from pgllens.database.introspect import Introspector
from pgllens.database.pool import Db
from pgllens.tools import register_all
from tests.conftest import FakeMCP

# Windows defaults asyncio.new_event_loop() to ProactorEventLoop, which
# psycopg's AsyncConnectionPool refuses to run under ("Psycopg cannot use the
# 'ProactorEventLoop' to run in async mode"). Only matters for a developer
# running this suite against a real DSN on Windows -- CI and the Docker image
# both run Linux, where this is a no-op.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DSN_ENV = "PGLLENS_TEST_DSN"
SCHEMAS_ENV = "PGLLENS_TEST_SCHEMAS"


@pytest.fixture(scope="session")
def dsn() -> str:
    value = os.environ.get(DSN_ENV)
    if not value:
        pytest.skip(f"{DSN_ENV} is not set; skipping integration tests")
    try:
        import psycopg

        with psycopg.connect(value, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
    except Exception as e:  # noqa: BLE001 -- any failure to reach PG is a skip
        pytest.skip(f"{DSN_ENV} is set but unreachable: {e}")
    return value


@pytest.fixture(scope="session")
def settings(dsn: str) -> Settings:
    return Settings(_env_file=None, database_url=dsn, exposed_schemas=os.environ.get(SCHEMAS_ENV, "public"))


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def live_db(settings: Settings) -> AsyncIterator[Db]:
    # One session loop for the pool and every test that touches it: the pool's
    # background tasks live on this loop, and closing it here is what lets
    # pytest-asyncio's runner shut down instead of hanging in _cancel_all_tasks.
    db = Db(settings)
    try:
        yield db
    finally:
        await db.close()


@pytest.fixture(scope="session")
def intro(live_db: Db, settings: Settings) -> Introspector:
    return Introspector(live_db, settings)


@pytest.fixture(scope="session")
def caps(live_db: Db) -> Capabilities:
    return Capabilities(live_db)


@pytest.fixture(scope="session")
def tools(live_db: Db, settings: Settings, intro: Introspector, caps: Capabilities) -> dict:
    mcp = FakeMCP()
    register_all(mcp, live_db, settings, intro, caps)
    return mcp.tools


@pytest_asyncio.fixture(loop_scope="session")
async def sample_table(live_db: Db, settings: Settings) -> str | None:
    """First table name in the configured schema, or None if it has none --
    tests needing a real table name skip individually rather than assume
    seeded content."""
    result = await live_db.run_system(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename LIMIT 1",
        (settings.exposed_schemas[0],),
    )
    return str(result.rows[0][0]) if result.rows else None


@pytest_asyncio.fixture(loop_scope="session")
async def sample_view(live_db: Db, settings: Settings) -> str | None:
    result = await live_db.run_system(
        "SELECT viewname FROM pg_views WHERE schemaname = %s ORDER BY viewname LIMIT 1",
        (settings.exposed_schemas[0],),
    )
    return str(result.rows[0][0]) if result.rows else None


@pytest_asyncio.fixture(loop_scope="session")
async def sample_enum_column(live_db: Db, settings: Settings) -> tuple[str, str, str] | None:
    """First (table, column, enum type name) whose column type is a
    user-defined enum in the configured schema, or None if there isn't one --
    tests needing a real enum column skip individually rather than assume the
    demo schema's `reading.quality` (or any other) enum is present."""
    result = await live_db.run_system(
        "SELECT c.relname, a.attname, t.typname "
        "FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_type t ON t.oid = a.atttypid "
        "WHERE n.nspname = %s AND t.typtype = 'e' AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY c.relname, a.attname LIMIT 1",
        (settings.exposed_schemas[0],),
    )
    if not result.rows:
        return None
    table, column, typname = result.rows[0]
    return str(table), str(column), str(typname)


@pytest_asyncio.fixture(loop_scope="session")
async def sample_function(live_db: Db, settings: Settings) -> str | None:
    result = await live_db.run_system(
        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = %s AND p.prokind = 'f' ORDER BY p.proname LIMIT 1",
        (settings.exposed_schemas[0],),
    )
    return str(result.rows[0][0]) if result.rows else None
