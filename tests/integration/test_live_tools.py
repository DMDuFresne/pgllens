"""Live-server integration tests: every registered tool against a real
PostgreSQL, plus three proofs only a real server/database can give (see
conftest.py for the skip-cleanly-with-no-DSN guard). Skips cleanly when no
`PGLLENS_TEST_DSN` is configured/reachable -- never fails a plain `pytest -q`.

One test per registered tool (27 via `register_all` + `get_erd_widget`,
registered separately through the MCP Apps extension -- see tools/erd.py) asserts it
returns markdown that renders as an LLens-style success (header, no
`· error` status) and passes the style linter. No seed.sql: tools that need a
real table/view/function name use the `sample_table`/`sample_view`/
`sample_function` fixtures (discovered from the live catalog) and skip
individually if the configured schema has none of that kind, rather than
assume specific seeded content.
"""

from __future__ import annotations

import pytest

from pgllens.llens_style import lint

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


def assert_markdown(out: str) -> None:
    assert isinstance(out, str) and out.strip()
    assert " · error" not in out.split("\n", 1)[0], out
    assert lint(out) == [], "\n".join(f"{v.rule}:{v.line} {v.message}" for v in lint(out)) + "\n" + out


# --- Query tools -------------------------------------------------------------


async def test_query(tools):
    assert_markdown(await tools["query"](sql="SELECT 1 AS probe"))


async def test_validate_query(tools):
    assert_markdown(await tools["validate_query"](sql="SELECT 1"))


async def test_explain_query(tools):
    assert_markdown(await tools["explain_query"](sql="SELECT 1"))


# --- Discovery tools ----------------------------------------------------------


async def test_list_tables(tools):
    assert_markdown(await tools["list_tables"]())


async def test_describe_table(tools, sample_table):
    if sample_table is None:
        pytest.skip("configured schema has no tables to describe")
    assert_markdown(await tools["describe_table"](table=sample_table))


async def test_describe_table_reports_the_real_enum_type_name(tools, sample_enum_column):
    # audit #6: an enum column's type must render as its actual catalog type
    # name (e.g. "quality"), never the generic "USER-DEFINED" the driver
    # exposes for enums.
    if sample_enum_column is None:
        pytest.skip("configured schema has no enum-typed columns")
    table, _column, typname = sample_enum_column
    out = await tools["describe_table"](table=table)
    assert_markdown(out)
    assert typname in out
    assert "USER-DEFINED" not in out


async def test_schema_overview(tools):
    assert_markdown(await tools["schema_overview"]())


async def test_search_columns(tools):
    assert_markdown(await tools["search_columns"](pattern="id"))


async def test_get_sample_data(tools, sample_table):
    if sample_table is None:
        pytest.skip("configured schema has no tables to sample")
    assert_markdown(await tools["get_sample_data"](table=sample_table))


async def test_refresh_schema(tools):
    assert_markdown(await tools["refresh_schema"]())


# --- Relationships / modules ---------------------------------------------------


async def test_get_relationships(tools):
    assert_markdown(await tools["get_relationships"]())


async def test_get_view_definition(tools, sample_view):
    if sample_view is None:
        pytest.skip("configured schema has no views")
    assert_markdown(await tools["get_view_definition"](view=sample_view))


async def test_list_functions(tools):
    assert_markdown(await tools["list_functions"]())


async def test_get_function_source(tools, sample_function):
    if sample_function is None:
        pytest.skip("configured schema has no functions")
    assert_markdown(await tools["get_function_source"](function=sample_function))


# --- Catalog / hypertables ------------------------------------------------------


async def test_list_extensions(tools):
    assert_markdown(await tools["list_extensions"]())


async def test_list_roles(tools):
    assert_markdown(await tools["list_roles"]())


async def test_list_hypertables(tools, caps):
    out = await tools["list_hypertables"]()
    if await caps.has_extension("timescaledb"):
        # The demo makes app_core.reading a hypertable, so the real surface
        # (dimensions, compression, policies) must render, not the envelope.
        assert_markdown(out)
        assert "reading" in out, out
        # "compress" alone is vacuous -- the table's "compression" column
        # header renders that substring for any hypertable regardless of
        # whether compression is configured. Assert on the compression
        # policy's own config instead: `compress_after: 7 days` only renders
        # when add_compression_policy(..., interval '7 days') is actually set
        # on `reading` (see ops/demo/01-schema.sql).
        assert '"compress_after": "7 days"' in out, (
            "reading's 7-day compression policy must be reported"
        )
    else:
        # Plain PostgreSQL DSN: the EXTENSION_MISSING envelope does NOT pass
        # assert_markdown (its header carries " · error") -- confirmed by
        # running the pre-existing one-line assertion against the un-migrated
        # demo, which failed. Check for the envelope explicitly instead.
        assert "EXTENSION_MISSING" in out or " · error" in out.split("\n", 1)[0]


# --- Diagnostics ----------------------------------------------------------------


async def test_server_info(tools):
    assert_markdown(await tools["server_info"]())


async def test_get_active_sessions(tools):
    assert_markdown(await tools["get_active_sessions"]())


async def test_get_blocking(tools):
    assert_markdown(await tools["get_blocking"]())


async def test_get_wait_stats(tools):
    assert_markdown(await tools["get_wait_stats"]())


async def test_get_index_health(tools):
    assert_markdown(await tools["get_index_health"]())


async def test_get_query_store(tools):
    assert_markdown(await tools["get_query_store"]())


async def test_get_table_health(tools):
    assert_markdown(await tools["get_table_health"]())


async def test_get_table_stats(tools, sample_table):
    if sample_table is None:
        pytest.skip("configured schema has no tables to report stats on")
    assert_markdown(await tools["get_table_stats"](table=sample_table))


async def test_get_space_usage(tools):
    assert_markdown(await tools["get_space_usage"]())


async def test_get_ontology(tools):
    assert_markdown(await tools["get_ontology"]())


# --- get_erd_widget: registered on the Apps extension, not register_all -------


@pytest.fixture(scope="session")
def erd_tool(live_db, settings, intro):
    from mcp.server.apps import Apps

    from pgllens.tools import erd

    apps = Apps()
    erd.register_apps(apps, live_db, settings, intro)
    binding = next(b for b in apps.tools() if b.fn.__name__ == "get_erd_widget")
    return binding.fn


async def test_get_erd(tools):
    assert_markdown(await tools["get_erd"](format="text"))


async def test_get_erd_widget(erd_tool):
    # No ctx: degrades to the plain text listing (see tools/erd.py).
    assert_markdown((await erd_tool()).content[0].text)


# --- Security proofs only a real server can give ------------------------------


async def test_write_is_rejected_by_the_engine_not_just_the_regex(live_db):
    # Prove the second wall independently: bypass assert_read_only via
    # run_system and confirm the session itself refuses the write.
    with pytest.raises(Exception, match="read-only"):
        await live_db.run_system("CREATE TEMP TABLE t_probe (a int)")


async def test_statement_timeout_is_enforced_server_side(dsn):
    # A fresh Db, NOT the shared fixture: statement_timeout is baked into the
    # conninfo when the pool opens, so mutating settings on an already-open
    # pool changes nothing and the test would silently pass for the wrong reason.
    from pgllens.config import Settings
    from pgllens.database.pool import Db

    db = Db(Settings(database_url=dsn, exposed_schemas="public", query_timeout_ms=500))
    try:
        with pytest.raises(Exception, match="statement timeout"):
            await db.run_system("SELECT pg_sleep(5)")
    finally:
        await db.close()


async def test_duplicate_column_join_returns_both_values(live_db):
    r = await live_db.run_readonly(
        "SELECT a.oid AS x, b.oid AS x FROM pg_class a JOIN pg_class b "
        "ON b.oid = a.oid ORDER BY 1 LIMIT 1"
    )
    assert r.columns == ["x", "x"]
    assert len(r.rows[0]) == 2
