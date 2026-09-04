from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.tools import statements
from pgllens.tools.statements import format_query_store
from tests.conftest import make_registered

_STATEMENTS_COLUMNS = [
    "queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
    "shared_blks_hit", "shared_blks_read", "query",
]

_SAMPLE_ROWS = [(1, 10, 123.4, 12.3, 100, 5, 1, "SELECT 1")]


def _fake_caps(version, has_ext=True, schema="public"):
    """`version` is the pg_stat_statements extversion tuple: 1.8 shipped with
    PG13, 1.9 with PG14 (adds pg_stat_statements_info), 1.11 with PG17 (adds
    stats_since)."""
    caps = MagicMock()
    caps.has_extension = AsyncMock(return_value=has_ext)
    caps.extension_version = AsyncMock(return_value=version)
    caps.extension_schema = AsyncMock(return_value=schema)
    return caps


def _statements_result():
    return QueryResult(_STATEMENTS_COLUMNS, _SAMPLE_ROWS, False)


async def test_header_renders_from_a_stats_reset_timestamp():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [("2026-01-01 00:00:00+00",)], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"]()
    assert "Statistics since reset: 2026-01-01 00:00:00+00" in out


async def test_null_stats_reset_reads_as_never_reset():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [(None,)], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"]()
    assert "Statistics since reset: never reset (since extension install)" in out


async def test_old_extension_header_falls_back_to_unknown():
    # extversion 1.8 has no pg_stat_statements_info -- true on PG13, and on a
    # PG16 that was pg_upgrade'd without ALTER EXTENSION UPDATE. Gating on the
    # server version would query a view that does not exist and turn the whole
    # tool into a database error.
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 8)))
    db.run_system = AsyncMock(return_value=_statements_result())
    out = await mcp.tools["get_query_store"]()
    assert ("Statistics since reset: unknown (pg_stat_statements_info needs "
            "pg_stat_statements 1.9+, PostgreSQL 14+)") in out
    # Never queries pg_stat_statements_info at all.
    assert db.run_system.call_count == 1
    assert "pg_stat_statements_info" not in db.run_system.call_args.args[0]


async def test_queries_are_qualified_with_the_extensions_own_schema():
    # The pgllens role's search_path is the exposed schema + pg_catalog only, so
    # an unqualified pg_stat_statements would not resolve; the extension's
    # actual schema (not always public) is quoted in.
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9), schema="Stats"))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [(None,)], False),
        _statements_result(),
    ])
    await mcp.tools["get_query_store"]()
    header_sql, main_sql = (c.args[0] for c in db.run_system.call_args_list)
    assert 'FROM "Stats".pg_stat_statements_info' in header_sql
    assert 'FROM "Stats".pg_stat_statements' in main_sql


async def test_since_on_pg17_adds_bound_where_clause():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 11)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [(None,)], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"](since="2026-01-01T00:00:00Z")
    main_call = db.run_system.call_args_list[1]
    sql, params = main_call.args[0], main_call.args[1]
    assert "WHERE stats_since >= %s::timestamptz" in sql
    assert params[0] == "2026-01-01T00:00:00Z"
    assert "since= needs" not in out


async def test_since_below_pg17_adds_caveat_and_no_where():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [(None,)], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"](since="2026-01-01T00:00:00Z")
    main_call = db.run_system.call_args_list[1]
    sql = main_call.args[0]
    assert "WHERE" not in sql
    assert "since= needs pg_stat_statements 1.11+ (PostgreSQL 17" in out
    assert "showing all entries." in out


async def test_invalid_since_returns_error_without_querying_db():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 11)))
    out = await mcp.tools["get_query_store"](since="not-a-date")
    assert "`since` is not an ISO-8601 timestamp: 'not-a-date'." in out
    db.run_system.assert_not_called()


async def test_valid_since_on_pg17_adds_a_filtered_tally_entry():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 11)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [(None,)], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"](since="2026-01-01T00:00:00Z")
    assert "since 2026-01-01T00:00:00Z" in out


async def test_missing_stats_reset_row_reads_as_unknown():
    # pg_stat_statements_info exists but returns no row (or the read was denied):
    # the header must say "unknown", not raise IndexError.
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["stats_reset"], [], False),
        _statements_result(),
    ])
    out = await mcp.tools["get_query_store"]()
    assert "Statistics since reset: unknown" in out


async def test_unknown_order_by_is_rejected_with_a_hint():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    out = await mcp.tools["get_query_store"](order_by="nope")
    assert "order_by must be one of" in out
    db.run_system.assert_not_called()


async def test_limit_out_of_range_is_rejected():
    mcp, db, _ = make_registered(statements, caps=_fake_caps((1, 9)))
    out = await mcp.tools["get_query_store"](limit=0)
    assert "limit" in out and "between 1 and 100" in out
    db.run_system.assert_not_called()


# --- format_query_store: Next only when the displayed query wasn't truncated ---

def test_next_omitted_when_top_query_was_truncated():
    long_query = "SELECT " + "a" * 200
    result = QueryResult(_STATEMENTS_COLUMNS, [(1, 10, 123.4, 12.3, 100, 5, 1, long_query)], False)
    resp = format_query_store(result, "unknown", None, False, "total_time")
    assert resp.next == ()


def test_next_offers_explain_query_for_a_short_top_query():
    result = format_query_store(_statements_result(), "unknown", None, False, "total_time")
    assert result.next[0].tool == "explain_query"
    assert result.next[0].kwargs == {"sql": "SELECT 1"}
