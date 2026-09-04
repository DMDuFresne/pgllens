import re

import pytest

from pgllens.database.format import QueryResult
from pgllens.tools import sessions
from pgllens.tools._util import respond
from pgllens.tools.sessions import format_active_sessions, format_blocking, format_wait_stats
from pgllens.tools.statements import ORDER_COLUMNS, format_query_store, order_clause
from tests.conftest import make_registered

_STATEMENTS_COLUMNS = ["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
                       "shared_blks_hit", "shared_blks_read", "query"]


def _empty(columns):
    return QueryResult(columns, [], False)


# --- order_clause / ORDER_COLUMNS (security-critical: the one argument in the
# whole port that is destined for an ORDER BY) ---

def test_order_by_allowlist_maps_to_a_column():
    assert order_clause("total_time") == ORDER_COLUMNS["total_time"]


def test_unknown_order_by_is_rejected_not_interpolated():
    with pytest.raises(ValueError, match="total_time"):
        order_clause("total_exec_time; DROP TABLE t --")


def test_every_allowlist_value_is_a_bare_identifier():
    for col in ORDER_COLUMNS.values():
        assert re.fullmatch(r"[a-z_]+( DESC| ASC)?", col), col


# --- format_query_store ---

def test_format_query_store_renders_rows():
    result = QueryResult(_STATEMENTS_COLUMNS,
        [(123, 10, 1500.456, 150.0456, 1000, 500, 5, "SELECT * FROM users")], False)
    out = respond(format_query_store(result, "unknown", None, False, "total_time"))
    assert "123" in out and "1500.5" in out and "150.0" in out and "SELECT * FROM users" in out


def test_format_query_store_empty_result_reports_zero_tally():
    result = _empty(_STATEMENTS_COLUMNS)
    out = respond(format_query_store(result, "unknown", None, False, "total_time"))
    assert "0 statements" in out
    assert "None found." not in out


# --- format_active_sessions ---

def test_format_active_sessions_renders_rows():
    result = QueryResult(
        ["pid", "usename", "application_name", "client_addr", "state", "backend_type",
         "wait_event_type", "wait_event", "duration", "query"],
        [(123, "app_user", "psql", "127.0.0.1", "active", "client backend", None, None,
          "00:00:05", "SELECT 1")], False)
    resp = format_active_sessions(result, include_idle=False)
    out = respond(resp)
    assert "app_user" in out and "SELECT 1" in out


def test_format_active_sessions_none_found_for_empty_result():
    result = _empty(["pid", "usename", "application_name", "client_addr", "state", "backend_type",
                     "wait_event_type", "wait_event", "duration", "query"])
    resp = format_active_sessions(result, include_idle=False)
    assert resp.tally[0] == "0 sessions"


# --- get_active_sessions: backend_type filtering ---

async def test_get_active_sessions_filters_to_client_backends_by_default():
    mcp, db, _ = make_registered(sessions)
    await mcp.tools["get_active_sessions"]()
    sql, params = db.run_system.call_args[0]
    assert "backend_type" in sql
    assert params == (False, False)


async def test_get_active_sessions_include_background_shows_everything():
    mcp, db, _ = make_registered(sessions)
    await mcp.tools["get_active_sessions"](include_background=True)
    _sql, params = db.run_system.call_args[0]
    assert params == (False, True)


# --- get_active_sessions: always excludes pgllens's own pooled connections ---

def test_active_sessions_sql_excludes_pgllens_own_connections():
    assert "application_name IS DISTINCT FROM 'pgllens'" in sessions._ACTIVE_SESSIONS_SQL


# --- format_blocking ---

def test_format_blocking_renders_rows():
    result = QueryResult(
        ["blocked_pid", "blocked_query", "blocker_pid", "blocker_query", "waiting_duration"],
        [(101, "UPDATE t SET x=1", 202, "UPDATE t SET x=2", "00:00:10")], False)
    out = respond(format_blocking(result))
    assert "101" in out and "202" in out


def test_format_blocking_reports_no_blocking_for_empty_result():
    result = _empty(["blocked_pid", "blocked_query", "blocker_pid", "blocker_query",
                     "waiting_duration"])
    resp = format_blocking(result)
    assert resp.tally[0] == "0 blocked sessions"


# --- format_wait_stats ---

def test_format_wait_stats_renders_rows():
    result = QueryResult(["wait_event_type", "wait_event", "sessions"],
                         [("Lock", "relation", 3)], False)
    out = respond(format_wait_stats(result))
    assert "Lock" in out and "relation" in out and "3" in out


def test_format_wait_stats_none_found_for_empty_result():
    result = _empty(["wait_event_type", "wait_event", "sessions"])
    resp = format_wait_stats(result)
    assert resp.tally[0] == "0 wait events"


# --- get_wait_stats: background workers excluded by default ---

def test_wait_stats_sql_guards_backend_type_on_a_bound_param():
    assert "backend_type = 'client backend'" in sessions._WAIT_STATS_SQL


async def test_get_wait_stats_excludes_background_by_default():
    mcp, db, _ = make_registered(sessions)
    await mcp.tools["get_wait_stats"]()
    _sql, params = db.run_system.call_args[0]
    assert params == (False,)


async def test_get_wait_stats_include_background_true_passes_true():
    mcp, db, _ = make_registered(sessions)
    await mcp.tools["get_wait_stats"](include_background=True)
    _sql, params = db.run_system.call_args[0]
    assert params == (True,)
