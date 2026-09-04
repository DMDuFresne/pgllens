import json

import pytest

from pgllens.database.format import QueryResult
from pgllens.database.safety import UnsafeQueryError
from pgllens.tools.explain import estimated_cost, format_plan, plan_estimate

PLAN = [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "item_master",
                  "Total Cost": 1234.5, "Plan Rows": 1200,
                  "Plans": [{"Node Type": "Index Scan", "Total Cost": 8.2,
                             "Relation Name": "lot", "Plan Rows": 3}]}}]


class FakeDb:
    def __init__(self, payload):
        self.payload = payload
        self.last_sql = None
        self.calls = []

    async def run_system(self, sql, params=(), max_rows=None):
        self.calls.append(sql)
        self.last_sql = sql
        return QueryResult(["QUERY PLAN"], [(self.payload,)], False)


async def test_estimated_cost_reads_the_top_node():
    assert await estimated_cost(FakeDb(PLAN), "SELECT 1") == 1234.5


async def test_estimated_cost_accepts_a_json_string_payload():
    # psycopg returns json as str on some server/driver combinations
    assert await estimated_cost(FakeDb(json.dumps(PLAN)), "SELECT 1") == 1234.5


async def test_estimated_cost_returns_none_on_an_unusable_plan():
    assert await estimated_cost(FakeDb([{"nope": 1}]), "SELECT 1") is None


async def test_estimated_cost_never_uses_analyze():
    db = FakeDb(PLAN)
    await estimated_cost(db, "SELECT 1")
    assert "ANALYZE" not in db.last_sql.upper(), "the cost gate must not execute the query"


# C1: the cost gate's own SQL call reaches db.run_system (which bypasses
# assert_read_only, unlike run_readonly) with the caller's raw SQL. If
# estimated_cost doesn't validate first, a stacked statement rides the same
# EXPLAIN round trip on the simple query protocol.
async def test_estimated_cost_rejects_a_stacked_statement_before_touching_the_db():
    db = FakeDb(PLAN)
    with pytest.raises(UnsafeQueryError):
        await estimated_cost(db, "SELECT 1; SET default_transaction_read_only=off")
    assert db.calls == [], "run_system must never be reached for an unsafe query"


# Task 1: plan_estimate is the shared primitive -- it returns BOTH numbers the
# governor needs; estimated_cost is now a one-line wrapper over it.
async def test_plan_estimate_returns_cost_and_plan_rows():
    assert await plan_estimate(FakeDb(PLAN), "SELECT 1") == (1234.5, 1200.0)


async def test_plan_estimate_returns_none_on_an_unusable_plan():
    assert await plan_estimate(FakeDb([{"nope": 1}]), "SELECT 1") is None


async def test_plan_estimate_tolerates_a_plan_without_plan_rows():
    # Cost is what the cost gate needs; a missing Plan Rows must not make the
    # whole estimate unavailable and fail the gate open. 0 rows can never trip
    # the row governor either.
    plan = [{"Plan": {"Node Type": "Seq Scan", "Total Cost": 5.0}}]
    assert await plan_estimate(FakeDb(plan), "SELECT 1") == (5.0, 0.0)
    assert await estimated_cost(FakeDb(plan), "SELECT 1") == 5.0


async def test_estimated_cost_returns_none_on_zero_row_explain():
    from unittest.mock import AsyncMock, MagicMock

    db = MagicMock()
    db.run_system = AsyncMock(return_value=QueryResult(["plan"], [], False))
    assert await estimated_cost(db, "SELECT 1") is None


def test_format_plan_indents_child_nodes():
    out = format_plan(PLAN)
    assert "Seq Scan" in out and "Index Scan" in out
    assert out.index("Seq Scan") < out.index("Index Scan")
    assert "  " in out.split("Index Scan")[0].splitlines()[-1]


ANALYZE_PLAN = [{
    "Plan": {
        "Node Type": "Aggregate",
        "Strategy": "Hashed",
        "Partial Mode": "Simple",
        "Total Cost": 500.0,
        "Plan Rows": 10,
        "Actual Rows": 12,
        "Actual Total Time": 3.456,
        "Actual Loops": 1,
        "Plans": [{
            "Node Type": "Index Scan",
            "Relation Name": "item_master",
            "Index Name": "item_master_pkey",
            "Index Cond": "(id = 1)",
            "Filter": "(qty > 0)",
            "Total Cost": 8.2,
            "Plan Rows": 3,
            "Actual Rows": 3,
            "Actual Total Time": 0.012,
            "Actual Loops": 1,
        }],
    },
    "Planning Time": 0.123,
    "Execution Time": 3.789,
}]


def test_format_plan_emits_analyze_timings_and_node_detail():
    out = format_plan(ANALYZE_PLAN)
    assert "actual=0.012ms rows=3 loops=1" in out
    assert "actual=3.456ms rows=12 loops=1" in out
    assert "Index Name: item_master_pkey" in out
    assert "Index Cond: (id = 1)" in out
    assert "Filter: (qty > 0)" in out
    assert "Strategy: Hashed" in out
    assert "Partial Mode: Simple" in out
    assert "Planning Time: 0.123ms" in out
    assert "Execution Time: 3.789ms" in out


def test_format_plan_omits_missing_actual_rows_and_loops():
    # A node can carry "Actual Total Time" with rows/loops absent (e.g. a
    # trimmed/partial plan payload) -- each segment must be independently
    # optional, never rendered as the literal string "None".
    plan = [{"Plan": {"Node Type": "Seq Scan", "Actual Total Time": 1.5}}]
    out = format_plan(plan)
    assert "actual=1.5ms" in out
    assert "None" not in out


def test_format_plan_plain_plan_has_no_actual_text():
    out = format_plan(PLAN)
    assert "actual=" not in out
    assert "Planning Time" not in out
    assert "Execution Time" not in out


def test_format_plan_function_scan_uses_function_name():
    # audit Q3: a Function Scan node has no "Relation Name" -- the function
    # it scans lives under "Function Name" instead, which the compact
    # renderer was silently dropping.
    plan = [{"Plan": {"Node Type": "Function Scan", "Function Name": "fn_x",
                       "Total Cost": 10.0, "Plan Rows": 5}}]
    out = format_plan(plan)
    assert "Function Scan on fn_x" in out


async def test_explain_query_analyze_sends_analyze_format_json():
    from pgllens.tools.explain import register

    db = FakeDb(PLAN)
    captured = {}

    class CapturingMCP:
        def tool(self, *args, **kwargs):
            def deco(fn):
                captured["explain_query"] = fn
                return fn
            return deco

    register(CapturingMCP(), db, None, None, None)
    await captured["explain_query"]("SELECT 1", analyze=True)
    assert db.calls, "run_system should have been called"
    assert "ANALYZE, FORMAT JSON" in db.calls[0]
