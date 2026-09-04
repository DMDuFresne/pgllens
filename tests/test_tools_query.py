from typing import ClassVar

import pytest

from pgllens.database.format import QueryResult
from pgllens.database.safety import UnsafeQueryError
from pgllens.tools._util import ArgOutOfRangeError, check_range, clean_db_error, tool_errors

DSN = "postgresql://u:p@localhost:5432/flux"


async def test_tool_errors_turns_a_rejection_into_markdown():
    @tool_errors
    async def boom() -> str:
        raise UnsafeQueryError("DELETE is not permitted (read-only lens)")

    out = await boom()
    assert "- code: `QUERY_REJECTED`" in out
    assert "DELETE" in out


# I3: tool_errors' finally block is the single audit + metrics instrumentation
# point for every @mcp.tool -- deleting it broke nothing in the existing suite.
async def test_tool_errors_emits_a_metric_and_an_audit_line_on_the_ok_path(monkeypatch):
    from pgllens.obs import audit as audit_mod
    from pgllens.obs import metrics

    metric_calls = []
    audit_calls = []
    monkeypatch.setattr(metrics, "record_tool_call",
                        lambda tool, outcome, duration_s, **_: metric_calls.append((tool, outcome)))
    monkeypatch.setattr(audit_mod, "audit",
                        lambda event, **fields: audit_calls.append((event, fields)))

    @tool_errors
    async def some_tool() -> str:
        return "ok"

    assert await some_tool() == "ok"
    assert metric_calls == [("some_tool", "ok")]
    assert audit_calls[0][0] == "tool_call"
    assert audit_calls[0][1]["tool"] == "some_tool"
    assert audit_calls[0][1]["outcome"] == "ok"
    assert "duration_ms" in audit_calls[0][1]


async def test_tool_errors_emits_a_metric_and_an_audit_line_on_the_rejected_path(monkeypatch):
    from pgllens.obs import audit as audit_mod
    from pgllens.obs import metrics

    metric_calls = []
    audit_calls = []
    monkeypatch.setattr(metrics, "record_tool_call",
                        lambda tool, outcome, duration_s, **_: metric_calls.append((tool, outcome)))
    monkeypatch.setattr(audit_mod, "audit",
                        lambda event, **fields: audit_calls.append((event, fields)))

    @tool_errors
    async def some_tool() -> str:
        raise UnsafeQueryError("nope")

    await some_tool()
    assert metric_calls == [("some_tool", "rejected")]
    assert audit_calls[0][0] == "tool_call"
    assert audit_calls[0][1]["tool"] == "some_tool"
    assert audit_calls[0][1]["outcome"] == "rejected"


async def test_tool_errors_turns_a_driver_error_into_markdown():
    @tool_errors
    async def boom() -> str:
        raise RuntimeError('column "nope" does not exist\nLINE 1: SELECT nope')

    out = await boom()
    assert "- code: `DB_ERROR`" in out
    assert "LINE 1" not in out, "the LINE/caret block is noise for the model"


def test_clean_db_error_keeps_the_sentence_drops_the_position_block():
    msg = ('column "nope" does not exist\n'
           "LINE 1: SELECT nope FROM t\n"
           "               ^\n")
    assert clean_db_error(RuntimeError(msg)) == 'column "nope" does not exist'


def test_check_range_rejects_out_of_range_value():
    with pytest.raises(ArgOutOfRangeError) as exc_info:
        check_range("page", 0, 1, 10000)
    err = exc_info.value
    assert err.message == "`page` must be between 1 and 10000 (got 0)."
    assert err.hint == "Pass `page` between 1 and 10000."


def test_check_range_accepts_in_range_value():
    check_range("page", 1, 1, 10000)  # no raise


def test_page_out_of_range_is_rejected_not_clamped():
    from pgllens.tools.query import build_paged_sql
    with pytest.raises(ValueError):
        build_paged_sql("SELECT 1 ORDER BY 1", page=0, max_rows=100)


def test_page_over_one_requires_order_by():
    from pgllens.tools.query import build_paged_sql
    with pytest.raises(ValueError, match="ORDER BY"):
        build_paged_sql("SELECT 1", page=2, max_rows=100)


def test_paging_appends_limit_offset_on_its_own_line():
    from pgllens.tools.query import build_paged_sql
    sql = build_paged_sql("SELECT a FROM t ORDER BY a -- newest first", page=3,
                          max_rows=100)
    # Own line: a same-line append is swallowed by a trailing -- comment,
    # silently returning page 1 mislabeled as page 3.
    assert sql.splitlines()[-1] == "LIMIT 101 OFFSET 200"


def test_page_one_is_unmodified():
    from pgllens.tools.query import build_paged_sql
    assert build_paged_sql("SELECT 1", page=1, max_rows=100) == "SELECT 1"


def test_redaction_masks_a_matching_column():
    from pgllens.database.format import redact
    r = QueryResult(["sku", "password_hash"], [("A1", "deadbeef")], False)
    out = redact(r, ["%password%"])
    assert out.rows == [("A1", "[masked]")]


# --- I2: query.py's cost gate (query.py:~59-64) had zero coverage -- deleting
# it broke nothing. These pin the gate itself, not just the estimated_cost
# helper (test_tools_explain.py). ---

class _CostGateDb:
    def __init__(self, plan_cost, plan_rows=1, truncated=False):
        self._plan_cost = plan_cost
        self._plan_rows = plan_rows
        self._truncated = truncated
        self.run_system_calls = []
        self.run_readonly_calls = []
        self.run_readonly_caps = []

    async def run_system(self, sql, params=(), max_rows=None):
        self.run_system_calls.append(sql)
        plan = [{"Plan": {"Node Type": "Seq Scan", "Total Cost": self._plan_cost,
                          "Plan Rows": self._plan_rows}}]
        return QueryResult(["QUERY PLAN"], [(plan,)], False)

    async def run_readonly(self, sql, max_rows=None):
        self.run_readonly_calls.append(sql)
        self.run_readonly_caps.append(max_rows)
        return QueryResult(["a"], [(1,)], self._truncated)


class _CostGateSettings:
    max_rows = 1000
    redact_columns: ClassVar[list[str]] = []

    def __init__(self, max_estimated_cost=None, tool_cost_budget_per_minute=None):
        self.max_estimated_cost = max_estimated_cost
        self.tool_cost_budget_per_minute = tool_cost_budget_per_minute


def _register_query():
    from pgllens.tools import query as query_mod
    from tests.conftest import FakeMCP

    mcp = FakeMCP()
    return mcp, query_mod


class _TruncatedDb:
    def __init__(self, truncated):
        self._truncated = truncated

    async def run_readonly(self, sql, max_rows=None):
        return QueryResult(["a"], [(1,)], self._truncated)


async def test_truncated_query_output_has_exactly_one_truncation_message():
    # The envelope's tally is the only truncation signal now -- one line, not
    # a generic format.py warning plus query.py's page suffix both firing.
    mcp, query_mod = _register_query()
    settings = _CostGateSettings()
    query_mod.register(mcp, _TruncatedDb(True), settings, None, None)

    out = await mcp.tools["query"]("SELECT 1 ORDER BY 1")

    assert out.count("more rows exist") == 1
    assert 'query(sql=' in out


async def test_truncated_query_without_order_by_does_not_suggest_a_page():
    # build_paged_sql rejects page > 1 when the query has no ORDER BY, so the
    # truncation message must not dangle a "request page=2" suggestion the
    # validator would immediately bounce -- offer LIMIT/WHERE or ORDER BY instead.
    mcp, query_mod = _register_query()
    settings = _CostGateSettings()
    query_mod.register(mcp, _TruncatedDb(True), settings, None, None)

    out = await mcp.tools["query"]("SELECT 1")

    assert "more rows exist" in out
    assert "Next: query(sql=" not in out
    assert "ORDER BY" in out


async def test_untruncated_query_output_has_no_truncation_message():
    mcp, query_mod = _register_query()
    settings = _CostGateSettings()
    query_mod.register(mcp, _TruncatedDb(False), settings, None, None)

    out = await mcp.tools["query"]("SELECT 1")

    assert "more rows exist" not in out


async def test_query_over_the_cost_limit_is_rejected_and_never_runs():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=5000.0)
    settings = _CostGateSettings(max_estimated_cost=100.0)
    query_mod.register(mcp, db, settings, None, None)

    out = await mcp.tools["query"]("SELECT 1")

    assert "- code: `QUERY_REJECTED`" in out
    assert "Rejected by the cost gate" in out
    assert "100" in out
    assert db.run_readonly_calls == [], "the query must never run once the gate rejects it"


async def test_query_under_the_cost_limit_runs_normally():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=10.0)
    settings = _CostGateSettings(max_estimated_cost=100.0)
    query_mod.register(mcp, db, settings, None, None)

    out = await mcp.tools["query"]("SELECT 1")

    assert "Rejected by the cost gate" not in out
    assert db.run_readonly_calls == ["SELECT 1"]


# --- Task 8 fix round: exercise the real charge_cost call through the query
# tool (not just charge_cost() in isolation, tested in test_limits.py) ---

async def test_query_charges_the_real_cost_budget_and_rejects_once_spent():
    from pgllens.caller import Caller, reset_caller, set_caller
    from pgllens.limits import InMemoryLimitStore, configure_limits

    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=60.0)
    settings = _CostGateSettings(tool_cost_budget_per_minute=100.0)
    query_mod.register(mcp, db, settings, None, None)
    configure_limits(settings, InMemoryLimitStore())

    token = set_caller(Caller(client_id="c1"))
    try:
        first = await mcp.tools["query"]("SELECT 1")
        second = await mcp.tools["query"]("SELECT 1")  # 60 + 60 = 120 > 100
    finally:
        reset_caller(token)

    assert "Rejected by the cost gate" not in first
    assert "- code: `QUERY_REJECTED`" in second
    assert "Rejected by the cost budget" in second
    # The first call ran; the second was rejected before touching the DB.
    assert db.run_readonly_calls == ["SELECT 1"]


async def test_query_cost_budget_keys_on_peer_ip_when_no_authenticated_client_id():
    # Point 4: the budget must never pool every unauthenticated caller onto
    # one shared "anonymous" bucket -- it falls back to the peer IP, same as
    # ConcurrencyLimitMiddleware.
    from pgllens.caller import Caller, reset_caller, set_caller
    from pgllens.limits import InMemoryLimitStore, configure_limits

    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=60.0)
    settings = _CostGateSettings(tool_cost_budget_per_minute=100.0)
    query_mod.register(mcp, db, settings, None, None)
    store = InMemoryLimitStore()
    configure_limits(settings, store)

    token = set_caller(Caller(client_id="anonymous", ip="10.0.0.5"))
    try:
        await mcp.tools["query"]("SELECT 1")
    finally:
        reset_caller(token)

    assert await store.incr("cost:10.0.0.5", 0, 60) == 60.0
    assert await store.incr("cost:anonymous", 0, 60) == 0.0


# --- Task 1: `limit` page size and the EXPLAIN-first row governor ---


def test_explicit_limit_appends_a_limit_on_page_one():
    from pgllens.tools.query import build_paged_sql
    # +1 row so has-more detection still works, and on its OWN line so a
    # trailing -- comment can't swallow it.
    sql = build_paged_sql("SELECT a FROM t -- newest", page=1, max_rows=20,
                          always_limit=True)
    assert sql.splitlines()[-1] == "LIMIT 21"


def test_explicit_limit_pages_with_the_limit_as_the_page_size():
    from pgllens.tools.query import build_paged_sql
    sql = build_paged_sql("SELECT a FROM t ORDER BY a", page=2, max_rows=20,
                          always_limit=True)
    assert sql.splitlines()[-1] == "LIMIT 21 OFFSET 20"


def test_a_query_with_its_own_limit_is_rejected_rather_than_double_limited():
    # PostgreSQL rejects "multiple LIMIT clauses"; the has-more footer suggests
    # both `limit=` and adding LIMIT, so a model following both hits this.
    import pytest

    from pgllens.tools.query import build_paged_sql
    for sql in ("SELECT id FROM t ORDER BY id LIMIT 5",
                "select id from t order by id\nlimit 5;",
                "SELECT id FROM t ORDER BY id FETCH FIRST 5 ROWS ONLY"):
        with pytest.raises(ValueError, match="already has its own LIMIT/FETCH"):
            build_paged_sql(sql, page=1, max_rows=20, always_limit=True)
        with pytest.raises(ValueError, match="already has its own LIMIT/FETCH"):
            build_paged_sql(sql, page=2, max_rows=20)
    # Page 1 with no `limit` never touches the SQL, so the user's LIMIT stands.
    assert build_paged_sql("SELECT 1 LIMIT 5", page=1, max_rows=20) == "SELECT 1 LIMIT 5"


async def test_query_limit_is_used_as_the_page_size():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    settings = _CostGateSettings()
    query_mod.register(mcp, db, settings, None, None)

    await mcp.tools["query"]("SELECT a FROM t", limit=20)

    assert db.run_readonly_calls[0].splitlines()[-1] == "LIMIT 21"
    assert db.run_readonly_caps == [20], "run_readonly must truncate at the limit"


async def test_query_without_limit_leaves_page_one_untouched():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    await mcp.tools["query"]("SELECT a FROM t")

    assert db.run_readonly_calls == ["SELECT a FROM t"]
    assert db.run_readonly_caps == [None]


@pytest.mark.parametrize("bad", [0, 1001])
async def test_query_limit_out_of_range_is_rejected_naming_the_server_max(bad):
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", limit=bad)

    assert "- code: `ARG_OUT_OF_RANGE`" in out
    assert db.run_readonly_calls == []


async def test_explain_first_prefixes_the_estimate_and_still_runs():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=42.0, plan_rows=1500)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", explain_first=True)

    assert "- estimated rows:" in out
    assert "~1.5K" in out
    assert "- estimated cost: `42`" in out
    assert db.run_readonly_calls == ["SELECT 1"]


async def test_explain_first_says_unavailable_and_still_runs_on_a_bad_plan(monkeypatch):
    from unittest.mock import AsyncMock

    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)
    monkeypatch.setattr("pgllens.tools.explain.plan_estimate", AsyncMock(return_value=None))

    out = await mcp.tools["query"]("SELECT 1", explain_first=True)

    assert "plan estimate: `unavailable`" in out
    assert db.run_readonly_calls == ["SELECT 1"]


async def test_max_estimated_rows_refuses_before_executing():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0, plan_rows=500)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", max_estimated_rows=10)

    assert "- code: `QUERY_REJECTED`" in out
    assert "500 rows" in out
    assert "`max_estimated_rows`=10" in out
    assert db.run_readonly_calls == [], "the query must never run once refused"


async def test_max_estimated_rows_under_the_ceiling_runs_with_the_estimate_line():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0, plan_rows=5)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", max_estimated_rows=10)

    assert "- estimated rows:" in out
    assert db.run_readonly_calls == ["SELECT 1"]


async def test_max_estimated_rows_never_refuses_on_an_unavailable_estimate(monkeypatch):
    from unittest.mock import AsyncMock

    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)
    monkeypatch.setattr("pgllens.tools.explain.plan_estimate", AsyncMock(return_value=None))

    out = await mcp.tools["query"]("SELECT 1", max_estimated_rows=1)

    assert "Refused" not in out
    assert "QUERY_REJECTED" not in out
    assert db.run_readonly_calls == ["SELECT 1"]


async def test_max_estimated_rows_below_one_is_rejected():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", max_estimated_rows=0)

    assert "- code: `ARG_OUT_OF_RANGE`" in out
    assert db.run_readonly_calls == []


async def test_governor_explains_the_unpaged_sql_not_the_paged_one():
    # The governor is about TOTAL work, so it must plan the query the caller
    # wrote -- not the LIMIT-ed page, whose estimate would always pass.
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0, plan_rows=5)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    await mcp.tools["query"]("SELECT a FROM t", limit=20, explain_first=True)

    assert db.run_system_calls == ["EXPLAIN (FORMAT JSON) SELECT a FROM t"]


# --- Task 1, fix round 1 ---


async def test_truncation_footer_carries_the_limit_into_the_next_page():
    # Without "with limit=20" the model calls query(sql, page=2), whose OFFSET
    # is then computed from settings.max_rows -- silently skipping rows 21..1000.
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0, truncated=True)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT a FROM t ORDER BY a", limit=20)

    assert 'query(sql="SELECT a FROM t ORDER BY a", page=2, limit=20)' in out


async def test_truncation_footer_omits_the_limit_when_none_was_given():
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0, truncated=True)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT a FROM t ORDER BY a")

    assert 'query(sql="SELECT a FROM t ORDER BY a", page=2)' in out
    assert "limit=" not in out.split("Next:")[1]


async def test_explain_first_estimate_is_shown_on_a_cost_gate_rejection():
    # The estimate is most useful exactly when the gate refuses: it tells the
    # model how far over it was without a second round trip.
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=5000.0, plan_rows=900)
    settings = _CostGateSettings(max_estimated_cost=100.0)
    query_mod.register(mcp, db, settings, None, None)

    out = await mcp.tools["query"]("SELECT 1", explain_first=True)

    assert "- code: `QUERY_REJECTED`" in out
    assert "Rejected by the cost gate" in out
    assert db.run_readonly_calls == []


# L5: an unparseable EXPLAIN shape makes the cost gate fail open silently --
# never observable in the logs. It must at least warn.
async def test_unparseable_cost_logs_a_warning_instead_of_silently_skipping(
    caplog, monkeypatch
):
    from unittest.mock import AsyncMock

    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=5000.0)
    settings = _CostGateSettings(max_estimated_cost=100.0)
    query_mod.register(mcp, db, settings, None, None)
    monkeypatch.setattr(
        "pgllens.tools.explain.estimated_cost", AsyncMock(return_value=None)
    )

    with caplog.at_level("WARNING", logger="pgllens"):
        await mcp.tools["query"]("SELECT 1")

    assert any("cost gate skipped" in r.message for r in caplog.records)


async def test_max_estimated_rows_above_the_ceiling_is_rejected():
    # The message promises "between 1 and 2000000000"; the guard must enforce
    # the upper bound too, and refuse before planning or executing anything.
    mcp, query_mod = _register_query()
    db = _CostGateDb(plan_cost=1.0)
    query_mod.register(mcp, db, _CostGateSettings(), None, None)

    out = await mcp.tools["query"]("SELECT 1", max_estimated_rows=10**15)

    assert "- code: `ARG_OUT_OF_RANGE`" in out
    assert db.run_system_calls == [], "must never plan once out of range"
    assert db.run_readonly_calls == []


# --- Reconcile: the LIMIT guard runs on scrubbed, paren-stripped SQL ---


def test_limit_guard_ignores_literals_identifiers_comments_and_subqueries():
    from pgllens.tools.query import build_paged_sql
    for sql in ("SELECT 'limit exceeded' AS msg FROM t",
                'SELECT "limit" FROM t',
                "SELECT a FROM t -- limit 5 rows please",
                "SELECT * FROM (SELECT a FROM t ORDER BY a LIMIT 100) s"):
        assert build_paged_sql(sql, page=1, max_rows=50,
                               always_limit=True).splitlines()[-1] == "LIMIT 51"


def test_limit_guard_still_rejects_a_real_top_level_limit():
    import pytest

    from pgllens.tools.query import build_paged_sql
    with pytest.raises(ValueError, match="already has its own LIMIT/FETCH"):
        build_paged_sql("SELECT a FROM t ORDER BY a LIMIT 10", page=1,
                        max_rows=50, always_limit=True)


def test_order_by_paging_check_reads_scrubbed_top_level_sql():
    import pytest

    from pgllens.tools.query import build_paged_sql
    for sql in ("SELECT a FROM t -- order by a later",
                "SELECT 'order by' AS msg FROM t",
                "SELECT * FROM (SELECT a FROM t ORDER BY a) s"):
        with pytest.raises(ValueError, match="requires an ORDER BY"):
            build_paged_sql(sql, page=2, max_rows=20)
    assert build_paged_sql("SELECT a FROM t ORDER BY a", page=2,
                           max_rows=20).splitlines()[-1] == "LIMIT 21 OFFSET 20"
