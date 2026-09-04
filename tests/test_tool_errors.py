from datetime import UTC, datetime

import pytest
from mcp.types import CallToolResult

from pgllens.database.capability import ExtensionMissingError
from pgllens.database.format import QueryResult, table_from
from pgllens.database.introspect import TableNotFoundError
from pgllens.database.pool import UnknownSchemaError
from pgllens.database.safety import UnsafeQueryError
from pgllens.llens_style import ErrorCode, lint
from pgllens.tools import _util
from pgllens.tools._util import ArgOutOfRangeError, ToolError, check_range, tool_errors

NOW = datetime(2026, 9, 3, 15, 49, 3, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _frozen(monkeypatch):
    monkeypatch.setattr(_util, "_now", lambda: NOW)
    monkeypatch.setattr(_util, "_request_id", lambda: "01TESTREQUESTID")


def _tool(exc):
    @tool_errors
    async def some_tool() -> str:
        raise exc
    return some_tool


@pytest.mark.parametrize(("exc", "code", "outcome"), [
    (UnsafeQueryError("DELETE is not allowed"), "QUERY_REJECTED", "rejected"),
    (UnknownSchemaError("Schema 'x' is not exposed"), "SCHEMA_UNKNOWN", "unknown_schema"),
    (TableNotFoundError("Table 'nope' not found."), "TABLE_NOT_FOUND", "not_found"),
    (ExtensionMissingError("timescaledb"), "EXTENSION_MISSING", "unavailable"),
    (ArgOutOfRangeError("limit", 0, 1, 1000), "ARG_OUT_OF_RANGE", "rejected"),
    (ToolError(ErrorCode.FORMAT_UNKNOWN, "bad", "Pass one of: a, b."), "FORMAT_UNKNOWN", "rejected"),
    (RuntimeError("canceling statement due to statement timeout"), "TIMEOUT", "db_error"),
    (RuntimeError("relation does not exist\nLINE 1: ..."), "DB_ERROR", "db_error"),
])
async def test_exception_map(exc, code, outcome, monkeypatch):
    seen = {}
    monkeypatch.setattr(_util.metrics, "record_tool_call",
                        lambda tool, oc, d, **_: seen.setdefault("outcome", oc))
    out = await _tool(exc)()
    assert out.startswith("## pgllens · some_tool · error\n")
    assert f"- code: `{code}`" in out
    assert "- request_id: `01TESTREQUESTID`" in out
    assert lint(out) == []
    assert seen["outcome"] == outcome


async def test_empty_exception_message_falls_back_to_type_name(monkeypatch):
    seen = {}
    monkeypatch.setattr(_util.metrics, "record_tool_call",
                        lambda tool, oc, d, **_: seen.setdefault("outcome", oc))
    out = await _tool(RuntimeError())()
    assert "- code: `DB_ERROR`" in out
    assert "- message: RuntimeError\n" in out
    assert seen["outcome"] == "db_error"


async def test_db_error_message_is_first_line_only():
    out = await _tool(RuntimeError("relation does not exist\nLINE 1: ..."))()
    assert "- message: relation does not exist\n" in out and "LINE 1" not in out


async def test_timeout_carries_retry_after():
    out = await _tool(RuntimeError("canceling statement due to statement timeout"))()
    assert "- retry_after: `" in out


async def test_call_tool_result_functions_get_wrapped_errors():
    @tool_errors
    async def widget() -> CallToolResult:
        raise UnsafeQueryError("nope")
    res = await widget()
    assert isinstance(res, CallToolResult)
    assert res.content[0].text.startswith("## pgllens · widget · error")
    assert res.structured_content is None


def test_check_range():
    check_range("limit", 5, 1, 10)
    with pytest.raises(ArgOutOfRangeError) as ei:
        check_range("limit", 0, 1, 10)
    assert ei.value.message == "`limit` must be between 1 and 10 (got 0)."
    assert ei.value.hint == "Pass `limit` between 1 and 10."


def test_table_from_formats_cells_and_can_rename_columns():
    t = table_from(QueryResult(["a", "b"], [(None, True), ("x|y", 3)], False), columns=["col a", "b"])
    assert t.columns == ("col a", "b")
    assert t.rows == (("NULL", "true"), ("x|y", "3"))


def test_request_id_prefers_correlation_context(monkeypatch):
    monkeypatch.undo()
    from pgllens.obs import correlation
    correlation.set_correlation_id("abc123")
    assert _util._request_id() == "abc123"
