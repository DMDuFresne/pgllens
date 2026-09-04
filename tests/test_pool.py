import pytest

from pgllens.config import Settings
from pgllens.database.pool import Db, UnknownSchemaError, rows_from_cursor
from pgllens.database.safety import UnsafeQueryError

DSN = "postgresql://u:p@localhost:5432/flux"


def _settings(**kw):
    return Settings(database_url=DSN, exposed_schemas="public,wms", **kw)


class FakeCursor:
    """Stands in for psycopg's cursor: description is a sequence of column
    descriptors whose .name may REPEAT across a join."""

    def __init__(self, names, rows):
        self.description = [type("D", (), {"name": n})() for n in names]
        self._rows = rows

    def fetchmany(self, n):
        return self._rows[:n]


def test_duplicate_column_names_are_all_preserved():
    # The TS server dropped these: mapping rows to objects keeps only the last
    # `created_at`. QueryResult is positional, so both values survive.
    cur = FakeCursor(["created_at", "created_at"], [("2026-01-01", "2026-02-02")])
    result = rows_from_cursor(cur, max_rows=10)
    assert result.columns == ["created_at", "created_at"]
    assert result.rows == [("2026-01-01", "2026-02-02")]


def test_truncation_flag_set_when_over_max_rows():
    cur = FakeCursor(["a"], [(1,), (2,), (3,)])
    result = rows_from_cursor(cur, max_rows=2)
    assert len(result.rows) == 2
    assert result.truncated is True


def test_no_truncation_at_exactly_max_rows():
    cur = FakeCursor(["a"], [(1,), (2,)])
    result = rows_from_cursor(cur, max_rows=2)
    assert result.truncated is False


def test_non_select_statement_yields_empty_result():
    cur = FakeCursor([], [])
    cur.description = None
    assert rows_from_cursor(cur, max_rows=10).columns == []


def test_resolve_schema_defaults_to_first_exposed():
    assert Db(_settings()).resolve_schema(None) == "public"


def test_resolve_schema_is_case_insensitive():
    assert Db(_settings()).resolve_schema("WMS") == "wms"


def test_unexposed_schema_rejected_and_names_the_alternatives():
    with pytest.raises(UnknownSchemaError, match="public, wms"):
        Db(_settings()).resolve_schema("pg_catalog")


def test_conninfo_carries_read_only_and_timeout():
    ci = _settings(query_timeout_ms=5000).conninfo()
    assert "default_transaction_read_only%3Don" in ci or "default_transaction_read_only=on" in ci
    assert "statement_timeout" in ci


def test_conninfo_options_value_parses_as_a_single_uri_query_param():
    # Regression: the `-c key=value` pairs inside `options=` embed a literal `=`.
    # Only percent-encoding spaces (the old `.replace(' ', '%20')`) leaves that
    # `=` unescaped, and libpq's URI parser then reads it as a second key/value
    # separator inside the "options" query param and rejects the whole DSN with
    # 'extra key/value separator "=" in URI query parameter: "options"' --
    # every pool connection fails. conninfo_to_dict is psycopg's own URI parser
    # (no network needed) -- if it can parse this, so can libpq.
    from psycopg.conninfo import conninfo_to_dict

    ci = _settings(query_timeout_ms=5000).conninfo()
    parsed = conninfo_to_dict(ci)
    opts = parsed["options"]
    assert "default_transaction_read_only=on" in opts
    assert "statement_timeout=5000" in opts
    assert "idle_in_transaction_session_timeout=5000" in opts


def test_conninfo_options_parses_when_database_url_already_has_a_query_string():
    s = Settings(database_url=f"{DSN}?sslmode=require", exposed_schemas="public",
                 query_timeout_ms=5000)
    from psycopg.conninfo import conninfo_to_dict

    parsed = conninfo_to_dict(s.conninfo())
    assert parsed["sslmode"] == "require"
    assert "default_transaction_read_only=on" in parsed["options"]


def test_conninfo_sets_application_name_for_pg_stat_activity_visibility():
    # Without this, every session this server opens shows up in
    # pg_stat_activity/get_active_sessions with an empty application_name --
    # indistinguishable from any other unlabeled client.
    from psycopg.conninfo import conninfo_to_dict

    ci = _settings().conninfo()
    parsed = conninfo_to_dict(ci)
    assert "application_name=pgllens" in parsed["options"]


# I4: run_readonly's call to assert_read_only was previously only exercised by
# a DSN-gated integration test, which skips with no DSN in CI -- removing the
# gate call was invisible to the unit suite.
async def test_run_readonly_rejects_a_write_and_never_reaches_execute(monkeypatch):
    db = Db(_settings())

    async def _execute(sql, params, max_rows):
        raise AssertionError("_execute must never run for an unsafe query")

    monkeypatch.setattr(db, "_execute", _execute)
    with pytest.raises(UnsafeQueryError):
        await db.run_readonly("DROP TABLE t")


async def test_run_readonly_passes_the_validated_string_to_execute(monkeypatch):
    db = Db(_settings())
    seen = {}

    async def _execute(sql, params, max_rows):
        seen["sql"] = sql
        seen["params"] = params
        return "sentinel"

    monkeypatch.setattr(db, "_execute", _execute)
    result = await db.run_readonly("SELECT 1")
    assert result == "sentinel"
    assert seen["sql"] == "SELECT 1"
    assert seen["params"] == ()


async def test_run_readonly_max_rows_overrides_the_settings_cap(monkeypatch):
    db = Db(_settings())
    seen = {}

    async def _execute(sql, params, max_rows):
        seen["max_rows"] = max_rows
        return "sentinel"

    monkeypatch.setattr(db, "_execute", _execute)
    await db.run_readonly("SELECT 1")
    assert seen["max_rows"] == _settings().max_rows
    await db.run_readonly("SELECT 1", max_rows=20)
    assert seen["max_rows"] == 20


async def test_pool_is_built_with_reset_and_configured_max_size(monkeypatch):
    from pgllens.database import pool as pool_mod

    captured = {}

    class FakePool:
        def __init__(self, conninfo, **kw):
            captured.update(kw)
        async def open(self):
            pass

    monkeypatch.setattr(pool_mod, "AsyncConnectionPool", FakePool)
    settings = Settings(database_url=DSN, db_pool_max_size=7)
    db = pool_mod.Db(settings)
    await db.open()
    assert captured["max_size"] == 7
    assert captured["reset"] is pool_mod._reset_connection


async def test_pool_disables_server_side_prepared_statements(monkeypatch):
    # Regression: DISCARD ALL in _reset_connection deallocates server-side
    # prepared statements psycopg's per-connection cache still references,
    # so the next use hits "prepared statement ... does not exist" and the
    # pool discards/reopens the connection (churn). Disabling auto-prepare
    # removes the whole DISCARD-ALL-vs-cache conflict class.
    from pgllens.database import pool as pool_mod

    captured = {}

    class FakePool:
        def __init__(self, conninfo, **kw):
            captured.update(kw)
        async def open(self):
            pass

    monkeypatch.setattr(pool_mod, "AsyncConnectionPool", FakePool)
    settings = Settings(database_url=DSN)
    db = pool_mod.Db(settings)
    await db.open()
    assert captured["kwargs"] == {"prepare_threshold": None}


async def test_reset_connection_runs_discard_all():
    from unittest.mock import AsyncMock

    from pgllens.database.pool import _reset_connection

    conn = AsyncMock()
    await _reset_connection(conn)
    # Verify autocommit sequence: on -> DISCARD ALL -> off (in finally)
    assert conn.set_autocommit.await_count == 2
    conn.set_autocommit.assert_any_await(True)
    conn.set_autocommit.assert_any_await(False)
    conn.execute.assert_awaited_once_with("DISCARD ALL")
