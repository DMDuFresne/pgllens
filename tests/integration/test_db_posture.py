"""The database-side half of the read-only guarantee.

safety.py rejects write SQL before it reaches the server. These tests assert the
server would reject it anyway -- that the role created by
ops/sql/pgllens-role.sql genuinely cannot write, read server files, or reach
another database. Run against a DSN whose role was created by that script; skips
cleanly without PGLLENS_TEST_DSN, like every other test in this directory.
"""

from __future__ import annotations

import psycopg
import pytest

pytestmark = pytest.mark.integration


def _raw(dsn: str):
    """A connection with NO Settings.conninfo() options applied -- these tests
    must prove the ROLE's own grants and ALTER ROLE settings, not the session
    options the app happens to send."""
    return psycopg.connect(dsn, connect_timeout=5)


def test_the_role_cannot_create_a_table(dsn):
    with _raw(dsn) as conn, pytest.raises(psycopg.Error) as excinfo:
        conn.execute("CREATE TABLE pgllens_should_not_exist (id int)")
    assert isinstance(
        excinfo.value, (psycopg.errors.InsufficientPrivilege,
                        psycopg.errors.ReadOnlySqlTransaction)
    )


def test_the_role_cannot_insert(dsn, settings):
    with _raw(dsn) as conn:
        table = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename LIMIT 1",
            (settings.exposed_schemas[0],),
        ).fetchone()
        if table is None:
            pytest.skip("no table in the exposed schema to attempt a write against")
        with pytest.raises(psycopg.Error) as excinfo:
            conn.execute(f'INSERT INTO "{settings.exposed_schemas[0]}"."{table[0]}" '
                         "DEFAULT VALUES")
    assert isinstance(
        excinfo.value, (psycopg.errors.InsufficientPrivilege,
                        psycopg.errors.ReadOnlySqlTransaction,
                        psycopg.errors.FeatureNotSupported)
    )


def test_the_role_cannot_read_server_files(dsn):
    # pg_read_server_files / pg_read_file is a straight path from SQL execution
    # to the host filesystem. The role must not be a member of that role.
    with _raw(dsn) as conn, pytest.raises(psycopg.Error) as excinfo:
        conn.execute("SELECT pg_read_file('/etc/passwd')")
    assert isinstance(excinfo.value, psycopg.errors.InsufficientPrivilege)


def test_the_role_cannot_execute_server_programs(dsn):
    with _raw(dsn) as conn, pytest.raises(psycopg.Error):
        conn.execute("COPY (SELECT 1) TO PROGRAM 'id'")


def test_the_role_cannot_reach_another_database(dsn):
    # CONNECT is granted on exactly one database. If the cluster only has one
    # non-template database there is nothing to prove here.
    with _raw(dsn) as conn:
        rows = conn.execute(
            "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate"
        ).fetchall()
    current = psycopg.conninfo.conninfo_to_dict(dsn).get("dbname")
    others = [r[0] for r in rows if r[0] != current]
    if not others:
        pytest.skip("only one connectable database in this cluster")
    for other in others:
        params = psycopg.conninfo.conninfo_to_dict(dsn)
        params["dbname"] = other
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(**params, connect_timeout=5).close()


def test_the_role_is_not_a_superuser(dsn):
    with _raw(dsn) as conn:
        assert conn.execute("SELECT current_setting('is_superuser')").fetchone()[0] == "off"


def test_default_transaction_read_only_is_set_on_the_role_itself(dsn):
    # Settings.conninfo() sends this as a session option too. This asserts the
    # ALTER ROLE, so the guarantee survives a misconfigured conninfo.
    with _raw(dsn) as conn:
        assert conn.execute("SHOW default_transaction_read_only").fetchone()[0] == "on"


def test_statement_timeout_is_set_on_the_role_itself(dsn):
    with _raw(dsn) as conn:
        value = conn.execute("SHOW statement_timeout").fetchone()[0]
    assert value not in ("0", "0ms")


def test_idle_in_transaction_timeout_is_set_on_the_role_itself(dsn):
    with _raw(dsn) as conn:
        value = conn.execute("SHOW idle_in_transaction_session_timeout").fetchone()[0]
    assert value not in ("0", "0ms")
