from unittest.mock import AsyncMock

from pgllens.database.format import QueryResult
from pgllens.llens_style import Caveat
from pgllens.tools import catalog
from pgllens.tools._util import respond
from pgllens.tools.catalog import format_extensions, format_grants, format_roles
from pgllens.tools.hypertables import (
    format_chunk_stats,
    format_continuous_aggregates,
    format_hypertables,
    format_jobs,
)
from pgllens.tools.modules import format_functions
from pgllens.tools.server_info import format_server_info
from pgllens.tools.statements import format_query_store
from tests.conftest import make_registered


def _empty(columns):
    return QueryResult(columns, [], False)


# --- format_extensions ---

def test_format_extensions_shows_versions_and_upgrade_marker():
    result = QueryResult(["name", "installed_version", "available_version", "schema",
                          "description"],
                         [("pg_stat_statements", "1.9", "1.10", "public", "query stats"),
                          ("uuid-ossp", "1.1", "1.1", "public", None)], False)
    out = respond(format_extensions(result))
    assert "pg_stat_statements" in out and "1.9" in out and "yes" in out
    assert "uuid-ossp" in out


def test_format_extensions_none_found_for_empty_result():
    result = _empty(["name", "installed_version", "available_version", "schema", "description"])
    resp = format_extensions(result)
    assert resp.sections[0].blocks[0].rows == ()
    assert "0 extensions" in resp.tally


# --- format_roles ---

def test_format_roles_renders_flags_and_membership():
    result = QueryResult(
        ["name", "is_superuser", "can_login", "create_db", "create_role",
         "connection_limit", "member_of"],
        [("app_user", False, True, False, False, -1, ["reader", "writer"])], False)
    table = format_roles(result)
    out = "\n".join(" ".join(row) for row in table.rows)
    assert "app_user" in out and "`reader`, `writer`" in out and "unlimited" in out


def test_format_roles_none_found_for_empty_result():
    result = _empty(["name", "is_superuser", "can_login", "create_db", "create_role",
                     "connection_limit", "member_of"])
    assert format_roles(result).rows == ()


def test_format_roles_never_leaks_a_password_hash_column():
    # Simulates a query that (accidentally, or via a hostile SELECT *) also
    # returned the credential hash -- format_roles must render only the named
    # role-attribute columns, never echo the rest of result.columns/rows.
    result = QueryResult(
        ["name", "is_superuser", "can_login", "create_db", "create_role",
         "connection_limit", "member_of", "rolpassword"],
        [("app_user", False, True, False, False, 5, [], "md5deadbeef")], False)
    table = format_roles(result)
    out = "\n".join(" ".join(row) for row in table.rows)
    assert "app_user" in out
    assert "md5deadbeef" not in out
    assert "rolpassword" not in out


# --- format_grants ---

def test_format_grants_groups_by_role_then_schema_then_table():
    result = QueryResult(
        ["schema_name", "table_name", "grantee", "privilege"],
        [("public", "item", "app_user", "SELECT"),
         ("public", "item", "app_user", "INSERT"),
         ("wms", "shipment", "PUBLIC", "SELECT")], False)
    table, caveats = format_grants(result)
    assert caveats == []
    assert ("`app_user`", "`public.item`", "INSERT, SELECT") in table.rows
    assert ("`PUBLIC`", "`wms.shipment`", "SELECT") in table.rows


def test_format_grants_none_found_for_empty_result():
    result = _empty(["schema_name", "table_name", "grantee", "privilege"])
    table, caveats = format_grants(result)
    assert table.rows == ()
    assert caveats == []


def _grants(rows):
    return QueryResult(["schema_name", "table_name", "grantee", "privilege"], rows, False)


ALL_PRIVS = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
             "MAINTAIN"]


def test_format_grants_collapses_identical_privilege_sets_and_lists_exceptions():
    rows = [("public", f"t{i}", "postgres", p) for i in range(11) for p in ("SELECT", "INSERT")]
    rows += [("wms", "audit", "postgres", "SELECT")]
    table, _caveats = format_grants(_grants(rows))
    assert ("`postgres`", "11 relations in `public`", "INSERT, SELECT") in table.rows
    assert ("`postgres`", "`wms.audit`", "SELECT") in table.rows
    assert not any("t0" in r[1] for r in table.rows)  # collapsed relations never listed individually


def test_format_grants_collapse_lists_every_distinct_schema():
    rows = [(schema, f"t{i}", "postgres", "SELECT")
            for schema in ("public", "wms") for i in range(2)]
    table, _caveats = format_grants(_grants(rows))
    assert ("`postgres`", "4 relations in `public`, `wms`", "SELECT") in table.rows


def test_format_grants_uses_all_privileges_shorthand():
    rows = [("public", f"t{i}", "postgres", p) for i in range(3) for p in ALL_PRIVS]
    table, _caveats = format_grants(_grants(rows))
    assert ("`postgres`", "3 relations in `public`", "ALL (8 privileges)") in table.rows
    assert not any("MAINTAIN" in r[2] for r in table.rows)

    rows7 = [("public", f"t{i}", "postgres", p) for i in range(3) for p in ALL_PRIVS[:7]]
    table7, _caveats7 = format_grants(_grants(rows7))
    assert ("`postgres`", "3 relations in `public`", "ALL (7 privileges)") in table7.rows


def test_format_grants_no_exceptions_section_when_every_relation_matches():
    rows = [("public", f"t{i}", "postgres", "SELECT") for i in range(3)]
    table, _caveats = format_grants(_grants(rows))
    assert len(table.rows) == 1  # only the collapsed row, no per-relation exceptions


# --- list_roles tool ---

_ROLE_COLUMNS = ["name", "is_superuser", "can_login", "create_db", "create_role",
                 "connection_limit", "member_of"]


def _register_list_roles(roles=None, grants=None, hidden=0):
    """list_roles wired to a fake Db that dispatches on the SQL it is handed."""
    roles = roles or QueryResult(
        _ROLE_COLUMNS, [("postgres", True, True, True, True, -1, [])], False)
    grants = grants if grants is not None else _grants([])
    mcp, db, _ = make_registered(catalog)
    calls: list[tuple[str, tuple]] = []

    async def run(sql, params=(), max_rows=None):
        calls.append((sql, params, max_rows))
        if sql is catalog._ROLES_SQL:
            return roles
        if sql is catalog._BUILTIN_ROLE_COUNT_SQL:
            return QueryResult(["count"], [(hidden,)], False)
        return grants

    db.run_system = AsyncMock(side_effect=run)
    return mcp.tools["list_roles"], calls


def test_roles_sql_filters_builtin_roles_with_a_bound_parameter():
    # psycopg escaping: a literal % in the LIKE pattern must be doubled, and the
    # \_ must stay a backslash-escaped underscore (raw string, not a Python escape).
    assert r"(%s OR r.rolname NOT LIKE 'pg\_%%')" in catalog._ROLES_SQL


async def test_list_roles_binds_include_builtin_false_by_default():
    tool, calls = _register_list_roles()
    await tool()
    assert calls[0][0] is catalog._ROLES_SQL
    assert calls[0][1] == (False,)


async def test_list_roles_binds_include_builtin_true_when_asked():
    tool, calls = _register_list_roles(hidden=16)
    out = await tool(include_builtin=True)
    assert calls[0][1] == (True,)
    assert "built-in pg_* roles hidden" not in out
    assert all(sql is not catalog._BUILTIN_ROLE_COUNT_SQL for sql, *_ in calls)


async def test_list_roles_footer_reports_hidden_builtin_count():
    tool, _ = _register_list_roles(hidden=16)
    out = await tool()
    assert "16 built-in pg_* roles hidden; pass include_builtin=True to show them." in out


async def test_list_roles_footer_omitted_when_nothing_was_hidden():
    tool, _ = _register_list_roles(hidden=0)
    assert "built-in pg_* roles hidden" not in await tool()


async def test_list_roles_drops_pg_grantees_but_keeps_public():
    grants = _grants([("public", "item", "pg_monitor", "SELECT"),
                      ("public", "item", "PUBLIC", "SELECT")])
    tool, _ = _register_list_roles(grants=grants)
    out = await tool()
    assert "pg_monitor" not in out
    assert "PUBLIC" in out

    tool, _ = _register_list_roles(grants=grants)
    assert "pg_monitor" in await tool(include_builtin=True)


async def test_list_roles_output_stays_small_for_a_typical_database():
    # Acceptance target: the demo DB (16 pg_* roles hidden, 2 real roles,
    # postgres holding all 8 privileges on 11 relations) under 1,500 characters.
    roles = QueryResult(_ROLE_COLUMNS,
                        [("postgres", True, True, True, True, -1, []),
                         ("app_user", False, True, False, False, -1, ["reader"])], False)
    grants = _grants([("public", f"t{i}", "postgres", p)
                      for i in range(11) for p in ALL_PRIVS])
    tool, _ = _register_list_roles(roles=roles, grants=grants, hidden=16)
    out = await tool()
    assert len(out) < 1500, out


# --- format_functions ---

def test_format_functions_groups_by_schema_and_flags_volatile():
    result = QueryResult(
        ["schema", "name", "arguments", "return_type", "volatility", "comment"],
        [("public", "add_one", "x integer", "integer", "v", "adds one")], False)
    out = respond(format_functions(result, "public"))
    assert "public" in out and "add_one" in out and "volatile" in out


def test_format_functions_none_found_for_empty_result():
    result = _empty(["schema", "name", "arguments", "return_type", "volatility", "comment"])
    out = respond(format_functions(result, None))
    assert "0 functions" in out


# --- format_hypertables ---

def test_format_hypertables_shows_time_column_and_compression():
    result = QueryResult(
        ["hypertable_schema", "hypertable_name", "compression_enabled", "num_dimensions",
         "time_column", "time_interval"],
        [("public", "metrics", True, 1, "ts", "7 days")], False)
    table = format_hypertables(result)
    assert ("`public.metrics`", "`ts`", "7 days", "yes") in table.rows


def test_format_hypertables_none_found_for_empty_result():
    result = _empty(["hypertable_schema", "hypertable_name", "compression_enabled",
                     "num_dimensions", "time_column", "time_interval"])
    assert format_hypertables(result).rows == ()


# --- format_jobs ---

def test_format_jobs_shows_schedule_and_truncates_long_config():
    long_config = "x" * 80
    result = QueryResult(
        ["hypertable_schema", "hypertable_name", "job_id", "job_type", "schedule",
         "config", "next_start"],
        [("public", "metrics", 1000, "Compression Policy", "7 days",
          long_config, "2026-09-01T00:00:00")], False)
    table = format_jobs(result)
    row = table.rows[0]
    assert row[0] == "`metrics`" and row[1] == "Compression Policy" and row[2] == "7 days"
    assert "…" in row[3]
    assert row[4] == "2026-09-01T00:00:00"


def test_format_jobs_none_found_for_empty_result():
    result = _empty(["hypertable_schema", "hypertable_name", "job_id", "job_type",
                     "schedule", "config", "next_start"])
    assert format_jobs(result).rows == ()


# --- format_continuous_aggregates ---

def test_format_continuous_aggregates_shows_view_and_materialization():
    result = QueryResult(
        ["materialization_hypertable_schema", "materialization_hypertable_name",
         "view_schema", "view_name", "view_definition"],
        [("_timescaledb_internal", "_materialized_hypertable_1", "public",
          "hourly_metrics", "SELECT time_bucket('1 hour', ts), avg(v) FROM metrics")], False)
    blocks = format_continuous_aggregates(result)
    bullets = {b.key: b.value for b in blocks[0].items}
    assert bullets["view"] == "public.hourly_metrics"
    assert bullets["materialization"] == "_timescaledb_internal._materialized_hypertable_1"
    assert "time_bucket" in blocks[1].text


def test_format_continuous_aggregates_none_found_for_empty_result():
    result = _empty(["materialization_hypertable_schema", "materialization_hypertable_name",
                     "view_schema", "view_name", "view_definition"])
    assert format_continuous_aggregates(result) == ()


# --- format_chunk_stats ---

def test_format_chunk_stats_with_sizes_shows_compression_ratio():
    result = QueryResult(
        ["hypertable_schema", "hypertable_name", "chunk_count", "range_start", "range_end",
         "total_bytes", "compressed_bytes"],
        [("public", "metrics", 12, "2026-01-01T00:00:00", "2026-02-01T00:00:00",
          2_000_000, 1_000_000)], False)
    table = format_chunk_stats(result)
    row = table.rows[0]
    assert row[0] == "`metrics`" and row[1] == "12" and row[5] == "2.0x"
    assert "2026-01-01" in row[2] and "2026-02-01" in row[2]


def test_format_chunk_stats_fallback_without_sizes_still_reports_chunk_count():
    result = QueryResult(
        ["hypertable_schema", "hypertable_name", "chunk_count", "range_start", "range_end"],
        [("public", "metrics", 12, "2026-01-01T00:00:00", "2026-02-01T00:00:00")], False)
    table = format_chunk_stats(result)
    assert table.rows[0][0] == "`metrics`" and table.rows[0][1] == "12"
    assert "total" not in table.columns


def test_format_chunk_stats_none_found_for_empty_result():
    result = _empty(["hypertable_schema", "hypertable_name", "chunk_count", "range_start",
                     "range_end", "total_bytes", "compressed_bytes"])
    assert format_chunk_stats(result).rows == ()


# --- format_server_info ---

def test_format_server_info_renders_version_uptime_and_settings():
    settings = QueryResult(["name", "setting", "unit", "short_desc"],
                           [("max_connections", "100", None, "max client connections")], False)
    out = format_server_info(("PostgreSQL 16.2",), ("2026-01-01", "8 days", 12), settings)
    identity, config = out.sections
    bullets = {b.key: b.value for b in identity.blocks[0].items}
    assert bullets["version"] == "PostgreSQL 16.2"
    assert bullets["uptime"] == "8 days"
    assert bullets["connections"] == "12"
    table = config.blocks[0]
    assert ("`max_connections`", "100", "", "max client connections") in table.rows


def test_format_server_info_shows_zero_settings_for_empty_result():
    result = _empty(["name", "setting", "unit", "short_desc"])
    out = format_server_info(("PostgreSQL 16.2",), ("2026-01-01", "8 days", 12), result)
    assert "0 settings shown" in out.tally


# --- format_query_store ---

def _render_query_store(result):
    return respond(format_query_store(result, "unknown", None, False, "total_time"))


def test_format_query_store_shows_query_text():
    from pgllens.tools.statements import _QUERY_STORE_SQL
    # Pin: SQL must contain left(query for truncation and 121st-char detection
    assert "left(query" in _QUERY_STORE_SQL

    result = QueryResult(
        ["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
         "shared_blks_hit", "shared_blks_read", "query"],
        [(1, 5, 100.5, 20.1, 50, 200, 10, "SELECT * FROM users LIMIT 10")], False)
    out = _render_query_store(result)
    assert "SELECT * FROM users LIMIT 10" in out
    assert "1" in out  # queryid


def test_format_query_store_truncates_and_marks_long_query_text():
    # 121-char query: 121st char is the "was cut" signal, render first 120 + "…"
    query_121 = "SELECT " + "a" * 113 + " FROM t"  # 7 + 113 + 8 = 128 chars, > 121
    result = QueryResult(
        ["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
         "shared_blks_hit", "shared_blks_read", "query"],
        [(1, 5, 100.5, 20.1, 50, 200, 10, query_121)], False)
    out = _render_query_store(result)
    # Verify exactly first 120 chars + ellipsis
    assert query_121[:120] + "…" in out


def test_format_query_store_no_marker_for_short_query():
    # Query ≤120 chars: no ellipsis
    query_short = "SELECT * FROM users"
    result = QueryResult(
        ["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
         "shared_blks_hit", "shared_blks_read", "query"],
        [(1, 5, 100.5, 20.1, 50, 200, 10, query_short)], False)
    out = _render_query_store(result)
    assert query_short in out
    assert "…" not in out  # No truncation marker for short query


def test_format_query_store_handles_none_query_text():
    # pg_stat_statements.query is NULL when the external query-text file is
    # unreadable; must render an empty cell, not crash on len(None).
    result = QueryResult(
        ["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
         "shared_blks_hit", "shared_blks_read", "query"],
        [(1, 5, 100.5, 20.1, 50, 200, 10, None)], False)
    out = _render_query_store(result)
    assert "1" in out  # queryid still renders


def test_format_query_store_empty_result_reports_zero_tally():
    result = _empty(["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
                     "shared_blks_hit", "shared_blks_read", "query"])
    out = _render_query_store(result)
    assert "0 statements" in out
    assert "None found." not in out


def test_format_grants_privileges_are_sorted_and_deduped():
    # aclexplode yields one row per (grantor, grantee, privilege), so the same
    # privilege granted by two grantors arrives twice. The label must dedupe
    # and stay in a stable (sorted) order rather than echo catalog order.
    rows = [("public", "item", "app_user", p)
            for p in ("SELECT", "DELETE", "SELECT", "INSERT")]
    table, _caveats = format_grants(_grants(rows))
    assert ("`app_user`", "`public.item`", "DELETE, INSERT, SELECT") in table.rows


def test_format_grants_does_not_collapse_a_minority_privilege_set():
    # 2 of 4 relations share SELECT -- exactly half, not a majority. Collapsing
    # there would push half the role's relations into an Exceptions table for
    # no saving, so every relation is listed instead.
    rows = [("public", "a", "app_user", "SELECT"), ("public", "b", "app_user", "SELECT"),
            ("public", "c", "app_user", "INSERT"), ("public", "d", "app_user", "UPDATE")]
    table, _caveats = format_grants(_grants(rows))
    assert len(table.rows) == 4  # every relation listed, nothing collapsed
    assert not any("relations in" in r[1] for r in table.rows)


def test_format_grants_collapses_a_majority_privilege_set():
    rows = [("public", "a", "app_user", "SELECT"), ("public", "b", "app_user", "SELECT"),
            ("public", "c", "app_user", "SELECT"), ("public", "d", "app_user", "UPDATE")]
    table, _caveats = format_grants(_grants(rows))
    assert ("`app_user`", "3 relations in `public`", "SELECT") in table.rows
    assert ("`app_user`", "`public.d`", "UPDATE") in table.rows


def test_format_grants_warns_when_the_grant_set_was_truncated():
    # The collapsed "on N relations" count is derived from the rows returned,
    # so a truncated read must say so -- the counts are lower bounds.
    result = QueryResult(["schema_name", "table_name", "grantee", "privilege"],
                         [("public", f"t{i}", "postgres", "SELECT") for i in range(3)], True)
    _table, caveats = format_grants(result)
    assert caveats == [Caveat("Grants truncated at 5000 rows; counts are lower bounds.")]


async def test_list_roles_reads_grants_at_the_catalog_row_cap():
    tool, calls = _register_list_roles()
    await tool()
    caps = {max_rows for sql, _params, max_rows in calls
            if sql in (catalog._GRANTS_SQL, catalog._ROLES_SQL)}
    assert caps == {catalog._CATALOG_ROWS}
