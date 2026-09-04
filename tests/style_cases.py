"""Fixture inputs for the style contract and golden tests. One Case per tool
success path, plus error cases. Every migration task appends here."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.database.introspect import Column, ForeignKey, Table, TableNotFoundError

SITES = Table("app_core", "sites", "r", "Physical sites",
              [Column("site_id", "bigint", False, None, "Surrogate key", 1, is_identity="a"),
               Column("name", "text", False, None, None, 2)],
              ["site_id"], 12)
ASSETS = Table("app_core", "assets", "r", None,
               [Column("asset_id", "bigint", False, None, None, 1, is_identity="a"),
                Column("site_id", "bigint", False, None, "FK to sites", 2),
                Column("tag_name", "text", False, None, None, 3),
                Column("installed_at", "timestamptz", True, None, None, 4),
                Column("metadata", "jsonb", True, "'{}'::jsonb", None, 5)],
               ["asset_id"], 4100)
EVENTS = Table("app_audit", "events", "r", None,
               [Column("event_id", "bigint", False, None, None, 1),
                Column("created_at", "timestamptz", False, "now()", None, 2)],
               ["event_id"], 82)
VASSETS = Table("app_core", "v_assets", "v", None,
                [Column("asset_id", "bigint", True, None, None, 1)], [], 0)
TABLES = [SITES, ASSETS, EVENTS, VASSETS]
FKS = [ForeignKey("assets_site_id_fkey", "app_core", "assets", ["site_id"],
                  "app_core", "sites", ["site_id"], [True])]


def make_intro() -> MagicMock:
    intro = MagicMock()
    intro.tables = AsyncMock(return_value=TABLES)
    intro.foreign_keys = AsyncMock(return_value=FKS)
    intro.refresh = AsyncMock(return_value=len(TABLES))
    intro.enum_labels = AsyncMock(return_value=None)
    intro.search_columns = AsyncMock(return_value=[
        ("app_core", "assets", "site_id", "bigint"), ("app_core", "sites", "site_id", "bigint")])

    async def table(name: str, schema: str | None = None) -> Table:
        for t in TABLES:
            if t.name.lower() == name.lower() and (schema is None or t.schema == schema):
                return t
        raise TableNotFoundError(f"Table '{name}' not found in the exposed schemas.")

    intro.table = AsyncMock(side_effect=table)
    return intro


def make_caps(extensions: tuple[str, ...] = (), version: tuple[int, int] = (16, 3)) -> MagicMock:
    caps = MagicMock()
    caps.has_extension = AsyncMock(side_effect=lambda n: n in extensions)
    caps.extension_schema = AsyncMock(return_value="public")
    caps.extension_version = AsyncMock(return_value=(1, 11))
    caps.server_version = AsyncMock(return_value=version)
    return caps


@dataclass
class Case:
    name: str                                  # golden file stem, e.g. "get_blocking"
    module: ModuleType
    tool: str                                  # registered tool name
    kwargs: dict[str, object] = field(default_factory=dict)
    system: list[QueryResult] = field(default_factory=list)   # run_system side_effect, in order
    readonly: QueryResult | None = None
    intro: MagicMock | None = None
    caps: MagicMock | None = None
    apps: bool = False                         # register via erd.register_apps (CallToolResult)
    error: bool = False                        # expected to render the error envelope


CASES: list[Case] = []

from pgllens.tools import sessions

CASES += [
    Case("get_active_sessions", sessions, "get_active_sessions", system=[QueryResult(
        ["pid", "usename", "application_name", "client_addr", "state", "backend_type",
         "wait_event_type", "wait_event", "duration", "query"],
        [(123, "app_user", "psql", "127.0.0.1", "active", "client backend", None, None,
          "0:00:05", "SELECT 1")], False)]),
    Case("get_blocking", sessions, "get_blocking", system=[QueryResult(
        ["blocked_pid", "blocked_query", "blocker_pid", "blocker_query", "waiting_duration"],
        [(101, "UPDATE t SET x=1", 202, "UPDATE t SET x=2", "0:00:10")], False)]),
    Case("get_blocking_empty", sessions, "get_blocking", system=[QueryResult(
        ["blocked_pid", "blocked_query", "blocker_pid", "blocker_query", "waiting_duration"],
        [], False)]),
    Case("get_wait_stats", sessions, "get_wait_stats", system=[QueryResult(
        ["wait_event_type", "wait_event", "sessions"], [("Lock", "relation", 3)], False)]),
]

from datetime import UTC, datetime, timedelta

from pgllens.tools import discovery, server_info, validate

CASES += [
    Case("server_info", server_info, "server_info", system=[
        QueryResult(["version"], [("PostgreSQL 16.3 on x86_64-pc-linux-gnu, compiled by gcc",)], False),
        QueryResult(["start_time", "uptime", "connections"],
                    [(datetime(2026, 8, 31, 11, 49, 3, tzinfo=UTC), timedelta(days=3, hours=4), 7)], False),
        QueryResult(["name", "setting", "unit", "short_desc"],
                    [("max_connections", "100", None, "Sets the maximum number of concurrent connections."),
                     ("shared_buffers", "16384", "8kB", "Sets the number of shared memory buffers.")], False),
    ]),
    Case("validate_query", validate, "validate_query", kwargs={"sql": "SELECT 1"}),
    Case("list_tables", discovery, "list_tables"),
    Case("list_tables_schema", discovery, "list_tables", kwargs={"schema": "app_audit"}),
    Case("list_tables_bad_schema", discovery, "list_tables", kwargs={"schema": "nope"},
         error=True),
    Case("describe_table", discovery, "describe_table", kwargs={"table": "assets"}, system=[
        QueryResult(["attname", "n_distinct", "mcv", "inherited"],
                    [("site_id", 3.0, ["1", "2", "3"], False)], False)]),
    Case("describe_table_no_stats", discovery, "describe_table", kwargs={"table": "events"},
         system=[QueryResult(["attname", "n_distinct", "mcv", "inherited"], [], False)]),
    Case("schema_overview", discovery, "schema_overview"),
    Case("search_columns", discovery, "search_columns", kwargs={"pattern": "site"}),
    Case("get_sample_data", discovery, "get_sample_data", kwargs={"table": "sites", "limit": 2},
         system=[QueryResult(["site_id", "name"], [(1, "Plant A"), (2, "Plant B")], False)]),
    Case("get_sample_data_range", discovery, "get_sample_data",
         kwargs={"table": "sites", "limit": 0}, error=True),
    Case("refresh_schema", discovery, "refresh_schema"),
    Case("describe_table_not_found", discovery, "describe_table", kwargs={"table": "nope"}, error=True),
]

from pgllens.tools import relationships

CASES += [
    Case("get_relationships", relationships, "get_relationships"),
    Case("get_relationships_table", relationships, "get_relationships", kwargs={"table": "assets"}),
    Case("get_relationships_schema", relationships, "get_relationships", kwargs={"schema": "app_core"}),
    Case("find_path", relationships, "find_path", kwargs={"from_table": "assets", "to_table": "sites"}),
    Case("find_path_none", relationships, "find_path", kwargs={"from_table": "assets", "to_table": "events"}),
    Case("find_path_same", relationships, "find_path",
         kwargs={"from_table": "assets", "to_table": "assets"}, error=True),
]

from pgllens.tools import constraints, modules, triggers

_CONSTRAINT_COLS = ["schema", "table", "name", "type", "definition", "references", "validated"]
_TRIGGER_COLS = ["schema", "table", "trigger", "enabled", "definition", "function"]
_FN_COLS = ["schema", "name", "arguments", "return_type", "volatility", "comment"]
_SRC_COLS = ["oid", "full_definition", "source", "return_type", "arguments", "language",
             "volatility", "security_definer", "is_strict", "kind", "comment"]

CASES += [
    Case("get_constraints", constraints, "get_constraints", system=[QueryResult(_CONSTRAINT_COLS, [
        ("app_core", "assets", "assets_pkey", "PRIMARY KEY", "PRIMARY KEY (asset_id)", None, True),
        ("app_core", "assets", "assets_site_id_fkey", "FOREIGN KEY",
         "FOREIGN KEY (site_id) REFERENCES sites(site_id)", "app_core.sites", True)], False)]),
    Case("get_constraints_truncated", constraints, "get_constraints",
         system=[QueryResult(_CONSTRAINT_COLS, [
             ("app_core", "assets", "c1", "CHECK", "CHECK (x > 0)", None, False)], True)]),
    Case("get_triggers", triggers, "get_triggers", system=[QueryResult(_TRIGGER_COLS, [
        ("app_core", "assets", "trg_touch", "enabled",
         "CREATE TRIGGER trg_touch BEFORE UPDATE ON app_core.assets FOR EACH ROW EXECUTE FUNCTION app_core.touch()",
         "app_core.touch")], False)]),
    Case("get_triggers_empty", triggers, "get_triggers", kwargs={"table": "sites"},
         system=[QueryResult(_TRIGGER_COLS, [], False)]),
    Case("get_view_definition", modules, "get_view_definition", kwargs={"view": "v_assets"}, system=[
        QueryResult(["nspname", "relkind", "definition", "comment"],
                    [("app_core", "v", "SELECT asset_id FROM app_core.assets", None)], False),
        QueryResult(["column_name", "data_type", "comment"], [("asset_id", "bigint", None)], False)]),
    Case("get_view_definition_missing", modules, "get_view_definition", kwargs={"view": "nope"},
         system=[QueryResult(["nspname", "relkind", "definition", "comment"], [], False)], error=True),
    Case("list_functions", modules, "list_functions", system=[QueryResult(_FN_COLS, [
        ("app_core", "touch", "", "trigger", "v", "Sets updated_at"),
        ("app_core", "site_name", "p_site_id bigint", "text", "s", None)], False)]),
    Case("get_function_source", modules, "get_function_source", kwargs={"function": "touch"},
         system=[QueryResult(_SRC_COLS, [
             (1, "CREATE FUNCTION app_core.touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$",
              "BEGIN NEW.updated_at = now(); RETURN NEW; END", "trigger", "", "plpgsql", "v", False,
              False, "f", "Sets updated_at")], False)]),
    Case("get_function_source_missing", modules, "get_function_source", kwargs={"function": "nope"},
         system=[QueryResult(_SRC_COLS, [], False), QueryResult(["proname"], [("touch",)], False)],
         error=True),
]

from pgllens.tools import catalog, hypertables, ontology

CASES += [
    Case("list_extensions", catalog, "list_extensions", system=[QueryResult(
        ["name", "installed_version", "available_version", "schema", "description"],
        [("plpgsql", "1.0", "1.0", "pg_catalog", "PL/pgSQL procedural language"),
         ("pg_stat_statements", "1.10", "1.11", "public",
          "track planning and execution statistics")], False)]),
    Case("list_roles", catalog, "list_roles", system=[
        QueryResult(["name", "is_superuser", "can_login", "create_db", "create_role",
                     "connection_limit", "member_of"],
                    [("app_rw", False, True, False, False, -1, ["app_ro"]),
                     ("app_ro", False, False, False, False, 5, [])], False),
        QueryResult(["count"], [(12,)], False),
        QueryResult(["schema_name", "table_name", "grantee", "privilege"],
                    [("app_core", "assets", "app_ro", "SELECT"),
                     ("app_core", "sites", "app_ro", "SELECT"),
                     ("app_core", "assets", "app_rw", "SELECT"),
                     ("app_core", "assets", "app_rw", "INSERT")], False)]),
    Case("list_hypertables", hypertables, "list_hypertables", caps=make_caps(("timescaledb",)),
         system=[
             QueryResult(["hypertable_schema", "hypertable_name", "compression_enabled",
                          "num_dimensions", "time_column", "time_interval"],
                         [("app_core", "readings", True, 1, "ts", "7 days")], False),
             QueryResult(["hypertable_schema", "hypertable_name", "job_id", "job_type", "schedule",
                          "config", "next_start"],
                         [("app_core", "readings", 1000, "Compression Policy", "1 day",
                           '{"compress_after": "30 days"}', "2026-09-04 02:00:00+00")], False),
             QueryResult(["ms", "mn", "vs", "vn", "view_definition"], [], False),
             QueryResult(["hypertable_schema", "hypertable_name", "chunk_count", "range_start",
                          "range_end", "total_bytes", "compressed_bytes"],
                         [("app_core", "readings", 12, "2026-06-01 00:00:00+00",
                           "2026-09-01 00:00:00+00", 5_242_880, 1_048_576)], False)]),
    Case("list_hypertables_missing", hypertables, "list_hypertables", caps=make_caps(()), error=True),
    Case("get_ontology", ontology, "get_ontology"),
]

from pgllens.tools import health, indexes, statements

CASES += [
    Case("get_index_health", indexes, "get_index_health", system=[
        QueryResult(["schema", "table", "index", "idx_scan", "index_size", "is_invalid", "indkey",
                     "indrelid", "constraint_type", "indpred", "indexprs"],
                    [("app_core", "assets", "assets_pkey", 1204, 16384, False, "1", 100, "p", None, None),
                     ("app_core", "assets", "assets_site_idx", 0, 8192, False, "2", 100, None, None, None),
                     ("app_core", "assets", "assets_site_idx2", 0, 8192, False, "2", 100, None, None, None)], False),
        QueryResult(["schema", "table", "column", "constraint", "conrelid", "leading_attnum"],
                    [("app_core", "assets", "site_id", "assets_site_id_fkey", 100, 2)], False),
        QueryResult(["since", "from_postmaster", "days"],
                    [(datetime(2026, 8, 1, 0, 0, 0, tzinfo=UTC), False, 33.2)], False)]),
    Case("get_table_health", health, "get_table_health", system=[QueryResult(
        ["schema", "table", "n_live_tup", "n_dead_tup", "dead_pct", "last_autovacuum",
         "n_ins_since_vacuum", "xid_age", "freeze_max_age", "db_xid_age", "bloat_pct", "bloat_bytes"],
        [("app_core", "assets", 4100, 410, 9.1, datetime(2026, 9, 1, 2, 0, 0, tzinfo=UTC), 12,
          48_213, 200_000_000, 61_902, 4.3, 32_768),
         ("app_core", "sites", 12, 0, 0, None, 0, 48_213, 200_000_000, 61_902, None, None)], False),
        QueryResult(["sequence", "owned_by", "last_value", "max_value", "pct_used"],
                    [("assets_asset_id_seq", "assets", 4100, 2147483647, 0.0)], False),
    ]),
    Case("get_table_stats", health, "get_table_stats", kwargs={"table": "sites"}, system=[
        QueryResult(["count"], [(12,)], False),
        QueryResult(["ord", "null_count", "distinct_count", "min_text", "max_text"],
                    [(0, 0, 12, "1", "12"), (1, 0, 12, "", "")], False)]),
    Case("get_space_usage", health, "get_space_usage", system=[
        QueryResult(["schema", "table", "total_size", "table_size", "index_size", "total_bytes"],
                    [("app_core", "assets", "1264 kB", "1024 kB", "240 kB", 1294336)], False),
        QueryResult(["size"], [("48 MB",)], False)]),
    Case("get_query_store", statements, "get_query_store", caps=make_caps(("pg_stat_statements",)),
         system=[
             QueryResult(["stats_reset"], [(datetime(2026, 8, 30, 2, 0, 0, tzinfo=UTC),)], False),
             QueryResult(["queryid", "calls", "total_exec_time", "mean_exec_time", "rows",
                          "shared_blks_hit", "shared_blks_read", "query"],
                         [(123, 10, 1500.456, 150.0456, 1000, 500, 5,
                           "SELECT * FROM app_core.assets")], False)]),
    Case("get_query_store_missing", statements, "get_query_store", caps=make_caps(()), error=True),
    Case("get_query_store_bad_order", statements, "get_query_store",
         caps=make_caps(("pg_stat_statements",)), kwargs={"order_by": "nope"}, error=True),
]

from pgllens.tools import explain, query

_PLAN = ('[{"Plan": {"Node Type": "Seq Scan", "Relation Name": "assets", '
        '"Total Cost": 12.5, "Plan Rows": 4100}}]')

CASES += [
    Case("query", query, "query",
         kwargs={"sql": "SELECT asset_id, tag_name FROM app_core.assets ORDER BY asset_id"},
         readonly=QueryResult(["asset_id", "tag_name"], [(1, "P1.M1"), (2, "P1.M2")], False)),
    Case("query_truncated", query, "query",
         kwargs={"sql": "SELECT asset_id FROM app_core.assets ORDER BY asset_id", "limit": 1},
         readonly=QueryResult(["asset_id"], [(1,)], True)),
    Case("query_explain_first", query, "query", kwargs={"sql": "SELECT 1", "explain_first": True},
         system=[QueryResult(["plan"], [(_PLAN,)], False)],
         readonly=QueryResult(["?column?"], [(1,)], False)),
    Case("query_governor", query, "query", kwargs={"sql": "SELECT 1", "max_estimated_rows": 10},
         system=[QueryResult(["plan"], [(_PLAN,)], False)], error=True),
    Case("query_rejected", query, "query", kwargs={"sql": "DELETE FROM app_core.assets"}, error=True),
    Case("explain_query", explain, "explain_query", kwargs={"sql": "SELECT 1"},
         system=[QueryResult(["plan"], [(_PLAN,)], False)]),
]

from pgllens.tools import erd

CASES += [
    Case("get_erd", erd, "get_erd"),
    Case("get_erd_text", erd, "get_erd", kwargs={"format": "text", "schema": "app_core"}),
    Case("get_erd_bad_format", erd, "get_erd", kwargs={"format": "widget"}, error=True),
    Case("get_erd_widget", erd, "get_erd_widget", apps=True),
    Case("get_erd_widget_range", erd, "get_erd_widget", kwargs={"max_nodes": 0}, apps=True, error=True),
]
