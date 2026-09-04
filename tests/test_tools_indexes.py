from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.tools import indexes
from pgllens.tools._util import respond
from pgllens.tools.indexes import format_index_health
from tests.conftest import FakeMCP, make_registered

_COLUMNS = ["schema", "table", "index", "idx_scan", "index_size", "is_invalid",
            "indkey", "indrelid", "constraint_type", "indpred", "indexprs"]

_FK_COLUMNS = ["schema", "table", "column", "constraint", "conrelid", "leading_attnum"]


def _empty():
    return QueryResult(_COLUMNS, [], False)


_WINDOW_COLUMNS = ["since", "from_postmaster", "days"]


def _window(days, from_postmaster=False):
    return QueryResult(_WINDOW_COLUMNS,
                       [(datetime(2026, 8, 1, 0, 0, 0, tzinfo=UTC), from_postmaster, days)], False)


def _render(result, fk_coverage=None, scope="wms", others=()):
    return respond(format_index_health(result, fk_coverage, scope, list(others)))


def test_format_index_health_flags_unused_invalid_and_duplicate():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_pkey", 100, 8192, False, "1", 100, "p", None, None),
        ("wms", "shipment", "shipment_status_idx", 0, 8192, False, "2", 100, None, None, None),
        ("wms", "shipment", "shipment_status_idx2", 5, 8192, True, "2", 100, None, None, None),
    ], False)
    out = _render(result)
    assert "shipment_pkey" in out
    assert "unused" in out and "shipment_status_idx" in out
    assert "invalid" in out and "shipment_status_idx2" in out
    assert "duplicates" in out
    assert "shipment_status_idx" in out and "shipment_status_idx2" in out


def test_format_index_health_zero_tally_for_empty_result():
    out = _render(_empty())
    assert "0 indexes" in out
    assert "### unused" not in out
    assert "### invalid" not in out


def test_format_index_health_excludes_pk_backed_index_from_unused():
    # 0 scans, but it's the primary key's backing index -- must not be
    # called out as an unused-index candidate.
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_pkey", 0, 8192, False, "1", 100, "p", None, None),
    ], False)
    out = _render(result)
    assert "shipment_pkey" in out
    assert "### unused" not in out


def test_format_index_health_excludes_unique_constraint_index_from_unused():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_code_key", 0, 8192, False, "3", 100, "u", None, None),
    ], False)
    out = _render(result)
    assert "### unused" not in out


def test_format_index_health_reports_fk_without_covering_index():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_pkey", 100, 8192, False, "1", 100, "p", None, None),
    ], False)
    fk_result = QueryResult(_FK_COLUMNS, [
        ("wms", "shipment", "customer_id", "shipment_customer_id_fkey", 100, 5),
    ], False)
    out = _render(result, fk_result)
    assert "fks without index" in out
    assert "shipment.customer_id" in out


def test_format_index_health_indexed_fk_not_reported():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_pkey", 100, 8192, False, "1", 100, "p", None, None),
        ("wms", "shipment", "shipment_customer_id_idx", 10, 8192, False, "5", 100, None, None, None),
    ], False)
    fk_result = QueryResult(_FK_COLUMNS, [
        ("wms", "shipment", "customer_id", "shipment_customer_id_fkey", 100, 5),
    ], False)
    out = _render(result, fk_result)
    assert "fks without index" not in out


def test_format_index_health_invalid_index_does_not_count_as_fk_coverage():
    # A failed CREATE INDEX CONCURRENTLY leaves an invalid index behind --
    # Postgres won't plan against it, so it must not suppress the
    # FK-without-coverage call-out just because its leading column matches.
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_pkey", 100, 8192, False, "1", 100, "p", None, None),
        ("wms", "shipment", "shipment_customer_id_idx", 10, 8192, True, "5", 100, None, None, None),
    ], False)
    fk_result = QueryResult(_FK_COLUMNS, [
        ("wms", "shipment", "customer_id", "shipment_customer_id_fkey", 100, 5),
    ], False)
    out = _render(result, fk_result)
    assert "fks without index" in out
    assert "shipment.customer_id" in out


# --- get_index_health: schema-scope caveat ---

async def test_get_index_health_notes_scope_when_schema_defaulted():
    mcp, db, _ = make_registered(indexes)
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(side_effect=[_empty(), QueryResult(_FK_COLUMNS, [], False), _window(30.0)])
    out = await mcp.tools["get_index_health"]()
    assert "Scope is public" in out and "wms" in out
    # Deliberate: the scope caveat still fires on an empty result -- an empty
    # default schema is exactly when the model needs to be told other exposed
    # schemas exist to check.
    assert "0 indexes" in out


async def test_get_index_health_no_scope_note_when_schema_passed_explicitly():
    mcp, db, _ = make_registered(indexes)
    db.resolve_schema = MagicMock(return_value="wms")
    db.run_system = AsyncMock(side_effect=[_empty(), QueryResult(_FK_COLUMNS, [], False), _window(30.0)])
    out = await mcp.tools["get_index_health"](schema="wms")
    assert "Scope is" not in out


async def test_get_index_health_no_scope_note_when_single_schema_exposed():
    mcp, db, settings = FakeMCP(), MagicMock(), MagicMock()
    settings.exposed_schemas = ["public"]
    settings.default_schema = "public"
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(side_effect=[_empty(), QueryResult(_FK_COLUMNS, [], False), _window(30.0)])
    indexes.register(mcp, db, settings, None, None)
    out = await mcp.tools["get_index_health"]()
    assert "Scope is" not in out


def test_get_index_health_docstring_notes_pk_unique_exclusion():
    # audit H5: the docstring claimed "unused indexes (idx_scan = 0)" without
    # mentioning that PK/UNIQUE-backed indexes are excluded from that call-out.
    mcp, _db, _ = make_registered(indexes)
    doc = mcp.tools["get_index_health"].__doc__ or ""
    assert "PK/UNIQUE" in doc


async def test_get_index_health_fk_check_is_scoped_to_exposed_schemas():
    # Introspection drops FKs whose parent table is outside EXPOSED_SCHEMAS
    # (schemas enter by allowlist only), so the uncovered-FK call-out must drop them too --
    # otherwise get_index_health flags a relationship get_relationships and
    # find_path refuse to name.
    mcp, db, _ = make_registered(indexes)
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(side_effect=[_empty(), QueryResult(_FK_COLUMNS, [], False), _window(30.0)])
    await mcp.tools["get_index_health"]()
    fk_call = db.run_system.await_args_list[1]
    assert "fn.nspname = ANY(%s)" in fk_call.args[0]
    assert fk_call.args[1] == ("public", ["public", "wms"])


def test_partial_indexes_with_different_predicates_are_not_duplicates():
    result = QueryResult(_COLUMNS, [
        ("wms", "order", "order_open_idx", 3, 8192, False, "4", 100, None, "(status = 'open')", None),
        ("wms", "order", "order_closed_idx", 3, 8192, False, "4", 100, None, "(status = 'closed')", None),
    ], False)
    out = _render(result)
    assert "duplicates" not in out


def test_expression_indexes_on_the_same_table_are_not_duplicates():
    # indkey is "0" for an expression column, so two unrelated expression
    # indexes collide unless indexprs is part of the key.
    result = QueryResult(_COLUMNS, [
        ("wms", "customer", "customer_lower_email_idx", 3, 8192, False, "0", 100, None, None, "lower(email)"),
        ("wms", "customer", "customer_lower_name_idx", 3, 8192, False, "0", 100, None, None, "lower(name)"),
    ], False)
    out = _render(result)
    assert "duplicates" not in out


def test_true_duplicates_with_same_predicate_are_still_flagged():
    result = QueryResult(_COLUMNS, [
        ("wms", "order", "order_open_idx", 3, 8192, False, "4", 100, None, "(status = 'open')", None),
        ("wms", "order", "order_open_idx2", 3, 8192, False, "4", 100, None, "(status = 'open')", None),
    ], False)
    out = _render(result)
    assert "duplicates" in out and "order_open_idx2" in out


def test_index_health_sql_selects_predicate_and_expressions():
    assert "pg_get_expr(ix.indpred, ix.indrelid) AS indpred" in indexes._INDEX_HEALTH_SQL
    assert "pg_get_expr(ix.indexprs, ix.indrelid) AS indexprs" in indexes._INDEX_HEALTH_SQL


def test_index_health_names_the_stats_window():
    out = respond(format_index_health(_empty(), None, "wms", [], window=_window(42.6)))
    assert "since 2026-08-01T00:00:00Z" in out
    assert "43 days" in out


def test_index_health_short_window_warns_against_dropping():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_status_idx", 0, 8192, False, "2", 100, None, None, None),
    ], False)
    out = respond(format_index_health(result, None, "wms", [], window=_window(0.06)))
    assert "unused" in out
    assert "do not drop" in out.lower()
    assert "0.1 days" in out


def test_index_health_long_window_has_no_drop_warning():
    result = QueryResult(_COLUMNS, [
        ("wms", "shipment", "shipment_status_idx", 0, 8192, False, "2", 100, None, None, None),
    ], False)
    out = respond(format_index_health(result, None, "wms", [], window=_window(90.0)))
    assert "do not drop" not in out.lower()


def test_index_health_window_falls_back_to_postmaster_start():
    out = respond(format_index_health(_empty(), None, "wms", [], window=_window(3.0, from_postmaster=True)))
    assert "server start" in out


async def test_get_index_health_queries_the_stats_window():
    mcp, db, _ = make_registered(indexes)
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(side_effect=[
        _empty(), QueryResult(_FK_COLUMNS, [], False), _window(10.0)])
    out = await mcp.tools["get_index_health"](schema="public")
    sqls = [c[0][0] for c in db.run_system.call_args_list]
    assert any("pg_stat_get_db_stat_reset_time" in s for s in sqls)
    assert "10 days" in out
