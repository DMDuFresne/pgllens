"""Behavioral tests for tools/triggers.py."""

from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.database.introspect import Table, TableNotFoundError
from pgllens.tools import triggers
from tests.conftest import make_registered

_ORDERS = Table(schema="public", name="orders", kind="r", comment=None)

COLUMNS = ["schema", "table", "trigger", "enabled", "definition", "function"]


def _rows(*rows):
    return QueryResult(COLUMNS, list(rows), False)


def _intro(table=None):
    intro = MagicMock()
    intro.table = AsyncMock(return_value=table or _ORDERS)
    return intro


async def test_renders_triggers_with_function_and_footer():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows(
        ("public", "orders", "orders_audit", "enabled",
         "CREATE TRIGGER orders_audit AFTER INSERT ON orders ...", "audit.log_change"),
    )
    out = await mcp.tools["get_triggers"]()
    assert "orders_audit" in out
    assert "audit.log_change" in out
    assert 'Next: get_function_source(function="log_change", schema="audit")' in out


async def test_disabled_trigger_label_renders():
    """tgenabled is mapped to a label in SQL; a disabled trigger must show it."""
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows(
        ("public", "orders", "orders_audit", "disabled", "CREATE TRIGGER ...", "audit.log_change"),
    )
    out = await mcp.tools["get_triggers"]()
    assert "disabled" in out
    sql = db.run_system.call_args[0][0]
    assert "WHEN 'D' THEN 'disabled'" in sql


async def test_internal_triggers_excluded():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows()
    await mcp.tools["get_triggers"]()
    assert "NOT t.tgisinternal" in db.run_system.call_args[0][0]


async def test_none_found_when_empty():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows()
    out = await mcp.tools["get_triggers"]()
    assert "0 triggers" in out


async def test_table_filter_passes_resolved_schema_and_name():
    intro = _intro(Table(schema="wms", name="Orders", kind="r", comment=None))
    mcp, db, _ = make_registered(triggers, intro=intro)
    db.resolve_schema.return_value = "wms"
    db.run_system.return_value = _rows()
    await mcp.tools["get_triggers"](table="orders", schema="WMS")
    sql, params = db.run_system.call_args[0]
    assert "(n.nspname, cl.relname) = (%s, %s)" in sql
    assert params[1:] == ("wms", "Orders")
    db.resolve_schema.assert_called_once_with("WMS")
    intro.table.assert_awaited_once_with("orders", "wms")


async def test_schema_only_narrows_to_that_schema():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.resolve_schema.return_value = "wms"
    db.run_system.return_value = _rows()
    await mcp.tools["get_triggers"](schema="wms")
    sql, params = db.run_system.call_args[0]
    assert "AND n.nspname = %s" in sql
    assert params == (["public", "wms"], "wms")


async def test_unknown_table_returns_not_found_message():
    intro = MagicMock()
    intro.table = AsyncMock(side_effect=TableNotFoundError("Table 'nope' not found."))
    mcp, _db, _ = make_registered(triggers, intro=intro)
    out = await mcp.tools["get_triggers"](table="nope")
    assert "- code: `TABLE_NOT_FOUND`" in out
    assert "not found" in out.lower()


async def test_defaults_to_all_exposed_schemas():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows()
    await mcp.tools["get_triggers"]()
    sql, params = db.run_system.call_args[0]
    assert "n.nspname = ANY(%s)" in sql
    assert params == (["public", "wms"],)
    db.resolve_schema.assert_not_called()


async def test_reads_at_the_catalog_row_cap():
    """Catalog metadata must not be silently cut at the 200-row query cap."""
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = _rows()
    await mcp.tools["get_triggers"]()
    assert db.run_system.call_args.kwargs["max_rows"] == triggers._CATALOG_ROWS


async def test_truncation_at_the_catalog_cap_is_reported():
    mcp, db, _ = make_registered(triggers, intro=_intro())
    db.run_system.return_value = QueryResult(
        COLUMNS,
        [("public", "orders", "orders_ai", "enabled", "CREATE TRIGGER ...", "public.f")],
        True)
    out = await mcp.tools["get_triggers"]()
    assert "truncated at 5000 rows" in out
    assert "Narrow with `table` or `schema`" in out
