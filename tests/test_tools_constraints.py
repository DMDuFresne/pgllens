"""Behavioral tests for tools/constraints.py."""

from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.database.introspect import Table, TableNotFoundError
from pgllens.tools import constraints
from pgllens.tools._util import respond
from tests.conftest import make_registered

_ORDERS = Table(schema="public", name="orders", kind="r", comment=None)

COLUMNS = ["schema", "table", "name", "type", "definition", "references", "validated"]


def _rows(*rows):
    return QueryResult(COLUMNS, list(rows), False)


def _intro(table=None):
    intro = MagicMock()
    intro.table = AsyncMock(return_value=table or _ORDERS)
    return intro


async def test_renders_every_constraint_type():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = _rows(
        ("public", "orders", "orders_pkey", "PRIMARY KEY", "PRIMARY KEY (id)", None, True),
        ("public", "orders", "orders_total_chk", "CHECK", "CHECK ((total > 0))", None, True),
    )
    out = await mcp.tools["get_constraints"]()
    assert "orders_pkey" in out
    assert "PRIMARY KEY (id)" in out
    assert "CHECK ((total > 0))" in out
    # A validated constraint renders yes, not blank.
    assert "NOT VALID" not in out
    assert "| yes |" in out


async def test_not_valid_marker_when_unvalidated():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = _rows(
        ("public", "orders", "orders_fk", "FOREIGN KEY",
         "FOREIGN KEY (cid) REFERENCES customers(id) NOT VALID", "public.customers", False),
    )
    out = await mcp.tools["get_constraints"]()
    assert "NOT VALID" in out


def test_format_constraints_renders_validated_and_fk_target_schema():
    result = QueryResult(
        columns=["schema", "table", "name", "type", "definition", "references", "validated"],
        rows=[
            ("app_custom", "tag_alias", "tag_alias_sensor_id_fkey", "FOREIGN KEY",
             "FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)", "app_core.sensor", True),
            ("app_core", "reading", "reading_value_check", "CHECK",
             "CHECK ((value IS NOT NULL))", None, False),
        ],
        truncated=False,
    )
    out = respond(constraints.format_constraints(result, "app_core"))
    assert "| references |" in out and "| validated |" in out
    assert "app_core.sensor" in out
    assert "| yes |" in out
    assert "NOT VALID" in out


async def test_none_found_when_empty():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = _rows()
    out = await mcp.tools["get_constraints"]()
    assert "0 constraints" in out


async def test_table_filter_passes_resolved_schema_and_name():
    """`table=` must filter in SQL by the (schema, name) Introspector resolved,
    not by the caller's raw casing/alias."""
    intro = _intro(Table(schema="wms", name="Orders", kind="r", comment=None))
    mcp, db, _ = make_registered(constraints, intro=intro)
    db.resolve_schema.return_value = "wms"
    db.run_system.return_value = _rows()
    await mcp.tools["get_constraints"](table="orders", schema="WMS")
    sql, params = db.run_system.call_args[0]
    assert "(n.nspname, cl.relname) = (%s, %s)" in sql
    assert params[1:] == ("wms", "Orders")
    db.resolve_schema.assert_called_once_with("WMS")
    intro.table.assert_awaited_once_with("orders", "wms")


async def test_schema_only_narrows_to_that_schema():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.resolve_schema.return_value = "wms"
    db.run_system.return_value = _rows()
    await mcp.tools["get_constraints"](schema="wms")
    sql, params = db.run_system.call_args[0]
    assert "AND n.nspname = %s" in sql
    assert params == (["public", "wms"], "wms")


async def test_unknown_table_returns_not_found_message():
    intro = MagicMock()
    intro.table = AsyncMock(side_effect=TableNotFoundError("Table 'nope' not found."))
    mcp, _db, _ = make_registered(constraints, intro=intro)
    out = await mcp.tools["get_constraints"](table="nope")
    assert "- code: `TABLE_NOT_FOUND`" in out
    assert "not found" in out.lower()


async def test_defaults_to_all_exposed_schemas():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = _rows()
    await mcp.tools["get_constraints"]()
    sql, params = db.run_system.call_args[0]
    assert "n.nspname = ANY(%s)" in sql
    assert params == (["public", "wms"],)
    db.resolve_schema.assert_not_called()


async def test_reads_at_the_catalog_row_cap():
    """Catalog metadata must not be silently cut at the 200-row query cap."""
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = _rows()
    await mcp.tools["get_constraints"]()
    assert db.run_system.call_args.kwargs["max_rows"] == constraints._CATALOG_ROWS


async def test_truncation_at_the_catalog_cap_is_reported():
    mcp, db, _ = make_registered(constraints, intro=_intro())
    db.run_system.return_value = QueryResult(
        COLUMNS,
        [("public", "orders", "orders_pkey", "PRIMARY KEY", "PRIMARY KEY (id)", None, True)],
        True)
    out = await mcp.tools["get_constraints"]()
    assert "truncated at 5000 rows" in out
    assert "Narrow with `table` or `schema`" in out
