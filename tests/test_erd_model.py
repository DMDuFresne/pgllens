from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.erd import model

TABLES = QueryResult(
    ["schema", "object", "type", "rows", "description"],
    # "rows" is a single varchar(20) column for the whole result set, so real
    # pyodbc returns numeric strings for tables (not ints) and the literal
    # "n/a" sentinel for views.
    [("dbo", "Customers", "USER_TABLE", "50", "People who buy things"),
     ("dbo", "Orders", "USER_TABLE", "900", None),
     ("dbo", "OrderLines", "USER_TABLE", "4200", None),
     ("dbo", "Lonely", "USER_TABLE", "1", None),
     ("dbo", "Widget", "USER_TABLE", "5", None),
     ("rpt", "Fact", "USER_TABLE", "10", None),
     ("rpt", "vOrders", "VIEW", "n/a", "Reporting view")],
    False,
)

FKS = QueryResult(
    ["fk", "schema", "table", "column", "ref_schema", "ref_table", "ref_column", "not_null"],
    [("FK_Orders_Customers", "dbo", "Orders", "CustomerId", "dbo", "Customers", "Id", True),
     ("FK_Lines_Orders", "dbo", "OrderLines", "OrderId", "dbo", "Orders", "Id", False),
     # cross-schema: dbo.Widget -> rpt.Fact, isolated from the Orders/Customers/
     # OrderLines cluster above so tests for those relationships stay unaffected.
     ("FK_Widget_Fact", "dbo", "Widget", "FactId", "rpt", "Fact", "Id", False)],
    False,
)

COLUMNS = {
    ("dbo", "Orders"): QueryResult(
        ["column", "type", "max_length", "precision", "scale", "is_nullable",
         "is_identity", "is_computed", "default", "description"],
        [("Id", "int", 4, 10, 0, False, True, False, None, None),
         ("CustomerId", "int", 4, 10, 0, False, False, False, None, None),
         ("Amount (USD)", "decimal", 9, 18, 2, True, False, False, None, None)],
        False),
}


def make_intro():
    intro = MagicMock()
    intro.list_tables = AsyncMock(return_value=TABLES)
    intro.relationships = AsyncMock(return_value=FKS)

    async def describe(database, schema, table):
        cols = COLUMNS.get((schema, table))
        return {"columns": cols or QueryResult(
                    ["column", "type", "max_length", "precision", "scale",
                     "is_nullable", "is_identity", "is_computed", "default",
                     "description"], [], False),
                "primary_key": QueryResult(["column"], [("Id",)], False),
                "foreign_keys": FKS,
                "description": None}

    intro.describe_table = AsyncMock(side_effect=describe)
    return intro


async def test_full_database_includes_tables_and_fk_edges():
    erd = await model.build_erd(make_intro(), "Sales")
    names = {(n.schema, n.table) for n in erd.nodes}
    assert ("dbo", "Customers") in names and ("dbo", "OrderLines") in names
    assert len(erd.edges) == 3  # + the isolated dbo.Widget -> rpt.Fact FK
    assert erd.truncated is False


async def test_schema_filter_excludes_other_schemas():
    erd = await model.build_erd(make_intro(), "Sales", schema="rpt")
    # Fact (rpt) is in-schema; Widget (dbo) is only present as the cross-schema
    # FK source kept per audit #9 -- non-external nodes must still be rpt-only.
    non_external = {n.schema for n in erd.nodes if not n.external}
    assert non_external == {"rpt"}


async def test_schema_filter_excludes_views_everywhere():
    # audit #12: a view has no FKs and must never appear in the ERD, filtered
    # or not -- pinned on both this (schema-filtered) and the unfiltered path
    # (test_full_database_includes_tables_and_fk_edges implicitly covers that
    # by never listing vOrders either).
    erd = await model.build_erd(make_intro(), "Sales", schema="rpt")
    assert "vOrders" not in {n.table for n in erd.nodes}
    erd_unfiltered = await model.build_erd(make_intro(), "Sales")
    assert "vOrders" not in {n.table for n in erd_unfiltered.nodes}


async def test_schema_filter_keeps_cross_schema_fk_as_external_stub():
    # audit #9: filtering to schema="dbo" would naively drop rpt.Fact, the
    # target of dbo.Widget's FK -- the edge (and a stub for Fact) must survive.
    erd = await model.build_erd(make_intro(), "Sales", schema="dbo")
    fact = next(n for n in erd.nodes if n.table == "Fact")
    assert fact.schema == "rpt"
    assert fact.external is True
    assert any(
        e.from_table == "Widget" and e.to_table == "Fact" for e in erd.edges
    )


async def test_view_row_count_sentinel_becomes_none_and_validates_via_erd_out():
    # LIST_TABLES emits the literal string "n/a" for views (no row count applies);
    # build_erd must translate that back to None rather than let it flow into the
    # typed int | None contract, where Pydantic would raise. Views are excluded
    # from the ERD (audit #12), so exercise the sentinel via a real table row.
    intro = make_intro()
    intro.list_tables = AsyncMock(return_value=QueryResult(
        TABLES.columns,
        [*(r for r in TABLES.rows if r[2] != "VIEW"),
         ("dbo", "NoStats", "USER_TABLE", "n/a", None)],
        False))
    erd = await model.build_erd(intro, "Sales")
    node = next(n for n in erd.nodes if n.table == "NoStats")
    assert node.rows is None

    out = model.to_erd_out(erd)
    out_node = next(n for n in out.nodes if n.table == "NoStats")
    assert out_node.rows is None


async def test_table_row_count_string_parses_to_int_via_erd_out():
    # LIST_TABLES' rows column is varchar(20), so real pyodbc hands back "50"
    # (a str) for tables too -- this must parse to an int, not become None.
    erd = await model.build_erd(make_intro(), "Sales")
    table = next(n for n in erd.nodes if n.table == "Customers")
    assert table.rows == 50

    out = model.to_erd_out(erd)
    out_table = next(n for n in out.nodes if n.table == "Customers")
    assert out_table.rows == 50


async def test_subset_pulls_in_immediate_fk_neighbours():
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"])
    names = {n.table for n in erd.nodes}
    assert names == {"Orders", "Customers", "OrderLines"}
    assert {n.table for n in erd.nodes if getattr(n, "related", False)} == {
        "Customers", "OrderLines"}


def test_subset_of_unrelated_table_has_no_edges():
    import asyncio
    erd = asyncio.run(model.build_erd(make_intro(), "Sales", tables=["Lonely"]))
    assert {n.table for n in erd.nodes} == {"Lonely"}
    assert erd.edges == []


async def test_columns_included_with_pk_and_fk_flags():
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"])
    orders = next(n for n in erd.nodes if n.table == "Orders")
    by_name = {c.name: c for c in orders.columns}
    assert by_name["Id"].is_pk is True
    assert by_name["CustomerId"].is_fk is True
    assert by_name["Amount (USD)"].nullable is True   # delimited name survives intact


async def test_include_columns_false_omits_columns_and_skips_describe():
    intro = make_intro()
    erd = await model.build_erd(intro, "Sales", include_columns=False)
    assert all(n.columns == [] for n in erd.nodes)
    intro.describe_table.assert_not_awaited()


async def test_max_nodes_truncates_deterministically_with_a_note():
    erd = await model.build_erd(make_intro(), "Sales", max_nodes=2)
    assert erd.truncated is True
    assert len(erd.nodes) == 2
    assert erd.note and "2" in erd.note
    # highest-degree nodes survive: Orders (2 edges) then Customers/OrderLines (1 each,
    # tie broken by name) — Lonely (0) must be gone
    assert "Lonely" not in {n.table for n in erd.nodes}
    again = await model.build_erd(make_intro(), "Sales", max_nodes=2)
    assert [(n.schema, n.table) for n in erd.nodes] == [
        (n.schema, n.table) for n in again.nodes]          # deterministic


async def test_edges_to_dropped_nodes_are_removed():
    erd = await model.build_erd(make_intro(), "Sales", max_nodes=1)
    kept = {(n.schema, n.table) for n in erd.nodes}
    for e in erd.edges:
        assert (e.from_schema, e.from_table) in kept
        assert (e.to_schema, e.to_table) in kept


async def test_to_dict_shape_is_the_widget_contract():
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"])
    d = model.to_dict(erd)
    assert set(d) >= {"database", "nodes", "edges", "truncated"}
    node = d["nodes"][0]
    assert set(node) >= {"schema", "table", "kind", "columns", "related"}
    edge = d["edges"][0]
    assert set(edge) >= {"from", "to", "constraint"}
    assert set(edge["from"]) == {"schema", "table", "column"}


async def test_to_dict_is_json_serialisable_and_round_trips():
    import json
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"])
    d = model.to_dict(erd)
    text = json.dumps(d)
    assert json.loads(text) == d


async def test_to_mermaid_quotes_identifiers_and_lists_relationships():
    erd = await model.build_erd(make_intro(), "Sales")
    text = model.to_mermaid(erd)
    assert text.startswith("erDiagram")
    assert '"dbo.Orders"' in text
    assert "FK_Orders_Customers" in text or "CustomerId" in text


async def test_to_mermaid_not_null_fk_renders_one_or_many_cardinality():
    """FK_Orders_Customers.CustomerId is NOT NULL (Task 4): child side must be
    }| (one-or-many), not }o (zero-or-many)."""
    erd = await model.build_erd(make_intro(), "Sales")
    text = model.to_mermaid(erd)
    assert '"dbo.Orders" }|--|| "dbo.Customers" : "FK_Orders_Customers"' in text


async def test_to_mermaid_nullable_fk_renders_zero_or_many_cardinality():
    """FK_Lines_Orders.OrderId is nullable in the fixture: child side stays }o."""
    erd = await model.build_erd(make_intro(), "Sales")
    text = model.to_mermaid(erd)
    assert '"dbo.OrderLines" }o--|| "dbo.Orders" : "FK_Lines_Orders"' in text


async def test_to_mermaid_survives_identifiers_with_spaces():
    intro = make_intro()
    intro.list_tables = AsyncMock(return_value=QueryResult(
        TABLES.columns, [("dbo", "Order Details", "USER_TABLE", 5, None)], False))
    intro.relationships = AsyncMock(return_value=QueryResult(FKS.columns, [], False))
    text = model.to_mermaid(await model.build_erd(intro, "Sales"))
    assert '"dbo.Order Details"' in text


async def test_to_mermaid_emits_unquoted_valid_attribute_syntax():
    """Regression: mermaid erDiagram attribute lines can't have quoted names --
    strict renderers reject `integer "asset_id" PK`. Types with spaces (e.g.
    Postgres's `timestamp with time zone`) must be underscored, not quoted."""
    intro = make_intro()
    intro.describe_table = AsyncMock(return_value={
        "columns": QueryResult(
            ["column", "type", "max_length", "precision", "scale", "is_nullable",
             "is_identity", "is_computed", "default", "description"],
            [("created_at", "timestamp with time zone", 8, 0, 0, False, False, False, None, None)],
            False),
        "primary_key": QueryResult(["column"], [], False),
        "foreign_keys": FKS,
        "description": None,
    })
    erd = await model.build_erd(intro, "Sales", tables=["Orders"])
    text = model.to_mermaid(erd)
    assert "timestamp_with_time_zone created_at" in text
    assert '"created_at"' not in text


def test_mermaid_token_strips_trailing_separators():
    # numeric(12,4) must sanitize to numeric_12_4, not numeric_12_4_
    # (trailing underscore from the sanitized closing paren)
    assert model._mermaid_token("numeric(12,4)") == "numeric_12_4"
    assert model._mermaid_token("timestamp with time zone") == "timestamp_with_time_zone"


def test_mermaid_token_never_returns_empty():
    # All-symbol identifiers (e.g. %%% or ___) must not collapse to empty
    assert model._mermaid_token("%%%") == "unknown"
    assert model._mermaid_token("___") == "unknown"
    assert model._mermaid_token("!@#$%") == "unknown"


def test_to_mermaid_dedupes_colliding_sanitized_column_tokens():
    # "size%" and "size_" both sanitize to "size" after stripping trailing
    # underscores -- the second must not silently shadow the first in the
    # rendered attribute block.
    node = model.ErdNode(
        schema="dbo", table="Widget", kind="table", rows=1, description=None,
        columns=[
            model.ErdColumn(name="size%", type="int", nullable=False, is_pk=False, is_fk=False),
            model.ErdColumn(name="size_", type="int", nullable=False, is_pk=False, is_fk=False),
        ],
    )
    erd = model.Erd(database=None, nodes=[node], edges=[])
    text = model.to_mermaid(erd)
    # Extract attribute lines and verify both size% and size_ are distinct
    lines = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("int ")]
    assert len(lines) == 2
    assert set(lines) == {"int size", "int size_2"}


def test_to_mermaid_all_symbol_column_names_use_unknown_fallback():
    # Column names that are all punctuation (%%%, ___) must not render as blank
    # attribute names -- they should use the "unknown" sentinel and still dedupe
    node = model.ErdNode(
        schema="dbo", table="Widget", kind="table", rows=1, description=None,
        columns=[
            model.ErdColumn(name="%%%", type="int", nullable=False, is_pk=False, is_fk=False),
            model.ErdColumn(name="___", type="int", nullable=False, is_pk=False, is_fk=False),
        ],
    )
    erd = model.Erd(database=None, nodes=[node], edges=[])
    text = model.to_mermaid(erd)
    # Both should render with distinct non-blank attribute names
    lines = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("int ")]
    assert len(lines) == 2
    # Both collapse to "unknown", dedupe makes them "unknown" and "unknown_2"
    assert set(lines) == {"int unknown", "int unknown_2"}


async def test_to_mermaid_omits_empty_braces_when_columns_are_excluded():
    # Empty-braces cleanup: include_columns=False must emit a bare entity name,
    # not `"dbo.Orders" {\n    }` -- an attribute-less block strict renderers
    # still accept, but it's pure noise.
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"], include_columns=False)
    text = model.to_mermaid(erd)
    assert '"dbo.Orders" {' not in text
    assert '"dbo.Orders"' in text
    assert "    }" not in text.splitlines()


async def test_unknown_table_name_produces_a_warning_with_a_fuzzy_hint():
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders", "Custmers"])
    assert any(
        w.startswith("ignored unknown table: Custmers") and "Customers" in w
        for w in erd.warnings
    )


async def test_unknown_table_name_with_no_close_match_has_no_hint():
    erd = await model.build_erd(make_intro(), "Sales", tables=["nonexistent_table"])
    assert erd.warnings == ["ignored unknown table: nonexistent_table"]


async def test_known_table_names_produce_no_warnings():
    erd = await model.build_erd(make_intro(), "Sales", tables=["Orders"])
    assert erd.warnings == []


async def test_view_name_in_tables_produces_exclusion_warning_not_unknown():
    # A view name passed via `tables=` is "known" (matches a real object), so
    # _unknown_table_warnings stays quiet -- but _select_nodes' kind filter
    # drops it anyway. That must surface as a warning, not vanish silently.
    erd = await model.build_erd(make_intro(), "Sales", tables=["vOrders"])
    assert "vOrders" not in {n.table for n in erd.nodes}
    assert erd.warnings == ["excluded (view): vOrders — views are not shown in the ERD"]


async def test_tables_depth_defaults_to_immediate_neighbours_only():
    erd = await model.build_erd(make_intro(), "Sales", tables=["OrderLines"])
    assert {n.table for n in erd.nodes} == {"OrderLines", "Orders"}


async def test_tables_depth_two_expands_a_second_hop_and_stops_there():
    # OrderLines -> Orders -> Customers is the only chain; depth=2 must reach
    # Customers and still leave the unrelated Widget/Fact cluster out.
    erd = await model.build_erd(make_intro(), "Sales", tables=["OrderLines"], depth=2)
    assert {n.table for n in erd.nodes} == {"OrderLines", "Orders", "Customers"}
    assert {n.table for n in erd.nodes if n.related} == {"Orders", "Customers"}


async def test_tables_depth_three_stops_when_the_frontier_is_exhausted():
    # Same two-hop chain: depth=3 finds nothing new and must exit early rather
    # than keep re-adding the frontier.
    erd = await model.build_erd(make_intro(), "Sales", tables=["OrderLines"], depth=3)
    assert {n.table for n in erd.nodes} == {"OrderLines", "Orders", "Customers"}


async def test_an_explicitly_requested_table_survives_max_nodes_truncation():
    # Degree-only truncation dropped OrderLines (degree 1) and kept its
    # depth-2 neighbour Customers (also degree 1, but earlier alphabetically).
    erd = await model.build_erd(make_intro(), "Sales", tables=["OrderLines"],
                                depth=2, max_nodes=1)
    assert [n.table for n in erd.nodes] == ["OrderLines"]


async def test_schema_filtered_truncation_keeps_the_in_schema_table():
    # rpt.Fact is in-schema; dbo.Widget is only an external FK stub kept so the
    # cross-schema edge survives. With max_nodes=1 the stub must be the one
    # dropped -- name order alone used to keep "dbo.Widget" over "rpt.Fact".
    erd = await model.build_erd(make_intro(), "Sales", schema="rpt", max_nodes=1)
    assert [(n.schema, n.table) for n in erd.nodes] == [("rpt", "Fact")]


def test_tables_expansion_skips_an_fk_whose_other_end_is_not_a_node():
    # An FK into a schema that is not exposed (or onto a view) has no node. The
    # depth expansion must not follow it: marking it `related` KeyError'd and
    # surfaced as a database error. (Introspector now filters such FKs at the
    # source; this pins the model's own guard.)
    nodes = {
        ("app", "orders"): model.ErdNode("app", "orders", "table", 1, None),
        ("app", "invoices"): model.ErdNode("app", "invoices", "table", 1, None),
    }
    edges = [
        model.ErdEdge("app", "orders", "customer_id", "hidden", "customers", "id", "fk_cust"),
        model.ErdEdge("app", "invoices", "order_id", "app", "orders", "id", "fk_order"),
    ]
    selected = model._select_nodes(nodes, edges, None, ["orders"], depth=3)
    assert selected == {("app", "orders"), ("app", "invoices")}
    assert nodes[("app", "invoices")].related
