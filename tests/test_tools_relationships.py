from pgllens.database.introspect import ForeignKey, Table
from pgllens.llens_style import Code, Section
from pgllens.tools import relationships
from pgllens.tools.relationships import format_path, format_relationships, shortest_fk_paths

FK = ForeignKey("fk_pos_item", "wms", "inventory_position", ["item_id"],
                "wms", "item_master", ["id"])


def test_format_relationships_renders_both_directions():
    resp = format_relationships([FK], focus="item_master", scope="wms.item_master")
    out = "\n".join(str(s) for s in resp.sections)
    assert "inventory_position" in out and "item_master" in out
    assert "item_id" in out and "id" in out


def test_format_relationships_reports_both_sections_even_when_one_is_empty():
    resp = format_relationships([FK], focus="audit_log", scope="wms.audit_log")
    headings = [s.heading for s in resp.sections]
    assert headings == ["outgoing", "incoming"]
    outgoing_table, incoming_table = (s.blocks[0] for s in resp.sections)
    assert outgoing_table.rows == ()
    assert incoming_table.rows == ()
    assert resp.tally == ("0 outgoing", "0 incoming")


def test_composite_key_columns_stay_paired():
    fk = ForeignKey("fk_c", "wms", "a", ["x", "y"], "wms", "b", ["p", "q"])
    resp = format_relationships([fk], focus=None, scope=None)
    table = resp.sections[0].blocks[0]
    assert table.rows[0][1] == "`x, y`" and table.rows[0][3] == "`p, q`"


# --- Task 4: find_path -- shortest FK join path between two tables ---

# Chain: order_line -> order -> customer -> region (child -> parent).
LINE_ORDER = ForeignKey("fk_line_order", "app", "order_line", ["order_id"],
                        "app", "order", ["id"])
ORDER_CUST = ForeignKey("fk_order_customer", "app", "order", ["customer_id"],
                        "app", "customer", ["id"])
CUST_REGION = ForeignKey("fk_customer_region", "app", "customer", ["region_id"],
                         "app", "region", ["id"])
CHAIN = [LINE_ORDER, ORDER_CUST, CUST_REGION]

# Diamond: a and c both reference b1 and b2, so a..c has two 2-hop paths.
A_B1 = ForeignKey("fk_a_b1", "app", "a", ["b1_id"], "app", "b1", ["id"])
A_B2 = ForeignKey("fk_a_b2", "app", "a", ["b2_id"], "app", "b2", ["id"])
C_B1 = ForeignKey("fk_c_b1", "app", "c", ["b1_id"], "app", "b1", ["id"])
C_B2 = ForeignKey("fk_c_b2", "app", "c", ["b2_id"], "app", "b2", ["id"])
DIAMOND = [A_B1, A_B2, C_B1, C_B2]


def test_shortest_fk_paths_finds_the_two_hop_path_along_a_chain():
    paths = shortest_fk_paths(CHAIN, ("app", "order_line"), ("app", "customer"), 6)
    assert [[fk.constraint for fk in p] for p in paths] == [
        ["fk_line_order", "fk_order_customer"]]


def test_shortest_fk_paths_traverses_an_fk_against_its_direction():
    # order_line -> order is traversed child->parent, order <- customer parent->child
    # is the same edge walked backwards: a join doesn't care about FK direction.
    paths = shortest_fk_paths(CHAIN, ("app", "customer"), ("app", "order_line"), 6)
    assert [fk.constraint for fk in paths[0]] == ["fk_order_customer", "fk_line_order"]


def test_shortest_fk_paths_returns_every_shortest_path_of_a_diamond():
    paths = shortest_fk_paths(DIAMOND, ("app", "a"), ("app", "c"), 6)
    assert [[fk.constraint for fk in p] for p in paths] == [
        ["fk_a_b1", "fk_c_b1"], ["fk_a_b2", "fk_c_b2"]]


def test_shortest_fk_paths_returns_empty_when_the_tables_are_unconnected():
    assert shortest_fk_paths(CHAIN, ("app", "order_line"), ("app", "audit_log"), 6) == []


def test_shortest_fk_paths_respects_max_hops():
    endpoints = (("app", "order_line"), ("app", "region"))
    assert len(shortest_fk_paths(CHAIN, *endpoints, 3)[0]) == 3
    assert shortest_fk_paths(CHAIN, *endpoints, 2) == []


def _join_code(resp) -> str:
    join_section = next(s for s in resp.sections if s.heading == "join")
    code = join_section.blocks[0]
    assert isinstance(code, Code)
    return code.text


def test_format_path_renders_hops_in_fk_direction_and_a_join_block():
    resp = format_path(shortest_fk_paths(CHAIN, ("app", "customer"), ("app", "order_line"), 6),
                       ("app", "customer"), ("app", "order_line"), 6)
    assert resp.scope == "app.customer → app.order_line"
    assert resp.tally == ("2 hops", "1 path")
    hops = next(s for s in resp.sections if s.heading == "hops").blocks[0]
    # Rendered child.col -> parent.col even though hop 1 was walked backwards.
    assert hops.rows[0] == ("1", "`app.order.customer_id`", "`app.customer.id`",
                            "`fk_order_customer`")
    assert ('FROM "app"."customer" "cu" JOIN "app"."order" "or2" '
            'ON "cu"."id" = "or2"."customer_id" '
            'JOIN "app"."order_line" "or3" ON "or2"."id" = "or3"."order_id"') in _join_code(resp)


def test_format_path_join_block_aliases_every_table_uniquely():
    resp = format_path(shortest_fk_paths(CHAIN, ("app", "order_line"), ("app", "region"), 6),
                       ("app", "order_line"), ("app", "region"), 6)
    join = _join_code(resp)
    aliases = [part.split()[1].strip('"') for part in join.split("JOIN ")[1:]]
    assert len(set(aliases)) == len(aliases)
    assert "as" not in aliases and "or" not in aliases  # 2-letter SQL keywords


def test_format_path_lists_alternative_paths():
    resp = format_path(shortest_fk_paths(DIAMOND, ("app", "a"), ("app", "c"), 6),
                       ("app", "a"), ("app", "c"), 6)
    alts = next(s for s in resp.sections if s.heading == "alternatives").blocks[0]
    assert ("`app.a` → `app.b2` → `app.c`", "`fk_a_b2`, `fk_c_b2`") in alts.rows


# --- Fix round 1 ---

# Two FKs between the same pair: the alternative line must name the constraint,
# otherwise both paths render as the same "order → address".
SHIP = ForeignKey("fk_ship", "app", "order", ["ship_addr_id"], "app", "address", ["id"])
BILL = ForeignKey("fk_bill", "app", "order", ["bill_addr_id"], "app", "address", ["id"])


def test_parallel_fks_between_one_pair_yield_two_paths():
    paths = shortest_fk_paths([SHIP, BILL], ("app", "order"), ("app", "address"), 6)
    assert [[fk.constraint for fk in p] for p in paths] == [["fk_bill"], ["fk_ship"]]


def test_alternative_paths_are_distinguishable_by_constraint():
    endpoints = (("app", "order"), ("app", "address"))
    resp = format_path(shortest_fk_paths([SHIP, BILL], *endpoints, 6), *endpoints, 6)
    hops = next(s for s in resp.sections if s.heading == "hops").blocks[0]
    assert hops.rows[0] == ("1", "`app.order.bill_addr_id`", "`app.address.id`", "`fk_bill`")
    alts = next(s for s in resp.sections if s.heading == "alternatives").blocks[0]
    assert ("`app.order` → `app.address`", "`fk_ship`") in alts.rows


def test_join_block_quotes_every_identifier():
    fk = ForeignKey("fk_wo_asset", "app core", "Work Order", ["Asset Id"],
                    "app core", "Asset", ["Id"])
    endpoints = (("app core", "Work Order"), ("app core", "Asset"))
    resp = format_path(shortest_fk_paths([fk], *endpoints, 6), *endpoints, 6)
    assert ('FROM "app core"."Work Order" "wo" JOIN "app core"."Asset" "as2" '
            'ON "as2"."Id" = "wo"."Asset Id"') in _join_code(resp)


def test_composite_hops_use_the_paren_style():
    fk = ForeignKey("fk_c", "app", "child", ["a", "b"], "app", "parent", ["x", "y"])
    endpoints = (("app", "child"), ("app", "parent"))
    resp = format_path(shortest_fk_paths([fk], *endpoints, 6), *endpoints, 6)
    hops = next(s for s in resp.sections if s.heading == "hops").blocks[0]
    assert hops.rows[0] == ("1", "`app.child.(a, b)`", "`app.parent.(x, y)`", "`fk_c`")


def test_shortest_fk_paths_caps_the_returned_paths():
    # 7 equally short routes from hub to goal; only the first 5 by constraint name.
    fan = [fk for i in range(7) for fk in (
        ForeignKey(f"fk_hub_m{i}", "app", "hub", [f"m{i}_id"], "app", f"m{i}", ["id"]),
        ForeignKey(f"fk_goal_m{i}", "app", "goal", [f"m{i}_id"], "app", f"m{i}", ["id"]))]
    paths = shortest_fk_paths(fan, ("app", "hub"), ("app", "goal"), 6)
    assert len(paths) == 5
    assert [p[0].constraint for p in paths] == [f"fk_hub_m{i}" for i in range(5)]


def test_self_referencing_fk_neither_loops_nor_joins_a_table_to_itself():
    self_fk = ForeignKey("fk_emp_manager", "app", "employee", ["manager_id"],
                         "app", "employee", ["id"])
    emp_dept = ForeignKey("fk_emp_dept", "app", "employee", ["dept_id"],
                          "app", "dept", ["id"])
    paths = shortest_fk_paths([self_fk, emp_dept], ("app", "employee"), ("app", "dept"), 6)
    assert [[fk.constraint for fk in p] for p in paths] == [["fk_emp_dept"]]
    assert shortest_fk_paths([self_fk], ("app", "employee"), ("app", "dept"), 6) == []


def test_format_path_reports_no_path_with_the_hop_budget():
    resp = format_path([], ("app", "order_line"), ("app", "audit_log"), 4)
    assert resp.tally == ("0 paths",)
    section = resp.sections[0]
    assert isinstance(section, Section)
    bullets = section.blocks[0]
    assert ("hops searched", "4") == (bullets.items[0].key, bullets.items[0].value)
    next_tools = [c.tool for c in resp.next]
    assert "get_relationships" in next_tools


class _FakeMCP:
    def __init__(self):
        self.tools = {}

    def tool(self, **kw):
        def deco(fn):
            self.tools[fn.__name__] = fn
            return fn
        return deco


class _FakeDb:
    def resolve_schema(self, schema):
        return schema


def _table(schema, name):
    return Table(schema=schema, name=name, kind="r", comment=None, columns=[],
                 primary_key=[], row_estimate=0)


class _FakeIntro:
    async def table(self, table, schema=None):
        return _table(schema or "app", table)

    async def foreign_keys(self):
        return CHAIN


def _find_path():
    mcp = _FakeMCP()
    relationships.register(mcp, _FakeDb(), None, _FakeIntro(), None)
    return mcp.tools["find_path"]


async def test_find_path_reports_identical_endpoints():
    out = await _find_path()("order", "order")
    assert "- code: `ARG_OUT_OF_RANGE`" in out


async def test_find_path_rejects_an_out_of_range_max_hops():
    out = await _find_path()("order_line", "region", max_hops=11)
    assert "- code: `ARG_OUT_OF_RANGE`" in out and "between 1 and 10" in out


async def test_find_path_renders_the_shortest_path():
    out = await _find_path()("order_line", "customer")
    assert "app.order_line → app.customer" in out
    assert "fk_line_order" in out
