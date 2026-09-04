from pgllens.config import Settings
from pgllens.database.introspect import Column, ForeignKey, Table
from pgllens.llens_style import Bullets
from pgllens.llens_style import Table as StyleTable
from pgllens.tools._util import respond
from pgllens.tools.ontology import build_ontology

DSN = "postgresql://u:p@localhost:5432/flux"

ITEM = Table("wms", "item_master", "r", "Items",
             [Column("id", "uuid", False, None, None, 1),
              Column("sku", "text", False, None, None, 2),
              Column("removed_at", "timestamptz", True, None, None, 3)],
             ["id"], 1200)
POS = Table("wms", "inventory_position", "r", None,
            [Column("id", "uuid", False, None, None, 1),
             Column("item_id", "uuid", False, None, None, 2),
             Column("quantity_on_hand", "numeric", False, None, None, 3),
             Column("removed_at", "timestamptz", True, None, None, 4)],
            ["id"], 98000)
FK = ForeignKey("fk_pos_item", "wms", "inventory_position", ["item_id"],
                "wms", "item_master", ["id"])


def _s(**kw):
    return Settings(database_url=DSN, exposed_schemas="wms", **kw)


def _section(resp, heading):
    return next((s for s in resp.sections if s.heading == heading), None)


def _table(section):
    return next(b for b in section.blocks if isinstance(b, StyleTable))


def _bullets(section):
    return next(b for b in section.blocks if isinstance(b, Bullets))


def test_ontology_groups_tables_and_names_the_links():
    out = respond(build_ontology([ITEM, POS], [FK], _s()))
    assert "item_master" in out and "inventory_position" in out
    assert "item_id" in out


def test_ontology_flags_the_soft_delete_convention():
    # Every table carrying removed_at is a soft-delete table; the ontology says
    # so once rather than leaving the model to infer it per table.
    resp = build_ontology([ITEM, POS], [FK], _s())
    conventions = _section(resp, "conventions")
    bullets = {b.key: b.value for b in _bullets(conventions).items}
    assert "removed_at" in bullets["soft delete"]
    assert "soft delete" in bullets


def test_ontology_appends_domain_context_when_configured():
    out = respond(build_ontology([ITEM], [], _s(domain_context="Lots expire per FEFO.")))
    assert "FEFO" in out


def test_ontology_omits_the_context_heading_when_unconfigured():
    resp = build_ontology([ITEM], [], _s())
    assert _section(resp, "domain context") is None


def test_ontology_marks_hub_tables_by_inbound_fk_count():
    out = respond(build_ontology([ITEM, POS], [FK], _s()))
    assert out.index("item_master") < out.index("inventory_position"), (
        "the most-referenced table should lead"
    )


# --- Task 10: real hubs (>=2 inbound FKs, no views), audit/time-series tagging ---

CUSTOMER = Table("wms", "customer", "r", None,
                  [Column("id", "uuid", False, None, None, 1),
                   Column("created_at", "timestamptz", False, None, None, 2)],
                  ["id"], 40)
ORDER_T = Table("wms", "order", "r", None,
                 [Column("id", "uuid", False, None, None, 1),
                  Column("customer_id", "uuid", False, None, None, 2),
                  Column("created_at", "timestamptz", False, None, None, 3)],
                 ["id"], 900)
SHIPMENT = Table("wms", "shipment", "r", None,
                  [Column("id", "uuid", False, None, None, 1),
                   Column("customer_id", "uuid", False, None, None, 2),
                   Column("created_at", "timestamptz", False, None, None, 3)],
                  ["id"], 300)
UNREFERENCED = Table("wms", "warehouse", "r", None,
                      [Column("id", "uuid", False, None, None, 1),
                       Column("created_at", "timestamptz", False, None, None, 2)],
                      ["id"], 5)
CUSTOMER_VIEW = Table("wms", "customer_view", "v", None,
                       [Column("id", "uuid", False, None, None, 1)],
                       ["id"], 40)
CHANGE_LOG = Table("wms", "change_log", "r", None,
                    [Column("id", "bigint", False, None, None, 1),
                     Column("recorded_at", "timestamptz", False, None, None, 2),
                     Column("diff", "jsonb", True, None, None, 3)],
                    ["id"], 500000)
READING = Table("wms", "reading", "r", None,
                 [Column("sensor_id", "uuid", False, None, None, 1),
                  Column("recorded_at", "timestamptz", False, None, None, 2),
                  Column("value", "numeric", False, None, None, 3)],
                 ["sensor_id", "recorded_at"], 2_000_000)
FK_ORDER_CUSTOMER = ForeignKey("fk_order_customer", "wms", "order", ["customer_id"],
                                "wms", "customer", ["id"])
FK_SHIPMENT_CUSTOMER = ForeignKey("fk_shipment_customer", "wms", "shipment", ["customer_id"],
                                   "wms", "customer", ["id"])
FK_VIEW_CUSTOMER = ForeignKey("fk_view_customer", "wms", "customer_view", ["id"],
                               "wms", "customer", ["id"])

_HUB_TABLES = [CUSTOMER, ORDER_T, SHIPMENT, UNREFERENCED, CUSTOMER_VIEW]
_HUB_FKS = [FK_ORDER_CUSTOMER, FK_SHIPMENT_CUSTOMER, FK_VIEW_CUSTOMER]


def test_ontology_hub_requires_two_inbound_fks():
    resp = build_ontology(_HUB_TABLES, _HUB_FKS, _s())
    hubs = _table(_section(resp, "hubs"))
    names = " ".join(row[0] for row in hubs.rows)
    assert "customer" in names


def test_ontology_zero_inbound_table_is_not_a_hub():
    resp = build_ontology(_HUB_TABLES, _HUB_FKS, _s())
    hubs = _table(_section(resp, "hubs"))
    names = " ".join(row[0] for row in hubs.rows)
    assert "warehouse" not in names


def test_ontology_view_is_never_a_hub_even_with_two_inbound_fks():
    # customer_view has 1 inbound fk here, but even if it had 2+ a view must
    # never be listed as a hub.
    resp = build_ontology(_HUB_TABLES + [ORDER_T], [*_HUB_FKS, FK_VIEW_CUSTOMER], _s())
    hubs = _table(_section(resp, "hubs"))
    names = " ".join(row[0] for row in hubs.rows)
    assert "customer_view" not in names


def test_ontology_reports_no_hub_tables_message_when_none_qualify():
    resp = build_ontology([UNREFERENCED, CUSTOMER_VIEW], [], _s())
    hubs = _table(_section(resp, "hubs"))
    assert hubs.rows == ()


def test_ontology_tags_change_log_as_audit():
    resp = build_ontology([CHANGE_LOG], [], _s())
    roles = _table(_section(resp, "roles"))
    row = next(r for r in roles.rows if "change_log" in r[0])
    assert "audit" in row[1].lower()


def test_ontology_tags_reading_as_time_series():
    resp = build_ontology([READING], [], _s())
    roles = _table(_section(resp, "roles"))
    row = next(r for r in roles.rows if "reading" in r[0])
    assert "time-series" in row[1].lower()


def test_ontology_reports_created_at_convention_when_half_of_tables_share_it():
    resp = build_ontology(_HUB_TABLES, _HUB_FKS, _s())
    conventions = _section(resp, "conventions")
    bullets = {b.key: b.value for b in _bullets(conventions).items}
    assert "created_at" in bullets["audit columns"]


# --- Review fixes: dedupe inbound by referencing table, roles skip views ---

FK_ORDER_CUSTOMER_BILLING = ForeignKey(
    "fk_order_billing_customer", "wms", "order", ["billing_customer_id"],
    "wms", "customer", ["id"],
)


def test_ontology_same_table_multi_fk_does_not_double_count_as_two_referencers():
    # order has two FKs to customer (customer_id, billing_customer_id) but is
    # still only ONE referencing table -- customer needs a second, distinct
    # child table to qualify as a hub.
    resp = build_ontology([CUSTOMER, ORDER_T], [FK_ORDER_CUSTOMER, FK_ORDER_CUSTOMER_BILLING], _s())
    hubs = _table(_section(resp, "hubs"))
    assert hubs.rows == ()


def test_ontology_two_distinct_referencing_tables_is_a_hub():
    resp = build_ontology([CUSTOMER, ORDER_T, SHIPMENT],
                          [FK_ORDER_CUSTOMER, FK_SHIPMENT_CUSTOMER], _s())
    hubs = _table(_section(resp, "hubs"))
    names = " ".join(row[0] for row in hubs.rows)
    assert "customer" in names


LOGIN_EVENT_VIEW = Table("wms", "login_event", "v", None,
                          [Column("id", "bigint", False, None, None, 1),
                           Column("recorded_at", "timestamptz", False, None, None, 2)],
                          [], 0)


def test_ontology_audit_shaped_view_is_not_tagged_a_table_role():
    resp = build_ontology([LOGIN_EVENT_VIEW], [], _s())
    assert _section(resp, "roles") is None


# --- Findings E/F: exact `date` type match for temporal detection, views
# excluded from the audit-column convention's numerator and denominator ---

AUDIT_DATE_ONLY = Table("wms", "audit_snapshot", "r", None,
                         [Column("id", "bigint", False, None, None, 1),
                          Column("recorded", "date", False, None, None, 2)],
                         ["id"], 10)


def test_ontology_date_typed_column_counts_as_temporal_for_audit_role():
    # An exact `date` catalog type must be recognized as temporal (not just
    # types containing "timestamp"), so a name-matched audit table with only
    # a `date` column still gets tagged.
    resp = build_ontology([AUDIT_DATE_ONLY], [], _s())
    roles = _table(_section(resp, "roles"))
    row = next(r for r in roles.rows if "audit_snapshot" in r[0])
    assert "audit" in row[1].lower()


def test_ontology_text_column_named_like_a_date_is_not_temporal():
    # A text-typed column merely named "validated_at" must not satisfy the
    # temporal check via substring match on "date" in unrelated text.
    t = Table("wms", "audit_thing", "r", None,
              [Column("id", "bigint", False, None, None, 1),
               Column("validated_at", "text", False, None, None, 2)],
              ["id"], 10)
    resp = build_ontology([t], [], _s())
    roles_section = _section(resp, "roles")
    roles = _table(roles_section) if roles_section else None
    assert roles is None or not any("audit_thing" in r[0] for r in roles.rows)


def test_ontology_view_excluded_from_audit_column_convention_ratio():
    # A single real table with created_at should trip the "half or more"
    # convention on its own; adding a view with created_at must not change
    # the ratio (it must be excluded from both numerator and denominator).
    real_table = Table("wms", "widget", "r", None,
                        [Column("id", "uuid", False, None, None, 1),
                         Column("created_at", "timestamptz", False, None, None, 2)],
                        ["id"], 10)
    view = Table("wms", "widget_view", "v", None,
                 [Column("id", "uuid", False, None, None, 1),
                  Column("created_at", "timestamptz", False, None, None, 2)],
                 [], 0)
    without_view = build_ontology([real_table], [], _s())
    with_view = build_ontology([real_table, view], [], _s())
    bullets_without = {b.key: b.value for b in _bullets(_section(without_view, "conventions")).items}
    bullets_with = {b.key: b.value for b in _bullets(_section(with_view, "conventions")).items}
    assert "1 of 1 tables" in bullets_without["audit columns"]
    assert "1 of 1 tables" in bullets_with["audit columns"]  # view must not shift the denominator
