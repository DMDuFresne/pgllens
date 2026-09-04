from types import SimpleNamespace
from typing import ClassVar

import pytest

from pgllens.database.format import QueryResult
from pgllens.database.introspect import Column, Table
from pgllens.database.pool import UnknownSchemaError
from pgllens.tools import discovery
from pgllens.tools.discovery import format_columns, format_table_list, sample_sql

ITEM = Table(schema="wms", name="item_master", kind="r", comment="Items",
             columns=[Column("id", "uuid", False, None, None, 1),
                      Column("sku", "text", False, None, "Stock code", 2),
                      Column("removed_at", "timestamptz", True, None, None, 3)],
             primary_key=["id"], row_estimate=1200)


def _row(t, name: str) -> tuple[str, ...]:
    return next(r for r in t.rows if r[0] == f"`{name}`")


def test_format_columns_marks_pk_and_nullability():
    out = format_columns(ITEM)
    pk_idx = out.columns.index("pk")
    assert _row(out, "id")[pk_idx] == "✓"
    sku_row = _row(out, "sku")
    assert sku_row[out.columns.index("comment")] == "Stock code"
    assert any(r[0] == "`removed_at`" for r in out.rows)


def test_format_columns_renders_identity_instead_of_a_blank_default():
    """D1: an identity column's default is blank in the catalog -- rendering
    it as "" reads as "you must supply this value", when in fact Postgres
    fills it in. GENERATED ALWAYS/BY DEFAULT AS IDENTITY must show instead."""
    table = Table(
        schema="wms", name="item_master", kind="r", comment=None,
        columns=[Column("id", "bigint", False, None, None, 1, is_identity="a"),
                 Column("seq", "bigint", False, None, None, 2, is_identity="d"),
                 Column("sku", "text", False, None, None, 3)],
        primary_key=["id"], row_estimate=0,
    )
    out = format_columns(table)
    default_idx = out.columns.index("default")
    assert _row(out, "id")[default_idx] == "`identity (always)`"
    assert _row(out, "seq")[default_idx] == "`identity (by default)`"
    assert _row(out, "sku")[default_idx] == ""


def test_format_table_list_shows_kind_and_row_estimate():
    out = format_table_list([ITEM])
    row = out.rows[0]
    assert row[out.columns.index("table")] == "`item_master`"
    assert row[out.columns.index("rows (estimate)")] == "~1.2K"


def test_sample_sql_quotes_identifiers_and_binds_the_limit():
    sql, params = sample_sql(ITEM, limit=5)
    assert '"wms"."item_master"' in sql
    assert params == (5,)
    assert "LIMIT %s" in sql


def test_sample_sql_rejects_a_limit_over_the_cap():
    with pytest.raises(ValueError, match="between 1 and 1000"):
        sample_sql(ITEM, limit=5000)


def test_sample_sql_quotes_a_hostile_identifier():
    hostile = Table(schema="wms", name='t"; DROP TABLE x --', kind="r",
                    comment=None, columns=[], primary_key=[], row_estimate=0)
    sql, _ = sample_sql(hostile, limit=1)
    # Identifiers cannot be bound as parameters, so they MUST be quoted with
    # doubled internal quotes -- this is the one place user input reaches SQL text.
    assert 't""; DROP TABLE x --' in sql


# --- M7: describe_table/get_sample_data on an unexposed schema must surface
# UnknownSchemaError, same as list_tables -- not intro.table's unrelated
# TableNotFoundError, which would give a different audit outcome for the
# same user mistake. ---

class _FakeDb:
    def resolve_schema(self, schema):
        raise UnknownSchemaError(f"Schema {schema!r} is not exposed. Available: wms")


class _FakeIntro:
    async def table(self, table, schema=None):
        raise AssertionError("intro.table must not be reached: resolve_schema rejects first")


class _FakeMCP:
    def __init__(self):
        self.tools = {}

    def tool(self, **kw):
        def deco(fn):
            self.tools[fn.__name__] = fn
            return fn
        return deco


class _FakeSettings:
    redact_columns: ClassVar[list] = []


async def test_describe_table_on_an_unexposed_schema_reports_unknown_schema():
    mcp = _FakeMCP()
    discovery.register(mcp, _FakeDb(), _FakeSettings(), _FakeIntro(), None)
    out = await mcp.tools["describe_table"]("item_master", schema="secret")
    # @tool_errors turns UnknownSchemaError into the SCHEMA_UNKNOWN error envelope --
    # same code list_tables produces for the identical mistake.
    assert "- code: `SCHEMA_UNKNOWN`" in out
    assert "not exposed" in out


async def test_get_sample_data_on_an_unexposed_schema_reports_unknown_schema():
    mcp = _FakeMCP()
    discovery.register(mcp, _FakeDb(), _FakeSettings(), _FakeIntro(), None)
    out = await mcp.tools["get_sample_data"]("item_master", schema="secret")
    assert "- code: `SCHEMA_UNKNOWN`" in out
    assert "not exposed" in out


# --- Task 2: enum labels + low-cardinality values in describe_table ---

_COLS = ["id", "quality", "state", "note", "one", "zero", "many"]
READINGS = Table(
    schema="app_core", name="reading", kind="r", comment=None,
    columns=[Column("id", "uuid", False, None, None, 1),
             Column("quality", "app_core.reading_quality", False, None, None, 2),
             *(Column(n, "text", True, None, None, i)
               for i, n in enumerate(_COLS[2:], start=3))],
    primary_key=["id"], row_estimate=10,
)

_STATS_COLUMNS = ["attname", "n_distinct", "mcv", "inherited"]
_STATS = QueryResult(
    columns=_STATS_COLUMNS,
    rows=[("state", 3.0, ["open", "closed", "pending"], False),
          # READINGS.row_estimate is 10, so abs(-3.0) * 10 = 30 distinct -- still
          # over _MAX_VALUES even after D2's negative-fraction resolution.
          ("note", -3.0, ["a", "b"], False),
          ("quality", 3.0, ["good", "good", "good"], False),
          # n_distinct boundary: 1 is in, 0 (unknown) and 21 are out.
          ("one", 1.0, ["only"], False),
          ("zero", 0.0, ["nope"], False),
          ("many", 21.0, ["nope"], False)],
    truncated=False,
)


class _StatsDb:
    def __init__(self, stats):
        self.stats = stats
        self.params = None
        self.max_rows = None

    def resolve_schema(self, schema):
        return schema

    async def run_system(self, sql, params=(), max_rows=None):
        self.params = params
        self.max_rows = max_rows
        return self.stats


class _EnumIntro:
    def __init__(self, table, enums):
        self._table = table
        self._enums = enums

    async def table(self, table, schema=None):
        return self._table

    async def enum_labels(self, data_type, type_schema=None):
        return self._enums.get(data_type)


def _describe(stats, table=READINGS, enums=None, redact=()):
    mcp = _FakeMCP()
    db = _StatsDb(stats)
    intro = _EnumIntro(table, {"app_core.reading_quality": ["good", "bad", "uncertain"]}
                       if enums is None else enums)

    settings = SimpleNamespace(redact_columns=list(redact))
    discovery.register(mcp, db, settings, intro, None)
    return mcp.tools["describe_table"], db


def _row_for(out: str, column: str) -> str:
    return next(line for line in out.splitlines() if line.startswith(f"| `{column}` |"))


async def test_describe_table_renders_enum_labels_and_low_cardinality_values():
    describe, db = _describe(_STATS)
    out = await describe("reading")
    assert "| values |" in out                          # new header column
    assert "enum: good, bad, uncertain" in _row_for(out, "quality")
    assert "values: open, closed, pending" in _row_for(out, "state")
    assert db.params == ("app_core", "reading")


async def test_negative_n_distinct_is_not_low_cardinality():
    describe, _ = _describe(_STATS)
    out = await describe("reading")
    assert "values:" not in _row_for(out, "note")


# --- D2: pg_stats.n_distinct is negative -- a fraction of row_estimate -- on
# any table where the column's distinct count exceeds ~10% of rows. Dropping
# every negative n_distinct outright (the old gate) means the sample feature
# almost never fires on a small table. It must resolve against row_estimate
# before the _MAX_VALUES gate instead of being rejected on sign alone. ---

_FRACTION_TABLE = Table(
    schema="app_core", name="widget", kind="r", comment=None,
    columns=[Column("id", "uuid", False, None, None, 1),
             Column("small_frac", "text", True, None, None, 2),
             Column("big_frac", "text", True, None, None, 3)],
    primary_key=["id"], row_estimate=50,
)
_FRACTION_STATS = QueryResult(
    columns=_STATS_COLUMNS,
    rows=[("small_frac", -0.2, ["a", "b"], False),   # abs(-0.2) * 50 = 10 -> in range
          ("big_frac", -0.9, ["a", "b"], False)],     # abs(-0.9) * 50 = 45 -> over cap
    truncated=False,
)


async def test_negative_n_distinct_resolves_against_row_estimate_within_cap():
    describe, _ = _describe(_FRACTION_STATS, table=_FRACTION_TABLE)
    out = await describe("widget")
    assert "values: a, b" in _row_for(out, "small_frac")


async def test_negative_n_distinct_resolves_against_row_estimate_over_cap():
    describe, _ = _describe(_FRACTION_STATS, table=_FRACTION_TABLE)
    out = await describe("widget")
    assert "values:" not in _row_for(out, "big_frac")


async def test_enum_labels_win_over_pg_stats_values():
    describe, _ = _describe(_STATS)
    out = await describe("reading")
    assert "values:" not in _row_for(out, "quality")


async def test_n_distinct_boundary_is_one_through_twenty():
    describe, _ = _describe(_STATS)
    out = await describe("reading")
    assert "values: only" in _row_for(out, "one")     # 1 distinct value is still useful
    assert "values:" not in _row_for(out, "zero")     # 0 = unknown, not "no values"
    assert "values:" not in _row_for(out, "many")     # 21 > the cap


async def test_no_stats_rows_appends_the_analyze_footer():
    describe, _ = _describe(QueryResult(_STATS_COLUMNS, [], False))
    out = await describe("reading")
    assert "run ANALYZE" in out
    # enum labels come from the catalog, so they still render without ANALYZE
    assert "enum: good, bad, uncertain" in _row_for(out, "quality")


async def test_stats_present_but_no_low_cardinality_column_has_no_footer():
    stats = QueryResult(_STATS_COLUMNS, [("state", -0.5, ["a"], False)], False)
    describe, _ = _describe(stats)
    out = await describe("reading")
    assert "run ANALYZE" not in out


async def test_value_lists_are_capped_at_twenty():
    stats = QueryResult(_STATS_COLUMNS,
                        [("state", 20.0, [f"v{i}" for i in range(25)], False)], False)
    describe, _ = _describe(stats)
    row = _row_for(await describe("reading"), "state")
    assert "v19" in row
    assert "v20" not in row
    assert ", …" in row


def test_format_columns_without_values_renders_an_empty_column():
    out = format_columns(ITEM)
    assert "values" in out.columns
    assert all("enum:" not in cell for row in out.rows for cell in row)


# --- Fix round 1 ---

async def test_redacted_columns_show_no_sampled_values():
    """Q1: describe_table prints stored data (most_common_vals), so it must honour
    REDACT_COLUMNS the same way get_sample_data and query do."""
    describe, _ = _describe(_STATS, redact=["%state%"])
    out = await describe("reading")
    row = _row_for(out, "state")
    assert "values:" not in row
    assert "[masked]" not in row, "a redacted column renders nothing, not a marker"
    assert "values: only" in _row_for(out, "one"), "non-matching column is unaffected"


async def test_enum_labels_are_not_redacted():
    """Labels are catalog metadata (the type's definition), not stored rows."""
    describe, _ = _describe(_STATS, redact=["%quality%"])
    assert "enum: good, bad, uncertain" in _row_for(await describe("reading"), "quality")


async def test_own_stats_row_wins_over_the_inherited_one():
    """Q4: an inheritance/partition parent has two pg_stats rows per attname
    (inherited false and true); the own-rows one must win deterministically."""
    stats = QueryResult(_STATS_COLUMNS,
                        [("state", 2.0, ["own1", "own2"], False),
                         ("state", 3.0, ["inh1", "inh2", "inh3"], True)], False)
    describe, _ = _describe(stats)
    row = _row_for(await describe("reading"), "state")
    assert "values: own1, own2" in row
    assert "inh1" not in row


async def test_partitioned_parent_with_only_inherited_stats_still_shows_values():
    """A declarative partitioned parent has no own rows -- only inherited=true --
    so the query must not filter them out."""
    stats = QueryResult(_STATS_COLUMNS,
                        [("state", 3.0, ["a", "b", "c"], True)], False)
    describe, _ = _describe(stats)
    assert "values: a, b, c" in _row_for(await describe("reading"), "state")


def test_stats_sql_orders_by_attname_and_inherited():
    assert "ORDER BY attname, inherited" in discovery._STATS_SQL
    assert "inherited = false" not in discovery._STATS_SQL


async def test_stats_query_is_not_capped_at_the_default_row_limit():
    """Q5: run_system defaults to settings.max_rows (200) and truncates silently --
    a wide table would lose columns off the end of the stats query."""
    describe, db = _describe(_STATS)
    await describe("reading")
    assert db.max_rows == discovery._CATALOG_ROWS


async def test_a_view_with_no_stats_gets_no_analyze_footer():
    """Q6: ANALYZE is invalid on a view or foreign table -- don't tell the caller
    to run it."""
    view = Table(schema="app_core", name="reading_v", kind="v", comment=None,
                 columns=list(READINGS.columns), primary_key=[], row_estimate=0)
    describe, _ = _describe(QueryResult(_STATS_COLUMNS, [], False), table=view)
    assert "run ANALYZE" not in await describe("reading_v")


async def test_a_matview_with_no_stats_still_gets_the_footer():
    matview = Table(schema="app_core", name="reading_m", kind="m", comment=None,
                    columns=list(READINGS.columns), primary_key=[], row_estimate=0)
    describe, _ = _describe(QueryResult(_STATS_COLUMNS, [], False), table=matview)
    assert "run ANALYZE" in await describe("reading_m")
