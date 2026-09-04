from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.tools import health
from pgllens.tools._util import respond
from pgllens.tools.health import (
    _column_stats,
    _is_stats_eligible,
    format_space_usage,
    format_table_health,
    format_table_stats,
)
from tests.conftest import make_registered

_HEALTH_COLUMNS = ["schema", "table", "n_live_tup", "n_dead_tup", "dead_pct", "last_autovacuum",
                   "n_ins_since_vacuum", "xid_age", "freeze_max_age", "db_xid_age",
                   "bloat_pct", "bloat_bytes"]


def _row(table, n_live, n_dead, dead_pct, last_auto, n_ins, *, schema="wms",
         xid_age=1_000_000, freeze_max_age=200_000_000, db_xid_age=1_000_000,
         bloat_pct=0.0, bloat_bytes=0):
    return (schema, table, n_live, n_dead, dead_pct, last_auto, n_ins,
            xid_age, freeze_max_age, db_xid_age, bloat_pct, bloat_bytes)


def _health(rows):
    return QueryResult(_HEALTH_COLUMNS, list(rows), False)


def _render_health(result, scope="wms", others=()):
    return respond(format_table_health(result, scope, list(others)))


def _render_stats(qualified_name, total_rows, columns):
    return respond(format_table_stats(qualified_name, total_rows, columns))


def _render_space(result, database_size, scope="wms", others=()):
    return respond(format_space_usage(result, database_size, scope, list(others)))


def test_table_health_flags_a_bloated_table():
    r = _health([_row("inventory_position", 98000, 61000, 38.3, None, 0)])
    out = _render_health(r)
    assert "inventory_position" in out and "38.3" in out
    assert "never" in out.lower(), "a NULL last_autovacuum must read as 'never', not NULL"


def test_table_health_empty_reports_zero_tally():
    out = _render_health(_health([]))
    assert "0 tables" in out


def test_table_health_small_clean_table_not_flagged_never_vacuumed():
    # No dead tuples, low insert-since-vacuum, and never autovacuumed -- a
    # tiny freshly-created table looks exactly like this and should not be
    # called out as neglected.
    r = _health([_row("tiny_lookup", 12, 0, 0, None, 3)])
    out = _render_health(r)
    assert "never vacuumed" not in out.lower()


def test_table_health_dead_tuples_still_flagged_never_vacuumed():
    r = _health([_row("churny", 100, 5, 4.8, None, 0)])
    out = _render_health(r)
    assert "never vacuumed" in out.lower()
    assert "### needs attention" in out


def test_table_health_high_insert_since_vacuum_flagged_never_vacuumed():
    r = _health([_row("loaded", 50000, 0, 0, None, 5000)])
    out = _render_health(r)
    assert "never vacuumed" in out.lower()


def test_table_health_sql_selects_live_count_from_reltuples_not_stats_collector():
    # list_tables' row-count column comes from pg_class.reltuples; table_health
    # must use the same source so the two tools don't disagree (n_live_tup is a
    # different, independently-updated stats-collector counter).
    sql = health._table_health_sql(13)
    assert "c.reltuples::bigint AS n_live_tup" in sql
    assert "s.n_live_tup AS n_live_tup" not in sql
    # dead_pct is still legitimately sourced from the stats collector.
    assert "s.n_live_tup + s.n_dead_tup" in sql


def test_table_health_header_labels_live_tuples_as_estimate():
    r = _health([_row("shipment", 10, 0, 0, None, 0)])
    out = _render_health(r)
    assert "rows (estimate)" in out


def test_table_health_needs_attention_is_its_own_section():
    r = _health([_row("shipment", 100, 6, 6.0, None, 0)])
    from pgllens.tools.health import format_table_health as fth
    resp = fth(r, "wms", [])
    assert resp.sections[1].heading == "needs attention"


def test_table_stats_computes_null_percentage():
    columns = [("id", "integer", 0, 100, "1", "999"), ("note", "text", 40, 60, "", "")]
    out = _render_stats("wms.shipment", 100, columns)
    assert "wms.shipment" in out
    assert "100" in out
    assert "40.0%" in out


def test_table_stats_renders_min_max_for_eligible_columns_and_blank_for_others():
    columns = [
        ("id", "integer", 0, 100, "1", "999"),
        ("recorded_at", "timestamp with time zone", 0, 100, "2020-01-01", "2026-01-01"),
        ("note", "text", 40, 60, "", ""),
    ]
    out = _render_stats("wms.shipment", 100, columns)
    assert "min" in out and "max" in out
    assert "1" in out and "999" in out
    assert "2020-01-01" in out and "2026-01-01" in out


def test_table_stats_zero_rows_reports_zero_pct_not_a_division_error():
    columns = [("id", "integer", 0, 0, "", "")]
    out = _render_stats("wms.empty_table", 0, columns)
    assert "0 rows" in out
    assert "0%" in out


def test_format_space_usage_renders_rows_and_database_size():
    result = QueryResult(
        ["schema", "table", "total_size", "table_size", "index_size"],
        [("wms", "shipment", "128 MB", "100 MB", "28 MB")], False)
    out = _render_space(result, "2048 MB")
    assert "2048 MB" in out and "shipment" in out and "128 MB" in out


def test_format_space_usage_zero_tally_for_empty_result():
    result = QueryResult(["schema", "table", "total_size", "table_size", "index_size"], [], False)
    out = _render_space(result, "2048 MB")
    assert "0 tables" in out


def test_table_stats_renders_na_for_a_column_with_no_stats():
    columns = [("id", "integer", 5, 10, "1", "50"), ("payload", "json", None, None, "", "")]
    out = _render_stats("wms.shipment", 100, columns)
    assert "n/a" in out
    assert "5.0%" in out  # id's null pct still computed normally


# --- Task 3: min/max eligibility (numeric and temporal types only) ---

def test_stats_eligible_types_match_by_prefix():
    for dtype in ("smallint", "integer", "bigint", "numeric(12,4)", "real",
                  "double precision", "date", "timestamp with time zone",
                  "time without time zone", "interval", "money"):
        assert _is_stats_eligible(dtype), dtype


def test_stats_eligible_excludes_non_matching_types():
    assert not _is_stats_eligible("text")
    assert not _is_stats_eligible("json")
    assert not _is_stats_eligible("boolean")


def test_stats_eligible_excludes_arrays_even_with_matching_prefix():
    assert not _is_stats_eligible("integer[]")
    assert not _is_stats_eligible("timestamp with time zone[]")


def test_stats_eligible_is_an_exact_type_match_not_a_prefix():
    # Range types and user types share a prefix with date/time/money but have
    # no min()/max() aggregate; matching them made the batch query fail and the
    # per-column retry mark null/distinct N/A too.
    for dtype in ("daterange", "datemultirange", "timeslot", "money_bucket", "date_dim"):
        assert not _is_stats_eligible(dtype), dtype
    # Modifiers and interval fields are stripped/allowed.
    for dtype in ("timestamp(3) with time zone", "time(0) without time zone",
                  "interval day to second", "interval(6)", "NUMERIC(10, 2)"):
        assert _is_stats_eligible(dtype), dtype


# --- _column_stats (Task 11 fix round 1: batch UNION ALL fails whole-query on
# any single COUNT(DISTINCT)-incompatible column, e.g. json/xml -- must
# degrade to a per-column fallback rather than erroring the whole tool) ---

class _Col:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type


class _FakeDb:
    """Batch (UNION ALL) query always fails, as a real json/xml column would
    make it fail server-side. Per-column retry succeeds for every column
    except 'payload', which fails there too -- that column alone must
    degrade to N/A rather than sinking the other columns' stats."""

    async def run_system(self, sql, params=()):
        if "UNION ALL" in sql:
            raise RuntimeError('could not identify an equality operator for type "json"')
        if '"payload"' in sql:
            raise RuntimeError('could not identify an equality operator for type "json"')
        if '"id"' in sql:
            return QueryResult(
                ["null_count", "distinct_count", "min_text", "max_text"],
                [(0, 5, "1", "100")], False)
        return QueryResult(
            ["null_count", "distinct_count", "min_text", "max_text"],
            [(2, 3, None, None)], False)


async def test_column_stats_falls_back_per_column_on_batch_failure():
    columns = [_Col("id", "integer"), _Col("payload", "json"), _Col("note", "text")]
    stats = await _column_stats(_FakeDb(), "wms.shipment", columns)
    assert stats == [
        ("id", "integer", 0, 5, "1", "100"),
        ("payload", "json", None, None, "", ""),
        ("note", "text", 2, 3, "", ""),
    ]


async def test_column_stats_batch_sql_computes_min_max_only_for_eligible_columns():
    db = MagicMock()
    db.run_system = AsyncMock(
        return_value=QueryResult(
            ["ord", "null_count", "distinct_count", "min_text", "max_text"],
            [(0, 0, 5, "1", "100"), (1, 0, 3, "2020-01-01", "2026-01-01"), (2, 1, 2, None, None)],
            False))
    columns = [_Col("id", "integer"), _Col("recorded_at", "timestamp with time zone"),
               _Col("note", "text")]
    stats = await _column_stats(db, "wms.shipment", columns)
    sql = db.run_system.call_args[0][0]
    # Aliased: the outer SELECT reads min_text/max_text by name -- unaliased,
    # Postgres names them min/max/text and the whole batch fails silently.
    assert 'min("id")::text AS min_text, max("id")::text AS max_text' in sql
    assert 'min("recorded_at")::text AS min_text, max("recorded_at")::text AS max_text' in sql
    assert 'NULL::text AS min_text, NULL::text AS max_text' in sql  # note (text) is not eligible
    assert sql.count("AS min_text") == 3 and sql.count("AS max_text") == 3
    assert stats == [
        ("id", "integer", 0, 5, "1", "100"),
        ("recorded_at", "timestamp with time zone", 0, 3, "2020-01-01", "2026-01-01"),
        ("note", "text", 1, 2, "", ""),
    ]


# --- Task 5 fix round 2: n_ins_since_vacuum is PG13+ only (README's supported
# floor is PG12) -- get_table_health must version-gate the column instead of
# raising UndefinedColumn on PG12. ---

def _fake_caps(major):
    caps = MagicMock()
    caps.server_version = AsyncMock(return_value=(major, 0))
    return caps


async def test_get_table_health_sql_omits_n_ins_since_vacuum_on_pg12():
    mcp, db, _ = make_registered(health, caps=_fake_caps(12))
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(
        side_effect=[_health([_row("tiny_lookup", 12, 0, 0, None, None)]), _seqs([])])
    out = await mcp.tools["get_table_health"]()
    sql = db.run_system.call_args_list[0][0][0]
    assert "s.n_ins_since_vacuum" not in sql
    # No dead tuples on PG12 (no n_ins_since_vacuum signal available at all)
    # must not be flagged as "never vacuumed".
    assert "never vacuumed" not in out.lower()


async def test_get_table_health_notes_scope_when_schema_defaulted_and_multi_schema():
    mcp, db, _ = make_registered(health, caps=_fake_caps(13))
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(
        side_effect=[_health([_row("t", 1, 0, 0, None, 0, schema="public")]), _seqs([])])
    out = await mcp.tools["get_table_health"]()
    assert "Scope is public" in out
    assert "wms" in out


async def test_get_table_health_no_scope_note_when_schema_passed_explicitly():
    mcp, db, _ = make_registered(health, caps=_fake_caps(13))
    db.resolve_schema = MagicMock(return_value="wms")
    db.run_system = AsyncMock(
        side_effect=[_health([_row("t", 1, 0, 0, None, 0)]), _seqs([])])
    out = await mcp.tools["get_table_health"](schema="wms")
    assert "Scope is" not in out


async def test_get_table_health_no_scope_note_when_single_schema_exposed():
    from tests.conftest import FakeMCP

    mcp, db, settings = FakeMCP(), MagicMock(), MagicMock()
    settings.exposed_schemas = ["public"]
    settings.default_schema = "public"
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(
        side_effect=[_health([_row("t", 1, 0, 0, None, 0, schema="public")]), _seqs([])])
    health.register(mcp, db, settings, None, _fake_caps(13))
    out = await mcp.tools["get_table_health"]()
    assert "Scope is" not in out


async def test_get_space_usage_notes_scope_when_schema_defaulted():
    mcp, db, _ = make_registered(health)
    db.resolve_schema = MagicMock(return_value="public")
    db.run_system = AsyncMock(side_effect=[
        QueryResult(["schema", "table", "total_size", "table_size", "index_size",
                      "total_bytes"], [("public", "t", "8 kB", "8 kB", "0 bytes", 8192)], False),
        QueryResult(["pg_size_pretty"], [("10 MB",)], False),
    ])
    out = await mcp.tools["get_space_usage"]()
    assert "Scope is public" in out and "wms" in out


async def test_get_table_health_sql_includes_n_ins_since_vacuum_on_pg13():
    mcp, db, _ = make_registered(health, caps=_fake_caps(13))
    db.resolve_schema = MagicMock(return_value="wms")
    db.run_system = AsyncMock(
        side_effect=[_health([_row("loaded", 50000, 0, 0, None, 5000)]), _seqs([])])
    out = await mcp.tools["get_table_health"]()
    sql = db.run_system.call_args_list[0][0][0]
    assert "s.n_ins_since_vacuum" in sql
    assert "never vacuumed" in out.lower()


_SEQ_COLUMNS = ["sequence", "owned_by", "last_value", "max_value", "pct_used"]


def _seqs(rows):
    return QueryResult(_SEQ_COLUMNS, list(rows), False)


def _render_health_with_seqs(rows, seq_rows, scope="wms"):
    return respond(format_table_health(_health(rows), scope, [], sequences=_seqs(seq_rows)))


def test_sequences_section_omitted_when_none_above_50pct():
    out = _render_health_with_seqs(
        [_row("t", 1, 0, 0, None, 0)],
        [("t_id_seq", "t", 10, 2_147_483_647, 0.0)])
    assert "sequences near limit" not in out


def test_sequences_above_50pct_are_listed_and_above_80pct_flagged():
    out = _render_health_with_seqs(
        [_row("t", 1, 0, 0, None, 0)],
        [("t_id_seq", "t", 1_900_000_000, 2_147_483_647, 88.5),
         ("u_id_seq", "u", 1_200_000_000, 2_147_483_647, 55.9)])
    assert "sequences near limit" in out
    assert "t_id_seq" in out and "u_id_seq" in out
    assert "88.5%" in out and "55.9%" in out
    assert "needs attention" in out
    assert "sequence t_id_seq 88.5% used" in out
    assert "sequence u_id_seq" not in out.split("needs attention")[1]


def test_sequences_unowned_render_a_dash_owner():
    out = _render_health_with_seqs(
        [_row("t", 1, 0, 0, None, 0)],
        [("global_seq", None, 1_500_000_000, 2_147_483_647, 69.8)])
    assert "global_seq" in out and "| - |" in out


def test_sequences_sql_shape():
    sql = health._SEQUENCES_SQL
    for needle in ("pg_sequences", "pg_depend", "last_value IS NOT NULL", "pct_used"):
        assert needle in sql, needle


async def test_get_table_health_runs_the_sequence_query_for_the_same_schema():
    mcp, db, _ = make_registered(health, caps=_fake_caps(16))
    db.resolve_schema = MagicMock(return_value="wms")
    db.run_system = AsyncMock(side_effect=[_health([]), _seqs([])])
    await mcp.tools["get_table_health"](schema="wms")
    assert db.run_system.call_count == 2
    sql, params = db.run_system.call_args_list[1][0]
    assert "pg_sequences" in sql and params == ("wms",)


# --- Task 4: xid age and bloat estimate ---

def test_table_health_renders_xid_age_and_bloat_columns():
    out = _render_health(_health([_row("t", 100, 0, 0, None, 0, xid_age=12_345, bloat_pct=3.2)]))
    assert "xid age" in out and "bloat est." in out
    assert "12,345" in out and "3.2%" in out


def test_table_health_flags_xid_age_over_75pct_of_freeze_max_age():
    out = _render_health(_health([
        _row("t", 100, 0, 0, None, 0, xid_age=160_000_000, freeze_max_age=200_000_000)]))
    assert "needs attention" in out
    assert "xid age 160,000,000 (80% of autovacuum_freeze_max_age)" in out


def test_table_health_does_not_flag_xid_age_under_threshold():
    out = _render_health(_health([
        _row("t", 100, 0, 0, None, 0, xid_age=100_000_000, freeze_max_age=200_000_000)]))
    assert "xid age" in out  # column present
    assert "needs attention" not in out


def test_table_health_flags_bloat_over_20pct_and_10mb():
    out = _render_health(_health([
        _row("t", 100, 0, 0, None, 0, bloat_pct=41.0, bloat_bytes=50 * 1024 * 1024)]))
    assert "41.0% estimated bloat (50.0 MB)" in out


def test_table_health_ignores_bloat_on_small_tables():
    out = _render_health(_health([
        _row("t", 100, 0, 0, None, 0, bloat_pct=60.0, bloat_bytes=1024 * 1024)]))
    assert "estimated bloat" not in out


def test_table_health_bloat_na_when_no_stats():
    out = _render_health(_health([_row("t", 100, 0, 0, None, 0, bloat_pct=None, bloat_bytes=None)]))
    assert "| n/a |" in out


def test_table_health_footer_states_database_xid_age():
    out = _render_health(_health([_row("t", 100, 0, 0, None, 0, db_xid_age=214_748_364)]))
    assert "database xid age 214,748,364 (10% of the 2^31 wraparound ceiling)" in out


def test_table_health_sql_selects_xid_and_bloat_columns():
    sql = health._table_health_sql(16)
    for needle in ("CASE WHEN c.relfrozenxid <> '0'::xid THEN age(c.relfrozenxid) END AS xid_age",
                   "current_setting('autovacuum_freeze_max_age')::bigint AS freeze_max_age",
                   "age(d.datfrozenxid)", "pg_stats", "block_size", "bloat_pct", "bloat_bytes"):
        assert needle in sql, needle


# --- Final review round fixes ---

def test_table_health_partitioned_parent_xid_age_renders_dash_and_no_flag():
    # A partitioned parent has relfrozenxid = 0 -- the SQL now selects NULL for
    # it (finding 1), so xid_age can be None here.
    out = _render_health(_health([
        _row("orders", 100, 0, 0, None, 0, xid_age=None, freeze_max_age=200_000_000)]))
    assert "| - |" in out
    assert "needs attention" not in out


def test_table_health_tally_counts_distinct_tables_not_flags():
    # One table tripping both dead-tuples and xid-age flags must still count
    # as 1 table needing attention, not 2 (finding 2).
    out = _render_health(_health([
        _row("t", 100, 10, 10.0, None, 0, xid_age=160_000_000, freeze_max_age=200_000_000)]))
    assert "1 need attention" in out


def test_sequences_sql_uses_numeric_arithmetic_to_avoid_bigint_overflow():
    # bigint subtraction on a full-range sequence (min -9223372036854775808,
    # max 9223372036854775807) overflows; numeric cast avoids it (finding 3).
    sql = health._SEQUENCES_SQL
    assert "s.last_value::numeric - s.min_value" in sql
    assert "s.max_value::numeric - s.min_value" in sql


def test_sequences_unowned_past_80pct_next_call_does_not_target_dash_table():
    out = _render_health_with_seqs(
        [_row("t", 1, 0, 0, None, 0)],
        [("global_seq", None, 1_900_000_000, 2_147_483_647, 88.5)])
    assert "sequence global_seq 88.5% used" in out
    assert 'table="wms.-"' not in out
    assert ".-\"" not in out
