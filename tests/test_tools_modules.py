"""Behavioral tests for tools/modules.py (view/function introspection)."""

from unittest.mock import AsyncMock, MagicMock

from pgllens.database.format import QueryResult
from pgllens.llens_style import Bullets, Code, lint
from pgllens.tools import modules
from pgllens.tools._util import respond
from tests.conftest import FakeMCP, make_registered


def _register_with_schemas(exposed_schemas, default_schema):
    """Like make_registered, but with a caller-chosen EXPOSED_SCHEMAS/default --
    needed for the multi-match tie-break tests below, which require more than
    the two schemas make_registered's fixture settings hard-code."""
    mcp, db, settings = FakeMCP(), MagicMock(), MagicMock()
    db.run_system = AsyncMock(return_value=QueryResult(["a"], [(1,)], False))
    settings.exposed_schemas = exposed_schemas
    settings.default_schema = default_schema
    modules.register(mcp, db, settings, None, None)
    return mcp, db


async def test_get_view_definition_searches_all_exposed_schemas_when_unqualified():
    """audit #3: a view living only in a non-default exposed schema (app_custom)
    must be found without the caller passing schema= explicitly, the same way
    describe_table resolves unqualified table names across the allowlist."""
    mcp, db = _register_with_schemas(["public", "app_custom", "wms"], "public")

    async def fake_run_system(sql, params):
        if "ANY(%s)" in sql:
            return QueryResult(
                ["schema", "relkind", "definition", "comment"],
                [("app_custom", "v", "SELECT 1", None)],
                False,
            )
        return QueryResult(["column_name", "data_type", "comment"], [], False)

    db.run_system = AsyncMock(side_effect=fake_run_system)
    result = await mcp.tools["get_view_definition"]("v_alias_resolved")
    assert "app_custom.v_alias_resolved" in result


async def test_get_view_definition_explicit_wrong_schema_still_not_found():
    mcp, db, _ = make_registered(modules)
    db.resolve_schema.return_value = "public"
    db.run_system.return_value = QueryResult(["relkind", "definition", "comment"], [], False)
    result = await mcp.tools["get_view_definition"]("v_alias_resolved", schema="public")
    assert "not found" in result.lower()


async def test_get_view_definition_multi_match_default_schema_wins():
    """Default schema wins over other matches, and the others are named --
    regardless of the (undefined) row order the catalog scan returns."""
    mcp, db = _register_with_schemas(["public", "app_custom", "wms"], "public")

    async def fake_run_system(sql, params):
        if "ANY(%s)" in sql:
            # Rows deliberately NOT in exposed-schema order.
            return QueryResult(
                ["schema", "relkind", "definition", "comment"],
                [("wms", "v", "SELECT 2", None), ("public", "v", "SELECT 1", None)],
                False,
            )
        return QueryResult(["column_name", "data_type", "comment"], [], False)

    db.run_system = AsyncMock(side_effect=fake_run_system)
    result = await mcp.tools["get_view_definition"]("v_dupe")
    assert "public.v_dupe" in result
    assert "Also defined in: wms" in result


async def test_get_view_definition_multi_match_no_default_ties_break_by_exposed_order():
    """Default schema isn't among the matches -- the winner is the earliest
    match in EXPOSED_SCHEMAS order, not catalog-scan order (rows below are
    given in the reverse of exposed-schema order to prove that)."""
    mcp, db = _register_with_schemas(["public", "app_custom", "wms"], "public")

    async def fake_run_system(sql, params):
        if "ANY(%s)" in sql:
            # Reverse of exposed order: wms before app_custom.
            return QueryResult(
                ["schema", "relkind", "definition", "comment"],
                [("wms", "v", "SELECT 2", None), ("app_custom", "v", "SELECT 1", None)],
                False,
            )
        return QueryResult(["column_name", "data_type", "comment"], [], False)

    db.run_system = AsyncMock(side_effect=fake_run_system)
    result = await mcp.tools["get_view_definition"]("v_dupe")
    assert "app_custom.v_dupe" in result
    assert "Also defined in: wms" in result


async def test_list_functions_default_is_scoped_to_exposed_schemas():
    mcp, db, _ = make_registered(modules)
    await mcp.tools["list_functions"]()
    sql, params = db.run_system.await_args.args
    assert "n.nspname = ANY(%s)" in sql
    assert params == (["public", "wms"],)


async def test_list_functions_with_schema_narrows_within_the_allowlist():
    mcp, db, _ = make_registered(modules)
    db.resolve_schema.return_value = "wms"
    await mcp.tools["list_functions"](schema="WMS")
    sql, params = db.run_system.await_args.args
    assert "n.nspname = ANY(%s)" in sql          # allowlist still applied
    assert "AND n.nspname = %s" in sql            # schema-specific constraint added
    assert params == (["public", "wms"], "wms")


async def test_get_function_source_not_found_matches_describe_table_shape():
    """audit N1: a missing function should read like describe_table's not-found
    message (names the function/schema, offers a Did you mean hint) instead of
    a bare "None found." sentence."""
    mcp, db, _ = make_registered(modules)
    db.resolve_schema.return_value = "public"

    async def fake_run_system(sql, params):
        if "pg_get_functiondef" in sql:
            return QueryResult(["oid"], [], False)
        return QueryResult(["proname"], [("get_widget",)], False)

    db.run_system = AsyncMock(side_effect=fake_run_system)
    result = await mcp.tools["get_function_source"]("get_widgit", schema="public")
    assert "- code: `FUNCTION_NOT_FOUND`" in result
    assert "not found" in result.lower()
    assert "Did you mean: get_widget" in result


async def test_format_view_definition_renders_blank_not_null_for_empty_comment():
    """audit V1: a column with no comment must render as an empty cell, not
    the literal string "NULL"."""
    columns = QueryResult(
        ["column_name", "data_type", "comment"],
        [("id", "integer", None)],
        False,
    )
    resp = modules.format_view_definition("public", "v_x", ("v", "SELECT 1", None), columns, [])
    out = respond(resp)
    assert "NULL" not in out


def _src_row(full_definition):
    # matches _FUNCTION_SOURCE_SQL's column order: oid, full_definition, source,
    # return_type, arguments, language, volatility, security_definer, is_strict, kind, comment
    return (1, full_definition, "body", "trigger", "", "plpgsql", "v", False, False, "f", None)


def test_truncate_sources_cuts_each_overload_and_extends_the_tally():
    big_a, big_b = "a" * 60_000, "b" * 60_000
    result = QueryResult(
        ["oid", "full_definition", "source", "return_type", "arguments", "language",
         "volatility", "security_definer", "is_strict", "kind", "comment"],
        [_src_row(big_a), _src_row(big_b)], False)
    resp = modules.format_function_source("public", "touch", result)
    out = modules._truncate_sources(resp)

    per = modules._MAX_SOURCE_CHARS // 2
    codes = [b for s in out.sections for b in s.blocks if isinstance(b, Code)]
    assert codes and all(len(c.text) <= per for c in codes)
    assert out.tally[-1] == "source truncated at 100,000 chars"
    # Non-Code blocks (the Bullets facts) are untouched.
    orig_bullets = [b for s in resp.sections for b in s.blocks if isinstance(b, Bullets)]
    new_bullets = [b for s in out.sections for b in s.blocks if isinstance(b, Bullets)]
    assert new_bullets == orig_bullets
    assert lint(respond(out)) == []


def test_truncate_sources_is_a_no_op_below_the_cap():
    result = QueryResult(
        ["oid", "full_definition", "source", "return_type", "arguments", "language",
         "volatility", "security_definer", "is_strict", "kind", "comment"],
        [_src_row("small body")], False)
    resp = modules.format_function_source("public", "touch", result)
    out = modules._truncate_sources(resp)
    assert out == resp
    assert out.tally == resp.tally
