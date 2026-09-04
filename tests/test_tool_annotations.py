from pgllens.config import Settings
from pgllens.server import create_mcp

DSN = "postgresql://u:p@localhost:5432/flux"


async def test_every_tool_declares_title_and_read_only_hint():
    # The Claude Connector Directory requires both on every tool.
    mcp = create_mcp(Settings(database_url=DSN, exposed_schemas="public"), db=None)
    tools = await mcp.list_tools()
    assert tools, "no tools registered"
    for t in tools:
        assert t.annotations is not None, t.name
        assert t.annotations.title, t.name
        assert t.annotations.read_only_hint is True, t.name


EXPECTED_TOOLS = {
    "query", "validate_query", "explain_query", "list_tables", "describe_table",
    "schema_overview", "search_columns", "get_sample_data", "get_relationships", "find_path",
    "get_table_stats", "get_view_definition", "get_ontology", "refresh_schema",
    "list_functions", "get_function_source", "list_extensions", "list_roles",
    "get_table_health", "list_hypertables", "get_active_sessions", "get_blocking",
    "get_wait_stats", "get_index_health", "get_query_store", "get_space_usage",
    "server_info", "get_erd", "get_erd_widget", "get_constraints", "get_triggers",
}


async def test_tool_count_matches_the_parity_target():
    # 31 = 30 mcp-registered tools (get_erd among them, via the uniform
    # register_all pass) + get_erd_widget (Apps extension, still model-visible);
    # spec's parity matrix counted 27; find_path, the get_erd/get_erd_widget
    # split, and get_constraints/get_triggers (all pass 3) are the additions.
    mcp = create_mcp(Settings(database_url=DSN, exposed_schemas="public"), db=None)
    tools = await mcp.list_tools()
    assert {t.name for t in tools} == EXPECTED_TOOLS
    assert len(tools) == 31


async def test_every_tool_declares_explicit_visibility():
    # An omitted _meta.ui.visibility defaults to ["model","app"] -- wide open.
    mcp = create_mcp(Settings(database_url=DSN, exposed_schemas="public"), db=None)
    for t in await mcp.list_tools():
        assert t.meta and t.meta.get("ui", {}).get("visibility"), t.name
