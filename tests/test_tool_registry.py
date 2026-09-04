"""tool_errors is the single instrumentation point; it must also be the single
registry of tool names, so metrics.preregister_tools() sees all 31 without
reaching into FastMCP internals."""

from pgllens.config import Settings
from pgllens.database.pool import Db
from pgllens.obs import metrics
from pgllens.server import create_mcp
from pgllens.tools._util import registered_tool_names


async def test_registry_matches_the_mcp_tool_list():
    settings = Settings(_env_file=None, database_url="postgresql://u:p@localhost:5432/flux",
                        exposed_schemas="public")
    server = create_mcp(settings, Db(settings), intro=None)
    listed = {t.name for t in await server.list_tools()}
    assert registered_tool_names() == frozenset(listed)
    assert len(listed) == 31


def test_outcome_enum_in_util_matches_metrics():
    from pgllens.tools._util import OUTCOME

    assert set(OUTCOME.values()) | {"ok"} == set(metrics.TOOL_OUTCOMES)
