"""Every registered tool: lint passes, header names the tool, golden matches."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp.server.apps import Apps
from mcp.types import CallToolResult

from pgllens.config import Settings
from pgllens.database.format import QueryResult
from pgllens.database.pool import UnknownSchemaError
from pgllens.database.safety import assert_read_only
from pgllens.llens_style import lint
from pgllens.server import create_mcp
from pgllens.tools import erd
from tests.conftest import FakeMCP
from tests.style_cases import CASES, Case, make_caps, make_intro

GOLDEN = Path(__file__).parent / "golden"


def _settings() -> MagicMock:
    s = MagicMock()
    s.exposed_schemas = ["app_core", "app_audit"]
    s.default_schema = "app_core"
    s.redact_columns = []
    s.max_estimated_cost = None
    s.tool_cost_budget_per_minute = None
    s.domain_context_text = None
    s.max_rows = 200
    return s


async def _run(case: Case) -> str:
    db = MagicMock()
    db.run_system = AsyncMock(side_effect=case.system or [QueryResult(["a"], [(1,)], False)])

    async def run_readonly(sql: str, max_rows: int | None = None) -> QueryResult:
        # Mirrors Db.run_readonly: real assert_read_only gate on the exact SQL
        # a case's `sql` kwarg produces, so a rejected query actually raises
        # UnsafeQueryError here instead of the mock silently succeeding.
        assert_read_only(sql)
        return case.readonly or QueryResult(["a"], [(1,)], False)

    db.run_readonly = AsyncMock(side_effect=run_readonly)

    def resolve_schema(s: str | None) -> str:
        resolved = s or "app_core"
        if resolved.lower() in ("app_core", "app_audit"):
            return resolved
        raise UnknownSchemaError(f"Schema '{s}' is not exposed.")

    db.resolve_schema = resolve_schema
    intro = case.intro or make_intro()
    caps = case.caps or make_caps()
    if case.apps:
        apps = Apps()
        erd.register_apps(apps, db, _settings(), intro)
        fn = apps.tools()[0].fn
    else:
        mcp = FakeMCP()
        case.module.register(mcp, db, _settings(), intro, caps)
        fn = mcp.tools[case.tool]
    out = await fn(**case.kwargs)
    if isinstance(out, CallToolResult):
        return out.content[0].text  # type: ignore[union-attr]
    return out


@pytest.mark.parametrize("case", CASES, ids=lambda c: c.name)
async def test_tool_output_is_style_compliant(case: Case, request):
    out = await _run(case)
    assert lint(out) == [], "\n".join(f"{v.rule}:{v.line} {v.message}" for v in lint(out)) + "\n" + out
    head = out.split("\n", 1)[0]
    assert head.startswith(f"## pgllens · {case.tool}"), head
    assert (" · error" in head) == case.error, f"{case.name}: error flag mismatch: {head}"
    path = GOLDEN / f"{case.name}.md"
    if request.config.getoption("--update-golden"):
        path.write_text(out + "\n", encoding="utf-8")
    assert path.exists(), f"missing golden {path.name}; run with --update-golden and review"
    assert out + "\n" == path.read_text(encoding="utf-8")


async def test_every_registered_tool_has_a_success_case():
    settings = Settings(database_url="postgresql://u:p@h/db", exposed_schemas=["app_core"])
    mcp = create_mcp(settings, db=None)
    registered = {t.name for t in await mcp.list_tools()}
    registered |= {"get_erd_widget"}
    covered = {c.tool for c in CASES if not c.error}
    assert registered <= covered, sorted(registered - covered)
