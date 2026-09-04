from datetime import UTC, datetime

import pytest

from pgllens.llens_style.errors import ErrorCode
from pgllens.llens_style.lint import lint
from pgllens.llens_style.model import Bullet, Bullets, Call, Error, Response, Section, Table
from pgllens.llens_style.render import render, render_error

NOW = datetime(2026, 9, 3, 15, 49, 3, tzinfo=UTC)
META = "*catalog · 2026-09-03T15:49:03Z*"

GOOD = (
    "## pgllens · schema_overview · pgllens-lab\n"
    "*catalog+stats · 2026-09-03T15:47:12Z*\n"
    "\n"
    "| schema | tables | views | rows (estimate) |\n"
    "|---|---|---|---|\n"
    "| `app_core` | 7 | 1 | ~9.2K |\n"
    "\n"
    "---\n"
    "3 schemas · 13 objects · ~9.3K rows\n"
    '*Next: list_tables(schema="app_core") · get_relationships(schema="app_core")*'
)

GOOD_ERROR = (
    "## pgllens · query · error\n"
    "\n"
    "- code: `QUERY_REJECTED`\n"
    "- message: WITH/CTE statements are not accepted; single SELECT only.\n"
    "- hint: Inline the CTE as a subquery.\n"
    "- request_id: `01J8Q4V7`"
)


def rules(md):
    return {v.rule for v in lint(md)}


def test_guide_examples_pass():
    assert lint(GOOD) == []
    assert lint(GOOD_ERROR) == []


def test_pk_check_glyph_allowed():
    assert "NO_EMOJI" not in rules(GOOD.replace("| 7 |", "| ✓ |"))


@pytest.mark.parametrize(("bad", "rule"), [
    (f"# Table\n{META}\n\n- a: `1`", "H2_SHAPE"),
    (f"## Results for app_core.asset\n{META}\n\n- a: `1`", "H2_SHAPE"),
    (f"## PgLLens — Describe Table\n{META}\n\n- a: `1`", "H2_SHAPE"),
    ("## pgllens · x\ncatalog · 2026-09-03T15:49:03Z\n\n- a: `1`", "META_LINE"),
    ("## pgllens · x\n*catalog · 9/3/2026*\n\n- a: `1`", "META_LINE"),
    (f"## pgllens · x\n{META}\n\n#### deep\n- a: `1`", "NO_H1_H4"),
    (f"## pgllens · x\n{META}\n\n### Identity\n- a: `1`\n\n### b\n- c: `1`", "H3_ONLY"),
    (f"## pgllens · x\n{META}\n\n- **Product:** x", "NO_BOLD"),
    (f"## pgllens · x\n{META}\n\n- a: ⚠️ x", "NO_EMOJI"),
    (f"## pgllens · x\n{META}\n\n```\nselect 1\n```", "FENCE_LANG"),
    (f"## pgllens · x\n{META}\n\n| a |\n|---|\n| 1 |", "TABLE_NEEDS_TALLY"),
    ((f"## pgllens · x\n{META}\n\n| a |\n|---|\n| 1 |\n\n---\n"
      "Showing some results. Use the cursor to see more.\nextra"),
     "FOOTER_SHAPE"),
    (f"## pgllens · x\n{META}\n\n- a: Here are the results", "NO_FILLER"),
    ("## pgllens · x · error\n\n- code: `X`\n- message: m", "ERROR_SHAPE"),
    ("## pgllens · x · error\n\n- code: `X`\n- message: m\n- hint: h\n- request_id: `r`\n\nprose",
     "ERROR_SHAPE"),
])
def test_dont_examples_are_caught(bad, rule):
    assert rule in rules(bad), rules(bad)


def test_rendered_response_passes():
    r = Response(server="pgllens", tool="list_roles", scope=None, plane="catalog",
                 sections=(Section(None, (Table(("role",), (("`a`",),)),)),), tally=("1 role",),
                 next=(Call("list_extensions"),))
    assert lint(render(r, now=NOW, request_id="r")) == []


def test_rendered_error_passes():
    e = Error("pgllens", "q", ErrorCode.DB_ERROR, "m", "h")
    assert lint(render_error(e, now=NOW, request_id="r")) == []


def test_bullets_only_body_needs_no_footer():
    r = Response(server="pgllens", tool="refresh_schema", scope=None, plane="catalog",
                 sections=(Section(None, (Bullets((Bullet("cached", "13"),)),)),))
    assert lint(render(r, now=NOW, request_id="r")) == []


def test_unterminated_fence_is_a_violation():
    bad = ("## pgllens · x\n*catalog · 2026-09-03T15:49:03Z*\n\n```sql\nSELECT 1\n\n"
           "# H1\n- a: **bold**\n")
    assert "FENCE_LANG" in rules(bad)
