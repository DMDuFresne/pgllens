from datetime import UTC, datetime

from pgllens.llens_style.errors import ErrorCode
from pgllens.llens_style.model import (
    Bullet,
    Bullets,
    Call,
    Caveat,
    Code,
    Error,
    Response,
    Section,
    Table,
)
from pgllens.llens_style.render import render, render_call, render_error

NOW = datetime(2026, 9, 3, 15, 49, 3, tzinfo=UTC)
RID = "01TESTREQUESTID"


def test_full_success_layout():
    r = Response(
        server="pgllens", tool="describe_table", scope="app_core.assets", plane="catalog+stats",
        sections=(
            Section("columns", (Table(("column", "type"), (("`asset_id`", "`bigint`"),)),)),
            Section("indexes", (
                Table(("name", "scans"), (("`assets_pkey`", "1,204"),)),
                Caveat("Scan counts since last stats reset 2026-08-30T02:00:00Z."),
            )),
        ),
        tally=("1 column", "1 index", "~4.1K rows (estimate)"),
        next=(Call("get_sample_data", {"table": "app_core.assets", "limit": 5}),
              Call("get_relationships", {"table": "app_core.assets"})),
    )
    assert render(r, now=NOW, request_id=RID) == (
        "## pgllens · describe_table · app_core.assets\n"
        "*catalog+stats · 2026-09-03T15:49:03Z*\n"
        "\n"
        "### columns\n"
        "| column | type |\n"
        "|---|---|\n"
        "| `asset_id` | `bigint` |\n"
        "\n"
        "### indexes\n"
        "| name | scans |\n"
        "|---|---|\n"
        "| `assets_pkey` | 1,204 |\n"
        "\n"
        "> Scan counts since last stats reset 2026-08-30T02:00:00Z.\n"
        "\n"
        "---\n"
        "1 column · 1 index · ~4.1K rows (estimate)\n"
        '*Next: get_sample_data(table="app_core.assets", limit=5) · '
        'get_relationships(table="app_core.assets")*'
    )


def test_bullets_render_code_qualifier_raw_and_plain():
    r = Response(
        server="pgllens", tool="server_info", scope=None, plane="catalog",
        sections=(Section(None, (Bullets((
            Bullet("version", "16.3"),
            Bullet("fault", "minor recoverable", qualifier="decoded", raw="status word `0x3160`"),
            Bullet("summary", "A sentence value.", is_code=False),
        )),)),),
    )
    assert render(r, now=NOW, request_id=RID) == (
        "## pgllens · server_info\n"
        "*catalog · 2026-09-03T15:49:03Z*\n"
        "\n"
        "- version: `16.3`\n"
        "- fault: `minor recoverable` (decoded) — status word `0x3160`\n"
        "- summary: A sentence value."
    )


def test_status_slot_and_no_footer_without_tally():
    r = Response(server="pgllens", tool="refresh_schema", scope=None, plane="catalog",
                 sections=(Section(None, (Bullets((Bullet("cached", "13"),)),)),),
                 status="stats stale (last analyze 3d)")
    out = render(r, now=NOW, request_id=RID)
    assert out.splitlines()[1] == "*catalog · 2026-09-03T15:49:03Z · stats stale (last analyze 3d)*"
    assert "---" not in out and "Next:" not in out


def test_tally_without_next():
    r = Response(server="pgllens", tool="list_roles", scope=None, plane="catalog",
                 sections=(Section(None, (Table(("role",), (("`a`",),)),)),), tally=("1 role",))
    assert render(r, now=NOW, request_id=RID).endswith("---\n1 role")


def test_code_block_has_lang():
    r = Response(server="pgllens", tool="get_view_definition", scope="app_core.v", plane="catalog",
                 sections=(Section(None, (Code("sql", "SELECT 1"),)),))
    assert "```sql\nSELECT 1\n```" in render(r, now=NOW, request_id=RID)


def test_table_cells_escape_pipes_and_newlines():
    r = Response(server="pgllens", tool="query", scope=None, plane="query",
                 sections=(Section(None, (Table(("q",), (("a|b\nc",),)),)),), tally=("1 row",))
    assert "| a\\|b c |" in render(r, now=NOW, request_id=RID)


def test_render_call_forms():
    assert render_call(Call("list_tables")) == "list_tables()"
    assert render_call(Call("q", {"sql": "SELECT 1", "n": 5, "f": True, "z": None})) == (
        'q(sql="SELECT 1", n=5, f=True, z=None)')


def test_error_layout():
    e = Error("pgllens", "query", ErrorCode.QUERY_REJECTED,
              "WITH/CTE statements are not accepted; single SELECT only.",
              "Inline the CTE as a subquery.")
    assert render_error(e, now=NOW, request_id=RID) == (
        "## pgllens · query · error\n"
        "\n"
        "- code: `QUERY_REJECTED`\n"
        "- message: WITH/CTE statements are not accepted; single SELECT only.\n"
        "- hint: Inline the CTE as a subquery.\n"
        "- request_id: `01TESTREQUESTID`"
    )


def test_error_retry_after():
    e = Error("pgllens", "query", ErrorCode.TIMEOUT, "m", "h", retry_after="22s")
    assert render_error(e, now=NOW, request_id=RID).endswith("- retry_after: `22s`")
