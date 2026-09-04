# tests/test_widget_drilldown.py — static assertions for the ERD drill-down drawer.
# The behavioural proof is a Playwright run driving tests/fixtures/mock_host.html
# (extended to answer tools/call with realistic per-tool markdown).
import re
from pathlib import Path

WIDGET = Path("src/pgllens/widgets/erd_view.html")


def widget() -> str:
    return WIDGET.read_text(encoding="utf-8")


def test_drawer_control_surface_exists():
    html = widget()
    for needle in ('id="detail"', 'id="detail-close"', 'id="detail-tab-rows"',
                   'id="detail-tab-columns"', 'id="detail-tab-stats"',
                   'id="detail-limit"'):
        assert needle in html, f"missing {needle}"


def test_only_allowlisted_tools_are_called():
    """The widget must not reference a tool it is not permitted to call."""
    html = widget()
    called = set(re.findall(r'name:\s*"(\w+)"', html))
    assert called <= {"get_sample_data", "describe_table", "get_table_stats"}, called
    assert "query" not in called


def test_limit_options_never_exceed_the_tool_cap():
    """get_sample_data rejects limit > 20; offering 50 would be a guaranteed error."""
    html = widget()
    opts = {int(v) for v in re.findall(r'data-limit="(\d+)"', html)}
    assert opts and max(opts) <= 20


def test_results_are_inserted_as_text_not_html():
    """The detail drawer renders tool results via textContent/createElement,
    never innerHTML -- a table/column name or cell value containing markup
    must never execute. Pin the actual render function's body (not just a
    substring guess at a variable name) so a regression that swaps in
    `.innerHTML = ...` on the detail-body element is caught."""
    html = widget()
    m = re.search(
        r"function renderDetailBodyText\([^)]*\)\s*\{(.*?)\n\}", html, re.DOTALL
    )
    assert m, "renderDetailBodyText not found in widget"
    fn_body = m.group(1)
    assert '$("detail-body")' in fn_body
    assert "createElement" in fn_body
    assert "textContent" in fn_body
    assert "innerHTML" not in fn_body


def test_serverTools_capability_is_checked():
    assert "serverTools" in widget()
