import json
import re
from pathlib import Path

import pytest

from pgllens.design import widgetbuild

WIDGET = Path("src/pgllens/widgets/erd_view.html")


def test_committed_widget_matches_the_template_build():
    assert WIDGET.exists(), "run scripts/build_widgets.py"
    assert WIDGET.read_text(encoding="utf-8") == widgetbuild.build_widget_html(), (
        "erd_view.html is stale — run scripts/build_widgets.py")


def test_widget_declares_itself_generated():
    assert "GENERATED FILE" in WIDGET.read_text(encoding="utf-8")[:400]


def test_widget_loads_nothing_external():
    html = WIDGET.read_text(encoding="utf-8")
    assert not re.search(r'src=["\']https?://', html)
    assert not re.search(r'href=["\']https?://(?!www\.w3\.org)', html)
    assert "@import" not in html
    assert "fetch(" not in html and "XMLHttpRequest" not in html
    assert "eval(" not in html


def test_widget_has_the_empty_data_block_for_the_bridge_path():
    html = WIDGET.read_text(encoding="utf-8")
    m = re.search(r'<script id="erd-data" type="application/json">(.*?)</script>',
                  html, re.DOTALL)
    assert m, "erd-data block missing"
    assert json.loads(m.group(1).strip() or "{}") == {}


@pytest.mark.parametrize("needle", [
    # No "zoom-reset": it was a plain duplicate of "fit" (both just called
    # fitAll()) and was deleted rather than kept as dead weight -- see the
    # comment above the toolbar markup in erd_view.template.html.
    'id="zoom-in"', 'id="zoom-out"', 'id="fit"', 'id="fullscreen"',
    'id="copy-mermaid"', 'id="search"',
])
def test_widget_exposes_the_control_surface(needle):
    assert needle in WIDGET.read_text(encoding="utf-8")


def test_widget_has_no_export_buttons():
    # SVG/PNG export was removed outright (not disabled): no supported host
    # download path exists in the ext-apps revision Claude negotiates (neither
    # showSaveFilePicker/<a download>, blocked by the sandboxed iframe, nor
    # ui/download-file, which is draft-only). A disabled-forever control is
    # tech debt, so the buttons -- and their ids -- must not exist at all.
    html = WIDGET.read_text(encoding="utf-8")
    assert 'id="export-svg"' not in html
    assert 'id="export-png"' not in html


def test_widget_names_related_tables_instead_of_just_fading_them():
    # A related (FK-neighbour) table rendered as a plain opacity fade once read as a
    # rendering bug ("why is this table transparent / overlapping another one?"). The
    # fix names the state instead: a RELATED badge on the card, a conditional legend,
    # and no bare ".dimmed"/opacity class on the card itself.
    html = WIDGET.read_text(encoding="utf-8")
    assert "erd-related-badge" in html
    assert ">RELATED<" in html
    assert 'id="legend"' in html
    assert "Dashed = related table" in html
    assert '" related dimmed"' not in html


def test_widget_never_sends_ui_download_file():
    # ui/download-file is DRAFT-ONLY (ext-apps PR #475, Feb 2026) and absent from the
    # 2026-01-26 stable revision Claude Desktop/web/mobile negotiate -- calling it does
    # nothing there. It's fine for the string to appear in a comment explaining that,
    # but it must never appear in the *sent-message* form (a quoted JSON-RPC method).
    html = WIDGET.read_text(encoding="utf-8")
    assert '"ui/download-file"' not in html


def test_widget_has_no_export_machinery():
    # The disabled-forever SVG/PNG export buttons were removed outright, along with
    # every code path that only existed to serve them: the capability probe, the
    # rasterization/export-SVG builders, and every browser download API they used.
    # This is the inverse of the old "export is gated behind a capability check"
    # test -- there is no longer a capability to gate, because there is no feature.
    html = WIDGET.read_text(encoding="utf-8")
    assert "function localSaveIsPossible" not in html
    assert "HOST_DOWNLOAD_CAPABLE" not in html
    assert "function updateExportButtonsState" not in html
    assert "File export is not supported by this client yet" not in html
    assert "function buildExportSvg" not in html
    assert "function exportSvg" not in html
    assert "function exportPng" not in html
    assert "function sanitizeFilenameStem" not in html
    assert "function erdFilename" not in html
    assert "async function download(" not in html
    # The dead browser-download machinery those functions used must be gone too,
    # not just unreferenced -- genuinely deleted, not merely orphaned.
    assert "showSaveFilePicker" not in html
    assert "canvas.toBlob" not in html
    assert re.search(r"\ba\.download\s*=", html) is None
    assert "URL.createObjectURL" not in html
