"""Build the self-contained ERD widget from its template + the design package.

The committed output (src/pgllens/widgets/erd_view.html) is GENERATED — edit the
template and run scripts/build_widgets.py. tests/test_widget_sync.py fails on drift.

Each <!--==NAME==--> marker in the template is substituted with generated CSS/tokens.
"""
from __future__ import annotations

import importlib.resources
from pathlib import Path

from pgllens.design import brand, css, tokens

_BANNER = (
    "<!-- GENERATED FILE — do not edit. Source: src/pgllens/widgets/"
    "erd_view.template.html; build: scripts/build_widgets.py -->"
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def output_paths() -> tuple[Path, ...]:
    return (_repo_root() / "src" / "pgllens" / "widgets" / "erd_view.html",)


def build_widget_html() -> str:
    template = (
        importlib.resources.files("pgllens.widgets")
        .joinpath("erd_view.template.html")
        .read_text(encoding="utf-8")
    )
    # No <!--==MARK==--> marker: the Abelara mark/wordmark was tried inline in the canvas
    # (both as a floating bottom-left overlay and inside the top toolbar) and the user
    # rejected both -- it kept colliding with or crowding the advisory banner in a
    # widget this small. The favicon is the only brand asset this surface ships; the mark
    # itself stays canonical in design.brand for any future surface that has room for it.
    replacements: dict[str, str] = {
        "/*==TOKENS==*/": tokens.css_variables(),
        "/*==CSS==*/": css.components_css(),
        "<!--==FAVICON==-->": (
            f'<link rel="icon" type="image/svg+xml" href="{brand.FAVICON_DATA_URI}">'
        ),
    }
    out = template
    for marker, value in replacements.items():
        if out.count(marker) != 1:
            raise RuntimeError(f"template marker {marker!r} count != 1")
        out = out.replace(marker, value)
    return out.replace("<!doctype html>", f"<!doctype html>\n{_BANNER}", 1)


def write_outputs() -> None:
    html = build_widget_html()
    for path in output_paths():
        path.write_text(html, encoding="utf-8", newline="\n")
        print(f"wrote {path}")
