"""Render the self-contained ERD widget with an ERD payload baked in.

Option A (data-baked) of the ERD widget: the vendored renderer reads its data from a
``<script id="erd-data">`` JSON block; this module substitutes an ``erd.model.to_dict()``
dict into that block server-side. Pure string templating -- no database contact,
no external loads.
"""

from __future__ import annotations

import importlib.resources
import json
import re
from pathlib import Path
from typing import Any

_DATA_BLOCK = re.compile(
    r'(<script id="erd-data" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)

# mtime-validated cache: (mtime of erd_view.html at read time, its content).
# An lru_cache here would serve stale HTML forever after scripts/build_widgets.py
# regenerates the file under a running server; validating against st_mtime keeps
# the fast path a single stat() while picking up a rebuild on the next call.
_cache: tuple[float, str] | None = None


def _widget_path() -> Path:
    # The widget is a real on-disk file in this repo/container (never zip-packed),
    # so the importlib.resources traversable resolves to a stat-able Path.
    return Path(
        str(importlib.resources.files("pgllens.widgets").joinpath("erd_view.html"))
    )


def load_widget_html() -> str:
    """The committed ERD widget HTML, cached and revalidated by file mtime.

    Single shared loader for the packaged static asset: the baked path
    (``render_erd_view``) and the static ``ui://`` resource in server.py both go
    through it, so there is one loader and one cache.
    """
    global _cache
    path = _widget_path()
    mtime = path.stat().st_mtime
    if _cache is None or _cache[0] != mtime:
        _cache = (mtime, path.read_text(encoding="utf-8"))
    return _cache[1]


def render_erd_view(data: dict[str, Any]) -> str:
    """Return the renderer HTML with ``data`` baked into the erd-data block.

    ``data`` is an ``erd.model.to_dict()`` dict. The JSON is escaped so a
    ``</script>`` (or any ``</``) appearing in the data (e.g. a table or column name)
    cannot break out of the script tag; ``<\\/`` is valid inside a JSON-in-HTML string
    and JSON.parse yields the original text.
    """
    payload = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    template = load_widget_html()
    if not _DATA_BLOCK.search(template):
        raise RuntimeError('erd_view.html is missing the <script id="erd-data"> block')
    return _DATA_BLOCK.sub(lambda m: f"{m.group(1)}{payload}{m.group(3)}", template, count=1)
