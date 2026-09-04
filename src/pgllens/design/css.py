"""The ERD widget's component sheet: tokens + reset + Chalkboard component set.

No external loads: no @import, no url(http...), no webfonts — the widget uses the
system font stack (see tokens.FONT_BRAND / FONT_MONO).

Component inventory (keep in sync with docs/design/STYLE.md):
  .erd-canvas .erd-viewport                                — canvas/viewport
  .erd-table .erd-table-head .erd-table-col                — table cards
  .erd-pk .erd-fk .erd-null                                — PK/FK markers, nullability dots
  .erd-table.related .erd-table.dimmed                     — related/dimmed variants
  .erd-edge                                                — edge/connector strokes
  .erd-controls .erd-btn                                   — control cluster (zoom/fit/copy)
  .erd-search                                               — search box
  .erd-banner                                               — advisory banner (truncation)
  .erd-empty                                                — empty state
  .erd-detail .erd-detail-header .erd-detail-tabs           — drill-down drawer (Task 6.3)
  .erd-detail-tab .erd-detail-limit .erd-detail-body        — bottom sheet / side panel, one code path
"""
from __future__ import annotations

from functools import lru_cache

from pgllens.design import tokens

_COMPONENTS = """
* { box-sizing: border-box; }
.erd-canvas { position:relative; background:var(--bg); color:var(--ink);
        font:14px/1.5 var(--font-brand); width:100%; height:100%; overflow:hidden; }
.erd-viewport { position:absolute; inset:0; overflow:auto; }

.erd-table { background:var(--panel); border:1px solid var(--edge); border-radius:var(--r-md);
        min-width:200px; box-shadow:0 4px 16px rgba(0,0,0,.35); }
.erd-table-head { font-family:var(--font-caps); font-weight:600; font-size:.85rem;
        padding:.5rem .75rem; border-bottom:1px solid var(--edge); color:var(--blue);
        border-radius:var(--r-md) var(--r-md) 0 0; background:var(--panel-inset); }
.erd-table-col { display:flex; align-items:center; gap:.5rem; padding:.32rem .75rem;
        font-family:var(--font-mono); font-size:.78rem; border-bottom:1px solid var(--edge); }
.erd-table-col:last-child { border-bottom:none; }
.erd-table-col .col-type { color:var(--muted); margin-left:auto; }

.erd-pk { color:var(--yellow); font-weight:700; }
.erd-fk { color:var(--blue); }
.erd-null { width:6px; height:6px; border-radius:50%; background:var(--muted);
        display:inline-block; flex:none; }
.erd-null.required { background:var(--green); }

.erd-table.related { border-color:var(--blue); }
.erd-table.dimmed { opacity:.35; }

.erd-edge { stroke:var(--edge); stroke-width:1.5; fill:none; }
.erd-edge.related { stroke:var(--blue); stroke-width:2; }

.erd-controls { position:absolute; top:12px; right:12px; display:flex; gap:.4rem;
        z-index:2; }
.erd-btn { background:var(--panel); border:1px solid var(--edge); border-radius:var(--r-sm);
        color:var(--ink); padding:.4rem .65rem; font-size:.8rem; cursor:pointer;
        font-family:var(--font-brand); }
.erd-btn:hover { border-color:var(--blue); }
.erd-btn:focus-visible { outline:2px solid var(--blue); outline-offset:1px; }
.erd-btn.primary { background:var(--green); color:var(--bg); border-color:var(--green);
        font-weight:600; }

.erd-search { position:absolute; top:12px; left:12px; z-index:2; width:220px;
        padding:.45rem .7rem; border:1px solid var(--edge); border-radius:var(--r-sm);
        background:var(--panel); color:var(--ink); font:.85rem var(--font-brand); }
.erd-search:focus { outline:2px solid var(--blue); outline-offset:1px; border-color:var(--blue); }

.erd-banner { position:absolute; bottom:12px; left:12px; right:12px; z-index:2;
        background:rgba(255,255,169,.08); border:1px solid rgba(255,255,169,.35);
        border-radius:var(--r-md); padding:.6rem .85rem; font-size:.8rem;
        color:var(--yellow); display:flex; gap:.5rem; align-items:flex-start; }
.erd-banner b { color:var(--ink); }
.erd-banner.error { background:rgba(245,96,43,.08); border-color:rgba(245,96,43,.4);
        color:var(--red); }

.erd-empty { display:flex; flex-direction:column; align-items:center; justify-content:center;
        height:100%; color:var(--muted); text-align:center; gap:.5rem; }
.erd-empty .erd-empty-title { font-family:var(--font-caps); font-weight:600;
        text-transform:uppercase; letter-spacing:.1em; font-size:.8rem; color:var(--ink); }

/* Drill-down drawer: ONE component, two positions driven by JS (see
   updateDetailLayout()/containerDimensions in the widget script) -- a bottom sheet
   on a narrow container, a side panel on a wide one via the .wide modifier. No
   @media mode switch (deliberately deleted elsewhere in this widget already). */
.erd-detail { position:absolute; z-index:5; left:0; right:0; bottom:0; max-height:70%;
        background:var(--panel); border:1px solid var(--edge); border-radius:var(--r-md) var(--r-md) 0 0;
        box-shadow:0 -8px 24px rgba(0,0,0,.4); display:flex; flex-direction:column; }
.erd-detail.wide { left:auto; top:0; right:0; bottom:0; max-height:none; height:100%;
        width:max(40%,280px); border-radius:var(--r-md) 0 0 var(--r-md);
        box-shadow:-8px 0 24px rgba(0,0,0,.4); }
.erd-detail-header { display:flex; align-items:center; gap:.5rem; padding:.6rem .75rem;
        border-bottom:1px solid var(--edge); flex:none; }
.erd-detail-title { font-family:var(--font-caps); font-weight:600; font-size:.85rem;
        color:var(--blue); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; flex:1; }
.erd-detail-tabs { display:flex; gap:.3rem; padding:.4rem .75rem; border-bottom:1px solid var(--edge);
        flex:none; flex-wrap:wrap; }
.erd-detail-tab { background:var(--panel-inset); border:1px solid var(--edge); border-radius:var(--r-sm);
        color:var(--muted); padding:.35rem .6rem; font-size:.75rem; font-family:var(--font-brand);
        cursor:pointer; min-height:36px; }
.erd-detail-tab[aria-selected="true"] { color:var(--ink); border-color:var(--blue); }
.erd-detail-tab:focus-visible { outline:2px solid var(--blue); outline-offset:1px; }
.erd-detail-limit { display:flex; align-items:center; gap:.3rem; padding:.4rem .75rem;
        border-bottom:1px solid var(--edge); flex:none; }
.erd-detail-limit-btn { background:var(--panel-inset); border:1px solid var(--edge); border-radius:var(--r-sm);
        color:var(--muted); padding:.25rem .55rem; font-size:.72rem; font-family:var(--font-mono);
        cursor:pointer; min-width:36px; min-height:32px; }
.erd-detail-limit-btn[aria-pressed="true"] { color:var(--ink); border-color:var(--green); }
.erd-detail-limit-btn:focus-visible { outline:2px solid var(--blue); outline-offset:1px; }
/* A plain scrolling flex column, not itself preformatted text -- each markdown
   "block" (split on blank lines, see renderDetailBodyText in the widget script)
   is its own child (<table>, <h4> or <pre>), so a wide table can scroll
   horizontally without forcing every other line in the drawer to do the same. */
.erd-detail-body { flex:1; overflow-y:auto; overflow-x:hidden; margin:0; padding:.75rem;
        display:flex; flex-direction:column; gap:.6rem; color:var(--ink); }
/* Blocks are flex items: once the body overflows, flex-shrink would squeeze them to
   min-content -- which for a display:block <table> is 0px (it vanished under 30
   rows). Never shrink; the body scrolls instead. */
.erd-detail-body > * { flex:none; }
.erd-detail-body pre { margin:0; font-family:var(--font-mono); font-size:.78rem;
        line-height:1.5; white-space:pre-wrap; word-break:break-word; }
.erd-detail-body h4 { margin:.2rem 0 -.3rem; font-family:var(--font-caps); font-weight:600;
        font-size:.72rem; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); }
/* A markdown pipe table rendered as a real table: compact, never wraps mid-row --
   the block scrolls horizontally instead when wider than the drawer. */
.erd-detail-table { display:block; overflow-x:auto; max-width:100%; border-collapse:collapse;
        font-family:var(--font-mono); font-size:.74rem; line-height:1.4; }
.erd-detail-table th, .erd-detail-table td { padding:.22rem .55rem; text-align:left;
        vertical-align:top; white-space:nowrap; border-bottom:1px solid var(--edge); }
.erd-detail-table th { color:var(--blue); font-family:var(--font-caps); font-weight:600;
        background:var(--panel-inset); }

/* Scrollbars match the widget, not the OS -- every region that opts into overflow
   (.erd-viewport, .erd-detail-body, .erd-detail-table, the copy-fallback textarea).
   Standard properties first; the ::-webkit-* rules are the fallback for a Chromium
   that ignores them (a Chromium that honours scrollbar-color ignores ::-webkit-*,
   so the hover brightening only shows on the fallback path). */
.erd-canvas, .erd-canvas * { scrollbar-width:thin; scrollbar-color:var(--edge) transparent; }
.erd-canvas ::-webkit-scrollbar { width:8px; height:8px; }
.erd-canvas ::-webkit-scrollbar-track { background:transparent; }
.erd-canvas ::-webkit-scrollbar-thumb { background:var(--edge); border-radius:4px;
        border:2px solid transparent; background-clip:padding-box; }
.erd-canvas ::-webkit-scrollbar-thumb:hover { background:var(--muted); background-clip:padding-box; }
.erd-canvas ::-webkit-scrollbar-button { display:none; }
"""


@lru_cache(maxsize=1)
def widget_css() -> str:
    """The full stylesheet the ERD widget embeds: tokens + the component set above."""
    return f"{tokens.css_variables()}\n{_COMPONENTS}"


def components_css() -> str:
    """The component set alone (no tokens) -- for templates that place their own
    /*==TOKENS==*/ marker separately from /*==CSS==*/ (see design.widgetbuild)."""
    return _COMPONENTS
