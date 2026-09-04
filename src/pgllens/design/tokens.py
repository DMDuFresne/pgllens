"""Chalkboard design tokens (Abelara Brand Book, June 2025).

Semantics — the hue IS the meaning; keep it consistent everywhere:
  BLUE   structure / identity / informational accents
  GREEN  affirmative / primary action / live-TRUE
  YELLOW advisory / attention / power rails
  RED    errors and faults ONLY — never decoration (brand: bold counterpoint)

An ERD is structure, so it is blue-led.
"""
from __future__ import annotations

# ---- field ----
BG = "#252525"           # chalkboard black (PMS Black 3C)
PANEL = "#2c2c2c"        # raised surface
PANEL_INSET = "#232323"  # inset surface
INK = "#ffffff"          # chalk white
MUTED = "#9a9a92"        # chalk dust — secondary text
EDGE = "#3a3a38"         # hairline borders

# ---- chalk hues (primary) + counterpoint ----
BLUE = "#b3e6e1"         # Light Blue (PMS 324C)
GREEN = "#d4fdb1"        # Light Green (PMS 358C)
YELLOW = "#ffffa9"       # Pale Yellow (PMS 7499C)
RED = "#f5602b"          # Red (PMS 171C) — errors/faults ONLY
CHALK_HUES: tuple[str, str, str] = (BLUE, GREEN, YELLOW)

# ---- gradients (brand Gradient 01: Light Green -> Light Blue, as in the mark) ----
GRADIENT_MARK = f"linear-gradient(90deg, {GREEN}, {BLUE})"

# ---- type ----
FONT_BRAND = '"Commissioner", Arial, system-ui, -apple-system, "Segoe UI", sans-serif'
FONT_CAPS = '"Exo", Arial, system-ui, -apple-system, "Segoe UI", sans-serif'
FONT_MONO = 'ui-monospace, "Cascadia Code", Consolas, monospace'

# ---- scales ----
RADIUS_SM = "4px"
RADIUS_MD = "6px"
RADIUS_LG = "10px"


def css_variables() -> str:
    """The shared :root custom-property block every surface embeds."""
    return (
        ":root{"
        f"--bg:{BG};--panel:{PANEL};--panel-inset:{PANEL_INSET};--ink:{INK};"
        f"--muted:{MUTED};--edge:{EDGE};"
        f"--blue:{BLUE};--green:{GREEN};--yellow:{YELLOW};--red:{RED};"
        f"--grad-mark:{GRADIENT_MARK};"
        f"--font-brand:{FONT_BRAND};--font-caps:{FONT_CAPS};--font-mono:{FONT_MONO};"
        f"--r-sm:{RADIUS_SM};--r-md:{RADIUS_MD};--r-lg:{RADIUS_LG};"
        "}"
    )
