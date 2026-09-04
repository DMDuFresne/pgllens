"""Official Abelara brand assets, inlined.

The mark is the official artwork verbatim (gradient id namespaced `ablMarkGrad` to avoid
collisions). Per the brand book it must never be edited, recolored, rotated, or combined
with added elements. The Abelara name and mark are trademarks and copyright of Abelara —
see the top-level NOTICE carve-out; they are NOT Apache-2.0-licensed content.

Inline SVG needs no CSP relaxation (it is not a resource load). The favicon is a data:
URI (an in-document image load) and is why pages that use it serve `img-src data:`.
"""
from __future__ import annotations

# Base64 of the mark as a standalone SVG document (favicon). Copied verbatim from
# the widget's <link rel="icon"> href.
FAVICON_DATA_URI = (
    "data:image/svg+xml;base64,"
    "PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz48c3ZnIGlkPSJMYXllcl8xIiB4bWxu"
    "cz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9y"
    "Zy8xOTk5L3hsaW5rIiB2aWV3Qm94PSIwIDAgMjE2LjU0IDI0OS41MyI+PGRlZnM+PHN0eWxlPi5jbHMt"
    "MXtmaWxsOnVybCgjbGluZWFyLWdyYWRpZW50KTt9PC9zdHlsZT48bGluZWFyR3JhZGllbnQgaWQ9Imxp"
    "bmVhci1ncmFkaWVudCIgeDE9IjIxMC4xNyIgeTE9IjQ1LjYyIiB4Mj0iNTYuOTUiIHkyPSIxNjQuNjQi"
    "IGdyYWRpZW50VW5pdHM9InVzZXJTcGFjZU9uVXNlIj48c3RvcCBvZmZzZXQ9IjAiIHN0b3AtY29sb3I9"
    "IiNkNGZkYjEiLz48c3RvcCBvZmZzZXQ9IjEiIHN0b3AtY29sb3I9IiNiM2U2ZTEiLz48L2xpbmVhckdy"
    "YWRpZW50PjwvZGVmcz48cGF0aCBjbGFzcz0iY2xzLTEiIGQ9Ik0yMTYuMTcsMTg0LjMybC0zNC40Mi01"
    "OS41NCwzNC40Mi01OS41NGMuNjMtMS4wOC40NC0yLjQ2LS40NC0zLjM0LS4yMy0uMjMtLjQ5LS40LS43"
    "Ny0uNTNsLjA1LS4wOEwxMTAuOTkuNzNjLTEuNjgtLjk4LTMuNzYtLjk3LTUuNDQsMEwxLjUzLDYxLjNs"
    "LjA1LjA4Yy0uMjguMTMtLjU0LjMxLS43Ny41My0uODkuODgtMS4wNywyLjI2LS40NCwzLjM0bDM0LjQx"
    "LDU5LjU0TC4zNywxODQuMzJjLS42MywxLjA4LS40NSwyLjQ2LjQ0LDMuMzQuMjIuMjIuNDguMzkuNzUu"
    "NTJsLS4wMy4wNSwxMDQuMDIsNjAuNTZjLjg0LjQ5LDEuNzguNzMsMi43Mi43M3MxLjg4LS4yNCwyLjcy"
    "LS43M2wxMDQuMDItNjAuNTYtLjAyLS4wNGMuMjctLjEzLjUyLS4zMS43NS0uNTMuODktLjg4LDEuMDct"
    "Mi4yNi40NC0zLjM0Wk0yMDcuNiwxNzkuNTFsLTQyLjk4LTI1LjA5LDE0LjI0LTI0LjYzLDI4Ljc0LDQ5"
    "LjcyWk0xMDguMjcsNy42Nmw0OS41Myw4NS42OS00OS4xLDI4LjY2Yy0uMjktLjA1LS41Ny0uMDUtLjg2"
    "LDBsLTQ5LjEtMjguNjZMMTA4LjI3LDcuNjZaTTU0LjQyLDkwLjgzTDcuNjUsNjMuNTMsMTAxLjkzLDgu"
    "NjNsLTQ3LjUxLDgyLjE5Wk0xMTQuNjEsOC42M2w5NC4yOCw1NC44OS00Ni43NywyNy4zTDExNC42MSw4"
    "LjYzWk0xNjAuMzEsOTcuNjhsMTUuNjcsMjcuMTEtMTUuNjcsMjcuMTEtNDYuNDQtMjcuMTEsNDYuNDMt"
    "MjcuMTFaTTEwMi42NywxMjQuNzlsLTQ2LjQ0LDI3LjExLTE1LjY3LTI3LjExLDE1LjY3LTI3LjExLDQ2"
    "LjQ0LDI3LjExWk0xMDcuODQsMTI3LjU2Yy4yOS4wNS41Ny4wNS44NiwwbDQ5LjEsMjguNjYtNDkuNTMs"
    "ODUuNjktNDkuNTMtODUuNjksNDkuMS0yOC42NlpNMTAxLjg5LDI0MC44N0w3LjY5LDE4Ni4wM2w0Ni43"
    "My0yNy4yOCw0Ny40Nyw4Mi4xM1pNMTYyLjEyLDE1OC43NWw0Ni43MywyNy4yOC05NC4yLDU0Ljg1LDQ3"
    "LjQ3LTgyLjEyWk0xNzguODcsMTE5Ljc5bC0xNC4yNC0yNC42Myw0Mi45OC0yNS4wOS0yOC43NCw0OS43"
    "MlpNOC45Myw3MC4wN2w0Mi45OCwyNS4wOS0xNC4yNCwyNC42My0yOC43NC00OS43MlpNMzcuNjcsMTI5Ljc5"
    "bDE0LjI0LDI0LjYzLTQyLjk4LDI1LjA5LDI4Ljc0LTQ5LjcyWiIvPjwvc3ZnPg=="
)
