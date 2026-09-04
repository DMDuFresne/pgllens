"""Mechanical checks of the style guide checklist. Used as the contract test
for every tool in every LLens server."""

from __future__ import annotations

import re
from dataclasses import dataclass

_H2 = re.compile(r"^## [a-z][a-z0-9]* · [a-z_][a-z0-9_]*( · .+)?$")
_META = re.compile(
    r"^\*[a-z]+(\+[a-z]+)* · \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ( \(cached \d+[smhd]\))?( · .+)?\*$")
_H3 = re.compile(r"^### [a-z][a-z0-9 ()+-]*$")
_CALL = r"[a-z_][a-z0-9_]*\(.*?\)"
_NEXT = re.compile(rf"^\*Next: {_CALL}( · {_CALL}){{0,2}}\*$")
# ruling: a tally line is any non-empty line that does not start with *, #, >, |, or whitespace —
# compliant tallies like "PostgreSQL 16.3 · 7 connections" start with letters, not digits.
_TALLY = re.compile(r"^[^*#>|\s].*")
_FILLER = re.compile(r"\b(Here are|Successfully|Note that|Unfortunately|I found)\b")
_ALLOWED_SYMBOLS = {"\u2713", "\u2714", "\u2014", "\u00b7", "\u2192", "\u2026"}


def _is_emoji(ch: str) -> bool:
    if ch in _ALLOWED_SYMBOLS:
        return False
    cp = ord(ch)
    return (0x1F000 <= cp <= 0x1FAFF or 0x2600 <= cp <= 0x27BF or 0x2B00 <= cp <= 0x2BFF
            or cp == 0xFE0F)


@dataclass(frozen=True)
class Violation:
    rule: str
    line: int
    message: str


def _scan_body(lines: list[str], out: list[Violation]) -> bool:
    """Body-wide rules. Returns whether a pipe table was seen outside fences."""
    in_fence = False
    fence_start = 0
    saw_table = False
    for i, line in enumerate(lines, 1):
        if line.startswith("```"):
            if not in_fence and line.strip() == "```":
                out.append(Violation("FENCE_LANG", i, "opening fence needs a language tag"))
            in_fence = not in_fence
            fence_start = i if in_fence else 0
            continue
        if in_fence:
            continue
        if line.startswith(("# ", "#### ")):
            out.append(Violation("NO_H1_H4", i, "only H2 (header) and H3 (sections)"))
        if line.startswith("### ") and not _H3.match(line):
            out.append(Violation("H3_ONLY", i, "H3 is lowercase, short, unpunctuated"))
        if "**" in line:
            out.append(Violation("NO_BOLD", i, "bold is not used"))
        if any(_is_emoji(ch) for ch in line):
            out.append(Violation("NO_EMOJI", i, "emoji are not used"))
        if _FILLER.search(line):
            out.append(Violation("NO_FILLER", i, "no filler phrases"))
        if line.startswith("|") and i > 2:
            saw_table = True
    if in_fence:
        out.append(Violation("FENCE_LANG", fence_start, "code fence is never closed"))
    return saw_table


def _check_error(lines: list[str], out: list[Violation]) -> None:
    body = [ln for ln in lines[1:] if ln.strip()]
    keys = [ln.split(":", 1)[0] for ln in body]
    want = ["- code", "- message", "- hint", "- request_id"]
    ok = keys[:4] == want and len(keys) in (4, 5)
    if ok and len(keys) == 5:
        ok = keys[4] == "- retry_after"
    if not body or not ok or not body[0].startswith("- code: `"):
        out.append(Violation("ERROR_SHAPE", 2,
                             "error body is exactly code/message/hint/request_id[/retry_after]"))


def _check_footer(lines: list[str], saw_table: bool, out: list[Violation]) -> None:
    if "---" not in lines:
        if saw_table:
            out.append(Violation("TABLE_NEEDS_TALLY", len(lines), "table body needs a footer"))
        return
    idx = len(lines) - 1 - lines[::-1].index("---")
    tail = lines[idx + 1:]
    ok = 1 <= len(tail) <= 2 and bool(_TALLY.match(tail[0]))
    if ok and len(tail) == 2:
        ok = bool(_NEXT.match(tail[1]))
    if not ok:
        out.append(Violation("FOOTER_SHAPE", idx + 1,
                             "`---` then a tally line then optional `*Next: …*`, nothing after"))


def lint(markdown: str) -> list[Violation]:
    out: list[Violation] = []
    lines = markdown.split("\n")
    if not _H2.match(lines[0]):
        out.append(Violation("H2_SHAPE", 1, "line 1 must be `## server · tool[ · scope]`"))
    if lines[0].endswith(" · error"):
        _check_error(lines, out)
        return out
    if len(lines) < 2 or not _META.match(lines[1]):
        out.append(Violation("META_LINE", 2, "line 2 must be `*plane · ISO-Z[ · status]*`"))
    saw_table = _scan_body(lines, out)
    _check_footer(lines, saw_table, out)
    return out
