"""The data a tool produces. Frozen; validated at construction so a malformed
response fails in the tool's own test, never in a client."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .errors import ErrorCode

PLANES = frozenset({"catalog", "stats", "catalog+stats", "query"})
_HEADING_RE = re.compile(r"^[a-z][a-z0-9 ()+-]{0,39}$")
_KEY_RE = re.compile(r"^[a-z][a-z0-9_ ]*$")
_SENTENCE_END = re.compile(r"[.!?]\s+\S")


@dataclass(frozen=True)
class Bullet:
    key: str
    value: str
    is_code: bool = True
    qualifier: str | None = None
    raw: str | None = None

    def __post_init__(self) -> None:
        if not _KEY_RE.match(self.key):
            raise ValueError(f"bullet key must be lowercase: {self.key!r}")


@dataclass(frozen=True)
class Bullets:
    items: tuple[Bullet, ...]


@dataclass(frozen=True)
class Table:
    columns: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("table needs at least one column")
        for row in self.rows:
            if len(row) != len(self.columns):
                raise ValueError(
                    f"row width {len(row)} does not match {len(self.columns)} columns")


@dataclass(frozen=True)
class Code:
    lang: str
    text: str

    def __post_init__(self) -> None:
        if not self.lang:
            raise ValueError("code block needs a lang tag")


@dataclass(frozen=True)
class Caveat:
    text: str

    def __post_init__(self) -> None:
        if _SENTENCE_END.search(self.text.strip()):
            raise ValueError("caveat must be one sentence")


Block = Bullets | Table | Code | Caveat


@dataclass(frozen=True)
class Section:
    heading: str | None
    blocks: tuple[Block, ...]

    def __post_init__(self) -> None:
        if self.heading is not None and not _HEADING_RE.match(self.heading):
            raise ValueError(f"heading must be lowercase, short, unpunctuated: {self.heading!r}")
        if not self.blocks:
            raise ValueError("section needs at least one block")


@dataclass(frozen=True)
class Call:
    tool: str
    kwargs: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class Response:
    server: str
    tool: str
    scope: str | None
    plane: str
    sections: tuple[Section, ...]
    tally: tuple[str, ...] = ()
    next: tuple[Call, ...] = ()
    status: str | None = None

    def __post_init__(self) -> None:
        if self.scope is not None and (
            self.scope.lower() == "error" or "\n" in self.scope or " · " in self.scope
        ):
            raise ValueError(
                "scope must not be 'error' or contain a newline or the separator")
        if self.plane not in PLANES:
            raise ValueError(f"plane must be one of {sorted(PLANES)}: {self.plane!r}")
        if not self.sections:
            raise ValueError("response needs at least one section")
        if len(self.sections) == 1 and self.sections[0].heading is not None:
            raise ValueError("a single section has no heading")
        if len(self.sections) > 1 and any(s.heading is None for s in self.sections):
            raise ValueError("every section needs a heading when there are several")
        if len(self.next) > 3:
            raise ValueError("next holds at most three calls")
        has_table = any(isinstance(b, Table) for s in self.sections for b in s.blocks)
        if has_table and not self.tally:
            raise ValueError("a body with a table needs a tally")
        if self.next and not self.tally:
            raise ValueError("next needs a tally line")


@dataclass(frozen=True)
class Error:
    server: str
    tool: str
    code: ErrorCode
    message: str
    hint: str
    retry_after: str | None = None

    def __post_init__(self) -> None:
        if not self.hint.strip():
            raise ValueError("error needs a hint")
        if not self.message.strip():
            raise ValueError("error needs a message")
