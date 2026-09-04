"""Response/Error -> markdown in the LLens style guide shape. Pure: clock and
request id are arguments, never read from the environment."""

from __future__ import annotations

import json
from datetime import datetime

from .model import (
    Block,
    Bullets,
    Call,
    Caveat,
    Code,
    Error,
    Response,
    Table,
)
from .values import iso

SEP = " · "


def _cell(s: str) -> str:
    return s.replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _table(t: Table) -> list[str]:
    head = "| " + " | ".join(t.columns) + " |"
    rule = "|" + "---|" * len(t.columns)
    body = ["| " + " | ".join(_cell(c) for c in row) + " |" for row in t.rows]
    return [head, rule, *body]


def _bullets(b: Bullets) -> list[str]:
    out = []
    for item in b.items:
        value = f"`{item.value}`" if item.is_code else item.value
        line = f"- {item.key}: {value}"
        if item.qualifier:
            line += f" ({item.qualifier})"
        if item.raw:
            line += f" — {item.raw}"
        out.append(line)
    return out


def _block(b: Block) -> list[str]:
    if isinstance(b, Table):
        return _table(b)
    if isinstance(b, Bullets):
        return _bullets(b)
    if isinstance(b, Code):
        return [f"```{b.lang}", b.text, "```"]
    if isinstance(b, Caveat):
        return [f"> {b.text}"]
    raise TypeError(f"unknown block {type(b).__name__}")


def _arg(v: object) -> str:
    if isinstance(v, str):
        return json.dumps(v)
    if v is None or isinstance(v, bool | int | float):
        return repr(v)
    return json.dumps(v)


def render_call(c: Call) -> str:
    args = ", ".join(f"{k}={_arg(v)}" for k, v in c.kwargs.items())
    return f"{c.tool}({args})"


def render(r: Response, *, now: datetime, request_id: str) -> str:
    del request_id  # accepted for symmetry with render_error; never emitted on success
    header = SEP.join(p for p in (r.server, r.tool, r.scope) if p)
    meta = SEP.join(p for p in (r.plane, iso(now), r.status) if p)
    lines = [f"## {header}", f"*{meta}*", ""]
    for i, section in enumerate(r.sections):
        if i:
            lines.append("")
        if section.heading is not None:
            lines.append(f"### {section.heading}")
        for j, block in enumerate(section.blocks):
            if j:
                lines.append("")
            lines.extend(_block(block))
    if r.tally:
        lines += ["", "---", SEP.join(r.tally)]
        if r.next:
            lines.append("*Next: " + SEP.join(render_call(c) for c in r.next) + "*")
    return "\n".join(lines)


def render_error(e: Error, *, now: datetime, request_id: str) -> str:
    del now  # errors carry no metadata line
    lines = [
        f"## {e.server}{SEP}{e.tool}{SEP}error",
        "",
        f"- code: `{e.code.value}`",
        f"- message: {e.message}",
        f"- hint: {e.hint}",
        f"- request_id: `{request_id}`",
    ]
    if e.retry_after:
        lines.append(f"- retry_after: `{e.retry_after}`")
    return "\n".join(lines)
