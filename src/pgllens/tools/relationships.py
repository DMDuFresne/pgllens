"""Foreign-key relationships between tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import ForeignKey, Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.llens_style import (
    Bullet,
    Bullets,
    Call,
    Caveat,
    Code,
    ErrorCode,
    Response,
    Section,
    Table,
    nof,
)
from pgllens.tools._util import (
    SERVER,
    ToolError,
    _quote_ident,
    check_range,
    resolve_table,
    respond,
    tool_errors,
)

PLANE = "catalog"

Node = tuple[str, str]  # (schema, table)

# Two-letter aliases that PostgreSQL would parse as keywords -- pre-claimed so
# the alias generator skips straight to the numbered form ("as" -> "as2").
_KEYWORD_ALIASES = {"as", "by", "do", "if", "in", "is", "no", "on", "or", "to"}

MAX_PATHS = 5  # Deliberately simple: alternatives past the 5th are noise; raise if anyone asks

_FK_COLUMNS = ("from table", "from columns", "to table", "to columns", "constraint")


def _fk_row(fk: ForeignKey) -> tuple[str, ...]:
    return (f"`{fk.from_schema}.{fk.from_table}`", f"`{', '.join(fk.from_columns)}`",
            f"`{fk.to_schema}.{fk.to_table}`", f"`{', '.join(fk.to_columns)}`",
            f"`{fk.constraint}`")


def format_relationships(fks: list[ForeignKey], focus: str | None, scope: str | None) -> Response:
    """Render foreign keys as a style Response. With `focus`, splits into
    outgoing (focus references another table) and incoming (another table
    references focus) sections, always both, even when empty, so the reader
    sees the zero. Without a focus, lists every foreign key in scope."""
    if focus is None:
        ordered = sorted(fks, key=lambda f: (f.from_schema, f.from_table, f.constraint))
        rows = tuple(_fk_row(fk) for fk in ordered)
        first = fks[0] if fks else None
        return Response(
            SERVER, "get_relationships", scope, PLANE,
            (Section(None, (Table(_FK_COLUMNS, rows),)),),
            tally=(nof(len(rows), "foreign key"),),
            next=(Call("get_erd", {"schema": scope}) if scope else Call("get_erd"),
                  *((Call("find_path", {"from_table": first.from_table,
                                        "to_table": first.to_table}),)
                    if first else ())),
        )
    needle = focus.lower()
    outgoing = [fk for fk in fks if fk.from_table.lower() == needle]
    incoming = [fk for fk in fks if fk.to_table.lower() == needle]
    sections = (
        Section("outgoing", (Table(_FK_COLUMNS, tuple(_fk_row(fk) for fk in outgoing)),)),
        Section("incoming", (Table(_FK_COLUMNS, tuple(_fk_row(fk) for fk in incoming)),)),
    )
    return Response(
        SERVER, "get_relationships", scope, PLANE, sections,
        tally=(f"{len(outgoing)} outgoing", f"{len(incoming)} incoming"),
        next=(Call("get_erd", {"tables": scope}), Call("describe_table", {"table": scope})),
    )


def shortest_fk_paths(fks: list[ForeignKey], start: Node, goal: Node,
                      max_hops: int) -> list[list[ForeignKey]]:
    """Every shortest FK path from `start` to `goal`, each as the list of FKs
    traversed in order; empty when none exists within `max_hops` edges.

    The graph is **undirected**: a join reads an FK from either end, so an edge
    is walked child->parent or parent->child indifferently. Neighbours are
    sorted by (schema, table, constraint) and the returned paths by their
    constraint names, so the same schema always yields the same answer.
    """
    adjacency: dict[Node, list[tuple[Node, ForeignKey]]] = {}
    for fk in fks:
        child = (fk.from_schema, fk.from_table)
        parent = (fk.to_schema, fk.to_table)
        adjacency.setdefault(child, []).append((parent, fk))
        adjacency.setdefault(parent, []).append((child, fk))
    for edges in adjacency.values():
        edges.sort(key=lambda e: (e[0][0], e[0][1], e[1].constraint))

    frontier: list[tuple[Node, list[ForeignKey]]] = [(start, [])]
    seen = {start}
    for _ in range(max_hops):
        found: list[list[ForeignKey]] = []
        nxt: dict[Node, list[list[ForeignKey]]] = {}
        for node, path in frontier:
            for neighbour, fk in adjacency.get(node, []):
                if neighbour in seen:
                    continue
                if neighbour == goal:
                    found.append([*path, fk])
                elif len(nxt.setdefault(neighbour, [])) < MAX_PATHS:
                    # Deliberately simple: keeping at most MAX_PATHS routes per node
                    # bounds the frontier; we only ever render that many anyway.
                    nxt[neighbour].append([*path, fk])
        if found:
            found.sort(key=lambda p: [fk.constraint for fk in p])
            return found[:MAX_PATHS]
        seen.update(nxt)
        frontier = [(node, path) for node, paths in nxt.items() for path in paths]
    return []


def _walk(path: list[ForeignKey], start: Node) -> list[Node]:
    """The nodes a path visits, in traversal order, starting at `start`."""
    nodes = [start]
    current = start
    for fk in path:
        child = (fk.from_schema, fk.from_table)
        parent = (fk.to_schema, fk.to_table)
        current = parent if current == child else child
        nodes.append(current)
    return nodes


def _aliases(nodes: list[Node]) -> dict[Node, str]:
    """First two letters of each table name, suffixed with a digit when that
    collides with an earlier alias or a SQL keyword."""
    used = set(_KEYWORD_ALIASES)
    out: dict[Node, str] = {}
    for node in nodes:
        base = node[1][:2].lower() or "t"
        alias, n = base, 1
        while alias in used:
            n += 1
            alias = f"{base}{n}"
        used.add(alias)
        out[node] = alias
    return out


def _qualified(node: Node) -> str:
    """`schema.table` -- hop and alternative lines always qualify, because two
    exposed schemas may each hold an `order`."""
    return f"{node[0]}.{node[1]}"


def _quoted(node: Node) -> str:
    return f"{_quote_ident(node[0])}.{_quote_ident(node[1])}"


def _cols(columns: list[str]) -> str:
    """A composite key renders `(a, b)` -- format_relationships' style, and the
    only way the pairing survives the arrow. A single column stays bare."""
    return columns[0] if len(columns) == 1 else "(" + ", ".join(columns) + ")"


def _join_sql(path: list[ForeignKey], start: Node) -> str:
    """A ready-to-paste FROM/JOIN chain for one path. Each ON clause reads
    parent.col = child.col, whichever way the edge was traversed. Every
    identifier is quoted (same rule as discovery.py): a table named
    `Work Order` or a mixed-case ORM name is otherwise unparseable SQL."""
    nodes = _walk(path, start)
    alias = _aliases(nodes)
    parts = [f"FROM {_quoted(start)} {_quote_ident(alias[start])}"]
    for fk, node in zip(path, nodes[1:], strict=True):
        child = (fk.from_schema, fk.from_table)
        parent = (fk.to_schema, fk.to_table)
        on = " AND ".join(
            f"{_quote_ident(alias[parent])}.{_quote_ident(to_col)} = "
            f"{_quote_ident(alias[child])}.{_quote_ident(from_col)}"
            for from_col, to_col in zip(fk.from_columns, fk.to_columns, strict=True))
        parts.append(f"JOIN {_quoted(node)} {_quote_ident(alias[node])} ON {on}")
    return " ".join(parts)


def format_path(paths: list[list[ForeignKey]], start: Node, goal: Node, max_hops: int) -> Response:
    """Render the shortest path as numbered hops plus a JOIN clause, with any
    equally short alternatives listed after it. Every hop prints in the FK's own
    direction (child.col -> parent.col) even when it was traversed backwards, so
    each line is a valid join predicate on its own."""
    src, dst = _qualified(start), _qualified(goal)
    scope = f"{src} → {dst}"
    if not paths:
        return Response(
            SERVER, "find_path", scope, PLANE,
            (Section(None, (
                Bullets((Bullet("hops searched", str(max_hops)), Bullet("paths", "0"))),
                Caveat("Tables may be linked only through views or application logic."),
            )),),
            tally=("0 paths",),
            next=(Call("get_relationships", {"table": src}),
                  Call("get_relationships", {"table": dst})),
        )
    best = paths[0]
    hops = tuple(
        (str(i), f"`{_qualified((fk.from_schema, fk.from_table))}.{_cols(fk.from_columns)}`",
         f"`{_qualified((fk.to_schema, fk.to_table))}.{_cols(fk.to_columns)}`",
         f"`{fk.constraint}`")
        for i, fk in enumerate(best, 1))
    sections = [
        Section("hops", (Table(("hop", "from", "to", "constraint"), hops),)),
        Section("join", (Code("sql", _join_sql(best, start)),)),
    ]
    if len(paths) > 1:
        alts = tuple((" → ".join(f"`{_qualified(n)}`" for n in _walk(p, start)),
                      ", ".join(f"`{fk.constraint}`" for fk in p)) for p in paths[1:])
        sections.append(Section("alternatives", (Table(("route", "constraints"), alts),)))
    return Response(
        SERVER, "find_path", scope, PLANE, tuple(sections),
        tally=(nof(len(best), "hop"), nof(len(paths), "path")),
        next=(Call("get_relationships", {"table": src}), Call("get_relationships", {"table": dst})),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Relationships"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_relationships(table: str | None = None, schema: str | None = None) -> str:
        """List foreign-key relationships. Defaults to all exposed schemas.
        With `table`, shows outgoing and incoming relationships for that
        table; without it, lists every foreign key."""
        focus = scope = None
        if table is not None:
            t = await resolve_table(db, intro, table, schema)
            focus, scope = t.name, t.qualified
        elif schema is not None:
            scope = db.resolve_schema(schema)
        fks = await intro.foreign_keys()
        if scope and focus is None:
            fks = [fk for fk in fks if scope in (fk.from_schema, fk.to_schema)]
        return respond(format_relationships(fks, focus, scope))

    @mcp.tool(annotations=read_only("Find Join Path"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def find_path(from_table: str, to_table: str, schema: str | None = None,
                        max_hops: int = 6) -> str:
        """Shortest foreign-key join path between two tables, with a ready-to-paste
        JOIN clause. Defaults to all exposed schemas and treats each FK as
        traversable in either direction; `schema` only disambiguates the two table
        names. `max_hops` (1-10) bounds the search."""
        check_range("max_hops", max_hops, 1, 10)
        src = await resolve_table(db, intro, from_table, schema)
        dst = await resolve_table(db, intro, to_table, schema)
        start, goal = (src.schema, src.name), (dst.schema, dst.name)
        if start == goal:
            raise ToolError(ErrorCode.ARG_OUT_OF_RANGE,
                            f"`from_table` and `to_table` are the same table (`{src.qualified}`).",
                            "Pass two different tables.")
        fks = await intro.foreign_keys()
        paths = shortest_fk_paths(fks, start, goal, max_hops)
        return respond(format_path(paths, start, goal, max_hops))
