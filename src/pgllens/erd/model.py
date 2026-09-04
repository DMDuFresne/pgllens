"""Pure-Python ERD data model built from Introspector reads -- no SQL of its own.

Read-only: every value here comes from Introspector.list_tables / .relationships /
.describe_table, which already route through the safety-checked, parameterised
introspection queries. This module just shapes those rows into a graph.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Protocol, cast

from pydantic import BaseModel, ConfigDict, Field

from pgllens.database.format import QueryResult
from pgllens.database.introspect import TableDetail


class _IntrospectorLike(Protocol):
    async def list_tables(self, database: str | None = None) -> QueryResult: ...
    async def relationships(self, database: str | None = None) -> QueryResult: ...
    async def describe_table(
        self, database: str | None, schema: str, table: str
    ) -> TableDetail: ...


@dataclass
class ErdColumn:
    name: str
    type: str
    nullable: bool
    is_pk: bool
    is_fk: bool


@dataclass
class ErdNode:
    schema: str
    table: str
    kind: str  # "table" | "view"
    rows: int | None
    description: str | None
    columns: list[ErdColumn] = field(default_factory=list)
    related: bool = False  # True when pulled in only as a neighbour of a requested table
    external: bool = False  # True when pulled in only as an out-of-schema FK target
    # under a `schema` filter (audit #9) -- keeps the edge instead of dropping it


@dataclass
class ErdEdge:
    from_schema: str
    from_table: str
    from_column: str
    to_schema: str
    to_table: str
    to_column: str
    constraint: str
    # NOT NULL-ness of from_column (the FK's child/referencing side) -- drives
    # to_mermaid's child-side cardinality token (}o vs }|).
    not_null: bool = False


@dataclass
class Erd:
    database: str | None
    nodes: list[ErdNode]
    edges: list[ErdEdge]
    truncated: bool = False
    note: str | None = None
    warnings: list[str] = field(default_factory=list)


NodeKey = tuple[str, str]


async def build_erd(
    intro: _IntrospectorLike,
    database: str | None,
    schema: str | None = None,
    tables: list[str] | None = None,
    include_columns: bool = True,
    max_nodes: int = 60,
    depth: int = 1,
) -> Erd:
    tables_result = await intro.list_tables(database)
    fks_result = await intro.relationships(database)

    all_nodes: dict[NodeKey, ErdNode] = {}
    for row in tables_result.rows:
        # list_tables columns: schema/object/type are str. rows is whatever the
        # adapter emitted (tools/erd.py::_IntroAdapter stringifies the planner's
        # row_estimate, so it's a str like "50", or None); it is coerced through
        # str() here so the digit check below works regardless of the source
        # type, and non-digit values fall through to an unknown count.
        # description is str or None.
        obj_schema = cast(str, row[0])
        obj_name = cast(str, row[1])
        type_desc = cast(str, row[2])
        raw_rows = row[3]
        raw_rows_str = str(raw_rows) if raw_rows is not None else None
        row_count = int(raw_rows_str) if raw_rows_str is not None and raw_rows_str.isdigit() else None
        description = cast("str | None", row[4])
        kind = "view" if type_desc == "VIEW" else "table"
        all_nodes[(obj_schema, obj_name)] = ErdNode(
            schema=obj_schema, table=obj_name, kind=kind, rows=row_count, description=description
        )

    all_edges = [
        ErdEdge(
            from_schema=cast(str, fk_schema), from_table=cast(str, fk_table),
            from_column=cast(str, fk_column), to_schema=cast(str, ref_schema),
            to_table=cast(str, ref_table), to_column=cast(str, ref_column),
            constraint=cast(str, fk_name), not_null=bool(fk_not_null),
        )
        # FOREIGN_KEYS columns are all sysname (str); not_null is the FK
        # column's attnotnull (bool).
        for fk_name, fk_schema, fk_table, fk_column, ref_schema, ref_table, ref_column, fk_not_null
        in fks_result.rows
    ]

    warnings = _unknown_table_warnings(all_nodes, tables) + _view_exclusion_warnings(all_nodes, tables)
    selected = _select_nodes(all_nodes, all_edges, schema, tables, depth)

    degree: dict[NodeKey, int] = dict.fromkeys(selected, 0)
    for edge in all_edges:
        src, dst = (edge.from_schema, edge.from_table), (edge.to_schema, edge.to_table)
        if src in degree:
            degree[src] += 1
        if dst in degree:
            degree[dst] += 1

    truncated = False
    note = None
    total = len(selected)
    if total > max_nodes:
        # `related`/`external` first (False sorts before True): a node the
        # caller asked for -- named in `tables`, or in the filtered `schema` --
        # must never be dropped in favour of a node only pulled in for it (a
        # depth>=2 neighbour, or an out-of-schema FK stub). Degree alone did
        # exactly that. Within each group, highest FK degree wins as before.
        ordered = sorted(selected, key=lambda k: (all_nodes[k].related or all_nodes[k].external,
                                                  -degree[k], k[0], k[1]))
        selected = set(ordered[:max_nodes])
        note = (
            f"Showing {max_nodes} of {total} nodes ({total - max_nodes} dropped); "
            "narrow with `schema` or `tables` to see the rest."
        )
        truncated = True

    final_nodes = sorted((all_nodes[k] for k in selected), key=lambda n: (n.schema, n.table))
    final_edges = [
        e for e in all_edges
        if (e.from_schema, e.from_table) in selected and (e.to_schema, e.to_table) in selected
    ]

    if include_columns:
        # Deliberately simple: one describe_table() round-trip per node, N queries
        # bounded by max_nodes. Upgrade path if this proves slow: a single batched
        # query joining sys.columns/sys.index_columns/sys.foreign_key_columns across
        # all selected object_ids, mirroring Introspector.describe_table's per-table
        # SQL.
        for node in final_nodes:
            detail = await intro.describe_table(database, node.schema, node.table)
            pk_cols = {row[0] for row in detail["primary_key"].rows}
            fk_cols = {
                row[3] for row in detail["foreign_keys"].rows
                if row[1] == node.schema and row[2] == node.table
            }
            for col_row in detail["columns"].rows:
                # DESCRIBE_COLUMNS: c.name/t.name are sysname (str), is_nullable is bit.
                name = cast(str, col_row[0])
                col_type = cast(str, col_row[1])
                is_nullable = col_row[5]
                node.columns.append(ErdColumn(
                    name=name, type=col_type, nullable=bool(is_nullable),
                    is_pk=name in pk_cols, is_fk=name in fk_cols,
                ))

    return Erd(
        database=database, nodes=final_nodes, edges=final_edges,
        truncated=truncated, note=note, warnings=warnings,
    )


def _unknown_table_warnings(all_nodes: dict[NodeKey, ErdNode], tables: list[str] | None) -> list[str]:
    """audit #10: `tables` names that match nothing must be surfaced, not silently
    dropped. difflib.get_close_matches is duplicated (not imported) from
    Introspector.table()'s inline suggestion logic in database/introspect.py --
    that logic is a few lines inline in a method, not an importable helper, and
    importing Introspector here would tangle erd.model into the DB layer for a
    one-call stdlib lookup."""
    if not tables:
        return []
    known_names = sorted({k[1] for k in all_nodes})
    warnings = []
    for name in tables:
        if name in known_names:
            continue
        near = difflib.get_close_matches(name, known_names, n=3)
        hint = f" Did you mean: {', '.join(near)}?" if near else ""
        warnings.append(f"ignored unknown table: {name}{hint}")
    return warnings


def _view_exclusion_warnings(all_nodes: dict[NodeKey, ErdNode], tables: list[str] | None) -> list[str]:
    """A `tables=` name that resolves only to a view is "known" (so
    _unknown_table_warnings stays quiet) but _select_nodes' kind filter drops
    it anyway -- surface that exclusion instead of silently returning nothing
    for it (recreates the silent-ignore class audit #10 closed)."""
    if not tables:
        return []
    wanted = set(tables)
    return [
        f"excluded (view): {n.table} — views are not shown in the ERD"
        for k, n in all_nodes.items()
        if k[1] in wanted and n.kind == "view"
    ]


def _select_nodes(
    all_nodes: dict[NodeKey, ErdNode],
    all_edges: list[ErdEdge],
    schema: str | None,
    tables: list[str] | None,
    depth: int = 1,
) -> set[NodeKey]:
    if tables is not None:
        wanted = set(tables)
        # audit #12: views own no FKs and are excluded from the ERD everywhere;
        # an explicit `tables` name that only matches a view yields no node,
        # same as full-database/schema-filtered mode below.
        selected = {k for k, n in all_nodes.items() if k[1] in wanted and n.kind == "table"}
        # `depth` rounds of FK expansion: each round adds the neighbours of the
        # current frontier only, so depth=1 is the original immediate-neighbour
        # behaviour. Deliberately simple: re-scans all_edges per round, at most 3
        # rounds over an FK list bounded by the catalog, cheaper than building an
        # adjacency map.
        frontier = selected
        related: set[NodeKey] = set()
        for _ in range(max(depth, 1)):
            neighbours: set[NodeKey] = set()
            for edge in all_edges:
                src, dst = (edge.from_schema, edge.from_table), (edge.to_schema, edge.to_table)
                if src in frontier:
                    neighbours.add(dst)
                if dst in frontier:
                    neighbours.add(src)
            # `& all_nodes.keys()`: an FK whose other end is not a node (a parent
            # outside the exposed schemas, or a view) must not be followed --
            # the `related` marking below would KeyError on it.
            frontier = (neighbours - selected - related) & all_nodes.keys()
            if not frontier:
                break
            related |= frontier
        for key in related:
            all_nodes[key].related = True
        return selected | related

    if schema is not None:
        selected = {k for k, n in all_nodes.items() if k[0] == schema and n.kind == "table"}
        # audit #9: a schema filter must not silently drop an FK's other end --
        # keep it as a stub node (full data, just flagged `external`) so the
        # edge survives instead of being filtered out by the selected-nodes check.
        externals: set[NodeKey] = set()
        for edge in all_edges:
            src, dst = (edge.from_schema, edge.from_table), (edge.to_schema, edge.to_table)
            if src in selected and dst not in selected and dst in all_nodes:
                externals.add(dst)
            if dst in selected and src not in selected and src in all_nodes:
                externals.add(src)
        for key in externals:
            all_nodes[key].external = True
        return selected | externals

    # Full database: base tables only -- views own no FKs, so they never carry
    # a relationship worth drawing (audit #12: consistent with the schema-filtered
    # and tables-filtered branches above, which also drop kind == "view").
    return {k for k, n in all_nodes.items() if n.kind == "table"}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def to_dict(erd: Erd, mermaid: str | None = None) -> dict[str, Any]:
    """The widget's JSON contract -- plain primitives only, safe to json.dumps().

    `mermaid` (the same text to_mermaid(erd) produces) rides along so the
    widget's "Copy Mermaid" button can read DATA.mermaid instead of
    reimplementing the renderer in JS -- one generator, not two.
    """
    return {
        "database": erd.database,
        "truncated": erd.truncated,
        "note": erd.note,
        "warnings": erd.warnings,
        "mermaid": mermaid,
        "nodes": [
            {
                "schema": n.schema,
                "table": n.table,
                "kind": n.kind,
                "rows": _jsonable(n.rows),
                "description": n.description,
                "related": n.related,
                "external": n.external,
                "columns": [
                    {
                        "name": c.name,
                        "type": c.type,
                        "nullable": c.nullable,
                        "is_pk": c.is_pk,
                        "is_fk": c.is_fk,
                    }
                    for c in n.columns
                ],
            }
            for n in erd.nodes
        ],
        "edges": [
            {
                "from": {
                    "schema": e.from_schema, "table": e.from_table, "column": e.from_column,
                },
                "to": {"schema": e.to_schema, "table": e.to_table, "column": e.to_column},
                "constraint": e.constraint,
            }
            for e in erd.edges
        ],
    }


# --- wire-protocol output model ---------------------------------------------
#
# ErdOut types the structuredContent the widget path attaches (get_erd itself
# is annotated `-> CallToolResult` so no outputSchema is advertised and
# mermaid/text/error results can stay text-only -- see tools/erd.py's module
# docstring for why). It is built by validating to_dict()'s own output so the
# two never drift apart (to_dict() stays the single source of truth; its
# json-round-trip test still applies unchanged).


class ErdColumnOut(BaseModel):
    name: str
    type: str
    nullable: bool
    is_pk: bool
    is_fk: bool


class ErdNodeOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_: str = Field(alias="schema")
    table: str
    kind: str
    rows: int | None
    description: str | None
    related: bool
    external: bool
    columns: list[ErdColumnOut]


class ErdEndpointOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_: str = Field(alias="schema")
    table: str
    column: str


class ErdEdgeOut(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: ErdEndpointOut = Field(alias="from")
    to: ErdEndpointOut
    constraint: str


class ErdOut(BaseModel):
    """get_erd's declared return shape -- see the module comment above."""

    database: str | None
    truncated: bool
    note: str | None
    warnings: list[str] = []
    mermaid: str | None = None
    nodes: list[ErdNodeOut]
    edges: list[ErdEdgeOut]


def to_erd_out(erd: Erd, mermaid: str | None = None) -> ErdOut:
    """Build the declared output model from the same dict the widget consumes."""
    return ErdOut.model_validate(to_dict(erd, mermaid))


def _mermaid_ident(schema: str, table: str) -> str:
    return f'"{schema}.{table}"'


_INVALID_MERMAID_TOKEN_CHARS = re.compile(r"[^A-Za-z0-9_]")


def _mermaid_token(value: str) -> str:
    """A bare mermaid erDiagram attribute token: identifiers can't contain
    spaces or other punctuation, so sanitize with underscores rather than
    quoting (quoted attribute names are rejected by strict renderers)."""
    # Replace invalid chars with underscore
    sanitized = _INVALID_MERMAID_TOKEN_CHARS.sub("_", value)
    # Strip leading/trailing underscores
    sanitized = sanitized.strip("_")
    # Collapse consecutive underscores to single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    # If trimming/collapsing left us empty, use a sentinel
    return sanitized or "unknown"


def to_mermaid(erd: Erd) -> str:
    """Text fallback for clients that can't render the widget."""
    lines = ["erDiagram"]
    for n in erd.nodes:
        ident = _mermaid_ident(n.schema, n.table)
        if not n.columns:
            # audit #12 empty-braces cleanup: include_columns=false (or a table
            # with none) must not emit a bare `{ }` block -- just the entity name.
            lines.append(f"    {ident}")
            continue
        lines.append(f"    {ident} {{")
        seen_names: dict[str, int] = {}
        for c in n.columns:
            flags = " ".join(f for f in ("PK" if c.is_pk else "", "FK" if c.is_fk else "") if f)
            col_type = _mermaid_token(c.type) or "unknown"
            col_name = _mermaid_token(c.name)
            # Two distinct column names (e.g. "size%" and "size_") can sanitize
            # to the same token -- suffix the repeats so sibling attributes in
            # one entity block stay distinct instead of silently colliding.
            if col_name in seen_names:
                seen_names[col_name] += 1
                col_name = f"{col_name}_{seen_names[col_name]}"
            else:
                seen_names[col_name] = 1
            suffix = f" {flags}" if flags else ""
            lines.append(f"        {col_type} {col_name}{suffix}")
        lines.append("    }")
    for e in erd.edges:
        left = _mermaid_ident(e.from_schema, e.from_table)
        right = _mermaid_ident(e.to_schema, e.to_table)
        # Child side: NOT NULL FK -> one-or-many (}|), nullable FK -> zero-or-many
        # (}o). Parent side is always exactly-one (||) -- an FK always points at
        # exactly one parent row.
        child = "}|" if e.not_null else "}o"
        lines.append(f'    {left} {child}--|| {right} : "{e.constraint}"')
    return "\n".join(lines)
