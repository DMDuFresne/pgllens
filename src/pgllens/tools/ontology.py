"""get_ontology: a semantic summary of the schema -- which tables matter, how they
connect, and which naming conventions the schema follows -- built once from the
same Table/ForeignKey dataclasses every other discovery tool reads.

`build_ontology` is pure (no db, no Introspector) so it is driven entirely by unit
tests. The section order and the "flag a convention once, not per table" shape are
ported from TS/tools/get-ontology.ts's overview/domain-context renderers; TS has no
column-level catalog (check constraints/enums/views/indexes/triggers) equivalent
here, since Introspector only ever loads tables, columns and foreign keys -- the
hub-ranking and soft-delete/audit/lookup/junction heuristics below read only that
data.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import ForeignKey, Introspector
    from pgllens.database.introspect import Table as Table_
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.llens_style import Bullet, Bullets, Call, Caveat, Code, Response, Section, Table, nof
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "catalog"

_SOFT_DELETE_COLUMNS = ("removed_at", "deleted_at")
_AUDIT_COLUMNS = ("created_at", "updated_at", "created_by", "updated_by")
_LOOKUP_ROW_CEILING = 500

# Schema-wide audit-column convention: which of these does a table carry.
_AUDIT_TIMESTAMP_COLUMNS = ("created_at", "updated_at")

# Deliberately simple: name+shape heuristics only, no data sampling. A table
# named "audit_trail" with no timestamp column, or one that stores history in a
# JSONB blob under a different column name, won't be caught. Ceiling: rename
# discipline and single-column timestamps. Upgrade path is sampling actual
# column values if this ever needs to be exhaustive.
_AUDIT_NAME_RE = re.compile(r"^audit|_log$|_event$|_history$", re.IGNORECASE)
_TIME_SERIES_NAME_RE = re.compile(r"reading|measurement|metric|sample", re.IGNORECASE)
_VIEW_KINDS = ("v", "m")  # pg_class.relkind: view, materialized view

_ROLE_REASON = {"audit": "audit/log/history/event name with a timestamp column",
                "time-series": "timestamp-leading key or reading/measurement/metric/sample name"}


def _is_temporal_type(data_type: str) -> bool:
    # "timestamp"/"timestamptz" match by substring; "date" is matched exactly
    # so a text column merely named like a date (e.g. "validated_at", whose
    # *type* is text) never counts -- only the real `date` catalog type does.
    lowered = data_type.lower()
    return "timestamp" in lowered or lowered == "date"


def _has_timestamp_column(t: Table_) -> bool:
    return any(_is_temporal_type(c.data_type) for c in t.columns)


def _pk_leads_on_timestamp(t: Table_) -> bool:
    # Deliberately simple: PK is the only ordering data Introspector loads. There
    # is no index catalog here, so a timestamp-leading secondary index is
    # invisible to this heuristic. Ceiling: false negatives on tables whose PK is
    # a surrogate id but whose real query pattern indexes on time.
    if not t.primary_key:
        return False
    lead = t.primary_key[0]
    return any(c.name == lead and _is_temporal_type(c.data_type) for c in t.columns)


def _table_role(t: Table_) -> str | None:
    has_ts = _has_timestamp_column(t)
    if _AUDIT_NAME_RE.search(t.name) and has_ts:
        return "audit"
    if _pk_leads_on_timestamp(t) or (_TIME_SERIES_NAME_RE.search(t.name) and has_ts):
        return "time-series"
    return None


def _qualified(t: Table_) -> str:
    return f"{t.schema}.{t.name}"


def _inbound_counts(tables: list[Table_], fks: list[ForeignKey]) -> dict[tuple[str, str], int]:
    # Count distinct REFERENCING TABLES, not FK rows -- two FKs from the same
    # child table (e.g. order.customer_id and order.billing_customer_id, both
    # -> customer) must count as one referencing table, not two.
    referencers: dict[tuple[str, str], set[tuple[str, str]]] = {
        (t.schema, t.name): set() for t in tables
    }
    for fk in fks:
        key = (fk.to_schema, fk.to_table)
        if key in referencers:
            referencers[key].add((fk.from_schema, fk.from_table))
    return {key: len(froms) for key, froms in referencers.items()}


def _outbound_fks(fks: list[ForeignKey]) -> dict[tuple[str, str], list[ForeignKey]]:
    by_table: dict[tuple[str, str], list[ForeignKey]] = {}
    for fk in fks:
        by_table.setdefault((fk.from_schema, fk.from_table), []).append(fk)
    return by_table


def _hubs_section(ranked: list[Table_], inbound: dict[tuple[str, str], int]) -> Section:
    # A hub is a real table referenced by 2+ other tables; views never count,
    # even if something happens to foreign-key at one.
    hubs = [t for t in ranked if t.kind not in _VIEW_KINDS and inbound[(t.schema, t.name)] >= 2]
    rows = tuple((f"`{_qualified(t)}`", str(inbound[(t.schema, t.name)]), t.comment or "")
                 for t in hubs)
    return Section("hubs", (Table(("table", "referenced by", "comment"), rows),
                            Caveat("Ranked by distinct referencing tables; views never count.")))


def _roles_section(tables: list[Table_]) -> Section | None:
    tagged = [(t, role) for t in tables
              if t.kind not in _VIEW_KINDS and (role := _table_role(t)) is not None]
    if not tagged:
        return None
    rows = tuple((f"`{_qualified(t)}`", role, _ROLE_REASON[role]) for t, role in tagged)
    return Section("roles", (Table(("table", "role", "why"), rows),
                             Caveat("Name and shape heuristics; no data sampled.")))


def _relationships_section(fks: list[ForeignKey]) -> Section | None:
    if not fks:
        return None
    rows = tuple((f"`{fk.from_schema}.{fk.from_table}.({', '.join(fk.from_columns)})`",
                  f"`{fk.to_schema}.{fk.to_table}.({', '.join(fk.to_columns)})`",
                  f"`{fk.constraint}`") for fk in fks)
    return Section("relationships", (Table(("from", "to", "constraint"), rows),))


def _conventions_section(tables: list[Table_], inbound: dict[tuple[str, str], int],
                          outbound: dict[tuple[str, str], list[ForeignKey]]) -> Section | None:
    soft_delete: list[tuple[Table_, str]] = []
    audited: list[Table_] = []
    lookups: list[Table_] = []
    junctions: list[Table_] = []

    for t in tables:
        cols = {c.name for c in t.columns}
        soft_col = next((c for c in _SOFT_DELETE_COLUMNS if c in cols), None)
        if soft_col is not None:
            soft_delete.append((t, soft_col))
        if sum(1 for c in _AUDIT_COLUMNS if c in cols) >= 2:
            audited.append(t)

        key = (t.schema, t.name)
        out_fks = outbound.get(key, [])
        if not out_fks and inbound[key] > 0 and t.row_estimate <= _LOOKUP_ROW_CEILING:
            lookups.append(t)

        # Junction/bridge table: every primary-key column is covered by an
        # outbound foreign key, and it references at least two other tables --
        # the classic many-to-many association table.
        fk_cols: set[str] = set()
        referenced: set[tuple[str, str]] = set()
        for fk in out_fks:
            fk_cols.update(fk.from_columns)
            referenced.add((fk.to_schema, fk.to_table))
        if t.primary_key and set(t.primary_key) <= fk_cols and len(referenced) >= 2:
            junctions.append(t)

    # Schema-wide audit-column convention: flagged once when at least half the
    # tables carry created_at/updated_at, naming the columns actually found --
    # distinct from the per-table "audited" bullet below, which requires 2+ of
    # the fuller created_at/updated_at/created_by/updated_by set on one table.
    # Views carry no data of their own -- counting them in either the
    # numerator or the denominator would shift the ratio for a convention
    # that only means something for real, written-to tables.
    audited_tables = [t for t in tables if t.kind not in _VIEW_KINDS]
    audit_pattern_cols: set[str] = set()
    audit_pattern_tables = 0
    for t in audited_tables:
        cols = {c.name for c in t.columns}
        found = [c for c in _AUDIT_TIMESTAMP_COLUMNS if c in cols]
        if found:
            audit_pattern_tables += 1
            audit_pattern_cols.update(found)
    audit_convention = bool(audited_tables) and audit_pattern_tables * 2 >= len(audited_tables)

    items = []
    if audit_convention:
        items.append(Bullet(
            "audit columns",
            f"{audit_pattern_tables} of {len(audited_tables)} tables carry "
            f"{', '.join(sorted(audit_pattern_cols))}",
            is_code=False, qualifier="heuristic"))
    if soft_delete:
        items.append(Bullet(
            "soft delete", ", ".join(f"{_qualified(t)} ({col})" for t, col in soft_delete),
            is_code=False))
    if audited:
        items.append(Bullet("audited tables", ", ".join(_qualified(t) for t in audited),
                            is_code=False))
    if lookups:
        items.append(Bullet("lookup tables", ", ".join(_qualified(t) for t in lookups),
                            is_code=False, qualifier="heuristic"))
    if junctions:
        items.append(Bullet("junction tables", ", ".join(_qualified(t) for t in junctions),
                            is_code=False))
    return Section("conventions", (Bullets(tuple(items)),)) if items else None


def build_ontology(tables: list[Table_], fks: list[ForeignKey], settings: Settings,
                    scope: str | None = None) -> Response:
    """Pure response assembly: hub ranking, then naming conventions, table
    roles, how tables connect, then optional operator-supplied domain
    context. No database access -- `tables`/`fks` are already loaded."""
    inbound = _inbound_counts(tables, fks)
    outbound = _outbound_fks(fks)
    ranked = sorted(tables, key=lambda t: (-inbound[(t.schema, t.name)], t.schema, t.name))

    sections = [s for s in (
        _hubs_section(ranked, inbound),
        _conventions_section(tables, inbound, outbound),
        _roles_section(tables),
        _relationships_section(fks),
    ) if s]
    if settings.domain_context_text is not None:
        sections.append(Section("domain context", (Code("text", settings.domain_context_text),)))

    hubs_table = sections[0].blocks[0]
    if len(sections) == 1:
        sections = [Section(None, sections[0].blocks)]
    n_hubs = len(hubs_table.rows) if isinstance(hubs_table, Table) else 0
    top = ranked[0] if ranked else None
    return Response(
        SERVER, "get_ontology", scope, PLANE, tuple(sections),
        tally=(nof(len(tables), "object"), nof(n_hubs, "hub"), nof(len(fks), "foreign key")),
        next=(Call("describe_table", {"table": _qualified(top)}),
              Call("get_erd", {"schema": scope} if scope else {})) if top else (),
    )


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("Get Ontology"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def get_ontology(schema: str | None = None) -> str:
        """Semantic summary of the schema: which tables are hubs (most
        referenced), how tables connect, and the naming conventions in use
        (soft delete, audit columns, lookup tables, junction tables), plus any
        operator-supplied domain context. Defaults to all exposed schemas;
        `schema` filters to one."""
        tables = await intro.tables()
        fks = await intro.foreign_keys()
        scope = db.resolve_schema(schema) if schema is not None else None
        if scope is not None:
            tables = [t for t in tables if t.schema == scope]
            names = {t.name for t in tables}
            fks = [fk for fk in fks if fk.from_table in names or fk.to_table in names]
        return respond(build_ontology(tables, fks, settings, scope))
