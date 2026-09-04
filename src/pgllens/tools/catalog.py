"""Extension and role catalog tools, lifted from TS/tools/list-extensions.ts
and list-roles.ts. list_roles reports pg_roles columns only -- pg_authid's
rolpassword hash is never selected, so there is no column to redact."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from mcp.server.mcpserver import MCPServer

    from pgllens.config import Settings
    from pgllens.database.capability import Capabilities
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db

from pgllens.annotations import MODEL_ONLY, read_only, visibility
from pgllens.database.format import QueryResult
from pgllens.llens_style import Call, Caveat, Response, Section, Table, nof
from pgllens.tools._util import SERVER, respond, tool_errors

PLANE = "catalog"

_EXTENSIONS_SQL = """
    SELECT e.extname AS name, e.extversion AS installed_version,
           a.default_version AS available_version, n.nspname AS schema,
           a.comment AS description
    FROM pg_extension e
    JOIN pg_namespace n ON e.extnamespace = n.oid
    LEFT JOIN pg_available_extensions a ON e.extname = a.name
    ORDER BY e.extname
"""

# rolpassword deliberately excluded: this reports role attributes, never the
# credential hash, even though pg_roles carries it for superusers.
#
# Raw string: `\_` must reach Postgres as a LIKE escape for a literal
# underscore, and `%%` is psycopg's escape for a literal % in a parameterised
# statement. include_builtin is bound (never interpolated) as the first param.
_ROLES_SQL = r"""
    SELECT r.rolname AS name, r.rolsuper AS is_superuser, r.rolcanlogin AS can_login,
           r.rolcreatedb AS create_db, r.rolcreaterole AS create_role,
           r.rolconnlimit AS connection_limit,
           COALESCE(array_agg(mr.rolname ORDER BY mr.rolname)
                    FILTER (WHERE mr.rolname IS NOT NULL), ARRAY[]::text[]) AS member_of
    FROM pg_roles r
    LEFT JOIN pg_auth_members m ON r.oid = m.member
    LEFT JOIN pg_roles mr ON m.roleid = mr.oid
    WHERE (%s OR r.rolname NOT LIKE 'pg\_%%')
    GROUP BY r.oid, r.rolname, r.rolsuper, r.rolcanlogin, r.rolcreatedb,
             r.rolcreaterole, r.rolconnlimit
    ORDER BY r.rolname
"""

# No params, so psycopg does no placeholder processing -- a bare % is safe here.
_BUILTIN_ROLE_COUNT_SQL = r"""
    SELECT count(*) FROM pg_roles WHERE rolname LIKE 'pg\_%'
"""

_GRANTS_SQL = """
    SELECT n.nspname AS schema_name, c.relname AS table_name,
           COALESCE(r.rolname, 'PUBLIC') AS grantee, a.privilege_type AS privilege
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS a
    LEFT JOIN pg_roles r ON a.grantee = r.oid
    WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p')
    ORDER BY n.nspname, c.relname, grantee, a.privilege_type
"""

# Fallback when acldefault() isn't usable here: only expands tables that carry
# an explicit ACL, same as list-roles.ts's own two-tier try/catch.
_GRANTS_FALLBACK_SQL = """
    SELECT n.nspname AS schema_name, c.relname AS table_name,
           COALESCE(r.rolname, 'PUBLIC') AS grantee, a.privilege_type AS privilege
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    CROSS JOIN LATERAL aclexplode(c.relacl) AS a
    LEFT JOIN pg_roles r ON a.grantee = r.oid
    WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'v', 'm', 'p') AND c.relacl IS NOT NULL
    ORDER BY n.nspname, c.relname, grantee, a.privilege_type
"""


# Role/grant rows are catalog metadata and must not be silently cut at the
# 200-row query cap -- mirrors introspect._CATALOG_ROWS / discovery._CATALOG_ROWS.
# The collapsed "on N relations" count is derived from the rows we got back, so
# a truncated read has to say the counts are lower bounds.
_CATALOG_ROWS = 5000


def format_extensions(result: QueryResult) -> Response:
    rows = []
    upgrades = 0
    for name, installed, available, schema, description in result.rows:
        upgrade = bool(available and available != installed)
        upgrades += upgrade
        rows.append((f"`{name}`", f"`{installed}`", f"`{available}`" if available else "",
                     "yes" if upgrade else "", f"`{schema}`",
                     str(description) if description else ""))
    return Response(
        SERVER, "list_extensions", None, PLANE,
        (Section(None, (Table(("extension", "installed", "available", "upgrade", "schema",
                               "description"), tuple(rows)),)),),
        tally=(nof(len(rows), "extension"), f"{upgrades} upgradable"),
        next=(Call("get_query_store"), Call("list_hypertables")),
    )


def format_roles(result: QueryResult) -> Table:
    """Only the named role-attribute columns -- never all of result.columns --
    so a password hash a query happened to select can never reach the output."""
    idx = {name: i for i, name in enumerate(result.columns)}

    def get(row: tuple[object, ...], col: str) -> object:
        return row[idx[col]] if col in idx else None

    def yn(v: object) -> str:
        return "✓" if v else ""

    rows = []
    for row in result.rows:
        conn_limit = get(row, "connection_limit")
        member_of = cast("list[str] | None", get(row, "member_of"))
        rows.append((f"`{get(row, 'name')}`", yn(get(row, "can_login")), yn(get(row, "is_superuser")),
                     yn(get(row, "create_db")), yn(get(row, "create_role")),
                     "unlimited" if conn_limit == -1 else str(conn_limit),
                     ", ".join(f"`{m}`" for m in member_of) if member_of else ""))
    return Table(("role", "login", "super", "createdb", "createrole", "conn limit", "member of"),
                 tuple(rows))


_ALL_TABLE_PRIVS = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
                              "REFERENCES", "TRIGGER", "MAINTAIN"})


def _privs_label(privs: list[str]) -> str:
    """`ALL (n privileges)` when the set is exactly Postgres's full table
    privilege set -- 8 on PG17+, or the same 7 before MAINTAIN existed. Any
    other set is listed sorted: aclexplode yields one row per grantor, so the
    raw order repeats a privilege granted twice and varies with catalog order."""
    held = frozenset(privs)
    if held in (_ALL_TABLE_PRIVS, _ALL_TABLE_PRIVS - {"MAINTAIN"}):
        return f"ALL ({len(held)} privileges)"
    return ", ".join(sorted(held))


_Relations = dict[tuple[str, str], list[str]]


def format_grants(result: QueryResult) -> tuple[Table, list[Caveat]]:
    """One row per (role, scope): a role whose dominant privilege set covers
    more than half its relations collapses to one `<schemas>` row plus exception rows."""
    by_role: dict[str, _Relations] = {}
    for grant_row in result.rows:
        schema, table, grantee, privilege = cast("tuple[str, str, str, str]", grant_row)
        by_role.setdefault(grantee, {}).setdefault((schema, table), []).append(privilege)
    rows: list[tuple[str, ...]] = []
    for role in sorted(by_role):
        rels = by_role[role]
        common, count = Counter(frozenset(p) for p in rels.values()).most_common(1)[0]
        if count < 2 or count * 2 <= len(rels):
            for (s, t), p in sorted(rels.items()):
                rows.append((f"`{role}`", f"`{s}.{t}`", _privs_label(p)))
            continue
        privs = next(p for p in rels.values() if frozenset(p) == common)
        schemas = sorted({s for (s, _t), p in rels.items() if frozenset(p) == common})
        joined = ", ".join(f"`{s}`" for s in schemas)
        rows.append((f"`{role}`", f"{count} relations in {joined}", _privs_label(privs)))
        for (s, t), p in sorted(rels.items()):
            if frozenset(p) != common:
                rows.append((f"`{role}`", f"`{s}.{t}`", _privs_label(p)))
    caveats = []
    if result.truncated:
        caveats.append(Caveat(f"Grants truncated at {_CATALOG_ROWS} rows; counts are lower bounds."))
    return Table(("role", "on", "privileges"), tuple(rows)), caveats


def register(mcp: MCPServer, db: Db, settings: Settings, intro: Introspector,
             caps: Capabilities | None) -> None:
    @mcp.tool(annotations=read_only("List Extensions"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def list_extensions() -> str:
        """List installed PostgreSQL extensions with installed/available versions."""
        result = await db.run_system(_EXTENSIONS_SQL)
        return respond(format_extensions(result))

    @mcp.tool(annotations=read_only("List Roles"), meta=visibility(*MODEL_ONLY))
    @tool_errors
    async def list_roles(include_builtin: bool = False) -> str:
        """List database roles: login/superuser/createdb/createrole flags,
        connection limit, role memberships, and table grants (via aclexplode)
        across all exposed schemas. Built-in `pg_*` roles -- and grants held by
        them -- are hidden unless include_builtin=true; a grant set shared by
        more than half of a role's relations is collapsed into one summary
        line, with only the differing relations listed. Never reports
        credentials."""
        result = await db.run_system(_ROLES_SQL, (include_builtin,), max_rows=_CATALOG_ROWS)
        role_blocks: list[Table | Caveat] = [format_roles(result)]
        hidden = 0
        if not include_builtin:
            h = await db.run_system(_BUILTIN_ROLE_COUNT_SQL)
            hidden = cast("int", h.rows[0][0]) if h.rows else 0
            if hidden:
                role_blocks.append(
                    Caveat(f"{hidden} built-in pg_* roles hidden; pass include_builtin=True "
                           "to show them."))
        schemas = list(settings.exposed_schemas)
        grants: QueryResult | None
        try:
            grants = await db.run_system(_GRANTS_SQL, (schemas,), max_rows=_CATALOG_ROWS)
        except Exception:  # noqa: BLE001 -- acldefault() 2-arg form not on every build
            try:
                grants = await db.run_system(_GRANTS_FALLBACK_SQL, (schemas,),
                                             max_rows=_CATALOG_ROWS)
            except Exception:  # noqa: BLE001 -- aclexplode needs privilege we may lack
                grants = None
        sections = [Section("roles", tuple(role_blocks))]
        tally = [nof(len(result.rows), "role"), f"{hidden} hidden"]
        if grants is None:
            sections.append(Section("grants", (Caveat(
                "Table grants unavailable: insufficient permission for aclexplode."),)))
        else:
            if not include_builtin:
                # grantee is column 2; 'PUBLIC' is not a pg_* role and always stays.
                grants = QueryResult(
                    grants.columns,
                    [r for r in grants.rows if not str(r[2]).startswith("pg_")],
                    grants.truncated)
            table, caveats = format_grants(grants)
            sections.append(Section("grants", (table, *caveats)))
            tally.append(nof(len(table.rows), "grant row"))
            if grants.truncated:
                tally.append(f"grants truncated at {_CATALOG_ROWS} rows")
        sections.append(Section("visibility", (Caveat(
            "Only roles and privileges visible to the current database user are shown."),)))
        return respond(Response(SERVER, "list_roles", None, PLANE, tuple(sections),
                                tally=tuple(tally), next=(Call("list_extensions"),)))
