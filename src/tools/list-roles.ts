/**
 * List Roles tool - Role/permission introspection with ACL expansion
 */

import { rawQuery } from '../database/client.js';
import { getConfig } from '../config.js';

interface ListRolesToolInput {
  schema?: string;
  role?: string;
}

export async function executeListRolesTool(
  input: ListRolesToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const config = getConfig();
    const lines: string[] = [];

    // 1. Load roles
    const roleParams: unknown[] = [];
    let roleFilter = '';
    if (input.role) {
      roleParams.push(input.role);
      roleFilter = 'WHERE r.rolname = $1';
    }

    const rolesSql = `
      SELECT
        r.rolname AS name,
        r.rolsuper AS is_superuser,
        r.rolcanlogin AS can_login,
        r.rolcreatedb AS create_db,
        r.rolcreaterole AS create_role,
        r.rolconnlimit AS connection_limit,
        COALESCE(
          array_agg(mr.rolname ORDER BY mr.rolname) FILTER (WHERE mr.rolname IS NOT NULL),
          ARRAY[]::text[]
        ) AS member_of
      FROM pg_roles r
      LEFT JOIN pg_auth_members m ON r.oid = m.member
      LEFT JOIN pg_roles mr ON m.roleid = mr.oid
      ${roleFilter}
      GROUP BY r.oid, r.rolname, r.rolsuper, r.rolcanlogin, r.rolcreatedb, r.rolcreaterole, r.rolconnlimit
      ORDER BY r.rolname
    `;

    const rolesResult = await rawQuery<{
      name: string;
      is_superuser: boolean;
      can_login: boolean;
      create_db: boolean;
      create_role: boolean;
      connection_limit: number;
      member_of: string[];
    }>(rolesSql, roleParams);

    if (rolesResult.rows.length === 0) {
      const msg = input.role
        ? `Role "${input.role}" not found.`
        : 'No roles found.';
      return { content: [{ type: 'text', text: msg }], isError: true };
    }

    // Render role summary
    lines.push('## Database Roles\n');
    lines.push('| Role | Login | Super | CreateDB | CreateRole | Conn Limit | Member Of |');
    lines.push('|------|-------|-------|----------|------------|------------|-----------|');

    for (const r of rolesResult.rows) {
      const memberOf = Array.isArray(r.member_of) && r.member_of.length > 0
        ? r.member_of.join(', ')
        : '—';
      const connLimit = r.connection_limit === -1 ? 'unlimited' : String(r.connection_limit);
      lines.push(
        `| ${r.name} | ${yn(r.can_login)} | ${yn(r.is_superuser)} | ${yn(r.create_db)} | ${yn(r.create_role)} | ${connLimit} | ${memberOf} |`
      );
    }
    lines.push('');

    // 2. Load table grants using aclexplode
    const schemas = input.schema ? [input.schema] : config.exposedSchemas;
    const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

    let grants: Array<{
      schema_name: string;
      table_name: string;
      grantee: string;
      privilege: string;
    }> = [];

    try {
      const grantsSql = `
        SELECT
          n.nspname AS schema_name,
          c.relname AS table_name,
          COALESCE(r.rolname, 'PUBLIC') AS grantee,
          a.privilege_type AS privilege
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS a
        LEFT JOIN pg_roles r ON a.grantee = r.oid
        WHERE n.nspname IN (${schemaPlaceholders})
          AND c.relkind IN ('r', 'v', 'm', 'p')
        ORDER BY n.nspname, c.relname, grantee, a.privilege_type
      `;

      const grantsResult = await rawQuery<{
        schema_name: string;
        table_name: string;
        grantee: string;
        privilege: string;
      }>(grantsSql, schemas);

      grants = grantsResult.rows;
    } catch {
      // Fallback: only expand tables with explicit ACLs
      try {
        const fallbackSql = `
          SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            COALESCE(r.rolname, 'PUBLIC') AS grantee,
            a.privilege_type AS privilege
          FROM pg_class c
          JOIN pg_namespace n ON c.relnamespace = n.oid
          CROSS JOIN LATERAL aclexplode(c.relacl) AS a
          LEFT JOIN pg_roles r ON a.grantee = r.oid
          WHERE n.nspname IN (${schemaPlaceholders})
            AND c.relkind IN ('r', 'v', 'm', 'p')
            AND c.relacl IS NOT NULL
          ORDER BY n.nspname, c.relname, grantee, a.privilege_type
        `;

        const fallbackResult = await rawQuery<{
          schema_name: string;
          table_name: string;
          grantee: string;
          privilege: string;
        }>(fallbackSql, schemas);

        grants = fallbackResult.rows;
      } catch {
        lines.push('*Could not retrieve table grants (insufficient permissions for aclexplode).*\n');
      }
    }

    // Filter grants by role if specified
    if (input.role && grants.length > 0) {
      grants = grants.filter(g => g.grantee === input.role);
    }

    // Group grants by role, then by schema
    if (grants.length > 0) {
      lines.push('## Table Privileges\n');

      const byRole = new Map<string, Map<string, Map<string, string[]>>>();
      for (const g of grants) {
        if (!byRole.has(g.grantee)) byRole.set(g.grantee, new Map());
        const roleMap = byRole.get(g.grantee)!;
        if (!roleMap.has(g.schema_name)) roleMap.set(g.schema_name, new Map());
        const schemaMap = roleMap.get(g.schema_name)!;
        if (!schemaMap.has(g.table_name)) schemaMap.set(g.table_name, []);
        schemaMap.get(g.table_name)!.push(g.privilege);
      }

      for (const [role, schemaMap] of Array.from(byRole.entries()).sort()) {
        lines.push(`### ${role}\n`);

        for (const [schema, tableMap] of Array.from(schemaMap.entries()).sort()) {
          lines.push(`**${schema}**`);
          lines.push('| Table | Privileges |');
          lines.push('|-------|-----------|');

          for (const [table, privs] of Array.from(tableMap.entries()).sort()) {
            lines.push(`| ${table} | ${privs.join(', ')} |`);
          }
          lines.push('');
        }
      }
    }

    lines.push('---');
    lines.push('*Only showing roles/privileges visible to the current database user.*');

    return { content: [{ type: 'text', text: lines.join('\n') }] };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [{ type: 'text', text: `Error listing roles: ${errorMessage}` }],
      isError: true,
    };
  }
}

function yn(val: boolean): string {
  return val ? 'Yes' : 'No';
}
