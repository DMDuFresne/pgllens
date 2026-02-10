/**
 * List Extensions tool - Shows installed PostgreSQL extensions
 */

import { rawQuery } from '../database/client.js';

interface ListExtensionsToolInput {
  name?: string;
}

export async function executeListExtensionsTool(
  input: ListExtensionsToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const params: unknown[] = [];
    let filterClause = '';

    if (input.name) {
      params.push(input.name);
      filterClause = `WHERE e.extname ILIKE '%' || $1 || '%'`;
    }

    const sql = `
      SELECT
        e.extname AS name,
        e.extversion AS installed_version,
        a.default_version AS available_version,
        n.nspname AS schema,
        a.comment AS description
      FROM pg_extension e
      JOIN pg_namespace n ON e.extnamespace = n.oid
      LEFT JOIN pg_available_extensions a ON e.extname = a.name
      ${filterClause}
      ORDER BY e.extname
    `;

    const result = await rawQuery<{
      name: string;
      installed_version: string;
      available_version: string | null;
      schema: string;
      description: string | null;
    }>(sql, params);

    if (result.rows.length === 0) {
      const msg = input.name
        ? `No extensions found matching "${input.name}".`
        : 'No extensions installed.';
      return { content: [{ type: 'text', text: msg }] };
    }

    const lines: string[] = [];
    lines.push('## Installed Extensions\n');
    lines.push('| Extension | Installed | Available | Schema | Description |');
    lines.push('|-----------|-----------|-----------|--------|-------------|');

    for (const ext of result.rows) {
      const upgrade =
        ext.available_version && ext.available_version !== ext.installed_version
          ? ` ⬆ ${ext.available_version}`
          : '';
      lines.push(
        `| ${ext.name} | ${ext.installed_version} | ${ext.available_version || '—'}${upgrade ? ` ${upgrade}` : ''} | ${ext.schema} | ${ext.description || '—'} |`
      );
    }

    lines.push('');
    lines.push(`*${result.rows.length} extension(s) installed.*`);

    return { content: [{ type: 'text', text: lines.join('\n') }] };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [{ type: 'text', text: `Error listing extensions: ${errorMessage}` }],
      isError: true,
    };
  }
}
