/**
 * Get View Definition tool - Full SQL source and column listing for views
 */

import { rawQuery } from '../database/client.js';
import { getDefaultSchema } from '../config.js';

interface GetViewDefinitionToolInput {
  schema?: string;
  view: string;
}

export async function executeGetViewDefinitionTool(
  input: GetViewDefinitionToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();

    // Get view definition and type
    const viewSql = `
      SELECT
        c.relkind,
        pg_get_viewdef(c.oid, true) AS definition,
        obj_description(c.oid, 'pg_class') AS comment
      FROM pg_class c
      JOIN pg_namespace n ON c.relnamespace = n.oid
      WHERE n.nspname = $1
        AND c.relname = $2
        AND c.relkind IN ('v', 'm')
    `;

    const viewResult = await rawQuery<{
      relkind: string;
      definition: string | null;
      comment: string | null;
    }>(viewSql, [schemaName, input.view]);

    if (viewResult.rows.length === 0) {
      return {
        content: [
          {
            type: 'text',
            text: `View ${schemaName}.${input.view} not found. Check the name and schema.`,
          },
        ],
        isError: true,
      };
    }

    const view = viewResult.rows[0];
    const viewType = view.relkind === 'm' ? 'MATERIALIZED VIEW' : 'VIEW';

    // Get column list
    const columnsSql = `
      SELECT
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS data_type,
        col_description(a.attrelid, a.attnum) AS comment
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
      WHERE n.nspname = $1
        AND c.relname = $2
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum
    `;

    const columnsResult = await rawQuery<{
      column_name: string;
      data_type: string;
      comment: string | null;
    }>(columnsSql, [schemaName, input.view]);

    // Build output
    const lines: string[] = [];
    lines.push(`## ${viewType}: ${schemaName}.${input.view}`);
    lines.push('');

    if (view.comment) {
      lines.push(`**Description**: ${view.comment}`);
      lines.push('');
    }

    // Column table
    if (columnsResult.rows.length > 0) {
      lines.push('### Columns');
      lines.push('| Column | Type | Comment |');
      lines.push('|--------|------|---------|');
      for (const col of columnsResult.rows) {
        lines.push(
          `| ${col.column_name} | ${col.data_type} | ${col.comment || '—'} |`
        );
      }
      lines.push('');
    }

    // Full SQL definition
    lines.push('### SQL Definition');
    lines.push('```sql');
    lines.push(view.definition || '-- Definition not available');
    lines.push('```');

    return { content: [{ type: 'text', text: lines.join('\n') }] };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';
    return {
      content: [
        { type: 'text', text: `Error getting view definition: ${errorMessage}` },
      ],
      isError: true,
    };
  }
}
