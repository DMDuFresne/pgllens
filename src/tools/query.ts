/**
 * Query tool - Execute read-only SQL queries
 */

import { query as executeQuery } from '../database/client.js';

interface QueryToolInput {
  sql: string;
}

export async function executeQueryTool(
  input: QueryToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const result = await executeQuery(input.sql);

    const text = formatQueryResult(result);

    return {
      content: [{ type: 'text', text }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error executing query: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}

function formatQueryResult(result: {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
}): string {
  const lines: string[] = [];

  if (result.rows.length === 0) {
    lines.push('Query returned 0 rows.');
    return lines.join('\n');
  }

  // Header
  lines.push('| ' + result.columns.join(' | ') + ' |');
  lines.push('|' + result.columns.map(() => '---').join('|') + '|');

  // Rows
  for (const row of result.rows) {
    const cells = result.columns.map(col => escapeCell(row[col]));
    lines.push('| ' + cells.join(' | ') + ' |');
  }

  lines.push('');
  lines.push(`*${result.rowCount} row(s)*`);

  if (result.truncated) {
    lines.push('');
    lines.push('> Results truncated. Add LIMIT to your query for better control.');
  }

  return lines.join('\n');
}

function escapeCell(val: unknown): string {
  if (val === null || val === undefined) return 'NULL';
  const s = typeof val === 'object' ? JSON.stringify(val) : String(val);
  return s.replace(/\|/g, '\\|').replace(/\n/g, ' ');
}
