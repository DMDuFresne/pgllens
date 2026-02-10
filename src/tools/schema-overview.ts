/**
 * Schema Overview tool - Get complete schema as markdown summary
 */

import { generateSchemaOverview } from '../descriptions/generator.js';
import { getTableDescription, getFunctionDescription } from '../descriptions/generator.js';

const MAX_OUTPUT_CHARS = 100_000;

export async function executeSchemaOverviewTool(
  _input: Record<string, never>
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schema = await generateSchemaOverview();
    const lines: string[] = [];

    lines.push('# Schema Overview');
    lines.push('');
    lines.push(`*Refreshed: ${schema.lastRefreshed.toISOString()}*`);
    lines.push('');

    // Tables section
    if (schema.tables.length > 0) {
      lines.push(`## Tables (${schema.tables.length})`);
      lines.push('');
      lines.push('| Schema | Table | Type | Columns | Description |');
      lines.push('|--------|-------|------|---------|-------------|');

      for (const t of schema.tables) {
        const desc = getTableDescription(t).split('\n')[0]; // first line only
        lines.push(
          `| ${t.schemaName} | ${t.tableName} | ${t.tableType} | ${t.columns.length} | ${desc} |`
        );
      }
      lines.push('');
    }

    // Views section
    if (schema.views.length > 0) {
      lines.push(`## Views (${schema.views.length})`);
      lines.push('');
      lines.push('| Schema | View | Columns | Description |');
      lines.push('|--------|------|---------|-------------|');

      for (const v of schema.views) {
        const desc = getTableDescription(v).split('\n')[0];
        lines.push(
          `| ${v.schemaName} | ${v.tableName} | ${v.columns.length} | ${desc} |`
        );
      }
      lines.push('');
    }

    // Functions section
    if (schema.functions.length > 0) {
      lines.push(`## Functions (${schema.functions.length})`);
      lines.push('');
      lines.push('| Schema | Function | Returns | Params | Description |');
      lines.push('|--------|----------|---------|--------|-------------|');

      for (const f of schema.functions) {
        const desc = getFunctionDescription(f).split('\n')[0];
        const paramCount = f.parameters.length;
        lines.push(
          `| ${f.schemaName} | ${f.functionName} | ${f.returnType} | ${paramCount} | ${desc} |`
        );
      }
      lines.push('');
    }

    lines.push('---');
    lines.push('*Use `describe_table` for detailed column info, `get_view_definition` for view SQL.*');

    let output = lines.join('\n');

    if (output.length > MAX_OUTPUT_CHARS) {
      output =
        output.slice(0, MAX_OUTPUT_CHARS) +
        '\n\n--- OUTPUT TRUNCATED ---\n' +
        'Use `list_tables` with a schema filter to narrow results.';
    }

    return {
      content: [{ type: 'text', text: output }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error generating schema overview: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
