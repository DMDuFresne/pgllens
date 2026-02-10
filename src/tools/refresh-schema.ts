/**
 * Refresh Schema tool - Force reload of schema metadata cache
 */

import { forceRefreshSchema } from '../database/schema-loader.js';

export async function executeRefreshSchemaTool(
  _input: Record<string, never>
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const startTime = Date.now();
    const summary = await forceRefreshSchema();
    const elapsed = Date.now() - startTime;

    const lines: string[] = [
      '# Schema Refresh Complete',
      '',
      `Refresh time: ${elapsed}ms`,
      '',
      '## Summary',
      `- **Tables**: ${summary.tables}`,
      `- **Views**: ${summary.views}`,
      `- **Functions**: ${summary.functions}`,
      '',
      '## Database Context',
      `- Database comment: ${summary.hasDatabaseComment ? 'Yes' : 'No'}`,
      `- Schemas with comments: ${summary.schemasWithComments}`,
      '',
      '## Ontology',
      `- Check constraints: ${summary.checkConstraints}`,
      `- Enum types: ${summary.enumTypes}`,
      `- Unique constraints: ${summary.uniqueConstraints}`,
      `- Indexes: ${summary.indexes}`,
      `- Triggers: ${summary.triggers}`,
      '',
      '> **Note:** This refreshed the MCP server\'s metadata cache only. Row count estimates',
      '> in `get_ontology` come from PostgreSQL statistics — run `ANALYZE schema.table`',
      '> to update those.',
    ];

    return {
      content: [{ type: 'text', text: lines.join('\n') }],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error refreshing schema: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
