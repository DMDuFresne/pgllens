/**
 * Explain Query tool - Run EXPLAIN ANALYZE on a query
 */

import { rawQuery } from '../database/client.js';
import { containsBlockedKeywords } from '../database/client.js';

interface ExplainQueryToolInput {
  sql: string;
  analyze?: boolean;
}

export async function executeExplainQueryTool(
  input: ExplainQueryToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    // Security check
    const blockedKeyword = containsBlockedKeywords(input.sql);
    if (blockedKeyword) {
      return {
        content: [
          {
            type: 'text',
            text: `Query contains blocked keyword: ${blockedKeyword}. Only SELECT queries are allowed.`,
          },
        ],
        isError: true,
      };
    }

    // Build EXPLAIN command
    const explainType = input.analyze ? 'EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)' : 'EXPLAIN (FORMAT TEXT)';
    const explainSql = `${explainType} ${input.sql}`;

    const result = await rawQuery<{ 'QUERY PLAN': string }>(explainSql);

    const plan = result.rows.map(r => r['QUERY PLAN']).join('\n');

    const lines: string[] = [];
    lines.push(`## Query Execution Plan${input.analyze ? ' (with ANALYZE)' : ''}`);
    lines.push('');
    lines.push('```');
    lines.push(plan);
    lines.push('```');

    if (input.analyze) {
      lines.push('');
      lines.push('*Note: ANALYZE actually executed the query to measure real timings.*');
    }

    // Add helpful tips based on plan content
    const tips: string[] = [];
    if (plan.includes('Seq Scan')) {
      tips.push('- **Seq Scan detected**: Consider adding an index if this table is large');
    }
    if (plan.includes('Sort') && !plan.includes('Index Scan')) {
      tips.push('- **Sort without index**: Consider adding an index on the ORDER BY columns');
    }
    if (plan.includes('Nested Loop') && plan.includes('rows=')) {
      tips.push('- **Nested Loop**: Check if JOIN conditions have proper indexes');
    }

    if (tips.length > 0) {
      lines.push('');
      lines.push('### Optimization Tips');
      lines.push(...tips);
    }

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
          text: `Error explaining query: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
