/**
 * Get Sample Data tool - Return sample rows from a table
 */

import { query } from '../database/client.js';
import { getTable } from '../database/schema-loader.js';
import { getDefaultSchema } from '../config.js';

interface GetSampleDataToolInput {
  schema?: string;
  table: string;
  limit?: number;
}

export async function executeGetSampleDataTool(
  input: GetSampleDataToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();
    const limit = input.limit || 5;

    // Verify table exists
    const tableInfo = await getTable(schemaName, input.table);
    if (!tableInfo) {
      return {
        content: [
          {
            type: 'text',
            text: `Table ${schemaName}.${input.table} not found.`,
          },
        ],
        isError: true,
      };
    }

    // Determine ordering - use logged_at for log tables, otherwise random
    const hasLoggedAt = tableInfo.columns.some(c => c.columnName === 'logged_at');
    const hasCreatedAt = tableInfo.columns.some(c => c.columnName === 'created_at');

    let orderClause: string;
    if (hasLoggedAt) {
      orderClause = 'ORDER BY logged_at DESC';
    } else if (hasCreatedAt) {
      orderClause = 'ORDER BY created_at DESC';
    } else {
      orderClause = 'ORDER BY RANDOM()';
    }

    // Build and execute query
    const sql = `SELECT * FROM ${schemaName}.${input.table} ${orderClause} LIMIT ${limit}`;
    const result = await query(sql);

    const output = {
      table: `${schemaName}.${input.table}`,
      sampleSize: result.rowCount,
      columns: result.columns,
      rows: result.rows,
    };

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(output, null, 2),
        },
      ],
    };
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error getting sample data: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
