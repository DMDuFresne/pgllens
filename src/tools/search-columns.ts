/**
 * Search Columns tool - Find columns by name pattern across all tables
 */

import { getSchemaMetadata } from '../database/schema-loader.js';

interface SearchColumnsToolInput {
  pattern: string;
  schema?: string;
}

export async function executeSearchColumnsTool(
  input: SearchColumnsToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schema = await getSchemaMetadata();
    const pattern = input.pattern.toLowerCase();

    const allTables = [...schema.tables, ...schema.views];
    const matches: Array<{
      schema: string;
      table: string;
      tableType: string;
      column: string;
      dataType: string;
      isPrimaryKey: boolean;
      isForeignKey: boolean;
      foreignKeyRef: string | null;
      comment: string | null;
    }> = [];

    for (const table of allTables) {
      // Filter by schema if specified
      if (input.schema && table.schemaName.toLowerCase() !== input.schema.toLowerCase()) {
        continue;
      }

      for (const col of table.columns) {
        if (col.columnName.toLowerCase().includes(pattern)) {
          matches.push({
            schema: table.schemaName,
            table: table.tableName,
            tableType: table.tableType,
            column: col.columnName,
            dataType: col.dataType,
            isPrimaryKey: col.isPrimaryKey,
            isForeignKey: col.isForeignKey,
            foreignKeyRef: col.isForeignKey
              ? `${col.foreignKeyTable}.${col.foreignKeyColumn}`
              : null,
            comment: col.comment,
          });
        }
      }
    }

    // Sort by schema, table, column
    matches.sort((a, b) => {
      if (a.schema !== b.schema) return a.schema.localeCompare(b.schema);
      if (a.table !== b.table) return a.table.localeCompare(b.table);
      return a.column.localeCompare(b.column);
    });

    if (matches.length === 0) {
      return {
        content: [
          {
            type: 'text',
            text: `No columns found matching pattern "${input.pattern}"`,
          },
        ],
      };
    }

    // Format output
    const lines: string[] = [];
    lines.push(`Found ${matches.length} columns matching "${input.pattern}":\n`);

    let currentTable = '';
    for (const match of matches) {
      const tableKey = `${match.schema}.${match.table}`;
      if (tableKey !== currentTable) {
        currentTable = tableKey;
        lines.push(`\n### ${tableKey} (${match.tableType})`);
      }

      let line = `- **${match.column}**: ${match.dataType}`;
      const flags: string[] = [];
      if (match.isPrimaryKey) flags.push('PK');
      if (match.isForeignKey) flags.push(`FK → ${match.foreignKeyRef}`);
      if (flags.length > 0) line += ` [${flags.join(', ')}]`;
      if (match.comment) line += `\n  *${match.comment}*`;

      lines.push(line);
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
          text: `Error searching columns: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}
