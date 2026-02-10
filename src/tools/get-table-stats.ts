/**
 * Get Table Stats tool - Row counts, null percentages, distinct values
 */

import { query, rawQuery } from '../database/client.js';
import { getTable } from '../database/schema-loader.js';
import { getDefaultSchema } from '../config.js';

interface GetTableStatsToolInput {
  schema?: string;
  table: string;
}

export async function executeGetTableStatsTool(
  input: GetTableStatsToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();

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

    // Get row count
    const countResult = await query(
      `SELECT COUNT(*) as total FROM ${schemaName}.${input.table}`
    );
    const totalRows = parseInt(countResult.rows[0]?.total as string, 10) || 0;

    // Empty table: show column listing with zero stats
    if (totalRows === 0) {
      const lines: string[] = [];
      lines.push(`## Statistics for ${schemaName}.${input.table}\n`);
      lines.push('**Total Rows:** 0\n');
      lines.push('### Column Statistics\n');
      lines.push('| Column | Type | Nulls | Null % | Distinct |');
      lines.push('|--------|------|-------|--------|----------|');
      for (const col of tableInfo.columns) {
        lines.push(`| ${col.columnName} | ${col.dataType} | 0 | 0% | 0 |`);
      }
      return {
        content: [{ type: 'text', text: lines.join('\n') }],
      };
    }

    // Batch column statistics with a single UNION ALL query
    const qualifiedTable = `${schemaName}.${input.table}`;
    const columnStats = await batchColumnStats(qualifiedTable, tableInfo.columns, totalRows);

    // Format output
    const lines: string[] = [];
    lines.push(`## Statistics for ${schemaName}.${input.table}\n`);
    lines.push(`**Total Rows:** ${totalRows.toLocaleString()}\n`);
    lines.push('### Column Statistics\n');
    lines.push('| Column | Type | Nulls | Null % | Distinct |');
    lines.push('|--------|------|-------|--------|----------|');

    for (const stat of columnStats) {
      const nullStr = stat.nullCount >= 0 ? stat.nullCount.toLocaleString() : 'N/A';
      const nullPctStr = stat.nullPercent >= 0 ? `${stat.nullPercent}%` : 'N/A';
      const distinctStr = stat.distinctCount >= 0 ? stat.distinctCount.toLocaleString() : 'N/A';

      lines.push(
        `| ${stat.column} | ${stat.dataType} | ${nullStr} | ${nullPctStr} | ${distinctStr} |`
      );
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
          text: `Error getting table stats: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}

interface ColumnStat {
  column: string;
  dataType: string;
  nullCount: number;
  nullPercent: number;
  distinctCount: number;
}

/**
 * Batch all column stats into a single UNION ALL query.
 * Falls back to per-column queries if the batch fails (e.g., json columns).
 */
async function batchColumnStats(
  qualifiedTable: string,
  columns: Array<{ columnName: string; dataType: string }>,
  totalRows: number
): Promise<ColumnStat[]> {
  try {
    // Each SELECT needs its own FROM clause in PostgreSQL UNION ALL
    const selectsWithFrom = columns.map(col => {
      const quoted = quoteIdentifier(col.columnName);
      return `SELECT '${col.columnName.replace(/'/g, "''")}' AS col, COUNT(*) - COUNT(${quoted}) AS null_count, COUNT(DISTINCT ${quoted}) AS distinct_count FROM ${qualifiedTable}`;
    });

    const unionSql = selectsWithFrom.join('\nUNION ALL\n');

    const result = await rawQuery<{
      col: string;
      null_count: string;
      distinct_count: string;
    }>(unionSql);

    // Map results back to column order
    const resultMap = new Map<string, { null_count: string; distinct_count: string }>();
    for (const row of result.rows) {
      resultMap.set(row.col, row);
    }

    return columns.map(col => {
      const r = resultMap.get(col.columnName);
      const nullCount = parseInt(r?.null_count || '0', 10);
      const distinctCount = parseInt(r?.distinct_count || '0', 10);
      return {
        column: col.columnName,
        dataType: col.dataType,
        nullCount,
        nullPercent: Math.round((nullCount / totalRows) * 100 * 10) / 10,
        distinctCount,
      };
    });
  } catch {
    // Fallback: per-column queries (handles json/xml columns that don't support COUNT DISTINCT)
    return perColumnStats(qualifiedTable, columns, totalRows);
  }
}

async function perColumnStats(
  qualifiedTable: string,
  columns: Array<{ columnName: string; dataType: string }>,
  totalRows: number
): Promise<ColumnStat[]> {
  const stats: ColumnStat[] = [];

  for (const col of columns) {
    try {
      const statsQuery = `
        SELECT
          COUNT(*) - COUNT(${quoteIdentifier(col.columnName)}) as null_count,
          COUNT(DISTINCT ${quoteIdentifier(col.columnName)}) as distinct_count
        FROM ${qualifiedTable}
      `;

      const statsResult = await rawQuery<{
        null_count: string;
        distinct_count: string;
      }>(statsQuery);

      const nullCount = parseInt(statsResult.rows[0]?.null_count || '0', 10);
      const distinctCount = parseInt(statsResult.rows[0]?.distinct_count || '0', 10);

      stats.push({
        column: col.columnName,
        dataType: col.dataType,
        nullCount,
        nullPercent: Math.round((nullCount / totalRows) * 100 * 10) / 10,
        distinctCount,
      });
    } catch {
      stats.push({
        column: col.columnName,
        dataType: col.dataType,
        nullCount: -1,
        nullPercent: -1,
        distinctCount: -1,
      });
    }
  }

  return stats;
}

function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
