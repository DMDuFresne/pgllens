/**
 * Get Relationships tool - Show FK relationships for a table or full schema
 */

import { getSchemaMetadata, getTable } from '../database/schema-loader.js';
import { getDefaultSchema } from '../config.js';

interface GetRelationshipsToolInput {
  table?: string;
  schema?: string;
  format?: 'text' | 'mermaid';
}

interface Relationship {
  fromSchema: string;
  fromTable: string;
  fromColumn: string;
  toSchema: string;
  toTable: string;
  toColumn: string;
}

export async function executeGetRelationshipsTool(
  input: GetRelationshipsToolInput
): Promise<{ content: Array<{ type: 'text'; text: string }>; isError?: boolean }> {
  try {
    const schemaName = input.schema || getDefaultSchema();
    const format = input.format || 'text';
    const metadata = await getSchemaMetadata();

    // Collect all relationships
    const relationships: Relationship[] = [];
    const allTables = [...metadata.tables, ...metadata.views];

    for (const table of allTables) {
      if (table.schemaName.toLowerCase() !== schemaName.toLowerCase()) continue;

      for (const col of table.columns) {
        if (col.isForeignKey && col.foreignKeyTable && col.foreignKeyColumn) {
          // Parse schema.table format
          const [fkSchema, fkTable] = col.foreignKeyTable.includes('.')
            ? col.foreignKeyTable.split('.')
            : [schemaName, col.foreignKeyTable];

          relationships.push({
            fromSchema: table.schemaName,
            fromTable: table.tableName,
            fromColumn: col.columnName,
            toSchema: fkSchema,
            toTable: fkTable,
            toColumn: col.foreignKeyColumn,
          });
        }
      }
    }

    // Filter for specific table if requested
    let filtered = relationships;
    if (input.table) {
      const tableLower = input.table.toLowerCase();
      filtered = relationships.filter(
        r =>
          r.fromTable.toLowerCase() === tableLower ||
          r.toTable.toLowerCase() === tableLower
      );

      if (filtered.length === 0) {
        // Check if table exists
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
        return {
          content: [
            {
              type: 'text',
              text: `Table ${schemaName}.${input.table} has no foreign key relationships.`,
            },
          ],
        };
      }
    }

    if (format === 'mermaid') {
      return {
        content: [{ type: 'text', text: generateMermaidDiagram(filtered, input.table) }],
      };
    } else {
      return {
        content: [{ type: 'text', text: generateTextOutput(filtered, input.table) }],
      };
    }
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred';

    return {
      content: [
        {
          type: 'text',
          text: `Error getting relationships: ${errorMessage}`,
        },
      ],
      isError: true,
    };
  }
}

function generateTextOutput(relationships: Relationship[], targetTable?: string): string {
  const lines: string[] = [];

  if (targetTable) {
    lines.push(`## Relationships for ${targetTable}\n`);

    const outgoing = relationships.filter(
      r => r.fromTable.toLowerCase() === targetTable.toLowerCase()
    );
    const incoming = relationships.filter(
      r => r.toTable.toLowerCase() === targetTable.toLowerCase()
    );

    if (outgoing.length > 0) {
      lines.push('### Outgoing (this table references)');
      for (const rel of outgoing) {
        lines.push(`- **${rel.fromColumn}** → ${rel.toTable}.${rel.toColumn}`);
      }
      lines.push('');
    }

    if (incoming.length > 0) {
      lines.push('### Incoming (referenced by)');
      for (const rel of incoming) {
        lines.push(`- ${rel.fromTable}.**${rel.fromColumn}** → this.${rel.toColumn}`);
      }
      lines.push('');
    }
  } else {
    lines.push('## All Foreign Key Relationships\n');

    // Group by from table
    const grouped = new Map<string, Relationship[]>();
    for (const rel of relationships) {
      if (!grouped.has(rel.fromTable)) {
        grouped.set(rel.fromTable, []);
      }
      grouped.get(rel.fromTable)!.push(rel);
    }

    for (const [table, rels] of Array.from(grouped.entries()).sort()) {
      lines.push(`### ${table}`);
      for (const rel of rels) {
        lines.push(`- ${rel.fromColumn} → ${rel.toTable}.${rel.toColumn}`);
      }
      lines.push('');
    }
  }

  return lines.join('\n');
}

function generateMermaidDiagram(relationships: Relationship[], targetTable?: string): string {
  const lines: string[] = [];
  lines.push('```mermaid');
  lines.push('erDiagram');

  // Collect unique tables
  const tables = new Set<string>();
  for (const rel of relationships) {
    tables.add(rel.fromTable);
    tables.add(rel.toTable);
  }

  // Add relationships
  for (const rel of relationships) {
    // Mermaid ER syntax: TABLE1 ||--o{ TABLE2 : "relationship"
    // ||--o{ means one-to-many (most FK relationships)
    lines.push(`    ${rel.toTable} ||--o{ ${rel.fromTable} : "${rel.fromColumn}"`);
  }

  lines.push('```');

  if (targetTable) {
    lines.push(`\n*Showing relationships for ${targetTable}*`);
  } else {
    lines.push(`\n*Showing ${relationships.length} relationships across ${tables.size} tables*`);
  }

  return lines.join('\n');
}
