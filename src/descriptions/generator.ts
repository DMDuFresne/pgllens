/**
 * Description generator module
 * Combines static descriptions with dynamic schema data and domain context
 */

import { getSchemaMetadata, getTable, getDatabaseContext } from '../database/schema-loader.js';
import { getConfig } from '../config.js';
import { TableInfo, FunctionInfo, SchemaMetadata } from '../types/index.js';
import {
  TABLE_DESCRIPTIONS,
  FUNCTION_DESCRIPTIONS,
  QUERY_TOOL_DESCRIPTION,
  DOMAIN_CONCEPTS,
  SCHEMA_OVERVIEW,
} from './static.js';

/**
 * Get the domain context from configuration
 * This is loaded once at startup and cached
 */
let cachedDomainContext: string | null = null;

export function getDomainContext(): string {
  if (cachedDomainContext === null) {
    const config = getConfig();
    cachedDomainContext = config.domainContext || '';
  }
  return cachedDomainContext;
}

/**
 * Get enhanced description for a table, combining database comment with static description
 */
export function getTableDescription(table: TableInfo): string {
  const key = `${table.schemaName}.${table.tableName}`;
  const staticDesc = TABLE_DESCRIPTIONS[key];
  const dbComment = table.comment;

  if (staticDesc && dbComment) {
    return `${staticDesc}\n\nDatabase comment: ${dbComment}`;
  }

  return staticDesc || dbComment || 'No description available.';
}

/**
 * Get enhanced description for a function
 */
export function getFunctionDescription(func: FunctionInfo): string {
  const key = `${func.schemaName}.${func.functionName}`;
  const staticDesc = FUNCTION_DESCRIPTIONS[key];
  const dbComment = func.comment;

  if (staticDesc && dbComment) {
    return `${staticDesc}\n\nDatabase comment: ${dbComment}`;
  }

  return staticDesc || dbComment || 'No description available.';
}

/**
 * Format column information for display
 */
export function formatColumnInfo(table: TableInfo): string {
  const lines: string[] = [];

  for (const col of table.columns) {
    let line = `  - ${col.columnName}: ${col.dataType}`;

    const modifiers: string[] = [];
    if (col.isPrimaryKey) modifiers.push('PK');
    if (col.isForeignKey) {
      modifiers.push(`FK → ${col.foreignKeyTable}.${col.foreignKeyColumn}`);
    }
    if (!col.isNullable) modifiers.push('NOT NULL');
    if (col.columnDefault) modifiers.push(`DEFAULT: ${col.columnDefault}`);

    if (modifiers.length > 0) {
      line += ` [${modifiers.join(', ')}]`;
    }

    if (col.comment) {
      line += `\n    "${col.comment}"`;
    }

    lines.push(line);
  }

  return lines.join('\n');
}

/**
 * Generate table list description with schema comments
 */
export async function generateTableListDescription(
  schemaFilter?: string
): Promise<string> {
  const schema = await getSchemaMetadata();
  const dbContext = await getDatabaseContext();
  let tables = [...schema.tables, ...schema.views];

  if (schemaFilter) {
    tables = tables.filter(
      t => t.schemaName.toLowerCase() === schemaFilter.toLowerCase()
    );
  }

  const grouped = new Map<string, TableInfo[]>();
  for (const table of tables) {
    if (!grouped.has(table.schemaName)) {
      grouped.set(table.schemaName, []);
    }
    grouped.get(table.schemaName)!.push(table);
  }

  const lines: string[] = [];

  // Add database context if available
  if (dbContext.databaseComment) {
    lines.push(`# Database: ${dbContext.databaseName}`);
    lines.push(dbContext.databaseComment);
    lines.push('');
  }

  for (const [schemaName, schemaTables] of grouped) {
    lines.push(`\n## Schema: ${schemaName}`);

    // Add schema comment if available
    const schemaComment = dbContext.schemaComments[schemaName];
    if (schemaComment) {
      lines.push(`*${schemaComment}*`);
    }
    lines.push('');

    const tableList = schemaTables.filter(t => t.tableType === 'table');
    const viewList = schemaTables.filter(t => t.tableType === 'view');

    if (tableList.length > 0) {
      lines.push('### Tables');
      for (const table of tableList) {
        const desc = getTableDescription(table);
        lines.push(`- **${table.tableName}**: ${desc.split('\n')[0]}`);
      }
      lines.push('');
    }

    if (viewList.length > 0) {
      lines.push('### Views');
      for (const view of viewList) {
        const desc = getTableDescription(view);
        lines.push(`- **${view.tableName}**: ${desc.split('\n')[0]}`);
      }
      lines.push('');
    }
  }

  return lines.join('\n');
}

/**
 * Generate detailed table description
 */
export async function generateTableDescription(
  schemaName: string,
  tableName: string
): Promise<string> {
  const table = await getTable(schemaName, tableName);

  if (!table) {
    return `Table ${schemaName}.${tableName} not found.`;
  }

  const lines: string[] = [];

  lines.push(`# ${table.schemaName}.${table.tableName}`);
  lines.push(`Type: ${table.tableType.toUpperCase()}`);
  lines.push('');
  lines.push('## Description');
  lines.push(getTableDescription(table));
  lines.push('');
  lines.push('## Columns');
  lines.push(formatColumnInfo(table));

  return lines.join('\n');
}

/**
 * Generate complete schema overview as JSON
 */
export async function generateSchemaOverview(): Promise<SchemaMetadata> {
  return await getSchemaMetadata();
}

/**
 * Generate the complete query tool description with domain context
 */
export function getQueryToolDescription(): string {
  const domainContext = getDomainContext();

  if (domainContext) {
    return `${domainContext}\n\n${QUERY_TOOL_DESCRIPTION}`;
  }

  // Use static content if available, otherwise just base description
  if (DOMAIN_CONCEPTS || SCHEMA_OVERVIEW) {
    return `${DOMAIN_CONCEPTS}\n\n${SCHEMA_OVERVIEW}\n\n${QUERY_TOOL_DESCRIPTION}`;
  }

  return QUERY_TOOL_DESCRIPTION;
}

/**
 * Generate list_tables tool description
 */
export function getListTablesToolDescription(): string {
  const domainContext = getDomainContext();
  const base = `List all tables and views in the database with their descriptions.

Returns table names organized by schema with:
- Table/view names and types
- Database comments describing each object
- Schema-level descriptions when available

Optionally filter by schema name.`;

  if (domainContext) {
    return `${base}\n\n${domainContext}`;
  }

  if (DOMAIN_CONCEPTS) {
    return `${base}\n\n${DOMAIN_CONCEPTS}`;
  }

  return base;
}

/**
 * Generate describe_table tool description
 */
export function getDescribeTableToolDescription(): string {
  const base = `Get detailed information about a specific table or view.

Returns:
- Column names and data types
- Primary key and foreign key constraints
- Nullability and default values
- Column-level comments/descriptions
- Foreign key relationships to other tables`;

  if (SCHEMA_OVERVIEW) {
    return `${base}\n\n${SCHEMA_OVERVIEW}`;
  }

  return base;
}

/**
 * Generate schema_overview tool description
 */
export function getSchemaOverviewToolDescription(): string {
  const domainContext = getDomainContext();
  const base = `Get complete schema metadata as structured JSON.

Returns all tables, views, functions with their columns, parameters, and descriptions.

Useful for understanding the database structure programmatically.`;

  if (domainContext) {
    return `${base}\n\n${domainContext}`;
  }

  if (DOMAIN_CONCEPTS) {
    return `${base}\n\n${DOMAIN_CONCEPTS}`;
  }

  return base;
}
