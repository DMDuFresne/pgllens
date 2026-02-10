/**
 * Type definitions for the PostgreSQL MCP Server
 */

/**
 * Column information from database introspection
 */
export interface ColumnInfo {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  columnDefault: string | null;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  foreignKeyTable: string | null;
  foreignKeyColumn: string | null;
  comment: string | null;
  ordinalPosition: number;
}

/**
 * Table or view information from database introspection
 */
export interface TableInfo {
  schemaName: string;
  tableName: string;
  tableType: 'table' | 'view';
  comment: string | null;
  columns: ColumnInfo[];
  rowCount?: number;
}

/**
 * Function parameter information
 */
export interface FunctionParameter {
  name: string;
  dataType: string;
  mode: 'IN' | 'OUT' | 'INOUT' | 'VARIADIC';
  defaultValue: string | null;
}

/**
 * Function information from database introspection
 */
export interface FunctionInfo {
  schemaName: string;
  functionName: string;
  returnType: string;
  parameters: FunctionParameter[];
  comment: string | null;
  isVolatile: boolean;
}

/**
 * Complete schema metadata
 */
export interface SchemaMetadata {
  tables: TableInfo[];
  views: TableInfo[];
  functions: FunctionInfo[];
  lastRefreshed: Date;
}

/**
 * Database and schema level context from PostgreSQL comments
 */
export interface DatabaseContext {
  databaseName: string;
  databaseComment: string | null;
  schemaComments: Record<string, string>;
}

/**
 * Query result with metadata
 */
export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  truncated: boolean;
}

/**
 * Tool execution result
 */
export interface ToolResult {
  success: boolean;
  data?: unknown;
  error?: string;
}

/**
 * Dangerous SQL statement-level keywords that should be blocked.
 * Defense-in-depth only — the database connection uses BEGIN READ ONLY.
 * Kept minimal to avoid false positives on legitimate SELECT queries
 * that contain these words in string literals or column names.
 */
export const BLOCKED_KEYWORDS = [
  'INSERT',
  'UPDATE',
  'DELETE',
  'DROP',
  'CREATE',
  'ALTER',
  'TRUNCATE',
  'GRANT',
  'REVOKE',
  'COPY',
] as const;

export type BlockedKeyword = typeof BLOCKED_KEYWORDS[number];

// ─── Ontology Types ─────────────────────────────────────────────────────────

/**
 * Check constraint extracted from pg_constraint
 */
export interface CheckConstraintInfo {
  constraintName: string;
  schemaName: string;
  tableName: string;
  expression: string;
  columns: string[];
}

/**
 * Custom enum type extracted from pg_type + pg_enum
 */
export interface EnumTypeInfo {
  schemaName: string;
  typeName: string;
  values: string[];
  comment: string | null;
}

/**
 * Unique constraint (natural keys beyond PKs) from pg_constraint
 */
export interface UniqueConstraintInfo {
  constraintName: string;
  schemaName: string;
  tableName: string;
  columns: string[];
}

/**
 * View definition (SQL) from pg_get_viewdef
 */
export interface ViewDefinitionInfo {
  schemaName: string;
  viewName: string;
  definition: string;
  comment: string | null;
}

/**
 * Index information from pg_index + pg_class + pg_am
 */
export interface IndexInfo {
  schemaName: string;
  tableName: string;
  indexName: string;
  columns: string[];
  isUnique: boolean;
  isPrimary: boolean;
  indexType: string;
  definition: string;
}

/**
 * Trigger information from pg_trigger with tgtype bitmask decoded
 */
export interface TriggerInfo {
  schemaName: string;
  tableName: string;
  triggerName: string;
  timing: string;
  events: string[];
  orientation: string;
  functionName: string;
  comment: string | null;
}

/**
 * Estimated row count from pg_class.reltuples
 */
export interface EstimatedRowCount {
  schemaName: string;
  tableName: string;
  estimatedRows: number;
  neverAnalyzed: boolean;
}

/**
 * Combined ontology metadata — semantic understanding of the database
 */
export interface OntologyMetadata {
  checkConstraints: CheckConstraintInfo[];
  enumTypes: EnumTypeInfo[];
  uniqueConstraints: UniqueConstraintInfo[];
  viewDefinitions: ViewDefinitionInfo[];
  indexes: IndexInfo[];
  triggers: TriggerInfo[];
  estimatedRowCounts: EstimatedRowCount[];
  lastRefreshed: Date;
}
