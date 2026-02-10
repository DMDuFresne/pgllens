/**
 * Schema introspection module
 * Queries PostgreSQL catalog to discover tables, columns, functions,
 * and database/schema-level comments for self-documentation
 */

import { rawQuery } from './client.js';
import { getConfig } from '../config.js';
import {
  TableInfo,
  ColumnInfo,
  FunctionInfo,
  FunctionParameter,
  SchemaMetadata,
  DatabaseContext,
  CheckConstraintInfo,
  EnumTypeInfo,
  UniqueConstraintInfo,
  ViewDefinitionInfo,
  IndexInfo,
  TriggerInfo,
  EstimatedRowCount,
  OntologyMetadata,
} from '../types/index.js';

let cachedSchema: SchemaMetadata | null = null;
let cachedDatabaseContext: DatabaseContext | null = null;
let cachedOntology: OntologyMetadata | null = null;
let lastRefreshTime: number = 0;

/**
 * Load table and view information from the database
 */
async function loadTablesAndViews(): Promise<{ tables: TableInfo[]; views: TableInfo[] }> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  // Query tables and views
  const tablesQuery = `
    SELECT
      t.table_schema,
      t.table_name,
      t.table_type,
      obj_description((t.table_schema || '.' || t.table_name)::regclass) as table_comment
    FROM information_schema.tables t
    WHERE t.table_schema IN (${schemaPlaceholders})
      AND t.table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY t.table_schema, t.table_name
  `;

  const tablesResult = await rawQuery<{
    table_schema: string;
    table_name: string;
    table_type: string;
    table_comment: string | null;
  }>(tablesQuery, schemas);

  // Query columns for all tables
  const columnsQuery = `
    SELECT
      c.table_schema,
      c.table_name,
      c.column_name,
      c.data_type,
      c.is_nullable,
      c.column_default,
      c.ordinal_position,
      col_description((c.table_schema || '.' || c.table_name)::regclass, c.ordinal_position) as column_comment,
      CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
      CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
      fk.foreign_table_schema || '.' || fk.foreign_table_name as fk_table,
      fk.foreign_column_name as fk_column
    FROM information_schema.columns c
    LEFT JOIN (
      SELECT
        n.nspname AS table_schema,
        cl.relname AS table_name,
        a.attname AS column_name
      FROM pg_constraint con
      JOIN pg_class cl ON con.conrelid = cl.oid
      JOIN pg_namespace n ON cl.relnamespace = n.oid
      JOIN pg_attribute a ON a.attrelid = con.conrelid
        AND a.attnum = ANY(con.conkey)
      WHERE con.contype = 'p'
    ) pk ON c.table_schema = pk.table_schema
        AND c.table_name = pk.table_name
        AND c.column_name = pk.column_name
    LEFT JOIN (
      SELECT
        n.nspname AS table_schema,
        cl.relname AS table_name,
        a.attname AS column_name,
        fn.nspname AS foreign_table_schema,
        fcl.relname AS foreign_table_name,
        fa.attname AS foreign_column_name
      FROM pg_constraint con
      JOIN pg_class cl ON con.conrelid = cl.oid
      JOIN pg_namespace n ON cl.relnamespace = n.oid
      JOIN pg_class fcl ON con.confrelid = fcl.oid
      JOIN pg_namespace fn ON fcl.relnamespace = fn.oid
      CROSS JOIN LATERAL unnest(con.conkey, con.confkey)
        WITH ORDINALITY AS cols(conkey_col, confkey_col, ord)
      JOIN pg_attribute a ON a.attrelid = con.conrelid
        AND a.attnum = cols.conkey_col
      JOIN pg_attribute fa ON fa.attrelid = con.confrelid
        AND fa.attnum = cols.confkey_col
      WHERE con.contype = 'f'
    ) fk ON c.table_schema = fk.table_schema
        AND c.table_name = fk.table_name
        AND c.column_name = fk.column_name
    WHERE c.table_schema IN (${schemaPlaceholders})
    ORDER BY c.table_schema, c.table_name, c.ordinal_position
  `;

  const columnsResult = await rawQuery<{
    table_schema: string;
    table_name: string;
    column_name: string;
    data_type: string;
    is_nullable: string;
    column_default: string | null;
    ordinal_position: number;
    column_comment: string | null;
    is_primary_key: boolean;
    is_foreign_key: boolean;
    fk_table: string | null;
    fk_column: string | null;
  }>(columnsQuery, schemas);

  // Build column map
  const columnMap = new Map<string, ColumnInfo[]>();
  for (const col of columnsResult.rows) {
    const key = `${col.table_schema}.${col.table_name}`;
    if (!columnMap.has(key)) {
      columnMap.set(key, []);
    }
    columnMap.get(key)!.push({
      columnName: col.column_name,
      dataType: col.data_type,
      isNullable: col.is_nullable === 'YES',
      columnDefault: col.column_default,
      isPrimaryKey: col.is_primary_key,
      isForeignKey: col.is_foreign_key,
      foreignKeyTable: col.fk_table,
      foreignKeyColumn: col.fk_column,
      comment: col.column_comment,
      ordinalPosition: col.ordinal_position,
    });
  }

  // Build table/view lists
  const tables: TableInfo[] = [];
  const views: TableInfo[] = [];

  for (const row of tablesResult.rows) {
    const key = `${row.table_schema}.${row.table_name}`;
    const info: TableInfo = {
      schemaName: row.table_schema,
      tableName: row.table_name,
      tableType: row.table_type === 'VIEW' ? 'view' : 'table',
      comment: row.table_comment,
      columns: columnMap.get(key) || [],
    };

    if (row.table_type === 'VIEW') {
      views.push(info);
    } else {
      tables.push(info);
    }
  }

  return { tables, views };
}

/**
 * Load function information from the database
 */
async function loadFunctions(): Promise<FunctionInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  // Note: prokind = 'f' filters for regular functions only
  // ('a' = aggregate, 'w' = window, 'p' = procedure, 'f' = function)
  // proisagg was removed in PostgreSQL 17, so we rely solely on prokind
  const functionsQuery = `
    SELECT
      n.nspname as schema_name,
      p.proname as function_name,
      pg_get_function_result(p.oid) as return_type,
      pg_get_function_arguments(p.oid) as arguments,
      d.description as function_comment,
      p.provolatile as volatility
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    LEFT JOIN pg_description d ON p.oid = d.objoid
    WHERE n.nspname IN (${schemaPlaceholders})
      AND p.prokind = 'f'
    ORDER BY n.nspname, p.proname
  `;

  const result = await rawQuery<{
    schema_name: string;
    function_name: string;
    return_type: string;
    arguments: string;
    function_comment: string | null;
    volatility: string;
  }>(functionsQuery, schemas);

  return result.rows.map(row => ({
    schemaName: row.schema_name,
    functionName: row.function_name,
    returnType: row.return_type,
    parameters: parseArguments(row.arguments),
    comment: row.function_comment,
    isVolatile: row.volatility === 'v',
  }));
}

/**
 * Parse PostgreSQL function arguments string into structured parameters
 */
function parseArguments(argsStr: string): FunctionParameter[] {
  if (!argsStr || argsStr.trim() === '') {
    return [];
  }

  const params: FunctionParameter[] = [];
  const args = argsStr.split(',').map(a => a.trim());

  for (const arg of args) {
    const parts = arg.split(/\s+/);
    let mode: FunctionParameter['mode'] = 'IN';
    let name = '';
    let dataType = '';
    let defaultValue: string | null = null;

    // Check for mode prefix
    if (parts[0] === 'IN' || parts[0] === 'OUT' || parts[0] === 'INOUT' || parts[0] === 'VARIADIC') {
      mode = parts[0] as FunctionParameter['mode'];
      parts.shift();
    }

    // Check for DEFAULT
    const defaultIdx = parts.findIndex(p => p.toUpperCase() === 'DEFAULT');
    if (defaultIdx !== -1) {
      defaultValue = parts.slice(defaultIdx + 1).join(' ');
      parts.splice(defaultIdx);
    }

    // Remaining parts: name and type
    if (parts.length >= 2) {
      name = parts[0];
      dataType = parts.slice(1).join(' ');
    } else if (parts.length === 1) {
      dataType = parts[0];
    }

    params.push({ name, dataType, mode, defaultValue });
  }

  return params;
}

/**
 * Load complete schema metadata
 */
export async function loadSchemaMetadata(): Promise<SchemaMetadata> {
  const { tables, views } = await loadTablesAndViews();
  const functions = await loadFunctions();

  return {
    tables,
    views,
    functions,
    lastRefreshed: new Date(),
  };
}

/**
 * Get schema metadata with caching
 */
export async function getSchemaMetadata(forceRefresh = false): Promise<SchemaMetadata> {
  const config = getConfig();
  const now = Date.now();

  if (
    forceRefresh ||
    !cachedSchema ||
    now - lastRefreshTime > config.schemaRefreshIntervalMs
  ) {
    cachedSchema = await loadSchemaMetadata();
    lastRefreshTime = now;
  }

  return cachedSchema;
}

/**
 * Get a specific table by schema and name
 */
export async function getTable(
  schemaName: string,
  tableName: string
): Promise<TableInfo | null> {
  const schema = await getSchemaMetadata();
  const allTables = [...schema.tables, ...schema.views];

  return (
    allTables.find(
      t =>
        t.schemaName.toLowerCase() === schemaName.toLowerCase() &&
        t.tableName.toLowerCase() === tableName.toLowerCase()
    ) || null
  );
}

/**
 * Clear the schema cache
 */
export function clearSchemaCache(): void {
  cachedSchema = null;
  cachedDatabaseContext = null;
  cachedOntology = null;
  lastRefreshTime = 0;
}

/**
 * Load database-level and schema-level comments for self-documentation
 */
async function loadDatabaseContext(): Promise<DatabaseContext> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  // Query database-level comment
  const dbCommentQuery = `
    SELECT
      current_database() as db_name,
      pg_catalog.shobj_description(oid, 'pg_database') as db_comment
    FROM pg_database
    WHERE datname = current_database()
  `;

  const dbResult = await rawQuery<{
    db_name: string;
    db_comment: string | null;
  }>(dbCommentQuery, []);

  // Query schema-level comments
  const schemaCommentQuery = `
    SELECT
      nspname as schema_name,
      obj_description(oid, 'pg_namespace') as schema_comment
    FROM pg_namespace
    WHERE nspname IN (${schemaPlaceholders})
  `;

  const schemaResult = await rawQuery<{
    schema_name: string;
    schema_comment: string | null;
  }>(schemaCommentQuery, schemas);

  // Build schema comments map
  const schemaComments: Record<string, string> = {};
  for (const row of schemaResult.rows) {
    if (row.schema_comment) {
      schemaComments[row.schema_name] = row.schema_comment;
    }
  }

  return {
    databaseName: dbResult.rows[0]?.db_name || 'unknown',
    databaseComment: dbResult.rows[0]?.db_comment || null,
    schemaComments,
  };
}

/**
 * Get database context with caching
 */
export async function getDatabaseContext(forceRefresh = false): Promise<DatabaseContext> {
  const config = getConfig();
  const now = Date.now();

  if (
    forceRefresh ||
    !cachedDatabaseContext ||
    now - lastRefreshTime > config.schemaRefreshIntervalMs
  ) {
    cachedDatabaseContext = await loadDatabaseContext();
  }

  return cachedDatabaseContext;
}

/**
 * Force refresh of all cached metadata
 * Returns a summary of what was loaded
 */
export async function forceRefreshSchema(): Promise<{
  tables: number;
  views: number;
  functions: number;
  schemasWithComments: number;
  hasDatabaseComment: boolean;
  checkConstraints: number;
  enumTypes: number;
  uniqueConstraints: number;
  indexes: number;
  triggers: number;
}> {
  clearSchemaCache();

  const [schema, context, ontology] = await Promise.all([
    getSchemaMetadata(true),
    getDatabaseContext(true),
    getOntologyMetadata(true),
  ]);

  return {
    tables: schema.tables.length,
    views: schema.views.length,
    functions: schema.functions.length,
    schemasWithComments: Object.keys(context.schemaComments).length,
    hasDatabaseComment: !!context.databaseComment,
    checkConstraints: ontology.checkConstraints.length,
    enumTypes: ontology.enumTypes.length,
    uniqueConstraints: ontology.uniqueConstraints.length,
    indexes: ontology.indexes.length,
    triggers: ontology.triggers.length,
  };
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/**
 * Normalize a value that may be a JS array or a PostgreSQL array string
 * (e.g., "{val1,val2}") into a proper JS string array.
 * node-postgres sometimes returns array_agg results as raw strings
 * when it can't resolve the type OID.
 */
function ensureArray(value: unknown): string[] {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    // PostgreSQL text array format: {val1,val2,"val with spaces"}
    const trimmed = value.replace(/^\{|\}$/g, '');
    if (trimmed === '') return [];
    // Simple split — handles unquoted and double-quoted elements
    const result: string[] = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < trimmed.length; i++) {
      const ch = trimmed[i];
      if (ch === '"') {
        inQuotes = !inQuotes;
      } else if (ch === ',' && !inQuotes) {
        result.push(current);
        current = '';
      } else {
        current += ch;
      }
    }
    result.push(current);
    return result;
  }
  return [];
}

// ─── Ontology Loaders ───────────────────────────────────────────────────────

/**
 * Load check constraints from pg_constraint WHERE contype = 'c'
 */
async function loadCheckConstraints(): Promise<CheckConstraintInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      con.conname AS constraint_name,
      n.nspname AS schema_name,
      cl.relname AS table_name,
      pg_get_constraintdef(con.oid, true) AS expression,
      COALESCE(
        array_agg(a.attname ORDER BY cols.ord) FILTER (WHERE a.attname IS NOT NULL),
        ARRAY[]::text[]
      ) AS columns
    FROM pg_constraint con
    JOIN pg_class cl ON con.conrelid = cl.oid
    JOIN pg_namespace n ON cl.relnamespace = n.oid
    LEFT JOIN LATERAL unnest(con.conkey)
      WITH ORDINALITY AS cols(col_num, ord) ON true
    LEFT JOIN pg_attribute a ON a.attrelid = con.conrelid
      AND a.attnum = cols.col_num
    WHERE con.contype = 'c'
      AND n.nspname IN (${schemaPlaceholders})
    GROUP BY con.oid, con.conname, n.nspname, cl.relname
    ORDER BY n.nspname, cl.relname, con.conname
  `;

  const result = await rawQuery<{
    constraint_name: string;
    schema_name: string;
    table_name: string;
    expression: string;
    columns: string[];
  }>(sql, schemas);

  return result.rows.map(row => ({
    constraintName: row.constraint_name,
    schemaName: row.schema_name,
    tableName: row.table_name,
    expression: row.expression,
    columns: ensureArray(row.columns),
  }));
}

/**
 * Load custom enum types from pg_type + pg_enum
 */
async function loadEnumTypes(): Promise<EnumTypeInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      n.nspname AS schema_name,
      t.typname AS type_name,
      array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values,
      obj_description(t.oid, 'pg_type') AS comment
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    JOIN pg_enum e ON e.enumtypid = t.oid
    WHERE t.typtype = 'e'
      AND n.nspname IN (${schemaPlaceholders})
    GROUP BY n.nspname, t.typname, t.oid
    ORDER BY n.nspname, t.typname
  `;

  const result = await rawQuery<{
    schema_name: string;
    type_name: string;
    values: string[];
    comment: string | null;
  }>(sql, schemas);

  return result.rows.map(row => ({
    schemaName: row.schema_name,
    typeName: row.type_name,
    values: ensureArray(row.values),
    comment: row.comment,
  }));
}

/**
 * Load unique constraints (excluding PKs) from pg_constraint WHERE contype = 'u'
 */
async function loadUniqueConstraints(): Promise<UniqueConstraintInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      con.conname AS constraint_name,
      n.nspname AS schema_name,
      cl.relname AS table_name,
      array_agg(a.attname ORDER BY cols.ord) AS columns
    FROM pg_constraint con
    JOIN pg_class cl ON con.conrelid = cl.oid
    JOIN pg_namespace n ON cl.relnamespace = n.oid
    CROSS JOIN LATERAL unnest(con.conkey)
      WITH ORDINALITY AS cols(col_num, ord)
    JOIN pg_attribute a ON a.attrelid = con.conrelid
      AND a.attnum = cols.col_num
    WHERE con.contype = 'u'
      AND n.nspname IN (${schemaPlaceholders})
    GROUP BY con.conname, n.nspname, cl.relname
    ORDER BY n.nspname, cl.relname, con.conname
  `;

  const result = await rawQuery<{
    constraint_name: string;
    schema_name: string;
    table_name: string;
    columns: string[];
  }>(sql, schemas);

  return result.rows.map(row => ({
    constraintName: row.constraint_name,
    schemaName: row.schema_name,
    tableName: row.table_name,
    columns: ensureArray(row.columns),
  }));
}

/**
 * Load view definitions via pg_get_viewdef (pretty-printed)
 */
async function loadViewDefinitions(): Promise<ViewDefinitionInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      n.nspname AS schema_name,
      c.relname AS view_name,
      pg_get_viewdef(c.oid, true) AS definition,
      obj_description(c.oid, 'pg_class') AS comment
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.relkind IN ('v', 'm')
      AND n.nspname IN (${schemaPlaceholders})
    ORDER BY n.nspname, c.relname
  `;

  const result = await rawQuery<{
    schema_name: string;
    view_name: string;
    definition: string | null;
    comment: string | null;
  }>(sql, schemas);

  return result.rows
    .filter(row => row.definition != null)
    .map(row => ({
      schemaName: row.schema_name,
      viewName: row.view_name,
      definition: row.definition!,
      comment: row.comment,
    }));
}

/**
 * Load index information from pg_index + pg_class + pg_am
 */
async function loadIndexes(): Promise<IndexInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      n.nspname AS schema_name,
      t.relname AS table_name,
      i.relname AS index_name,
      array_agg(a.attname ORDER BY k.ord) AS columns,
      ix.indisunique AS is_unique,
      ix.indisprimary AS is_primary,
      am.amname AS index_type,
      pg_get_indexdef(ix.indexrelid) AS definition
    FROM pg_index ix
    JOIN pg_class i ON ix.indexrelid = i.oid
    JOIN pg_class t ON ix.indrelid = t.oid
    JOIN pg_namespace n ON t.relnamespace = n.oid
    JOIN pg_am am ON i.relam = am.oid
    CROSS JOIN LATERAL unnest(ix.indkey)
      WITH ORDINALITY AS k(col_num, ord)
    JOIN pg_attribute a ON a.attrelid = t.oid
      AND a.attnum = k.col_num
    WHERE n.nspname IN (${schemaPlaceholders})
      AND k.col_num > 0
    GROUP BY n.nspname, t.relname, i.relname, ix.indisunique,
             ix.indisprimary, am.amname, ix.indexrelid
    ORDER BY n.nspname, t.relname, i.relname
  `;

  const result = await rawQuery<{
    schema_name: string;
    table_name: string;
    index_name: string;
    columns: string[];
    is_unique: boolean;
    is_primary: boolean;
    index_type: string;
    definition: string;
  }>(sql, schemas);

  return result.rows.map(row => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    indexName: row.index_name,
    columns: ensureArray(row.columns),
    isUnique: row.is_unique,
    isPrimary: row.is_primary,
    indexType: row.index_type,
    definition: row.definition,
  }));
}

/**
 * Load trigger information from pg_trigger with tgtype bitmask decoding.
 * tgtype bits: 0=ROW/STATEMENT, 1=BEFORE, 2=INSERT, 3=DELETE, 4=UPDATE, 5=TRUNCATE, 6=INSTEAD OF
 */
async function loadTriggers(): Promise<TriggerInfo[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      t.tgname AS trigger_name,
      CASE
        WHEN (t.tgtype & 66) = 66 THEN 'INSTEAD OF'
        WHEN (t.tgtype & 2) = 2 THEN 'BEFORE'
        ELSE 'AFTER'
      END AS timing,
      array_remove(ARRAY[
        CASE WHEN (t.tgtype & 4) = 4 THEN 'INSERT' END,
        CASE WHEN (t.tgtype & 8) = 8 THEN 'DELETE' END,
        CASE WHEN (t.tgtype & 16) = 16 THEN 'UPDATE' END,
        CASE WHEN (t.tgtype & 32) = 32 THEN 'TRUNCATE' END
      ], NULL) AS events,
      CASE WHEN (t.tgtype & 1) = 1 THEN 'ROW' ELSE 'STATEMENT' END AS orientation,
      pn.nspname || '.' || p.proname AS function_name,
      obj_description(t.oid, 'pg_trigger') AS comment
    FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_proc p ON t.tgfoid = p.oid
    JOIN pg_namespace pn ON p.pronamespace = pn.oid
    WHERE NOT t.tgisinternal
      AND n.nspname IN (${schemaPlaceholders})
    ORDER BY n.nspname, c.relname, t.tgname
  `;

  const result = await rawQuery<{
    schema_name: string;
    table_name: string;
    trigger_name: string;
    timing: string;
    events: string[];
    orientation: string;
    function_name: string;
    comment: string | null;
  }>(sql, schemas);

  return result.rows.map(row => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    triggerName: row.trigger_name,
    timing: row.timing,
    events: ensureArray(row.events),
    orientation: row.orientation,
    functionName: row.function_name,
    comment: row.comment,
  }));
}

/**
 * Load estimated row counts from pg_class.reltuples
 */
async function loadEstimatedRowCounts(): Promise<EstimatedRowCount[]> {
  const config = getConfig();
  const schemas = config.exposedSchemas;
  const schemaPlaceholders = schemas.map((_, i) => `$${i + 1}`).join(', ');

  const sql = `
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      CASE WHEN c.reltuples < 0 THEN 0 ELSE c.reltuples::bigint END AS estimated_rows,
      CASE
        WHEN c.reltuples < 0 THEN true
        WHEN c.reltuples = 0 AND s.last_analyze IS NULL AND s.last_autoanalyze IS NULL THEN true
        ELSE false
      END AS never_analyzed
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN pg_stat_user_tables s ON n.nspname = s.schemaname AND c.relname = s.relname
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname IN (${schemaPlaceholders})
    ORDER BY c.reltuples DESC
  `;

  const result = await rawQuery<{
    schema_name: string;
    table_name: string;
    estimated_rows: string;
    never_analyzed: boolean;
  }>(sql, schemas);

  return result.rows.map(row => ({
    schemaName: row.schema_name,
    tableName: row.table_name,
    estimatedRows: parseInt(row.estimated_rows, 10) || 0,
    neverAnalyzed: row.never_analyzed,
  }));
}

/**
 * Load all ontology metadata in parallel
 */
async function loadOntologyMetadata(): Promise<OntologyMetadata> {
  const [
    checkConstraints,
    enumTypes,
    uniqueConstraints,
    viewDefinitions,
    indexes,
    triggers,
    estimatedRowCounts,
  ] = await Promise.all([
    loadCheckConstraints(),
    loadEnumTypes(),
    loadUniqueConstraints(),
    loadViewDefinitions(),
    loadIndexes(),
    loadTriggers(),
    loadEstimatedRowCounts(),
  ]);

  return {
    checkConstraints,
    enumTypes,
    uniqueConstraints,
    viewDefinitions,
    indexes,
    triggers,
    estimatedRowCounts,
    lastRefreshed: new Date(),
  };
}

/**
 * Get ontology metadata with caching
 */
export async function getOntologyMetadata(forceRefresh = false): Promise<OntologyMetadata> {
  const config = getConfig();
  const now = Date.now();

  if (
    forceRefresh ||
    !cachedOntology ||
    now - lastRefreshTime > config.schemaRefreshIntervalMs
  ) {
    cachedOntology = await loadOntologyMetadata();
  }

  return cachedOntology;
}
