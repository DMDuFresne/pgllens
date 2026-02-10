/**
 * Static descriptions for database entities
 *
 * This module provides extensible placeholders for domain-specific documentation.
 * For domain-specific deployments, use environment variables:
 *   - DOMAIN_CONTEXT_FILE: Path to a markdown file with domain documentation
 *   - DOMAIN_CONTEXT: Inline domain context text
 *
 * Or add database comments via SQL:
 *   COMMENT ON SCHEMA public IS 'Your schema description';
 *   COMMENT ON TABLE my_table IS 'Table description';
 */

/**
 * Domain concepts (loaded dynamically from config or database comments)
 * This export is kept for backward compatibility but is empty by default.
 */
export const DOMAIN_CONCEPTS = '';

/**
 * Schema overview (populated dynamically from database introspection)
 * This export is kept for backward compatibility but is empty by default.
 */
export const SCHEMA_OVERVIEW = '';

/**
 * Query examples (can be provided via domain context file)
 * This export is kept for backward compatibility but is empty by default.
 */
export const QUERY_EXAMPLES = '';

/**
 * Query warnings (can be provided via domain context file)
 * This export is kept for backward compatibility but is empty by default.
 */
export const QUERY_WARNINGS = '';

/**
 * Base tool description for the query tool
 * Domain context is prepended dynamically at runtime.
 */
export const QUERY_TOOL_DESCRIPTION = `Execute read-only SQL queries against the PostgreSQL database.

## Security
- Only SELECT queries are allowed
- Queries run in READ ONLY transactions
- Statement timeout prevents long-running queries
- Results limited to prevent memory issues

## Best Practices
- Use the list_tables tool to discover available tables and views
- Use the describe_table tool to understand column types and relationships
- Filter by time columns for large tables to avoid full table scans
- Use LIMIT to prevent returning excessive rows`;

/**
 * Table descriptions to supplement database comments
 * Empty by default - use database COMMENT ON TABLE instead.
 */
export const TABLE_DESCRIPTIONS: Record<string, string> = {};

/**
 * Function descriptions to supplement database comments
 * Empty by default - use database COMMENT ON FUNCTION instead.
 */
export const FUNCTION_DESCRIPTIONS: Record<string, string> = {};
