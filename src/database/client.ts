/**
 * PostgreSQL database client with connection pooling
 * Provides read-only query execution with timeout support
 */

import pg from 'pg';
import { getConfig } from '../config.js';
import { QueryResult, BLOCKED_KEYWORDS } from '../types/index.js';

const { Pool } = pg;

let pool: pg.Pool | null = null;

/**
 * Get or create the database connection pool
 */
export function getPool(): pg.Pool {
  if (!pool) {
    const config = getConfig();
    pool = new Pool({
      connectionString: config.databaseUrl,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });

    pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
    });
  }
  return pool;
}

/**
 * Close the database connection pool
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

/**
 * Check if a SQL statement contains blocked keywords.
 * Strips string literals and comments first so that values like
 * "WHERE info LIKE '%DELETE%'" don't trigger false positives.
 */
export function containsBlockedKeywords(sql: string): string | null {
  // Neutralize string contents, line comments, and block comments
  const stripped = sql
    .replace(/\$([^$]*)\$[\s\S]*?\$\1\$/g, "''")  // Neutralize dollar-quoted strings ($$...$$ and $tag$...$tag$)
    .replace(/E'(?:[^'\\]|\\.)*'/gi, "''")         // Neutralize PostgreSQL escape strings (E'...')
    .replace(/'[^']*'/g, "''")                      // Neutralize string literal contents
    .replace(/--[^\n]*/g, '')                       // Remove line comments
    .replace(/\/\*[\s\S]*?\*\//g, '');              // Remove block comments

  for (const keyword of BLOCKED_KEYWORDS) {
    // Match keyword as a whole word (not part of another word)
    const regex = new RegExp(`\\b${keyword}\\b`, 'i');
    if (regex.test(stripped)) {
      return keyword;
    }
  }

  return null;
}

/**
 * Execute a read-only SQL query with timeout
 */
export async function query(
  sql: string,
  params?: unknown[]
): Promise<QueryResult> {
  const config = getConfig();

  // Defense-in-depth: Check for blocked keywords
  const blockedKeyword = containsBlockedKeywords(sql);
  if (blockedKeyword) {
    throw new Error(
      `Query contains blocked keyword: ${blockedKeyword}. Only SELECT queries are allowed.`
    );
  }

  const client = await getPool().connect();

  try {
    // Set statement timeout for this session
    await client.query(`SET statement_timeout = ${config.queryTimeoutMs}`);

    // Force read-only transaction
    await client.query('BEGIN READ ONLY');

    try {
      const result = await client.query(sql, params);
      await client.query('COMMIT');

      const rows = result.rows;
      const truncated = rows.length >= config.maxRows;

      return {
        columns: result.fields.map(f => f.name),
        rows: rows.slice(0, config.maxRows),
        rowCount: rows.length,
        truncated,
      };
    } catch (queryError) {
      await client.query('ROLLBACK');
      throw queryError;
    }
  } finally {
    client.release();
  }
}

/**
 * Execute a query and return raw pg.QueryResult
 * Used internally for schema introspection
 */
export async function rawQuery<T extends pg.QueryResultRow = pg.QueryResultRow>(
  sql: string,
  params?: unknown[]
): Promise<pg.QueryResult<T>> {
  const client = await getPool().connect();

  try {
    // Prevent schema introspection queries from hanging indefinitely
    await client.query('SET statement_timeout = 60000'); // 60 seconds
    const result = await client.query<T>(sql, params);
    return result;
  } finally {
    client.release();
  }
}

/**
 * Test database connectivity
 */
export async function testConnection(): Promise<boolean> {
  try {
    const result = await rawQuery('SELECT 1 as connected');
    return result.rows[0]?.connected === 1;
  } catch {
    return false;
  }
}
