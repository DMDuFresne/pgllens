/**
 * Configuration module for the PostgreSQL MCP Server
 * Parses and validates environment variables
 */

import { z } from 'zod';
import * as fs from 'node:fs';
import * as path from 'node:path';

const configSchema = z.object({
  databaseUrl: z.string().url().describe('PostgreSQL connection URL'),
  schemaRefreshIntervalMs: z.number().int().positive().default(300000), // 5 minutes
  queryTimeoutMs: z.number().int().positive().default(30000),
  maxRows: z.number().int().positive().default(1000),
  exposedSchemas: z.array(z.string()).default(['public']),
  defaultSchema: z.string().default('public').describe('Default schema for tools when not specified'),
  domainContext: z.string().optional().describe('Inline domain context text'),
  domainContextFile: z.string().optional().describe('Path to domain context markdown file'),
});

export type Config = z.infer<typeof configSchema>;

function parseExposedSchemas(value: string | undefined): string[] {
  if (!value) return ['public'];
  return value.split(',').map(s => s.trim()).filter(Boolean);
}

/**
 * Load domain context from file if specified
 */
function loadDomainContextFile(filePath: string | undefined): string | undefined {
  if (!filePath) return undefined;

  try {
    const resolvedPath = path.isAbsolute(filePath)
      ? filePath
      : path.resolve(process.cwd(), filePath);

    if (fs.existsSync(resolvedPath)) {
      const stats = fs.statSync(resolvedPath);
      const MAX_FILE_SIZE = 1 * 1024 * 1024; // 1MB
      if (stats.size > MAX_FILE_SIZE) {
        console.warn(`Domain context file exceeds 1MB limit (${(stats.size / 1024 / 1024).toFixed(2)}MB): ${resolvedPath}`);
        return undefined;
      }
      return fs.readFileSync(resolvedPath, 'utf-8');
    } else {
      console.warn(`Domain context file not found: ${resolvedPath}`);
      return undefined;
    }
  } catch (error) {
    console.warn(`Failed to load domain context file: ${error}`);
    return undefined;
  }
}

export function loadConfig(): Config {
  const databaseUrl = process.env.DATABASE_URL;

  if (!databaseUrl) {
    throw new Error('DATABASE_URL environment variable is required');
  }

  // Load domain context - prefer file over inline
  const domainContextFromFile = loadDomainContextFile(process.env.DOMAIN_CONTEXT_FILE);
  const domainContext = domainContextFromFile || process.env.DOMAIN_CONTEXT;

  // Default schema is the first exposed schema, or 'public' if not specified
  const exposedSchemas = parseExposedSchemas(process.env.EXPOSED_SCHEMAS);
  const defaultSchema = process.env.DEFAULT_SCHEMA || exposedSchemas[0] || 'public';

  const rawConfig = {
    databaseUrl,
    schemaRefreshIntervalMs: process.env.SCHEMA_REFRESH_INTERVAL_MS
      ? parseInt(process.env.SCHEMA_REFRESH_INTERVAL_MS, 10)
      : undefined,
    queryTimeoutMs: process.env.QUERY_TIMEOUT_MS
      ? parseInt(process.env.QUERY_TIMEOUT_MS, 10)
      : undefined,
    maxRows: process.env.MAX_ROWS
      ? parseInt(process.env.MAX_ROWS, 10)
      : undefined,
    exposedSchemas,
    defaultSchema,
    domainContext,
    domainContextFile: process.env.DOMAIN_CONTEXT_FILE,
  };

  const result = configSchema.safeParse(rawConfig);

  if (!result.success) {
    const errors = result.error.issues
      .map(issue => `${issue.path.join('.')}: ${issue.message}`)
      .join(', ');
    throw new Error(`Invalid configuration: ${errors}`);
  }

  return result.data;
}

// Singleton config instance
let cachedConfig: Config | null = null;

export function getConfig(): Config {
  if (!cachedConfig) {
    cachedConfig = loadConfig();
  }
  return cachedConfig;
}

/**
 * Get the default schema name for tools
 * Convenience function for consistent access across all tools
 */
export function getDefaultSchema(): string {
  return getConfig().defaultSchema;
}
