#!/usr/bin/env node
/**
 * PgLLens MCP Server
 *
 * A self-documenting PostgreSQL MCP server that provides rich schema documentation
 * and read-only query access to any PostgreSQL database.
 *
 * Usage:
 *   npm start              - Run with .env file
 *   npm run start:oauth    - Run with OAuth enabled
 *
 * Tools provided:
 *   - query: Execute read-only SQL queries with full schema documentation
 *   - list_tables: List tables/views with descriptions
 *   - describe_table: Get detailed column information
 *   - schema_overview: Get complete schema as JSON
 */

// Load .env file before anything else
import 'dotenv/config';

import { startServer } from './server.js';

// Start the server
startServer().catch((error) => {
  console.error('Fatal error starting server:', error);
  process.exit(1);
});
