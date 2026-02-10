/**
 * PostgreSQL MCP Server with Streamable HTTP Transport + OAuth Support
 *
 * A generic, self-documenting PostgreSQL MCP Server that dynamically
 * extracts context from database comments and supports optional domain configuration.
 *
 * Run modes:
 *   npm start          - No auth (local development)
 *   npm run start:oauth - With OAuth (Claude Desktop compatible)
 */

import { randomUUID, timingSafeEqual } from 'node:crypto';
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import { z } from 'zod';

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

import { testConnection, closePool } from './database/client.js';
import { getSchemaMetadata, forceRefreshSchema } from './database/schema-loader.js';
import { getConfig, getDefaultSchema } from './config.js';

// Import tool executors and schemas
import {
  executeQueryTool,
  executeListTablesTool,
  executeListFunctionsTool,
  executeDescribeTableTool,
  executeSchemaOverviewTool,
  executeGetSampleDataTool,
  executeSearchColumnsTool,
  executeExplainQueryTool,
  executeGetRelationshipsTool,
  executeGetTableStatsTool,
  executeValidateQueryTool,
  executeRefreshSchemaTool,
  executeGetOntologyTool,
  executeListExtensionsTool,
  executeGetViewDefinitionTool,
  executeGetFunctionSourceTool,
  executeListRolesTool,
  executeGetTableHealthTool,
  executeListHypertablesTool,
} from './tools/index.js';

// Import descriptions
import {
  getQueryToolDescription,
  getListTablesToolDescription,
  getDescribeTableToolDescription,
  getSchemaOverviewToolDescription,
} from './descriptions/generator.js';

const SERVER_NAME = 'postgres-mcp';
const SERVER_VERSION = '2.1.0';
const MCP_PORT = parseInt(process.env.MCP_PORT || '3000', 10);

// External base URL for OAuth metadata (important when running behind port mapping/proxy)
const EXTERNAL_BASE_URL = process.env.EXTERNAL_BASE_URL || `http://localhost:${MCP_PORT}`;

// Check for OAuth flag
const useOAuth = process.argv.includes('--oauth');

// OAuth token lifetime (seconds) — how long before client must re-authorize
const OAUTH_TOKEN_EXPIRES_IN = parseInt(process.env.MCP_OAUTH_TOKEN_EXPIRES_IN || '604800', 10); // 7 days

// Rate limiting configuration
const RATE_LIMIT_MAX_ATTEMPTS = parseInt(process.env.MCP_RATE_LIMIT_ATTEMPTS || '5', 10);
const RATE_LIMIT_WINDOW_MS = parseInt(process.env.MCP_RATE_LIMIT_WINDOW_MS || '900000', 10); // 15 minutes

// Rate limiting state (IP -> { attempts, lockoutUntil })
const rateLimitState = new Map<string, { attempts: number; lockoutUntil: number }>();

/**
 * Check if an IP is rate limited. Returns error message if limited, null if OK.
 */
function checkRateLimit(ip: string): string | null {
  const state = rateLimitState.get(ip);
  if (!state) return null;

  if (state.lockoutUntil > Date.now()) {
    const remainingSeconds = Math.ceil((state.lockoutUntil - Date.now()) / 1000);
    return `Too many failed attempts. Try again in ${remainingSeconds} seconds.`;
  }

  // Lockout expired, reset state
  if (state.lockoutUntil <= Date.now() && state.attempts >= RATE_LIMIT_MAX_ATTEMPTS) {
    rateLimitState.delete(ip);
  }

  return null;
}

/**
 * Record a failed auth attempt. Returns true if now locked out.
 */
function recordFailedAttempt(ip: string): boolean {
  const state = rateLimitState.get(ip) || { attempts: 0, lockoutUntil: 0 };
  state.attempts++;

  if (state.attempts >= RATE_LIMIT_MAX_ATTEMPTS) {
    state.lockoutUntil = Date.now() + RATE_LIMIT_WINDOW_MS;
    console.warn(`Rate limit triggered for IP: ${ip}`);
  }

  rateLimitState.set(ip, state);
  return state.attempts >= RATE_LIMIT_MAX_ATTEMPTS;
}

/**
 * Clear rate limit state for an IP (on successful auth)
 */
function clearRateLimit(ip: string): void {
  rateLimitState.delete(ip);
}

/**
 * Timing-safe password comparison to prevent timing attacks
 */
function safeComparePasswords(provided: string, expected: string): boolean {
  if (!provided || !expected) return false;

  const providedBuf = Buffer.from(provided);
  const expectedBuf = Buffer.from(expected);

  // Pad to same length to prevent length-based timing leaks
  if (providedBuf.length !== expectedBuf.length) {
    // Compare against expected anyway to maintain constant time
    const paddedProvided = Buffer.alloc(expectedBuf.length);
    providedBuf.copy(paddedProvided);
    timingSafeEqual(paddedProvided, expectedBuf);
    return false;
  }

  return timingSafeEqual(providedBuf, expectedBuf);
}

// Session management
const transports: Record<string, StreamableHTTPServerTransport> = {};

// OAuth client registry (in-memory for dynamic client registration)
interface OAuthClient {
  client_id: string;
  client_secret?: string;
  redirect_uris: string[];
  client_name?: string;
  created_at: number;
}
const oauthClients: Record<string, OAuthClient> = {};

// Authorization codes (short-lived, maps code to client_id and PKCE challenge)
const authCodes: Record<string, {
  client_id: string;
  redirect_uri: string;
  code_challenge?: string;
  code_challenge_method?: string;
  expires: number;
}> = {};

/**
 * Create and configure the MCP server with all tools
 */
function createMcpServer(): McpServer {
  const server = new McpServer(
    {
      name: SERVER_NAME,
      version: SERVER_VERSION,
    },
    {
      capabilities: {
        tools: {},
        logging: {},
      },
    }
  );

  // Register query tool
  server.tool(
    'query',
    getQueryToolDescription(),
    {
      sql: z.string().min(1).describe('The SQL SELECT query to execute'),
    },
    async ({ sql }) => {
      return executeQueryTool({ sql });
    }
  );

  // Register list_tables tool
  server.tool(
    'list_tables',
    getListTablesToolDescription(),
    {
      schema: z.string().optional().describe('Optional schema name to filter'),
    },
    async (args) => {
      return executeListTablesTool({ schema: args.schema });
    }
  );

  // Register list_functions tool
  server.tool(
    'list_functions',
    'List all stored functions/procedures in the database with their parameters and descriptions.',
    {
      schema: z.string().optional().describe('Optional schema name to filter'),
    },
    async (args) => {
      return executeListFunctionsTool({ schema: args.schema });
    }
  );

  // Register describe_table tool
  const defaultSchema = getDefaultSchema();
  server.tool(
    'describe_table',
    getDescribeTableToolDescription(),
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      table: z.string().min(1).describe('Table or view name'),
    },
    async (args) => {
      return executeDescribeTableTool({ schema: args.schema, table: args.table });
    }
  );

  // Register schema_overview tool
  server.tool(
    'schema_overview',
    getSchemaOverviewToolDescription(),
    {},
    async () => {
      return executeSchemaOverviewTool({});
    }
  );

  // Register get_sample_data tool
  server.tool(
    'get_sample_data',
    'Get sample rows from a table to understand its data patterns and actual values.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      table: z.string().min(1).describe('Table or view name'),
      limit: z.number().int().min(1).max(20).default(5).describe('Number of rows to return (1-20, default: 5)'),
    },
    async (args) => {
      return executeGetSampleDataTool({ schema: args.schema, table: args.table, limit: args.limit });
    }
  );

  // Register search_columns tool
  server.tool(
    'search_columns',
    'Search for columns by name pattern across all tables and views. Useful for finding related fields.',
    {
      pattern: z.string().min(1).describe('Search pattern (case-insensitive, partial match)'),
      schema: z.string().optional().describe('Optional schema name to filter'),
    },
    async (args) => {
      return executeSearchColumnsTool({ pattern: args.pattern, schema: args.schema });
    }
  );

  // Register explain_query tool
  server.tool(
    'explain_query',
    'Get the execution plan for a SQL query. Useful for understanding performance and identifying missing indexes.',
    {
      sql: z.string().min(1).describe('The SQL SELECT query to explain'),
      analyze: z.boolean().default(false).describe('Run EXPLAIN ANALYZE to get actual timings (default: false)'),
    },
    async (args) => {
      return executeExplainQueryTool({ sql: args.sql, analyze: args.analyze });
    }
  );

  // Register get_relationships tool
  server.tool(
    'get_relationships',
    'Show foreign key relationships between tables. Can output as text or Mermaid ER diagram.',
    {
      table: z.string().optional().describe('Specific table name (optional - if omitted, shows all relationships)'),
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      format: z.enum(['text', 'mermaid']).default('text').describe('Output format: text or mermaid diagram'),
    },
    async (args) => {
      return executeGetRelationshipsTool({ table: args.table, schema: args.schema, format: args.format });
    }
  );

  // Register get_table_stats tool
  server.tool(
    'get_table_stats',
    'Get statistics about a table including row count, null percentages, and distinct value counts.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      table: z.string().min(1).describe('Table or view name'),
    },
    async (args) => {
      return executeGetTableStatsTool({ schema: args.schema, table: args.table });
    }
  );

  // Register refresh_schema tool
  server.tool(
    'refresh_schema',
    'Force refresh of the MCP server\'s internal metadata cache. Use after DDL changes to see updates immediately. Note: This does NOT run PostgreSQL ANALYZE — row count estimates from get_ontology will still reflect pg_class.reltuples until Postgres runs auto-analyze or you run ANALYZE manually.',
    {},
    async () => {
      return executeRefreshSchemaTool({});
    }
  );

  // Register validate_query tool
  server.tool(
    'validate_query',
    'Validate SQL query syntax without executing it. Checks for syntax errors and verifies table/column names.',
    {
      sql: z.string().min(1).describe('The SQL query to validate'),
    },
    async (args) => {
      return executeValidateQueryTool({ sql: args.sql });
    }
  );

  // Register get_ontology tool
  server.tool(
    'get_ontology',
    'Get the semantic ontology of the database — check constraints, enum types, unique keys, view definitions, indexes, triggers, and row counts. Use this to understand business rules and data semantics beyond schema structure.',
    {
      schema: z.string().optional().describe('Filter to a specific schema name'),
      table: z.string().optional().describe('Focus on a specific table and its FK neighborhood'),
      sections: z
        .array(
          z.enum([
            'overview',
            'constraints',
            'enums',
            'relationships',
            'views',
            'indexes',
            'triggers',
            'domain_context',
          ])
        )
        .optional()
        .describe('Sections to include (default: all)'),
    },
    async (args) => {
      return executeGetOntologyTool({
        schema: args.schema,
        table: args.table,
        sections: args.sections,
      });
    }
  );

  // Register list_extensions tool
  server.tool(
    'list_extensions',
    'List installed PostgreSQL extensions with version, schema, and description.',
    {
      name: z.string().optional().describe('Filter by name (partial match, case-insensitive)'),
    },
    async (args) => {
      return executeListExtensionsTool({ name: args.name });
    }
  );

  // Register get_view_definition tool
  server.tool(
    'get_view_definition',
    'Get the full SQL definition and column listing for a view or materialized view.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      view: z.string().min(1).describe('View name'),
    },
    async (args) => {
      return executeGetViewDefinitionTool({ schema: args.schema, view: args.view });
    }
  );

  // Register get_function_source tool
  server.tool(
    'get_function_source',
    'Get the full source code and metadata for a function or procedure, with overload handling.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      function_name: z.string().min(1).describe('Function or procedure name'),
    },
    async (args) => {
      return executeGetFunctionSourceTool({ schema: args.schema, function_name: args.function_name });
    }
  );

  // Register list_roles tool
  server.tool(
    'list_roles',
    'List database roles and their table-level privileges.',
    {
      schema: z.string().optional().describe('Filter grants to a specific schema'),
      role: z.string().optional().describe('Filter to a specific role name'),
    },
    async (args) => {
      return executeListRolesTool({ schema: args.schema, role: args.role });
    }
  );

  // Register get_table_health tool
  server.tool(
    'get_table_health',
    'Get table health statistics: vacuum/analyze history, dead tuples, sizes, and index usage.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      table: z.string().optional().describe('Specific table name (omit for summary of all tables)'),
    },
    async (args) => {
      return executeGetTableHealthTool({ schema: args.schema, table: args.table });
    }
  );

  // Register list_hypertables tool
  server.tool(
    'list_hypertables',
    'List TimescaleDB hypertables with chunk intervals, compression, policies, and chunk statistics.',
    {
      schema: z.string().default(defaultSchema).describe(`Schema name (default: ${defaultSchema})`),
      table: z.string().optional().describe('Focus on a specific hypertable name'),
    },
    async (args) => {
      return executeListHypertablesTool({ schema: args.schema, table: args.table });
    }
  );

  return server;
}

/**
 * Simple OAuth middleware (validates Bearer token)
 * For production, replace with proper OAuth provider validation
 */
function createAuthMiddleware() {
  return (req: Request, res: Response, next: NextFunction) => {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      res.status(401).json({
        error: 'unauthorized',
        error_description: 'Bearer token required',
      });
      return;
    }

    const token = authHeader.substring(7);

    // Simple token validation - accept any non-empty token
    // Claude Desktop will provide one after OAuth flow
    if (!token) {
      res.status(401).json({
        error: 'invalid_token',
        error_description: 'Invalid or expired token',
      });
      return;
    }

    // Store auth info for logging
    req.app.locals.auth = { token: token.substring(0, 8) + '...' };
    next();
  };
}

/**
 * Start the MCP server with Streamable HTTP transport
 */
export async function startServer(): Promise<void> {
  // Test database connection
  console.log('Testing database connection...');
  const connected = await testConnection();

  if (!connected) {
    console.error('Failed to connect to database. Check DATABASE_URL.');
    process.exit(1);
  }
  console.log('Database connection successful.');

  // Pre-load schema metadata
  console.log('Loading schema metadata...');
  try {
    const schema = await getSchemaMetadata();
    console.log(
      `Schema loaded: ${schema.tables.length} tables, ${schema.views.length} views, ${schema.functions.length} functions`
    );
  } catch (error) {
    console.error('Warning: Failed to pre-load schema metadata:', error);
  }

  // Start background schema refresh pulse
  const config = getConfig();
  const refreshIntervalMs = config.schemaRefreshIntervalMs;
  let previousTableCount = 0;
  let previousViewCount = 0;

  const schemaRefreshInterval = setInterval(async () => {
    try {
      const oldSchema = await getSchemaMetadata();
      previousTableCount = oldSchema.tables.length;
      previousViewCount = oldSchema.views.length;

      const summary = await forceRefreshSchema();

      // Log if changes detected
      if (summary.tables !== previousTableCount || summary.views !== previousViewCount) {
        console.log(
          `Schema change detected: ${summary.tables} tables (was ${previousTableCount}), ` +
          `${summary.views} views (was ${previousViewCount})`
        );
      }
    } catch (error) {
      console.error('Background schema refresh failed:', error);
    }
  }, refreshIntervalMs);

  // Don't let the refresh interval keep the process alive
  schemaRefreshInterval.unref();
  console.log(`Background schema refresh enabled (interval: ${refreshIntervalMs / 1000}s)`);

  // Create Express app
  const app = express();

  // Middleware
  app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Mcp-Session-Id', 'Last-Event-Id'],
    exposedHeaders: ['Mcp-Session-Id'],
  }));
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // Auth middleware (conditional)
  const authMiddleware = useOAuth ? createAuthMiddleware() : null;

  // Health check endpoint
  app.get('/health', (_req, res) => {
    res.json({ status: 'healthy', server: SERVER_NAME, version: SERVER_VERSION });
  });

  // OAuth metadata endpoints (required for Claude Desktop discovery)
  if (useOAuth) {
    app.get('/.well-known/oauth-protected-resource', (_req, res) => {
      res.json({
        resource: EXTERNAL_BASE_URL,
        authorization_servers: [EXTERNAL_BASE_URL],
        bearer_methods_supported: ['header'],
      });
    });

    app.get('/.well-known/oauth-authorization-server', (_req, res) => {
      res.json({
        issuer: EXTERNAL_BASE_URL,
        authorization_endpoint: `${EXTERNAL_BASE_URL}/oauth/authorize`,
        token_endpoint: `${EXTERNAL_BASE_URL}/oauth/token`,
        registration_endpoint: `${EXTERNAL_BASE_URL}/oauth/register`,
        response_types_supported: ['code'],
        grant_types_supported: ['authorization_code', 'client_credentials'],
        code_challenge_methods_supported: ['S256'],
        token_endpoint_auth_methods_supported: ['none', 'client_secret_post'],
      });
    });

    // Dynamic Client Registration handler (RFC 7591)
    const handleClientRegistration = (req: Request, res: Response) => {
      const { redirect_uris, client_name } = req.body;

      if (!redirect_uris || !Array.isArray(redirect_uris) || redirect_uris.length === 0) {
        res.status(400).json({
          error: 'invalid_client_metadata',
          error_description: 'redirect_uris is required',
        });
        return;
      }

      const client_id = randomUUID();
      const client: OAuthClient = {
        client_id,
        redirect_uris,
        client_name: client_name || 'Unknown Client',
        created_at: Date.now(),
      };

      oauthClients[client_id] = client;
      console.log(`OAuth client registered: ${client_id} (${client.client_name})`);

      res.status(201).json({
        client_id,
        client_id_issued_at: Math.floor(client.created_at / 1000),
        redirect_uris,
        client_name: client.client_name,
        token_endpoint_auth_method: 'none',
      });
    };

    // Register at both paths (Claude Code uses /register, spec says /oauth/register)
    app.post('/oauth/register', handleClientRegistration);
    app.post('/register', handleClientRegistration);

    // OAuth authorize endpoint - shows login form if password is configured
    const authPassword = process.env.MCP_AUTH_PASSWORD;

    // Login page HTML
    const getLoginPage = (error?: string, redirect_uri?: string, state?: string, client_id?: string, code_challenge?: string, code_challenge_method?: string) => `
<!DOCTYPE html>
<html>
<head>
  <title>PgLLens - Authorization</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Commissioner:wght@400;700&family=Exo:wght@500&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
  <noscript><link href="https://fonts.googleapis.com/css2?family=Commissioner:wght@400;700&family=Exo:wght@500&display=swap" rel="stylesheet"></noscript>
  <style>
    body { font-family: 'Commissioner', Arial, sans-serif;
           display: flex; justify-content: center; align-items: center; height: 100vh;
           margin: 0; background: #252525; color: #fff; }
    .container { background: #1e1e1e; padding: 2.5rem; border-radius: 8px;
                 box-shadow: 0 4px 24px rgba(0,0,0,0.4); width: 340px;
                 border-top: 3px solid; border-image: linear-gradient(90deg, #D4FDB1, #B3E6E1) 1; }
    .brand { text-align: center; margin-bottom: 1.5rem; }
    .brand h1 { margin: 0; font-size: 1.6rem; font-weight: 700; color: #fff; }
    .brand h1 span { background: linear-gradient(90deg, #D4FDB1, #B3E6E1);
                     -webkit-background-clip: text; -webkit-text-fill-color: transparent;
                     background-clip: text; }
    .subtitle { font-family: 'Exo', Arial, sans-serif; font-weight: 500;
                text-transform: uppercase; letter-spacing: 0.075em;
                font-size: 0.7rem; color: #888; margin: 0.5rem 0 0 0; }
    p { margin: 0 0 1.5rem 0; color: #999; font-size: 0.9rem; text-align: center; }
    label { font-family: 'Exo', Arial, sans-serif; font-weight: 500;
            text-transform: uppercase; letter-spacing: 0.075em;
            font-size: 0.7rem; color: #888; display: block; margin-bottom: 0.4rem; }
    input[type="password"] { width: 100%; padding: 0.75rem; margin-bottom: 1.25rem;
            border: 1px solid #3a3a3a; border-radius: 4px; background: #252525;
            color: #fff; box-sizing: border-box; font-family: 'Commissioner', Arial, sans-serif;
            font-size: 0.95rem; transition: border-color 0.2s; }
    input[type="password"]:focus { outline: none; border-color: #B3E6E1; }
    button { width: 100%; padding: 0.75rem; border: none; border-radius: 4px;
             cursor: pointer; font-weight: 700; font-size: 0.95rem;
             font-family: 'Commissioner', Arial, sans-serif;
             background: linear-gradient(90deg, #D4FDB1, #B3E6E1); color: #252525;
             transition: opacity 0.2s; }
    button:hover { opacity: 0.88; }
    .error { color: #F5602B; margin-bottom: 1rem; font-size: 0.85rem; text-align: center; }
    .footer { text-align: center; margin-top: 1.5rem; }
    .footer a { font-family: 'Exo', Arial, sans-serif; font-weight: 500;
                text-transform: uppercase; letter-spacing: 0.075em;
                font-size: 0.65rem; color: #555; text-decoration: none; }
    .footer a:hover { color: #B3E6E1; }
  </style>
</head>
<body>
  <div class="container">
    <div class="brand">
      <h1><span>PgLLens</span></h1>
      <p class="subtitle">MCP Authorization</p>
    </div>
    <p>Enter password to continue</p>
    ${error ? `<div class="error">${error}</div>` : ''}
    <form method="POST">
      <input type="hidden" name="redirect_uri" value="${redirect_uri || ''}" />
      <input type="hidden" name="state" value="${state || ''}" />
      <input type="hidden" name="client_id" value="${client_id || ''}" />
      <input type="hidden" name="code_challenge" value="${code_challenge || ''}" />
      <input type="hidden" name="code_challenge_method" value="${code_challenge_method || ''}" />
      <label for="password">Password</label>
      <input type="password" id="password" name="password" placeholder="Enter passphrase" autofocus required />
      <button type="submit">Authorize</button>
    </form>
    <div class="footer">
      <a href="https://abelara.com" target="_blank" rel="noopener">Abelara</a>
    </div>
  </div>
</body>
</html>`;

    // GET - show login form (or auto-approve if no password set)
    app.get('/oauth/authorize', (req, res) => {
      const { redirect_uri, state, client_id, code_challenge, code_challenge_method } = req.query;

      // If no password configured, auto-approve (backward compatible)
      if (!authPassword) {
        const code = randomUUID();
        authCodes[code] = {
          client_id: client_id as string,
          redirect_uri: redirect_uri as string,
          expires: Date.now() + 60000,
        };
        res.redirect(`${redirect_uri}?code=${code}&state=${state}`);
        return;
      }

      // Show login form
      res.send(getLoginPage(undefined, redirect_uri as string, state as string,
        client_id as string, code_challenge as string, code_challenge_method as string));
    });

    // POST - validate password and issue code
    app.post('/oauth/authorize', (req, res) => {
      const { redirect_uri, state, client_id, code_challenge, code_challenge_method, password } = req.body;
      const clientIp = req.ip || req.socket.remoteAddress || 'unknown';

      // Check rate limiting
      const rateLimitError = checkRateLimit(clientIp);
      if (rateLimitError) {
        res.status(429).send(getLoginPage(rateLimitError, redirect_uri, state,
          client_id, code_challenge, code_challenge_method));
        return;
      }

      // Validate password using timing-safe comparison
      if (!safeComparePasswords(password, authPassword!)) {
        recordFailedAttempt(clientIp);
        res.status(401).send(getLoginPage('Invalid password', redirect_uri, state,
          client_id, code_challenge, code_challenge_method));
        return;
      }

      // Password correct - clear rate limit state
      clearRateLimit(clientIp);

      // Generate authorization code
      const code = randomUUID();
      authCodes[code] = {
        client_id: client_id as string,
        redirect_uri: redirect_uri as string,
        code_challenge: code_challenge as string,
        code_challenge_method: code_challenge_method as string,
        expires: Date.now() + 60000, // 1 minute expiry
      };

      console.log(`Authorization granted for client: ${client_id}`);
      res.redirect(`${redirect_uri}?code=${code}&state=${state}`);
    });

    // OAuth token endpoint (exchanges code for access token OR handles client_credentials)
    app.post('/oauth/token', (req, res) => {
      const { code, grant_type, client_id, client_secret } = req.body;

      // Support client_credentials grant for Claude Code (machine-to-machine)
      if (grant_type === 'client_credentials') {
        // For local dev, accept any registered client or auto-register
        if (client_id && !oauthClients[client_id]) {
          // Auto-register the client for convenience
          oauthClients[client_id] = {
            client_id,
            client_secret,
            redirect_uris: [],
            client_name: 'Auto-registered Client',
            created_at: Date.now(),
          };
          console.log(`Auto-registered OAuth client: ${client_id}`);
        }

        res.json({
          access_token: randomUUID(),
          token_type: 'Bearer',
          expires_in: OAUTH_TOKEN_EXPIRES_IN,
        });
        return;
      }

      // Handle authorization_code grant (for Claude Desktop browser flow)
      if (grant_type === 'authorization_code') {
        // Validate authorization code
        const authCode = authCodes[code];
        if (!authCode || authCode.expires < Date.now()) {
          delete authCodes[code];
          res.status(400).json({
            error: 'invalid_grant',
            error_description: 'Invalid or expired authorization code',
          });
          return;
        }

        // Clean up used code
        delete authCodes[code];

        res.json({
          access_token: randomUUID(),
          token_type: 'Bearer',
          expires_in: OAUTH_TOKEN_EXPIRES_IN,
        });
        return;
      }

      // Unsupported grant type
      res.status(400).json({
        error: 'unsupported_grant_type',
        error_description: 'Supported grant types: authorization_code, client_credentials',
      });
    });
  }

  // MCP POST handler - Initialize sessions and handle requests
  const mcpPostHandler = async (req: Request, res: Response) => {
    try {
      const sessionId = req.headers['mcp-session-id'] as string | undefined;
      let transport: StreamableHTTPServerTransport;

      if (sessionId && transports[sessionId]) {
        // Existing session
        transport = transports[sessionId];
      } else if (!sessionId && isInitializeRequest(req.body)) {
        // New session initialization
        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (sid) => {
            console.log(`Session initialized: ${sid}`);
            transports[sid] = transport;
          },
        });

        transport.onclose = () => {
          const sid = transport.sessionId;
          if (sid && transports[sid]) {
            console.log(`Session closed: ${sid}`);
            delete transports[sid];
          }
        };

        // Connect transport to MCP server
        const server = createMcpServer();
        await server.connect(transport);

        await transport.handleRequest(req, res, req.body);
        return;
      } else {
        res.status(400).json({
          jsonrpc: '2.0',
          error: { code: -32000, message: 'Bad Request: No valid session ID' },
          id: null,
        });
        return;
      }

      await transport.handleRequest(req, res, req.body);
    } catch (error) {
      console.error('Error handling MCP request:', error);
      if (!res.headersSent) {
        res.status(500).json({
          jsonrpc: '2.0',
          error: { code: -32603, message: 'Internal server error' },
          id: null,
        });
      }
    }
  };

  // MCP GET handler - SSE streams
  const mcpGetHandler = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;

    if (!sessionId || !transports[sessionId]) {
      res.status(400).send('Invalid or missing session ID');
      return;
    }

    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
  };

  // MCP DELETE handler - Session termination
  const mcpDeleteHandler = async (req: Request, res: Response) => {
    const sessionId = req.headers['mcp-session-id'] as string | undefined;

    if (!sessionId || !transports[sessionId]) {
      res.status(400).send('Invalid or missing session ID');
      return;
    }

    const transport = transports[sessionId];
    await transport.handleRequest(req, res);
  };

  // Register routes with conditional auth
  if (authMiddleware) {
    app.post('/mcp', authMiddleware, mcpPostHandler);
    app.get('/mcp', authMiddleware, mcpGetHandler);
    app.delete('/mcp', authMiddleware, mcpDeleteHandler);
  } else {
    app.post('/mcp', mcpPostHandler);
    app.get('/mcp', mcpGetHandler);
    app.delete('/mcp', mcpDeleteHandler);
  }

  // Start server
  const httpServer = app.listen(MCP_PORT, () => {
    console.log(`\n${SERVER_NAME} v${SERVER_VERSION} running on port ${MCP_PORT}`);
    console.log(`  MCP endpoint: http://localhost:${MCP_PORT}/mcp`);
    console.log(`  Health check: http://localhost:${MCP_PORT}/health`);
    if (useOAuth) {
      console.log(`  OAuth enabled: http://localhost:${MCP_PORT}/.well-known/oauth-protected-resource`);
    } else {
      console.log(`  OAuth: disabled (use --oauth flag to enable)`);
    }
    console.log('');
  });

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\nShutting down...');

    // Close all transports
    for (const sessionId in transports) {
      try {
        await transports[sessionId].close();
        delete transports[sessionId];
      } catch (error) {
        console.error(`Error closing session ${sessionId}:`, error);
      }
    }

    // Close database pool
    await closePool();

    httpServer.close(() => {
      console.log('Server shutdown complete.');
      process.exit(0);
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
