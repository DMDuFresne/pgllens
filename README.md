<div align="center">

<img src="images/Abelara_Icons_RGB-03.png" alt="PgLLens" width="120">

# PgLLens

### Give AI Eyes on Your Database

**The self-documenting PostgreSQL MCP server that turns any Postgres database into a rich, queryable knowledge source for AI models.**

[![MCP SDK](https://img.shields.io/badge/MCP_SDK-1.25+-blue?style=flat-square)](https://modelcontextprotocol.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.6+-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![Node.js](https://img.shields.io/badge/Node.js-20+-339933?style=flat-square&logo=nodedotjs&logoColor=white)](https://nodejs.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

[Getting Started](#getting-started) · [Tools](#tools) · [Configuration](#configuration) · [Docker](#docker) · [Security](#security) · [Domain Context](#domain-context)

</div>

---

## Why PgLLens?

AI models are powerful — but they're blind to your database. They don't know your schema, your constraints, your business rules, or what's actually in your tables.

**PgLLens fixes that.** It connects any PostgreSQL database to AI assistants via the [Model Context Protocol (MCP)](https://modelcontextprotocol.io), giving them 19 purpose-built tools to discover, understand, and query your data — safely, in read-only mode, with zero risk of accidental writes.

### What makes PgLLens different

- **Self-documenting** — Automatically extracts PostgreSQL comments, constraints, enums, triggers, and view definitions. Your AI assistant understands your schema the way your team does.
- **Domain-aware** — Inject a simple markdown file with business context (what tables mean, common query patterns, gotchas) and PgLLens weaves it into every tool response.
- **Security-first** — Defense-in-depth: SQL keyword blocking, read-only transactions, statement timeouts, row limits, and optional OAuth with rate limiting. Your data stays safe.
- **Works with read-only users** — Built on `pg_catalog` introspection, not `information_schema`. Fully functional even with the most restricted database roles.
- **Production-ready** — Docker image with health checks, graceful shutdown, background schema caching, and session management. Deploy in minutes.

---

## Getting Started

### Prerequisites

- **Node.js** 20+
- **PostgreSQL** 12+ (any hosted or self-managed instance)
- A database user with `SELECT` privileges (read-only recommended)

### Quick Start (npm)

```bash
# Clone and install
git clone https://github.com/DMDuFresne/pgllens.git
cd pgllens
npm install

# Configure
cp .env.example .env
# Edit .env with your DATABASE_URL

# Build and run
npm run build
npm start
```

PgLLens is now running at `http://localhost:3000` with the MCP endpoint at `/mcp`.

### Quick Start (Docker)

```bash
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/mydb" \
  dmdufresne/pgllens:latest
```

### Connect to Claude Desktop

Add PgLLens to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "pgllens": {
      "url": "http://localhost:3000/mcp"
    }
  }
}
```

With OAuth enabled:

```json
{
  "mcpServers": {
    "pgllens": {
      "url": "http://localhost:3000/mcp",
      "authorizationUrl": "http://localhost:3000/oauth/authorize",
      "tokenUrl": "http://localhost:3000/oauth/token",
      "registrationUrl": "http://localhost:3000/oauth/register"
    }
  }
}
```

### Connect to Claude Code

```json
{
  "mcpServers": {
    "pgllens": {
      "type": "url",
      "url": "http://localhost:3000/mcp"
    }
  }
}
```

---

## Tools

PgLLens exposes **19 MCP tools** organized into five categories:

### Core Query Tools

| Tool | Description |
|------|-------------|
| **`query`** | Execute read-only SQL SELECT queries. Results returned as markdown tables with row counts and truncation warnings. |
| **`explain_query`** | Get the execution plan for any query. Supports `EXPLAIN ANALYZE` for actual runtime statistics — essential for performance tuning. |
| **`validate_query`** | Check SQL syntax and verify table/column references without executing the query. Catch errors before they happen. |

### Schema Discovery Tools

| Tool | Description |
|------|-------------|
| **`list_tables`** | List all tables and views with their descriptions and row counts. Filter by schema. |
| **`list_functions`** | Discover stored functions and procedures with parameters, return types, and volatility. |
| **`describe_table`** | Deep-dive into any table or view: column types, nullability, defaults, primary keys, foreign keys, and comments. |
| **`schema_overview`** | Get the complete database schema as a markdown summary — tables, views, and functions with column counts and descriptions. |
| **`refresh_schema`** | Force-refresh the MCP server's metadata cache. Use after DDL changes to see updates immediately. Does not run PostgreSQL ANALYZE. |

### Data Exploration Tools

| Tool | Description |
|------|-------------|
| **`get_sample_data`** | Retrieve 1–20 sample rows from any table to understand data patterns and actual values. |
| **`search_columns`** | Search for columns by name pattern across all tables and views. Find related fields instantly. |
| **`get_relationships`** | Map foreign key relationships between tables. Output as text or as a **Mermaid ER diagram** for instant visualization. |
| **`get_table_stats`** | Table statistics: row counts, null percentages, and distinct value counts for every column. |

### Semantic Discovery Tools

| Tool | Description |
|------|-------------|
| **`get_ontology`** | The most powerful discovery tool. Returns the semantic layer of your database: CHECK constraints, ENUM types, UNIQUE keys, view definitions, indexes, triggers, row counts, and injected domain context. Filterable by section and table. Warns prominently about never-analyzed tables. |

### DBA & Operations Tools

| Tool | Description |
|------|-------------|
| **`get_table_health`** | Vacuum/analyze history, dead tuple counts, table/index sizes, and per-index usage stats. Identifies unused indexes wasting disk space. |
| **`get_view_definition`** | Full SQL definition and column listing for any view or materialized view. |
| **`get_function_source`** | Full source code and metadata for functions and procedures, with overload handling. |
| **`list_extensions`** | List installed PostgreSQL extensions with version, schema, and description. |
| **`list_roles`** | Database roles and their table-level privileges. Filter by schema or role. |
| **`list_hypertables`** | TimescaleDB awareness: hypertables, chunk intervals, compression policies, continuous aggregates, and chunk statistics. |

---

## Configuration

PgLLens is configured via environment variables. Create a `.env` file or pass them directly.

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@host:5432/mydb` |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_PORT` | `3000` | HTTP server port |
| `EXPOSED_SCHEMAS` | `public` | Comma-separated list of schemas to expose |
| `QUERY_TIMEOUT_MS` | `30000` | Maximum query execution time (ms) |
| `MAX_ROWS` | `1000` | Maximum rows returned per query |
| `SCHEMA_REFRESH_INTERVAL_MS` | `300000` | Schema cache refresh interval (ms) |
| `DOMAIN_CONTEXT_FILE` | — | Path to a markdown file with business context |
| `DOMAIN_CONTEXT` | — | Inline domain context string (alternative to file) |

### OAuth Options (when running with `--oauth`)

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_AUTH_PASSWORD` | — | Password for OAuth login form |
| `EXTERNAL_BASE_URL` | `http://localhost:3000` | Public URL (for running behind a proxy) |
| `MCP_OAUTH_TOKEN_EXPIRES_IN` | `604800` | Token lifetime in seconds (default: 7 days) |
| `MCP_RATE_LIMIT_ATTEMPTS` | `5` | Max login attempts per window |
| `MCP_RATE_LIMIT_WINDOW_MS` | `900000` | Rate limit window (ms, default: 15 min) |

---

## Docker

### Build

```bash
docker build -t pgllens .
```

### Run

```bash
# Without OAuth (local development, trusted networks)
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/mydb" \
  -e EXPOSED_SCHEMAS="public,app" \
  pgllens

# With OAuth (production, Claude Desktop)
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/mydb" \
  -e MCP_AUTH_PASSWORD="your-secure-password" \
  pgllens node dist/index.js --oauth

# With custom domain context
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/mydb" \
  -v ./my-context.md:/app/context.md \
  -e DOMAIN_CONTEXT_FILE="context.md" \
  pgllens
```

### Docker Compose

```yaml
services:
  pgllens:
    image: dmdufresne/pgllens:latest
    ports:
      - "3000:3000"
    environment:
      DATABASE_URL: postgresql://readonly:password@db:5432/myapp
      EXPOSED_SCHEMAS: public,app
      MAX_ROWS: 500
    volumes:
      - ./context.md:/app/context.md
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:3000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped
```

### Image Details

- **Base:** `node:20-alpine` (multi-stage build)
- **Size:** ~80MB
- **User:** Non-root (`nodejs:1001`)
- **Health check:** Built-in via `/health` endpoint

---

## Security

PgLLens implements defense-in-depth to protect your database:

### SQL Keyword Blocking

All queries are scanned for dangerous keywords **before execution**. The following operations are blocked at the application level, regardless of database permissions:

`INSERT` · `UPDATE` · `DELETE` · `DROP` · `CREATE` · `ALTER` · `TRUNCATE` · `GRANT` · `REVOKE` · `COPY`

The scanner neutralizes string literals and comments first, preventing false positives from queries like `SELECT * FROM logs WHERE message LIKE '%DELETE%'`.

### Read-Only Transactions

Every query executes inside a `BEGIN READ ONLY` transaction — a PostgreSQL-level guarantee that no writes can occur, even if the keyword scanner is bypassed.

### Statement Timeouts

Queries are killed after the configured timeout (default: 30 seconds), preventing runaway queries from consuming database resources.

### Row Limits

Results are capped at a configurable maximum (default: 1,000 rows) with a truncation warning, preventing accidental data dumps.

### OAuth + Rate Limiting

When running with `--oauth`, PgLLens provides:

- **RFC 7591** Dynamic Client Registration
- **PKCE** support (S256 code challenge)
- **Rate limiting** on password attempts (5 per 15 minutes by default)
- **Timing-safe** password comparison (prevents timing attacks)
- **Bearer token** validation on all MCP endpoints

### Recommended Database Role

```sql
-- Create a minimal read-only role
CREATE ROLE pgllens_readonly WITH LOGIN PASSWORD 'your-secure-password';
GRANT CONNECT ON DATABASE mydb TO pgllens_readonly;
GRANT USAGE ON SCHEMA public TO pgllens_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgllens_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pgllens_readonly;
```

---

## Domain Context

This is PgLLens's secret weapon. While schema introspection tells the AI **what** your tables look like, domain context tells it **what they mean**.

### How It Works

Create a markdown file describing your database's business logic, then point PgLLens at it:

```bash
# Via environment variable
DOMAIN_CONTEXT_FILE=context.md

# Or inline
DOMAIN_CONTEXT="This database tracks e-commerce orders. The 'status' column uses: pending, processing, shipped, delivered, cancelled."
```

PgLLens injects this context into tool descriptions, so your AI assistant understands your domain from the very first interaction.

### Example `context.md`

```markdown
# E-Commerce Database

## Key Concepts
- **Orders** go through a lifecycle: pending → processing → shipped → delivered
- **SKUs** follow the format: {CATEGORY}-{SEQUENCE} (e.g., ELEC-0042)
- Soft-deleted records have `deleted_at IS NOT NULL` — always filter these

## Common Queries
- Active orders: `SELECT * FROM orders WHERE status != 'cancelled' AND deleted_at IS NULL`
- Revenue by month: Use the `vw_monthly_revenue` view

## Gotchas
- The `price` column is stored in cents (integer), not dollars
- `created_at` is always UTC, `local_time` is the customer's timezone
```

### What Gets Enriched

Domain context is woven into:

- The `query` tool description (so AI writes better SQL)
- The `get_ontology` tool results (so AI understands business rules)
- Schema discovery responses (so AI asks better follow-up questions)

---

## API Reference

### Health Check

```
GET /health
```

Returns server status and version:

```json
{
  "status": "healthy",
  "server": "postgres-mcp",
  "version": "2.1.0"
}
```

### MCP Endpoint

```
POST /mcp          → JSON-RPC 2.0 request
GET  /mcp          → Server-Sent Events (SSE) stream
DELETE /mcp        → Session termination
```

All MCP communication uses [Streamable HTTP Transport](https://modelcontextprotocol.io/docs/concepts/transports#streamable-http) with session management via the `mcp-session-id` header.

### OAuth Endpoints (when `--oauth` enabled)

```
GET  /.well-known/oauth-protected-resource  → Resource metadata
GET  /.well-known/oauth-authorization-server → Server metadata
POST /oauth/register                         → Dynamic client registration
GET  /oauth/authorize                        → Login form
POST /oauth/authorize                        → Authenticate
POST /oauth/token                            → Token exchange
```

---

## Development

```bash
# Install dependencies
npm install

# Run in dev mode (hot reload)
npm run dev

# Run with OAuth in dev mode
npm run dev:oauth

# Type check
npm run typecheck

# Build for production
npm run build
```

### Project Structure

```
src/
├── index.ts                 # Entry point
├── config.ts                # Environment config with Zod validation
├── server.ts                # Express + MCP server, OAuth, session management
├── database/
│   ├── client.ts            # Connection pool, read-only query execution
│   └── schema-loader.ts     # pg_catalog introspection, schema caching
├── tools/
│   ├── index.ts             # Executor re-exports
│   ├── query.ts             # Execute SELECT queries (markdown output)
│   ├── list-tables.ts       # List tables and views
│   ├── list-functions.ts    # List stored functions
│   ├── describe-table.ts    # Column-level table details
│   ├── schema-overview.ts   # Full schema as markdown summary
│   ├── get-sample-data.ts   # Sample rows from tables
│   ├── search-columns.ts    # Cross-table column search
│   ├── explain-query.ts     # EXPLAIN / EXPLAIN ANALYZE
│   ├── validate-query.ts    # Syntax validation
│   ├── get-relationships.ts # FK relationships + Mermaid diagrams
│   ├── get-table-stats.ts   # Row counts, null %, distinct values
│   ├── refresh-schema.ts    # Force MCP cache refresh
│   ├── get-ontology.ts      # Semantic schema discovery
│   ├── get-table-health.ts  # Vacuum/analyze stats, dead tuples, index usage
│   ├── get-view-definition.ts # View SQL definitions
│   ├── get-function-source.ts # Function/procedure source code
│   ├── list-extensions.ts   # PostgreSQL extensions
│   ├── list-roles.ts        # Database roles and privileges
│   └── list-hypertables.ts  # TimescaleDB hypertable details
├── descriptions/
│   ├── generator.ts         # Dynamic description builder
│   └── static.ts            # Fallback descriptions
└── types/
    └── index.ts             # TypeScript interfaces
```

---

## Use Cases

| Use Case | How PgLLens Helps |
|----------|-----------------|
| **AI-powered data analysis** | Let Claude query your database directly, explore schemas, and write complex SQL — safely in read-only mode. |
| **Database documentation** | Generate ER diagrams, table docs, and relationship maps automatically from your live schema. |
| **Onboarding new developers** | Point an AI at your database with domain context and let it answer "what does this table do?" questions instantly. |
| **Query optimization** | Use `explain_query` with ANALYZE to identify slow queries and missing indexes through natural conversation. |
| **Data quality audits** | Combine `get_table_stats` and `get_ontology` to surface null rates, constraint gaps, and data anomalies. |
| **Schema migration planning** | Use `schema_overview` and `get_relationships` to understand impact before making DDL changes. |

---

## Compatibility

PgLLens works with any MCP-compatible client:

- **Claude Desktop** (with or without OAuth)
- **Claude Code** (CLI)
- **Cursor** / **Windsurf** / **VS Code** (via MCP extensions)
- **Custom MCP clients** (any client implementing the MCP specification)

And any PostgreSQL-compatible database:

- PostgreSQL 12+
- Amazon RDS / Aurora
- Google Cloud SQL
- Azure Database for PostgreSQL
- Supabase
- Neon
- TimescaleDB
- CockroachDB (PostgreSQL wire protocol)

---

## Troubleshooting

### "Permission denied" on schema introspection

PgLLens uses `pg_catalog` (not `information_schema`) specifically to work with restricted users. Ensure your user has at minimum:

```sql
GRANT USAGE ON SCHEMA your_schema TO your_user;
GRANT SELECT ON ALL TABLES IN SCHEMA your_schema TO your_user;
```

### Foreign keys not showing up

Composite foreign keys require the `pg_catalog` approach. If you're seeing incomplete relationships, ensure `EXPOSED_SCHEMAS` includes all relevant schemas.

### Schema changes not reflected

PgLLens caches schema metadata. Either wait for the next refresh cycle (default: 5 minutes) or call `refresh_schema` to update the MCP cache immediately. Note that `refresh_schema` only refreshes the MCP server's internal cache — row count estimates in `get_ontology` come from PostgreSQL's `pg_class.reltuples` and require running `ANALYZE` to update.

### OAuth login fails

Check `MCP_AUTH_PASSWORD` is set and the rate limiter hasn't kicked in (5 attempts per 15 minutes by default). Check server logs for details.

---

## License

MIT License. Free to use, modify, and distribute with attribution. Provided "as is" with no warranty of any kind — use at your own risk. See below.

<br><br>

<div align="center">

<a href="https://abelara.com">
  <img src="images/abelara-logo.svg" alt="Abelara" width="200">
</a>

**Built by [Abelara](https://abelara.com)**

PgLLens is part of the Abelara toolkit for industrial AI and edge computing.

[Report Bug](https://github.com/DMDuFresne/pgllens/issues) · [Request Feature](https://github.com/DMDuFresne/pgllens/issues) · [Learn More](https://abelara.com)

</div>
