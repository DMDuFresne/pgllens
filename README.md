<div align="center">

<img src="images/pgllens-icon.png" alt="PgLLens" width="120">

# PgLLens

### Give AI Eyes on Your Database

**A read-only MCP server that turns any PostgreSQL database into a queryable knowledge source for AI models.**

[![MCP SDK](https://img.shields.io/badge/MCP_SDK-2.0+-blue?style=flat-square)](https://modelcontextprotocol.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12+-336791?style=flat-square&logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org)
[![uv](https://img.shields.io/badge/uv-managed-DE5FE9?style=flat-square)](https://docs.astral.sh/uv/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-Apache_2.0-green?style=flat-square)](LICENSE)
[![CI](https://github.com/DMDuFresne/pgllens/actions/workflows/ci.yml/badge.svg)](https://github.com/DMDuFresne/pgllens/actions/workflows/ci.yml)
[![Stack](https://github.com/DMDuFresne/pgllens/actions/workflows/stack.yml/badge.svg)](https://github.com/DMDuFresne/pgllens/actions/workflows/stack.yml)

[Getting Started](#getting-started) · [Tools](#tools) · [Domain Context](#domain-context) · [Docs](#docs)

</div>

---

## Why PgLLens?

AI models are blind to your database: they do not know your schema, constraints, business rules,
or what is actually in your tables. PgLLens connects a PostgreSQL database to any
[MCP](https://modelcontextprotocol.io) client with 31 read-only tools that discover, explain,
diagnose, and query it.

- **Self-documenting.** Extracts comments, constraints, foreign keys, and view definitions, plus a
  semantic `get_ontology` summary (hub tables, naming conventions, soft-delete and audit columns).
- **Domain-aware.** A markdown file of business context is woven into tool output ([below](#domain-context)).
- **Read-only by construction.** Every connection opens with `default_transaction_read_only=on`,
  enforced by PostgreSQL; a SQL-text gate, timeouts, row limits, rate limits, column redaction, an
  audit trail, and optional OAuth 2.1 sit on top. Details: [`docs/DEPLOY.md`](docs/DEPLOY.md#security-posture).
- **DBA-aware.** Blocking sessions, wait stats, index health, vacuum/bloat, space usage, and
  `pg_stat_statements`, live and never cached.
- **Works with restricted roles.** Built on `pg_catalog`, not `information_schema`, so it is fully
  functional with a least-privilege reader role.
- **Production-ready.** One distroless container with health checks, Prometheus metrics,
  OpenTelemetry tracing, and an optional Grafana stack.

## Getting Started

### Prerequisites

- **Python** 3.11+ (or Docker, which needs nothing else installed)
- **PostgreSQL** 12+ (any hosted or self-managed instance)
- A database user with `SELECT` privileges (read-only recommended; see [Database role](docs/DEPLOY.md#database-role))

### Quick Start (uv)

```bash
git clone https://github.com/DMDuFresne/pgllens.git
cd pgllens
uv sync
cp .env.example .env      # set DATABASE_URL and EXPOSED_SCHEMAS
uv run pgllens
```

PgLLens is now running at `http://localhost:3000` with the MCP endpoint at `/mcp` and a health
check at `/health`.

### Quick Start (Docker)

```bash
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://user:pass@host:5432/mydb" \
  ghcr.io/dmdufresne/pgllens:2.0.0
```

`docker-compose.yml` runs the same image with the Prometheus/Grafana/Loki/Tempo stack behind
profiles; pick files and profiles once in `.env` (`COMPOSE_FILE`, `COMPOSE_PROFILES`), then
`docker compose up -d --build` (dev, builds from the checkout) or
`docker compose pull && docker compose up -d` (prod, pulls from GHCR). All tiers, every
environment variable, and the hardening applied: [`docs/DEPLOY.md`](docs/DEPLOY.md).

### Connect to Claude Desktop

```json
{
  "mcpServers": {
    "pgllens": { "url": "http://localhost:3000/mcp" }
  }
}
```

With password-mode OAuth enabled (`MCP_AUTH_MODE=password`), add the three OAuth routes:

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
    "pgllens": { "type": "url", "url": "http://localhost:3000/mcp" }
  }
}
```

Any MCP-compatible client works the same way (Cursor, Windsurf, VS Code MCP extensions, custom
clients). Supported databases and hosts: [`docs/DEPLOY.md#compatibility`](docs/DEPLOY.md#compatibility).

### Skills

`skills/` bundles eight [Claude skills](https://docs.claude.com/en/docs/claude-code/skills)
(`pgllens-using`, `pgllens-explore-a-database`, `pgllens-write-a-query`, `pgllens-tune-a-query`,
`pgllens-health-check`, `pgllens-triage-an-incident`, `pgllens-document-a-schema`,
`pgllens-verify-a-deployment`) that teach Claude how to drive PgLLens. Start with `pgllens-using`
(read-only posture, house rules, routing to the other seven); copy the directories into your
project's `.claude/skills/` and Claude Code picks the right one from each skill's trigger phrases.
See [`skills/README.md`](skills/README.md) for the index.

## Tools

31 tools, all read-only, in five groups. Parameters, return shapes, and the ERD widget:
[`docs/tools.md`](docs/tools.md).

| Group | Tools | What it covers |
|---|---|---|
| Discovery | 7 | Tables, columns, schema overview, column search, sample rows, per-column stats, cache refresh. |
| Business rules and structure | 5 | Ontology, foreign-key relationships, shortest join path, and the ERD as Mermaid or an interactive widget. |
| Programmable objects | 5 | Functions and their source, view definitions, constraints, triggers. |
| Query execution | 3 | Run, validate, or `EXPLAIN` a read-only query, with pagination and planner-cost guards. |
| DBA and diagnostics | 11 | Server info, sessions, blocking, waits, index and table health, space, `pg_stat_statements`, TimescaleDB hypertables, extensions, roles. |

## Domain Context

Point `DOMAIN_CONTEXT_FILE` at a markdown file (template: [`ops/context.md`](ops/context.md)) describing
what your tables mean, naming conventions, and gotchas an LLM writing SQL against your schema
should know. It is prepended to `get_ontology` output and to the MCP server's `instructions`
field, so the assistant sees it before writing a query. Optional; the shipped template is inert
until filled in.

## Docs

| Doc | Owns |
|---|---|
| [`docs/tools.md`](docs/tools.md) | The per-tool catalog, response shape, ERD widget, tool visibility, read-only gate. |
| [`docs/DEPLOY.md`](docs/DEPLOY.md) | Every environment variable, Docker and compose tiers, the database role, authentication, reverse proxy and tunnel, security posture, supply chain, health check, compatibility. |
| [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) | Metrics, tracing, audit records and sinks, alert rules, self-monitoring, `/metrics` exposure, proof of readiness. |
| [`docs/runbook.md`](docs/runbook.md) | Troubleshooting: startup, connections, stale tool catalogs, error codes, alerts. |
| [`docs/OKTA.md`](docs/OKTA.md) | `MCP_AUTH_MODE=okta` resource-server setup. |
| [`docs/STYLE.md`](docs/STYLE.md) | The LLens markdown style every tool response follows. |

## License

Apache License 2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE) for full terms, third-party
dependency licenses, and trademark notices. Provided "as is" with no warranty of any kind.

<br><br>

<div align="center">

<a href="https://abelara.com">
  <img src="images/abelara-logo.svg" alt="Abelara" width="200">
</a>

**Built by [Abelara](https://abelara.com)**

PgLLens is part of the Abelara toolkit for industrial AI and edge computing.

[Report Bug](https://github.com/DMDuFresne/pgllens/issues) · [Request Feature](https://github.com/DMDuFresne/pgllens/issues) · [Learn More](https://abelara.com)

</div>
