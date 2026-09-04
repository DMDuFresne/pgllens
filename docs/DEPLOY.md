# Deploy

PgLLens is a single stateless container (no database of its own) that talks to your PostgreSQL
instance over `psycopg` and speaks streamable-HTTP MCP on `:3000`. This doc owns everything about
running it: Docker and compose tiers, every environment variable, the least-privilege role,
authentication, a reverse proxy or tunnel in front of it, the security posture, the supply chain,
the health check, and compatibility. Metrics, tracing, and the audit trail are in
[`OBSERVABILITY.md`](OBSERVABILITY.md); what to do when it breaks is in [`runbook.md`](runbook.md).

## Docker

```bash
docker pull ghcr.io/dmdufresne/pgllens:${PGLLENS_VERSION:-2.0.0}   # or: docker build -t pgllens .
docker run -p 3000:3000 \
  -e DATABASE_URL="postgresql://pgllens_reader:...@host:5432/mydb" \
  -e EXPOSED_SCHEMAS=public \
  pgllens
```

`PGLLENS_VERSION` (default `2.0.0`) is the one lever for which image tag both `docker run` and
`docker-compose.yml` pull. The image is a two-stage build (`Dockerfile`): a `uv`-based builder
installs every dependency with `--require-hashes`, then a
`gcr.io/distroless/python3-debian12:nonroot` final stage carries only the venv and the app (no
shell, no package manager). It runs as uid 1001, never root; see [Supply chain](#supply-chain).
It does not bundle a PostgreSQL server; point it at yours via `DATABASE_URL`.

## Compose and tiers

`docker-compose.yml` and `ops/` are organized as four tiers selected by compose profile. Each tier
is a strict superset of the one below it: moving up never changes a value already set, and the app
never depends on the stack it sits inside (no `depends_on` from `pgllens` to any monitoring
service; an unreachable Tempo just means spans are dropped and logged once).

| Tier | Command | What runs |
|---|---|---|
| Play | `docker run -e DATABASE_URL=... -p 3000:3000 ghcr.io/dmdufresne/pgllens` | The image alone: `/health` and `/metrics` on, audit JSONL to stdout (`docker logs`) or to `AUDIT_LOG_FILE`/`AUDIT_SYSLOG` when set, tracing provider on but exporting nowhere. No compose, no `.env`, no volume. |
| Solo | `docker compose up -d` | pgllens only. Adds `.env`, the hardened service (`read_only`, `cap_drop: [ALL]`, `no-new-privileges`), the audit volume, and the optional `tunnel` profile. |
| Observed | `COMPOSE_PROFILES=observe` | Solo + Prometheus, Alertmanager, Grafana, Loki, Alloy, Tempo. Requires `GRAFANA_ADMIN_PASSWORD`. The Slack receiver and `Watchdog` heartbeat work here (they need only `SLACK_WEBHOOK_URL` and the rule files). |
| Production | `COMPOSE_PROFILES=observe,infra` | Observed + node-exporter, cAdvisor, postgres-exporter, plus retention and binds set for a real host. |

Profiles and compose files are chosen once per machine in `.env` (`COMPOSE_PROFILES`,
`COMPOSE_FILE`; compose reads both from there), so the commands never grow flags:

```bash
cp .env.example .env   # set DATABASE_URL, EXPOSED_SCHEMAS, GRAFANA_ADMIN_PASSWORD;
                       # uncomment one COMPOSE_FILE line and COMPOSE_PROFILES
docker compose up -d --build                    # dev: build from this checkout
docker compose pull && docker compose up -d     # prod: pull the published image
```

### Dev vs prod

The base `docker-compose.yml` is pull-only: it runs `ghcr.io/dmdufresne/pgllens:${PGLLENS_VERSION}`
and never builds. Dev adds `docker-compose.dev.yml` (via `COMPOSE_FILE`), which is the one place
`build: .` lives, so `docker compose up -d --build` rebuilds from the checkout. Prod leaves that
file out and needs no source tree on disk: only `docker-compose.yml`, `ops/demo/docker-compose.yml`
if the demo database is wanted, the `ops/` config directories the stack bind-mounts
(`ops/context.md`, `ops/prometheus`, `ops/alertmanager`, `ops/grafana`, `ops/loki`, `ops/alloy`,
`ops/tempo`, `ops/demo`), and `.env`. A sparse or shallow clone is enough.

### Publishing the image

Tag a release `vX.Y.Z` with `X.Y.Z` equal to `version` in `pyproject.toml` and push the tag;
`.github/workflows/release.yml` refuses a mismatch, then builds and pushes
`ghcr.io/dmdufresne/pgllens:X.Y.Z` and `:latest`. The GHCR package must be public for an
anonymous `docker compose pull`; otherwise the host needs a read token
(`docker login ghcr.io`). The default `PGLLENS_VERSION` must always name a tag the release
workflow has already published: the prod pull works once `v2.0.0` is tagged and pushed and the
release workflow has published it; before that it fails with `manifest unknown`.

Admin UIs with the `observe` profile: Grafana `http://localhost:3001` (admin /
`$GRAFANA_ADMIN_PASSWORD`), Prometheus `http://localhost:9090`, Alertmanager
`http://localhost:9093`. They bind `ADMIN_BIND` (default `127.0.0.1`, loopback only); the app's
`:3000` binds `APP_BIND`. Loki, Tempo, Alloy, and the `infra` exporters publish no ports and are
reachable only on the internal Docker network. Read [Reverse proxy](#reverse-proxy) before
exposing `:3000` past your own network.

"Enterprise" means "plugs into ours", not "run a bigger version of our stack": Solo plus
`OTEL_EXPORTER_OTLP_ENDPOINT` pointed at your own collector, and/or `AUDIT_SYSLOG` pointed at
your own syslog collector, is a fully supported shape with none of `ops/`'s containers involved.
At the Observed tier, `PROM_REMOTE_WRITE_URL` additionally forwards this stack's metrics to a
central Prometheus.

### Compose levers

One `.env` variable per lever, grouped in `.env.example` by the tier that first needs it. These are
read by compose, not by the app (the app's own variables are in the next section).

| Variable | Default | Tier | Description |
|---|---|---|---|
| `PGLLENS_VERSION` | `2.0.0` | Play/Solo | Image tag pulled. |
| `COMPOSE_FILE` | `docker-compose.yml` | any | Compose files to merge; add `docker-compose.dev.yml` to build from the checkout, `ops/demo/docker-compose.yml` for the demo database. |
| `COMPOSE_PROFILES` | unset | any | Profiles to enable (`observe`, `infra`, `tunnel`); replaces `--profile` flags. |
| `APP_BIND` | `127.0.0.1` | Play/Solo | Host bind for the app port. |
| `GRAFANA_ADMIN_PASSWORD` | (required) | Observed | Grafana admin password. |
| `ADMIN_BIND` | `127.0.0.1` | Observed | Host bind for Grafana/Prometheus/Alertmanager. |
| `PROM_RETENTION_TIME`, `PROM_RETENTION_SIZE` | `30d`, `2GB` | Observed | Prometheus TSDB retention. |
| `LOKI_RETENTION`, `TEMPO_RETENTION` | `2160h`, `168h` | Observed | Log and trace retention. |
| `PROM_REMOTE_WRITE_URL` | unset | Observed | Forward metrics to a central Prometheus. |
| `COMPOSE_PROJECT_NAME` | `pgllens` | Observed | Alloy discovers stack containers by the `com.docker.compose.project` label; set this if you run compose from a directory not named `pgllens` or with `-p`. |
| `SLACK_WEBHOOK_URL` | unset | Observed | Alertmanager Slack receiver. Unset, alerts route to a null receiver and are visible in the UI only. |
| `POSTGRES_EXPORTER_SSLMODE` | `disable` | Production | TLS mode for postgres-exporter when `DATABASE_URL` carries no `sslmode` (lib/pq has no `prefer`). An `sslmode` in the DSN wins. |
| `TUNNEL_TOKEN` | unset | any | Cloudflare Tunnel token for the `tunnel` profile. |

## Environment variables

Set as environment variables or in `.env` (`.env.example` is the commented reference).

### Connection

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | `postgresql://` URL naming the one database this server serves. `conninfo()` (`config.py`) appends `default_transaction_read_only=on`, `statement_timeout`, and `idle_in_transaction_session_timeout` as connection options. This is the hard read-only guarantee, enforced by PostgreSQL and independent of the SQL-text gate. |
| `DB_REQUIRE_VERIFY_FULL` | `false` | Refuse to start unless `DATABASE_URL` contains both `sslmode=verify-full` and a pinned `sslrootcert=`. `require`/`verify-ca` are rejected (they authenticate the wrong thing or nothing); `verify-full` without a pinned root still trusts the system CA store. Required for any internet deployment. |
| `DB_POOL_MAX_SIZE` | `5` | psycopg connection pool ceiling. `/health` shares this pool with tool calls, so it must exceed `MAX_CONCURRENT_CALLS_PER_CLIENT`. |
| `REDIS_URL` | unset | Shared rate-limit and token store across replicas (requires `pgllens[redis]`). Unset is in-memory, single process: fine for one container, wrong behind a load balancer where each replica would grant the full limit. |

### Scope and server

| Variable | Default | Description |
|---|---|---|
| `EXPOSED_SCHEMAS` | `public` | Comma-separated schema names this server may see or query; the allowlist is the whole universe, matched case-insensitively. Unset or empty falls back to `public`; a value with no names fails at boot. |
| `DEFAULT_SCHEMA` | first of `EXPOSED_SCHEMAS` | Default when a tool's `schema` argument is omitted. Must itself be in `EXPOSED_SCHEMAS`; the server refuses to start otherwise, since an unexposed default would be a silent hole. |
| `HOST` | `0.0.0.0` | Bind address. |
| `MCP_PORT` | `3000` | Bind port. |

### Limits

| Variable | Default | Description |
|---|---|---|
| `QUERY_TIMEOUT_MS` | `30000` | Per-query and idle-in-transaction timeout, both set as PostgreSQL session options. |
| `MAX_ROWS` | `200` | Row cap on any result set; tools report `truncated` if hit. (`.env.example` sets `1000`.) |
| `MAX_ESTIMATED_COST` | unset | Reject `query` calls whose `EXPLAIN` top-node cost (planner units) exceeds this. Unset is off. |
| `TOOL_COST_BUDGET_PER_MINUTE` | unset | Planner cost units one client may spend per minute. Unset is off. Meaningful across replicas only with `REDIS_URL`. |
| `SCHEMA_REFRESH_INTERVAL_MS` | `300000` | Introspection cache TTL; `refresh_schema` clears it early. |
| `TOOL_RATE_LIMIT_PER_MINUTE` | `120` | Inbound MCP tool calls allowed per client per minute. `0` is off. |
| `MAX_CONCURRENT_CALLS_PER_CLIENT` | `4` | In-flight tool calls per client (`0` is off). Stops one client occupying the whole pool; must stay below `DB_POOL_MAX_SIZE` or the server refuses to boot. |
| `REDACT_COLUMNS` | `%password%,%passwd%,%secret%,%api_key%,ssn,%_ssn,ssn_%,%_ssn_%,token,%_token,%_token_%` | Comma-separated column-name patterns (`%` any run, `_` literal, case-insensitive). The default matches `ssn` and `token` only as whole `_`-separated words (`user_ssn`, `api_token`; not `classname`, `token_count`). A value replaces the list, `off` disables masking, empty keeps the default. Matching output columns render as `[masked]` in `query` and `get_sample_data`. This is best-effort display masking by output-column name, not a security boundary: `SELECT upper(api_token)` or `SELECT api_token AS x` returns cleartext, because there is no SQL parser resolving output columns to their source. For a real guarantee use [column-level REVOKE](#column-level-secrecy-revoke-not-redact_columns). |

### Domain context

| Variable | Default | Description |
|---|---|---|
| `DOMAIN_CONTEXT` | unset | Inline domain-context text, prepended to `get_ontology` output and the MCP `instructions` field. |
| `DOMAIN_CONTEXT_FILE` | unset | Path to a domain-context file instead (`DOMAIN_CONTEXT` wins if both are set). Template: [`ops/context.md`](../ops/context.md). |

### Logging, audit, and observability

Behaviour and record format: [`OBSERVABILITY.md`](OBSERVABILITY.md).

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Python logging level. |
| `LOG_FORMAT` | `json` | `json` or `text`. |
| `AUDIT_LOG_FILE` | unset | Path to the append-only audit JSONL trail. In compose this is the only writable path under the read-only root, and Alloy tails `/audit/audit.jsonl` from the same volume, so changing it also means editing `ops/alloy/config.alloy`. |
| `AUDIT_SYSLOG` | unset | `host:port` of a UDP syslog collector to ship audit records to, alongside or instead of the file. |
| `AUDIT_STDOUT` | unset | Unset: stdout is used only when neither file nor syslog is set (Play tier). `true`: stdout always, alongside other sinks. `false`: never, which with no other sink turns auditing off. |
| `AUDIT_LOG_MAX_BYTES` | `100000000` | Size at which `AUDIT_LOG_FILE` rotates. `0` disables rotation. |
| `AUDIT_LOG_BACKUPS` | `10` | Rotated audit files kept. |
| `METRICS_ENABLED` | `true` | Serve Prometheus metrics at `/metrics`. `false` removes the route (`GET /metrics` returns 404). Requires the `observability` extra. |
| `OTEL_ENABLED` | `false` (app) / `true` (compose) | Enable the OpenTelemetry `TracerProvider` and ASGI span instrumentation. Requires the `observability` extra. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/gRPC endpoint spans are exported to (e.g. `http://tempo:4317`). No effect unless `OTEL_ENABLED=true`. |

### Authentication variables (opt-in, off by default)

| Variable | Default | Description |
|---|---|---|
| `MCP_AUTH_MODE` | `none` | `password` enables `/oauth/*` routes and requires a bearer token on `/mcp` (local/VPN use). `okta` makes PgLLens a pure OAuth resource server validating tokens from your Okta tenant, with no `/oauth/*` routes of its own ([`OKTA.md`](OKTA.md)). `MCP_OAUTH_ENABLED=true` is a deprecated alias for `password`. |
| `MCP_AUTH_PASSWORD` | unset | The shared password `/oauth/authorize` checks. Required in `password` mode. |
| `OKTA_ISSUER`, `OKTA_AUDIENCE` | unset | Required in `okta` mode. `OKTA_JWKS_URL_OVERRIDE` is optional (defaults to `<issuer>/v1/keys`; only for an internal JWKS mirror). |
| `EXTERNAL_BASE_URL` | `http://localhost:3000` | Must be set explicitly behind a reverse proxy or on a non-default port. Unset, it is derived from `HOST`/`MCP_PORT`, which is only correct for loopback use; a stale value silently breaks the OAuth discovery documents. See [Reverse proxy](#reverse-proxy). |
| `MCP_OAUTH_TOKEN_EXPIRES_IN` | `604800` (7 days) | Issued-token lifetime in seconds. |
| `MCP_RATE_LIMIT_ATTEMPTS` | `5` | Failed-auth attempts allowed per window. |
| `MCP_RATE_LIMIT_WINDOW_MS` | `900000` (15 min) | Rate-limit window. |
| `TRUST_PROXY_HEADERS` | `false` | Trust `X-Forwarded-For`/`X-Forwarded-Proto` for rate limiting, the audit `ip`, and discovery URLs. Only `true` when a proxy you control that overwrites the header is the sole path to this server. |

## Database role

The SQL-text safety gate (`database/safety.py`) is a second wall, not the primary one. On the
internet the role PgLLens connects as must genuinely be unable to write, read server files, or
reach another database. [`ops/sql/pgllens-role.sql`](../ops/sql/pgllens-role.sql) creates that
role:

```bash
psql -v pgllens_password="$(openssl rand -base64 32)" \
     -v pgllens_db=mydb -v pgllens_schema=public \
     -f ops/sql/pgllens-role.sql "$SUPERUSER_DSN"
```

Run it once as a superuser, connected to the database PgLLens will serve. It is re-runnable: every
statement is guarded or idempotent except the password, which is always reset to the value you
pass. Repeat section 4 of the script for every additional schema in `EXPOSED_SCHEMAS`.

| Grant | Why |
|---|---|
| `CONNECT` on the one database, after `REVOKE ALL ... FROM PUBLIC` | `PUBLIC` has `CONNECT` on every database by default; the revoke is what stops the role reaching other databases in the cluster. |
| `USAGE` on the exposed schema(s) | Lets the role resolve names inside the schema. |
| `SELECT` on all tables/sequences, plus `ALTER DEFAULT PRIVILEGES` for both | Read-only discovery and querying. Without the default-privileges grant, discovery silently goes blind on tables created later. |
| `pg_monitor` (`WITH INHERIT TRUE`) | The read-only bundle behind `get_active_sessions`, `get_blocking` (other sessions' query text) and `get_query_store`. Grants no write capability; drop it if you would rather those tools show `<insufficient privilege>` than other users' SQL. `get_query_store` additionally needs `USAGE`/`SELECT` on the `pg_stat_statements` views, which the script grants when the extension is present. |
| `ALTER ROLE ... SET default_transaction_read_only/statement_timeout/idle_in_transaction_session_timeout/lock_timeout/search_path` | Attached to the role, not the DSN, so these hold even if `conninfo()`'s session options are dropped by a pooler or a hand-edited URL. |

Four roles are deliberately never granted, listed in the script so a reviewer can see the decision,
and asserted absent at its end: `pg_read_server_files` (host filesystem read),
`pg_write_server_files` (host filesystem write), `pg_execute_server_program` (command execution),
`pg_signal_backend` (cancel/terminate other sessions).

The minimum by hand, if you cannot run the script, is `GRANT USAGE ON SCHEMA s` and
`GRANT SELECT ON ALL TABLES IN SCHEMA s` to a `LOGIN` role, optionally plus `GRANT pg_monitor`.

Verify the role holds after running the script; these tests attempt a write, a server-file read,
`COPY ... TO PROGRAM`, and a connection to another database, and assert each is rejected (they skip
with no `PGLLENS_TEST_DSN`, like the rest of `tests/integration`):

```bash
PGLLENS_TEST_DSN=postgresql://pgllens:<password>@host:5432/mydb \
  uv run pytest tests/integration/test_db_posture.py
```

For an internet-facing deployment, pair the role with `DB_REQUIRE_VERIFY_FULL=true` so the
connection itself is authenticated, not just encrypted.

### Column-level secrecy: REVOKE, not REDACT_COLUMNS

`REDACT_COLUMNS` is display masking in the application layer and cannot be made into a security
boundary. The real control is a standard grant on top of the table-level `SELECT` the role holds:

```sql
REVOKE SELECT (api_token) ON accounts FROM pgllens;
```

PostgreSQL enforces this for every query shape, including expressions and aliases. The tradeoff:
any `SELECT *`-shaped tool (`get_sample_data`) now errors on that table with an insufficient
privilege message, since a bare `*` includes a column the role cannot read. This is not wired up
by `pgllens-role.sql`; apply it per column, per table.

## Authentication

Off by default: with no config, `/mcp` is open and no `/oauth/*` routes exist. Password mode:

```bash
export MCP_AUTH_MODE=password
export MCP_AUTH_PASSWORD='...'
export EXTERNAL_BASE_URL='https://pgllens.example.com'   # required for anything but loopback use
uv run pgllens
```

Two properties to know before turning password mode on in production:

- **Sole issuer, sole resource.** Tokens minted by `/oauth/token` are validated only against this
  server's own `/mcp` (no audience check). Safe only while this server is both the sole issuer and
  sole resource for every token it accepts; see the comment in `src/pgllens/oauth/bearer.py`.
- **Open registration.** `/oauth/register` is unauthenticated (RFC 7591), so anyone can register a
  client with their own `redirect_uri`. `redirect_uri` matching prevents an open redirect, but not
  a convincing phish for the shared `MCP_AUTH_PASSWORD`. Keep this server off the open internet,
  or put your own auth in front of it, rather than relying on password mode as a public login.

For an internet-facing posture use `MCP_AUTH_MODE=okta`: [`OKTA.md`](OKTA.md).

## Direct exposure (no proxy)

Without a reverse proxy, PgLLens limits connections at the uvicorn layer: `limit_concurrency=100`
caps concurrent requests and `timeout_keep_alive=5` bounds how long idle clients hold a
connection. These are fixed values (`src/pgllens/__main__.py`); the 5-connection database pool
imposes its own ceiling on useful concurrency. uvicorn has no header or body read deadline, so a
slowloris client can hold idle connections until the concurrency cap fills and block legitimate
requests until `timeout_keep_alive` expires. Direct internet exposure without a proxy is
unsupported for production; use the tunnel profile below or a proxy that terminates TLS.

## Reverse proxy

Running behind nginx, Caddy, an ALB, or a tunnel:

- **Set `EXTERNAL_BASE_URL` explicitly.** The single most common deployment mistake. Without it,
  OAuth discovery URLs are derived from `HOST`/`MCP_PORT` as seen by the process (`0.0.0.0:3000`),
  and the server only warns in logs. Set the externally visible URL including scheme.
- Forward `Host`, `X-Forwarded-Proto`, and `X-Forwarded-For`, and set `TRUST_PROXY_HEADERS=true`
  only when the proxy is the sole path to this server; otherwise a client can spoof its IP for
  rate limiting or its scheme for discovery URLs.
- MCP over streamable HTTP is a normal request/response plus SSE endpoint at `/mcp`: no WebSocket
  upgrade handling, but make sure the proxy does not buffer or time out tool calls under
  `QUERY_TIMEOUT_MS` (30 s default).
- **Terminate TLS at the proxy; PgLLens always speaks plain HTTP.** There is no TLS option in the
  app, deliberately: one battle-tested proxy in front of a stateless app is less to get wrong than
  a home-grown TLS config.
- **`get_erd_widget`'s per-call resource is in-process** (`_baked_erd_resources`), so a
  multi-replica deployment must run a single replica or sticky routing, or a `resources/read` may
  land on a different replica than the `tools/call` that created it and return
  `ResourceNotFoundError` (a blank widget).

### Cloudflare Tunnel (the `tunnel` compose profile)

The supported public entry point is a dashboard-managed Cloudflare Tunnel: create the tunnel in
Zero Trust, Networks, Tunnels; route the public hostname to `http://pgllens:3000` (the compose
service name; no host port needs to be published); put the token in `.env` as `TUNNEL_TOKEN=...`;
set `EXTERNAL_BASE_URL=https://<public hostname>` and `TRUST_PROXY_HEADERS=true`. Then:

```bash
# add `tunnel` to COMPOSE_PROFILES in .env, then
docker compose up -d
```

`/metrics` refuses any request carrying `X-Forwarded-For` (see
[OBSERVABILITY.md](OBSERVABILITY.md#the-metrics-exposure-decision)), so a proxy-level path rule
hiding it is optional belt-and-braces, not required.

### Decommissioned: the `tls` compose profile (Caddy)

The `caddy` service is commented out of `docker-compose.yml` as of 2026-09-01: it was never
verified against a live hostname, and untested code does not ship as a supported path. The design
(kept for a future non-Cloudflare deployment; `ops/caddy/Caddyfile` remains and is test-pinned): an
opt-in `caddy` service (profile `tls`) terminating TLS with automatic ACME certificates. Reviving it
means uncommenting the service and verifying against a real hostname:

```bash
PGLLENS_DOMAIN=pgllens.example.com
ACME_EMAIL=ops@example.com
APP_BIND=127.0.0.1        # :3000 becomes loopback-only; Caddy is the only public entry point
TRUST_PROXY_HEADERS=true

docker compose --profile tls up -d
```

`PGLLENS_DOMAIN` must already resolve to this host, and ports `80`/`443` must be free (`:80` is
the HTTP-01 challenge and the redirect to `:443`). `ops/caddy/Caddyfile` is the whole config.

### Append vs. overwrite: `X-Forwarded-For`

`TRUST_PROXY_HEADERS=true` is only safe if the proxy overwrites `X-Forwarded-For` with the real
peer address rather than appending to whatever the client sent. Caddy appends by default;
`ops/caddy/Caddyfile` overrides it with `header_up X-Forwarded-For {remote_host}`. The nginx
equivalent is `proxy_set_header X-Forwarded-For $remote_addr;`, not
`$proxy_add_x_forwarded_for`, which appends. PgLLens's own `oauth/clientip.py::client_ip` reads
the rightmost hop regardless, which is safe even against an appending proxy, but an overwriting
proxy is the configuration this project tests and recommends.

## Security posture

Defense in depth, each layer independent of the others:

1. **Read-only session.** `default_transaction_read_only=on`, `statement_timeout`, and
   `idle_in_transaction_session_timeout` are baked into the DSN, enforced by PostgreSQL.
2. **SQL-text gate** (`database/safety.py`) rejects anything that is not a single read statement
   before it reaches the driver ([tools.md](tools.md#the-read-only-gate)).
3. **Least-privilege role** ([above](#database-role)) that cannot write, read files, or reach
   another database even if both of the above failed.
4. **Authentication** (opt-in): password mode with failed-attempt rate limiting, or Okta.
5. **Per-client limits**: call rate, concurrency, planner-cost budget.
6. **Column redaction** for LLM output, and column-level `REVOKE` for real secrecy.
7. **Audit trail**: one JSONL line per tool call, never SQL text or credentials
   ([OBSERVABILITY.md](OBSERVABILITY.md#audit-record-fields)).

Surface, binding, and auth for every port `docker-compose.yml` can expose:

| Surface | Binding | Auth |
|---|---|---|
| pgllens `:3000` | `APP_BIND` | OAuth/password on `/mcp`; `/health` and `/metrics` open |
| Grafana `:3001` | `ADMIN_BIND` | admin password |
| Prometheus `:9090`, Alertmanager `:9093` | `ADMIN_BIND` | none; trusted network only |
| Loki, Tempo, Alloy, exporters | not published | Docker network only |
| Docker socket (Alloy, cAdvisor) | not published | host-root-equivalent; see below |

Hardening applied to every stack container (the `x-hardened` anchor in `docker-compose.yml`):
`read_only: true` where the image tolerates it, `cap_drop: [ALL]`, `no-new-privileges`, and a
`mem_limit`. Writable volumes exist only where needed (the audit volume for pgllens; TSDB, chunks,
and blocks for Prometheus/Loki/Tempo). Every third-party image is pinned to a digest, with a
Renovate config to track updates.

**cAdvisor is the one documented exception** to the anchor: it needs host mounts (`/rootfs`,
`/var/run`, `/sys`, the Docker socket directory) and on some kernels elevated capabilities to read
cgroups. It still runs `privileged: false` and every mount is read-only.

**Alloy mounts the Docker socket, and that is the stack's largest privilege.** Alloy (observe
profile) binds `/var/run/docker.sock` read-only so `discovery.docker` can enumerate containers and
`loki.source.docker` can read their stdout. A read-only bind restricts writes to the file, not the
API: `POST /containers/create` still works, so anything reaching the socket is effectively host
root, `cap_drop` notwithstanding. Accepted because Alloy runs only committed config on a Docker
network with nothing else on it, and the alternative (parsing
`/var/lib/docker/containers/*/*-json.log`) needs the same host access with none of the metadata.
Future hardening is a socket proxy (e.g. `tecnativa/docker-socket-proxy`) allowlisting
`GET /containers/*`; it would cover cAdvisor's `/var/run` mount too. Run without the `observe`
profile if the tradeoff is unacceptable.

**Secrets stay in `.env`, not compose `secrets:`.** Compose `secrets:` delivers files under
`/run/secrets/`, and neither pgllens nor the third-party images (Grafana, Prometheus,
Alertmanager, postgres_exporter, cloudflared) consistently support a `_FILE` variant of every
credential. Splitting configuration across two mechanisms for the subset that does gains nothing
here. `.env` (excluded from the image, read only by compose) is the accepted tradeoff; generate it
from your vault at deploy time if you need more.

## Health check

```bash
curl http://localhost:3000/health
# {"status": "healthy", "server": "pgllens"}
```

`/health` needs no auth even when OAuth is enabled and pings the database with a real `SELECT 1`
(2 s timeout), so it reports whether PostgreSQL is reachable, not just that the process is up. A
successful ping is cached for 2 s, so a flood of health checks costs one query every two seconds.
It returns 200 `{"status": "healthy", "server": "pgllens"}` when the database answers, or 503
`{"status": "unhealthy", "database": "unreachable", "server": "pgllens"}` when it does not. The
image's `HEALTHCHECK` polls it every 30 s. The ping shares the connection pool with tool calls, so
under full pool saturation `/health` can return 503 without a real outage; a known tradeoff.

## Supply chain

Three properties the build holds, each asserted by `tests/test_supply_chain.py`:

1. **Hash-pinned dependencies.** The builder runs `uv export --frozen --no-dev --no-emit-project
   --extra observability --extra redis --format requirements-txt` to turn `uv.lock` into a
   requirements file with a hash per artifact, then `uv pip install --require-hashes`. A
   compromised index cannot substitute a wheel for a version the lock pinned. (`uv` has no single
   native lock-to-install path with hash enforcement yet, hence the two steps.)
2. **Distroless, non-root, no shell.** The final stage is
   `gcr.io/distroless/python3-debian12:nonroot`, running as uid 1001, with no `/bin/sh` or
   package manager. The distroless base pins CPython 3.11, so the builder pins 3.11 too
   (`ghcr.io/astral-sh/uv:0.9-python3.11-bookworm-slim`) so compiled extensions (`psycopg`,
   `pydantic-core`) match the interpreter ABI; the entrypoint invokes the image's own
   `/usr/bin/python3` with `PYTHONPATH` at the copied venv's `site-packages`, because the venv's
   `bin/python` symlink points at the builder's Python and does not resolve in the final stage.
3. **Hardened compose service.** `read_only: true` (with a `tmpfs` at `/tmp`), `cap_drop: [ALL]`,
   `security_opt: [no-new-privileges:true]`; the audit volume is the one writable path.

**SBOM:** `scripts/sbom.sh [output-path] [image-ref]` produces a CycloneDX JSON SBOM via
[`syft`](https://github.com/anchore/syft) if installed, or falls back to a hash-pinned `uv export`
listing, with a clear stderr message either way.

**CI:** `.github/workflows/ci.yml` runs `ruff check`, `mypy src`, and `pytest` on every push and
PR, builds the image, scans it with [Trivy](https://github.com/aquasecurity/trivy-action) (failing
on any fixable `CRITICAL`; an unfixable one in a base image cannot be actioned and would only train
everyone to ignore the scanner), and publishes a CycloneDX SBOM via
[`anchore/sbom-action`](https://github.com/anchore/sbom-action).

## Compatibility

Any MCP-compatible client: Claude Desktop (with or without OAuth), Claude Code, Cursor, Windsurf,
VS Code MCP extensions, custom clients. Any PostgreSQL 12+ instance, including Amazon RDS and
Aurora, Google Cloud SQL, Azure Database for PostgreSQL, Supabase, Neon, TimescaleDB, and
CockroachDB over the PostgreSQL wire protocol.
