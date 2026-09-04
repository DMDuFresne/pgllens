# Runbook

Grounded in this project's actual failure modes, not a generic checklist. How the server is meant
to be configured: [`DEPLOY.md`](DEPLOY.md). What each metric and alert means:
[`OBSERVABILITY.md`](OBSERVABILITY.md).

## The server won't start

| Message | Cause and fix |
|---|---|
| `EXPOSED_SCHEMAS must list at least one schema` | The value had no schema names (for example `EXPOSED_SCHEMAS=,`). Unset or empty falls back to `public`. Set `EXPOSED_SCHEMAS=public,...`. |
| `DATABASE_URL must be a postgresql:// URL` | `Settings._must_be_postgres` rejects anything not starting with `postgresql://` or `postgres://` (a bare host/port or an ODBC-style string). Set a full `postgresql://user:pass@host:port/db` URL. |
| `DEFAULT_SCHEMA '...' is not in EXPOSED_SCHEMAS (...)` | Failing at boot beats letting every tool's defaulted `schema` reach outside the allowlist. Add the schema to `EXPOSED_SCHEMAS` or drop `DEFAULT_SCHEMA`. |
| `MCP_AUTH_MODE=password requires MCP_AUTH_PASSWORD` | Password mode (or the legacy `MCP_OAUTH_ENABLED=true`) without a password would serve a flow no one can complete. Set `MCP_AUTH_PASSWORD`. |
| `MAX_CONCURRENT_CALLS_PER_CLIENT (...) must be below DB_POOL_MAX_SIZE (...)` | `/health` and other clients must always have a free connection. Lower the cap or raise the pool. |
| `JWKS fetch ... failed` (okta mode) | Egress to the tenant is blocked or `OKTA_ISSUER` is wrong. Fail-closed is deliberate; see [`OKTA.md`](OKTA.md#troubleshooting). |
| `ImportError` from `obs/metrics.py` or `obs/telemetry.py` | Should never happen: both guard every optional import behind `deps_available()`. This is a regression, not a config issue. File it; do not work around it by installing the extra. |

## Connections fail (server otherwise healthy)

Check in this order:

1. **Network reachability.** `nc -zv <host> <port>` from the container or host. Two Docker
   containers must share a network; a container cannot reach `localhost:5432` on the host by that
   name (use `host.docker.internal` or the service name).
2. **Credentials.** `password authentication failed for user "..."` is exactly what it says. A
   literal `@`, `:`, or `/` in the password must be percent-encoded in `DATABASE_URL`.
3. **Permission denied on introspection.** PgLLens uses `pg_catalog`, not `information_schema`,
   specifically to work with restricted roles, but the role still needs `GRANT USAGE ON SCHEMA s`
   and `GRANT SELECT ON ALL TABLES IN SCHEMA s`. The full role: [`DEPLOY.md#database-role`](DEPLOY.md#database-role).
4. **Schema not exposed.** `Schema '...' is not exposed. Available: ...` comes from the tool, not
   the connection; the connection is fine. Add the schema to `EXPOSED_SCHEMAS` (matching is
   case-insensitive).
5. **Windows and `AsyncConnectionPool`.** psycopg's async pool refuses Windows' default
   `ProactorEventLoop`. No operator action is needed: on `win32` the server hands uvicorn a
   selector loop factory (`src/pgllens/__main__.py`'s `LOOP`), and
   `tests/integration/conftest.py` does the same. Setting the event-loop policy does nothing here
   (uvicorn builds its loop via `asyncio.Runner`, which never consults the policy). If you embed
   `build_app` in your own uvicorn invocation, pass `loop="asyncio:SelectorEventLoop"`
   (uvicorn >= 0.36).

## Schema changes are not reflected

Introspection is cached for `SCHEMA_REFRESH_INTERVAL_MS` (default 5 minutes). Wait, or call
`refresh_schema`. Row estimates come from `pg_class.reltuples` and only move after `ANALYZE`.

## OAuth login fails (password mode)

Check `MCP_AUTH_MODE=password` and `MCP_AUTH_PASSWORD` are set, `EXTERNAL_BASE_URL` matches the
URL the client uses, and the rate limiter has not tripped (`MCP_RATE_LIMIT_ATTEMPTS`, default 5
per 15 minutes). The server logs the reason. Okta-mode symptoms: [`OKTA.md`](OKTA.md#troubleshooting).

## A new or renamed tool doesn't show up in the client

The server runs streamable HTTP in **stateless** mode: there is no long-lived session to push
`notifications/tools/list_changed` down, so a host that caches `tools/list` per connection
(claude.ai, Claude Desktop) keeps the old tool set until it reconnects. After a deploy that adds or
renames a tool, disconnect and reconnect the connector. Calling a moved tool under its old shape
returns a pointer to the new one (`get_erd format="widget"` points at `get_erd_widget`) so the model
can recover without a reconnect.

The same cache bites the ERD widget harder: a host that cached a tool-to-view binding from an
earlier build keeps that binding indefinitely; symptoms are "Connector not found" (claude.ai) or a
widget tool that never appears. Separately, a multi-replica deployment needs a single replica or
sticky routing for the widget ([`DEPLOY.md#reverse-proxy`](DEPLOY.md#reverse-proxy)).

**What does NOT refresh the catalog** (verified 2026-09-02 on claude.ai, all tried, all failed):
new conversations; toggling the connector in the `+` menu; per-user disconnect/reconnect and
re-authorize in Settings, Connectors; Organization settings, Connectors, "Refresh tools" (clicked
about 20 times, no effect; the server returns the full `tools/list` with a 200 every time and the
host discards it); quitting Claude Desktop and rebooting; rebuilding the container (the server was
serving all 31 tools the whole time; the stale list lives on claude.ai's connector record).

**What worked, and appears to be the only thing that does:**

1. Remove the org connector.
2. Re-add it **under a new name** (the same name reattaches to the same frozen catalog).
3. **Wait several minutes** (about 5) for claude.ai to index the fresh `tools/list`.

The tell for a stale catalog: the tool count is wrong (27 vs 31), or the descriptions the model
quotes do not match [`tools.md`](tools.md) for the deployed version. Because a rename and re-add
is disruptive org-wide, batch tool renames and widget-binding changes into as few releases as
possible; behaviour-only changes do not need it.

Server-side check for the widget path: `resources/list` must return `ui://pgllens/erd-widget` with
`mimeType: text/html;profile=mcp-app`, and `tools/list` must show the same URI under
`get_erd_widget`'s `_meta.ui.resourceUri`. If both pass, any remaining problem is host-side
staleness.

## A tool reports a missing extension or grant

By design: `get_query_store` needs `pg_stat_statements` and `list_hypertables` needs `timescaledb`
(`database/capability.py::requires_extension`). Both stay **registered and visible** rather than
disappearing (a vanished tool is indistinguishable from a broken server) and return an
`EXTENSION_MISSING` envelope whose hint names the exact `CREATE EXTENSION` statement
([example](tools.md#response-shape)).

`get_active_sessions`/`get_blocking` read `pg_stat_activity`, which PostgreSQL
restricts: a role without `pg_monitor` sees other sessions' `query` as `<insufficient privilege>`
(a valid row, not an error). That is correct behaviour for a deliberately scoped-down role, not a
bug; grant `pg_monitor` ([`DEPLOY.md#database-role`](DEPLOY.md#database-role)) if you want full
query text. If one of these tools instead returns a `DB_ERROR` envelope, that is a bug: file it
with the tool name, `request_id`, and the `psycopg` error text.

### Error codes

| code | raised when | what to do |
|---|---|---|
| `QUERY_REJECTED` | `assert_read_only` blocks the query text | Submit a single `SELECT`/`WITH`/`TABLE`/`VALUES` statement with no write, DDL, or side-effecting call. |
| `SCHEMA_UNKNOWN` | `schema` is not in `EXPOSED_SCHEMAS` | Pass one of the exposed schemas; `schema_overview()` lists them. |
| `TABLE_NOT_FOUND` | `table`/`view` does not resolve in the exposed schemas | `search_columns(pattern=...)` or `list_tables()` to locate it. |
| `FUNCTION_NOT_FOUND` | `function` does not resolve | `list_functions(schema=...)`. |
| `EXTENSION_MISSING` | `requires_extension` finds the extension absent | `CREATE EXTENSION <extension>;` as a superuser, then retry. |
| `ARG_OUT_OF_RANGE` | a numeric or enum-shaped argument is outside its bounds | Pass a value between the stated lo/hi or one of the allowed values. |
| `FORMAT_UNKNOWN` | an unrecognized `format`/`order_by` value | Pass one of the values named in the message. |
| `TIMEOUT` | the statement or pool wait times out | Narrow the query or raise `QUERY_TIMEOUT_MS`; retry after the stated delay. |
| `DB_ERROR` | any other `psycopg` error | The server logged the request under the envelope's `request_id`. |

## The read-only gate rejects a query that looks like SELECT

`database/safety.py::assert_read_only` neutralizes string literals, quoted identifiers, `--`
comments, block comments (including PostgreSQL's nested `/* /* */ */`, unlike SQL Server's), and
dollar-quoted bodies (`$$...$$`, `$tag$...$tag$`), then requires a single statement starting with
`SELECT`/`WITH`/`TABLE`/`VALUES` and no blocked keyword. Two shapes are correctly rejected:

- **A second statement with no visible separator.** PostgreSQL accepts some multi-statement shapes
  without a semicolon; the keyword blocklist, not statement counting, is the backstop, and it errs
  toward rejecting.
- **A read-shaped function call with a side effect.** `setval`, `nextval`, `pg_advisory_lock*`,
  `pg_sleep*`, `pg_terminate_backend`, `dblink*`, `lo_import`/`lo_export`, and the
  `pg_read_file`/`pg_ls_dir`/`pg_stat_file` family are blocked by `_BLOCKED_FUNCS` even inside a
  `SELECT`, because each writes, escapes the database, or can wedge the server (advisory locks
  are held for the connection's lifetime).

If a genuinely read-only query is rejected for another reason, read the exact error text
(`assert_read_only` names what it matched); an unbalanced quote or dollar-quote in the query is the
most common real cause. `tests/test_safety.py` pins every blocked keyword, function, and nesting case.

## Running the integration suite

PgLLens has no system driver layer to install (`psycopg[binary]` ships its own `libpq`), so
`tests/integration/` runs on any host with network access to a PostgreSQL instance:

```bash
export PGLLENS_TEST_DSN="postgresql://user:pass@host:5432/db"
export PGLLENS_TEST_SCHEMAS="app_core,app_audit,app_custom"   # optional; default public
uv run pytest tests/integration -v
```

Against the bundled demo database the DSN is `postgresql://pgllens:pgllens-demo@localhost:5432/demo`
and the schemas line above is what makes the table and enum tests run instead of skip. The Redis
test skips unless `PGLLENS_TEST_REDIS_URL` is set.

- The `dsn` fixture calls `pytest.skip()` per test, so a plain `uv run pytest` with no DSN reports
  every integration test as skipped, not failed. It also probes the DSN with `SELECT 1` first, so a
  set-but-unreachable DSN skips with the connection error in the reason.
- There is no `seed.sql`; every test works against whatever the database contains (catalog
  queries, `sample_table`/`sample_view`/`sample_function` discovery).

## Reading the audit log

Unset `AUDIT_LOG_FILE` writes audit lines to stdout (`docker logs`); with a file:

```bash
tail -f /data/audit/audit.jsonl | jq .
```

One JSON object per line; fields and sinks are in
[`OBSERVABILITY.md#audit-record-fields`](OBSERVABILITY.md#audit-record-fields). Because 30 of 31
tools write their line from one place (`tools/_util.py::tool_errors`) and `get_erd_widget` writes
the same shape from `tools/erd.py`, filtering this file by `tool`/`outcome` is a complete picture.
The file answers "did someone call `query`", never "what did they query for": SQL text belongs in
PostgreSQL's own `log_statement` or `pgaudit`.

## Alloy will not start after an image upgrade

Alloy's read-position state lives on the `alloy-data` volume at `/data`. A volume created before
the current image carries `473:473` ownership and blocks startup. Remove it once with
`docker volume rm <project>_alloy-data` (e.g. `pgllens_alloy-data`). Only tail positions are lost;
the audit JSONL is untouched and every existing line re-ships to Loki once on the next start.

## An alert fired

Rule definitions and thresholds: [`OBSERVABILITY.md#alert-rules`](OBSERVABILITY.md#alert-rules).
For every alert, start with the stack state and the relevant container's recent logs, then run the
query in the table:

```bash
docker compose ps
docker logs <container> --since 30m       # pgllens, pgllens-alloy, pgllens-alertmanager, pgllens-<service>, pgllens-node-exporter, pgllens-postgres-exporter
```

| Alert | Meaning | First check | Fix |
|---|---|---|---|
| `PgllensAuthFailureSpike` | A steady trickle is a misconfigured client; a spike from many IPs is a credential attack. | `pgllens` logs; `sum by (client_id) (increase(pgllens_auth_failures_total[15m]))` | Cross-check `OKTA_AUDIENCE` against the authorization server's audience, then pull the audit trail for source IPs. Silence reason: "known client misconfiguration, ticket open". |
| `PgllensRateLimitRejectionSpike` | A client in a retry loop or a deliberate flood. | `sum by (kind) (increase(pgllens_limit_rejections_total[15m]))` | `kind="scope"` is different in nature: a client calling tools its token is not entitled to. Silence reason: "expected burst from batch job". |
| `PgllensReadOnlyGateRejections` | A sustained rate means someone is probing the gate, not a model writing one bad query. | `sum by (tool) (rate(pgllens_tool_calls_total{outcome="rejected"}[5m]))` | Pull the audit trail for `outcome="rejected"`, group by `client_id`, correlate `args_hash` to tell one retried payload from many attempts. Silence reason: "confirmed probing, blocking at network layer". |
| `PgllensReadOnlyGateRejection` | One rejection; the info-severity sibling of the above. | `sum(increase(pgllens_tool_calls_total{outcome="rejected"}[15m]))` | Audit trail for the client and `args_hash`; note and move on unless it repeats. Silence reason: "single known-bad query from a model, no pattern". |
| `PgllensHighToolErrorRate` | Server-health problem (`rejected` is excluded). | `sum by (tool, outcome) (rate(pgllens_tool_calls_total{outcome=~"db_error\|unknown_schema"}[10m]))` | Check PostgreSQL reachability, the role's grants (`ops/sql/pgllens-role.sql`), and which tools fail in the audit trail. Silence reason: "known grant gap, role fix in progress". |
| `PgllensConnectionErrorsFiring` | Host unreachable, wrong credentials, or a broken network/TLS path. | `increase(pgllens_connection_errors_total[10m])` | Test connectivity from inside the pgllens container to `DATABASE_URL`'s host:port; check credentials and TLS mode. Silence reason: "planned database maintenance window". |
| `PgllensDown` | Process down, unhealthy, or unreachable on the monitoring network. | `up{job="pgllens"}`; `curl http://localhost:3000/health` | If health also fails, restart the `pgllens` service and read its startup logs for a config error. Silence reason: "planned redeploy". |
| `Watchdog` | The heartbeat stopped arriving: the alerting pipeline is broken, not the app. | See [below](#watchdog-stopped-arriving). | Never silence it. |
| `MonitoringTargetDown` | An `observe` container is down; the stack is partially blind. | `pgllens-<service>` logs; `up{job=~"prometheus\|alertmanager\|grafana\|loki\|tempo\|alloy"} == 0` | Restart the named container if it exited; check its logs for a config or permission error. Silence reason: "restarting monitoring component". |
| `InfraTargetDown` | Expected whenever the `infra` profile is not running. | `up{job=~"node\|cadvisor\|postgres"} == 0` | If the profile is running, the named exporter is unhealthy; otherwise ignore. Silence reason: "infra profile intentionally not running". |
| `AuditShippingStalled` | The Loki mirror is behind; the JSONL file is still the record. | `pgllens-alloy` logs; `{job="pgllens-audit"} \| json` | If the query returns nothing recent, confirm `pgllens-alloy` is running and the audit volume is mounted read-only into it. Silence reason: "alloy restart in progress". |
| `AuditShippingDropping` | Lines are in the file but never reached Loki. With unlimited retries (`ops/alloy/config.alloy`) this should never fire. | `pgllens-alloy` logs; `increase(loki_write_dropped_entries_total[10m])` | Look for a 4xx from Loki (schema mismatch, request too large); a 5xx would retry forever instead of dropping. Silence reason: "loki schema issue being fixed". |
| `DiskPressure` | Prometheus TSDB, Loki chunks, and Tempo blocks all live on this disk. | `pgllens-node-exporter` logs; `node_filesystem_avail_bytes{fstype!~"tmpfs\|overlay\|squashfs"} / node_filesystem_size_bytes{fstype!~"tmpfs\|overlay\|squashfs"}` | Lower `PROM_RETENTION_SIZE`/`LOKI_RETENTION` in `.env`, or `docker system prune`, before the stack stops writing. Silence reason: "cleanup in progress". |
| `PostgresDown` | Same root cause as `PgllensConnectionErrorsFiring`, different vantage point. | `pgllens-postgres-exporter` logs; `pg_up` | Check the database host, and that the exporter's DSN (`DATABASE_URL`, same role as pgllens) is still valid. Silence reason: "planned database maintenance window". |
| `PostgresTooManyConnections` | pgllens's pool is capped by `DB_POOL_MAX_SIZE`, so something else holds connections. | `pgllens-postgres-exporter` logs; `sum(pg_stat_activity_count) / max(pg_settings_max_connections)` | Query `pg_stat_activity` grouped by `application_name` on the database. Silence reason: "known other workload sharing this database". |

To silence any alert except `Watchdog` for a known cause, using the reason from the table:

```bash
amtool --alertmanager.url=http://localhost:9093 silence add alertname=<Alert> -d 2h -c "<reason>"
```

### Watchdog stopped arriving

```bash
docker logs pgllens-alertmanager --since 30m
```

```promql
ALERTS{alertname="Watchdog"}
```

Confirm Prometheus is evaluating rules (`http://localhost:9090/rules`), Alertmanager is receiving
them (`http://localhost:9093/#/alerts`), and `SLACK_WEBHOOK_URL` is set and valid. Do not silence
`Watchdog`; if it is expected to be quiet because the `observe` profile is intentionally stopped,
that is the profile being down, not a silence.

## Platform dashboard: container panels read No data

"Container memory" and "Container restarts (1h)" are empty while the host CPU/memory/disk panels
work. cAdvisor sees only the root cgroup. Two known triggers: Windows Docker Desktop hosts, and
Linux hosts where Docker uses the containerd image store (check with `docker info | grep
driver-type`; Docker Engine 29 uses `io.containerd.snapshotter.v1` by default). cAdvisor 0.52 logs
`failed to identify the read-write layer ID for container` for every container in that mode.

Options: accept the two empty panels (nothing else is affected), or switch Docker back to the
classic overlay2 image store in `/etc/docker/daemon.json`
(`"features": {"containerd-snapshotter": false}`), which re-pulls every image and takes the stack
down for a few minutes. Not a pgllens issue.

## Traces dashboard shows "<root span not yet received>"

Every tool-call trace in the Traces (Tempo) dashboard has an empty Service/Name and reads
`<root span not yet received>`, while `GET /health` and `GET /metrics` traces look normal. The MCP
client (for example the claude.ai connector) sends a W3C `traceparent` header, so the `POST /mcp`
server span is a child of a span that lives in the caller's infrastructure and never reaches Tempo.
The pgllens spans (`POST /mcp`, `tools/call <name>`) are complete; only the remote root is missing.
Cosmetic.

The "Slow traces" panel excludes `subscriptions/listen` long-polls, which are multi-minute by
design. Multi-minute traces anywhere else are worth a look.

Related noise: the Platform dashboard's "Stack container logs" panel filters out Tempo's
`level=error ... "no jobs found"` line, which Tempo 3.0's backend worker emits about once a minute
in monolithic mode when its backend scheduler has no compaction jobs. Benign.

## "Prometheus TSDB size" reads 0 B

The panel plots `prometheus_tsdb_storage_blocks_bytes`, which counts only persisted blocks.
Prometheus cuts its first block roughly two to three hours after start, so a fresh stack shows 0 B
even though the head is holding data (check `prometheus_tsdb_head_series`, or `du` on the
prometheus-data volume). It fills in on its own.

## `get_table_health` flags xid age or a sequence near its limit

**xid age above 75% of `autovacuum_freeze_max_age`.** Autovacuum has not frozen the table's old
tuples. At `autovacuum_freeze_max_age` PostgreSQL forces an anti-wraparound vacuum; if the database
xid age (the caveat under the table) approaches the 2^31 ceiling the server stops accepting writes.

```sql
VACUUM (FREEZE, VERBOSE) schema.table;
```

Then find why autovacuum fell behind: a long-running transaction or abandoned replication slot
holding back the horizon (`get_active_sessions`, `pg_replication_slots`), or
`autovacuum_vacuum_cost_delay` throttling on a large table.

**A sequence above 80% used.** An `integer` identity column reaching 2,147,483,647 makes every
INSERT fail. Plan the type change (`ALTER TABLE ... ALTER COLUMN ... TYPE bigint` and
`ALTER SEQUENCE ... AS bigint`) before it fills; it rewrites the table.

**`get_index_health` says "unused" but the stats window is short.** Read the caveat under the
table. Under 7 days of counters, 0 scans is not evidence; wait for a representative period (a
month-end, a reporting cycle) before dropping anything.

## Duplicate column names in a join drop a value

A pre-2.0.0 TypeScript-server bug, fixed in the Python port, recorded because it shaped the
aliasing habit the `pgllens-using` skill still recommends. `database/pool.py::rows_from_cursor`
returns a positional column list and tuples, never a dict, so `SELECT a.created_at, b.created_at`
returns both values. A result missing a column when two sources share a name is a regression in
that contract; the function's docstring states the invariant and is the first place to look.
