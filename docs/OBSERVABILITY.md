# Observability

PgLLens ships optional Prometheus metrics and OpenTelemetry tracing alongside always-on structured
JSON logging and a JSONL audit trail. The `observability` extra is optional to install: metrics are
on by default (`METRICS_ENABLED=true`), tracing is off by default (`OTEL_ENABLED=false`), and the
server runs identically whether or not the extra is present. Variable reference:
[`DEPLOY.md`](DEPLOY.md#logging-audit-and-observability); the compose tiers that run the
Grafana stack: [`DEPLOY.md#compose-and-tiers`](DEPLOY.md#compose-and-tiers).

## The one property that matters

> A monitoring feature that breaks startup is worse than no monitoring.

`src/pgllens/obs/metrics.py` and `src/pgllens/obs/telemetry.py` guard every
`opentelemetry`/`prometheus_client` import behind a module-load `try/except ImportError`.
`deps_available()` reports whether the extra is installed; every recording function
(`record_tool_call`, `record_query_duration`, `record_connection_error`,
`record_schema_cache_access`) is a safe no-op when metrics are disabled or the extra is missing.
The same holds for `configure_tracing`/`span`/`instrument_asgi`. Both configurations must pass
with the same test count (deps-gated tests skip, never fail):

```bash
uv sync --extra dev --extra observability && uv run pytest -q   # extra present
uv sync --extra dev                        && uv run pytest -q  # extra absent
```

## Metrics

```bash
pip install 'pgllens[observability]'   # or: uv sync --extra observability
uv run pgllens
curl http://localhost:3000/metrics
```

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `pgllens_tool_calls_total` | counter | `tool`, `outcome` | One per MCP tool invocation. `outcome` is `ok`, `rejected` (the read-only gate refused the SQL, an argument was out of range or an unknown format/order value, the row governor or cost gate turned it away), `unknown_schema`, `not_found` (a named table/view/function does not exist), `unavailable` (a required extension is missing), or `db_error` (driver or introspection failure, including a statement timeout). |
| `pgllens_tool_call_duration_seconds` | histogram | `tool`, `outcome` | Wall-clock duration of the call, including DB round-trips. |
| `pgllens_query_duration_seconds` | histogram | `outcome` (`ok`, `error`) | One `psycopg` round-trip (connect, execute, fetch). A failed connection is not observed here; it is counted by `pgllens_connection_errors_total`. |
| `pgllens_connection_errors_total` | counter | none | Failed database connection attempts. |
| `pgllens_schema_cache_hits_total` / `..._misses_total` | counter | none | Introspection cache hits and misses; a low hit rate suggests `SCHEMA_REFRESH_INTERVAL_MS` is too short. |
| `pgllens_auth_failures_total` | counter | none | One per rejected bearer token (bad, expired, wrong audience). Recorded by `oauth/bearer.py`. |
| `pgllens_limit_rejections_total` | counter | `kind` | One per request a limit turned away: `calls` (per-client rate 429, `middleware.py`), `cost` (query-cost budget, `tools/query.py`), `concurrency` (in-flight cap), `scope` (token missing a required scope). |

Both instrumentation call sites, the tool-call metric and the audit line, live in one place for
every tool but one: `src/pgllens/tools/_util.py::tool_errors`, the decorator every `@mcp.tool` is
wrapped in, so the next tool gets the same coverage for free. **The one exception is
`get_erd_widget`**: registered via `@apps.tool`, it is not wrapped in `tool_errors` and
self-instruments in a `finally` block in `src/pgllens/tools/erd.py` (both `record_tool_call` and
`audit(...)`). Its sibling `get_erd` is an ordinary `@mcp.tool`. If `get_erd_widget` metrics or
audit lines look wrong, look there.

### The label-cardinality rule

`tool` and `outcome` (and `kind` on the limits counter) are the only labels anywhere in this
module, all drawn from small, fixed, code-controlled enumerations: never a schema name, SQL text,
client IP, or any other user input. An unbounded label turns every distinct value into a new time
series and can take down Prometheus by exhausting memory. The rule is also a comment above the
label declaration in `obs/metrics.py`. Nothing puts a connection string, password, or token into
a label, span attribute, or log line either.

### First-event visibility

`obs/metrics.py::preregister_tools` runs once at startup with the full tool list and creates every
`tool` x `outcome` child of the two tool-call metrics at 0, plus every
`pgllens_query_duration_seconds{outcome=...}` and `pgllens_limit_rejections_total{kind=...}`
child (31 tools x 6 outcomes, so cheap). Without it, a never-incremented counter does not exist
as a series, and `rate()`/`increase()` over a range containing its first sample returns nothing;
pre-registering means the very first tool call or first `rejected` outcome (what
`PgllensReadOnlyGateRejections` watches) is visible immediately.

## Tracing

```bash
OTEL_ENABLED=true OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317 uv run pgllens
```

`configure_tracing` builds a `TracerProvider` whenever `OTEL_ENABLED=true` and the extra is
installed; it exports over OTLP/gRPC only when `OTEL_EXPORTER_OTLP_ENDPOINT` is also set
(otherwise spans go nowhere, harmlessly). The whole ASGI app is wrapped in
`OpenTelemetryMiddleware`, so every HTTP request (`/health`, `/mcp`, `/oauth/*`, `/metrics`) gets
a root span; this is how HTTP requests are observed, as traces rather than a separate counter.

Metrics deliberately do not go through OTel's meter API: `obs/metrics.py` talks to
`prometheus_client` directly, since Prometheus scrapes `/metrics` and an OTLP metrics path would
only add indirection.

When tracing is on, `pgllens_tool_call_duration_seconds_bucket` carries the current `trace_id` as
an OpenMetrics exemplar (`record_tool_call`), so a Prometheus data point can jump to its trace in
Tempo. Exemplars travel only over OpenMetrics exposition, which the scraper negotiates (Prometheus
3 does by default); `obs/metrics.py::render` picks the format from the `Accept` header.

An OTLP export failure (Tempo unreachable) is logged once, then suppressed for ten minutes
(`obs/telemetry.py::OnceEveryFilter`).

## The `/metrics` exposure decision

`/metrics` is on by default in every tier (`docker-compose.yml` sets
`METRICS_ENABLED: ${METRICS_ENABLED:-true}`) because it is the same exposure class as `/health`.
The route is not mounted at all when `METRICS_ENABLED=false` or when the `observability` extra is
missing; `GET /metrics` then returns 404, not an empty 200.

When mounted, it is served on the same port as `/mcp` and `/health` and is **not bearer-gated even
when OAuth is on**: `BearerAuthMiddleware` protects `/mcp` only (`src/pgllens/server.py`). Bind
exposure (`APP_BIND`) is the actual control. Two mitigations are built in: the route answers 404
to any request carrying an `X-Forwarded-For` header, unconditionally, since that always means a
proxy hop rather than the docker-network scrape path; and labels are cardinality-bounded (tool
name and fixed enums only), so the exposure is information about call volume, latency, and
outcome shape, never schema names or SQL. Do not publish the port to the public internet or a
tunnel; scrape it over a private or Docker network as the compose stack does. If you need it
public, put a reverse proxy with its own auth in front of it.

## Audit record fields

Every tool call writes one JSONL line to a dedicated `pgllens.audit` logger with
`propagate=False`, so audit never leaks into the operational logs or vice versa. Every line has
`event` (`tool_call`), a UTC `timestamp`, `tool`, `outcome` (the same enum as the metric label),
and the caller identity:

| Field | Meaning |
|---|---|
| `sub` | Authenticated subject (OIDC `sub`), or `null` when unauthenticated. |
| `client_id` | OAuth client id (`"anonymous"` when unauthenticated). |
| `ip` | Caller source IP (`"unknown"` if not resolvable). |
| `args_hash` | SHA-256 of the tool arguments, truncated to 16 hex chars, never the raw arguments. Answers "did they run this again?" without storing SQL. |
| `rows` | Row count the call produced (`database/pool.py::record_rows`); `0` for tools that fetch none. |
| `trace_id` | 32-hex OpenTelemetry trace id, present only when tracing is on and the call ran in a sampled span. |

`sub`, `client_id`, and `ip` come from `pgllens.caller.caller()`, the identity
`CallerContextMiddleware` attaches to the request. The Access audit Grafana dashboard
(`ops/grafana/dashboards/04-access-audit.json`) groups by `client_id` via `| json`. What is never
in the file, by design: credentials, connection strings, SQL text, or result rows. If you need to
know what SQL a client ran, that belongs in PostgreSQL's own `log_statement` or `pgaudit`.

## Durable audit sinks

The audit JSONL (`AUDIT_LOG_FILE`) is the source of truth; the compose stack tails it with Grafana
Alloy and ships it to Loki read-only, so Loki being down never blocks or drops writes to the file.
Outside that stack, `AUDIT_SYSLOG=host:port` also (or instead) ships every record as a UDP syslog
message (`logging.handlers.SysLogHandler`). Both sinks can be active together; a misconfigured or
unreachable syslog target never breaks startup or a tool call.

When neither is set (bare Play tier), `configure_audit` falls back to stdout, so `docker logs` is
the audit trail rather than audit being silently off. `AUDIT_STDOUT=true` forces stdout alongside
other sinks; `false` turns it off unconditionally, including the fallback.

`AUDIT_LOG_FILE` rotates by size at `AUDIT_LOG_MAX_BYTES` (default 100 MB), keeping
`AUDIT_LOG_BACKUPS` files (default 10) via `RotatingFileHandler`; `AUDIT_LOG_MAX_BYTES=0` disables
rotation for deployments that rotate the file themselves.

## Alert rules

`ops/prometheus/rules/pgllens.rules.yml` (app signals) and `ops/prometheus/rules/stack.rules.yml`
(the stack watching itself), both covered by promtool unit tests in
`ops/prometheus/rules/tests/`. Alertmanager routes to Slack when `SLACK_WEBHOOK_URL` is set and to
a null receiver otherwise (`ops/alertmanager/entrypoint.sh` picks `alertmanager.slack.yml` or
`alertmanager.null.yml`). What to do when each fires: [`runbook.md#an-alert-fired`](runbook.md#an-alert-fired).

| Alert | Severity | Fires when |
|---|---|---|
| `PgllensAuthFailureSpike` | warning | `pgllens_auth_failures_total` rate above 0.2/s (12/min) for 10m. |
| `PgllensRateLimitRejectionSpike` | warning | `pgllens_limit_rejections_total` rate above 0.5/s (30/min) for 10m. |
| `PgllensReadOnlyGateRejections` | critical | `outcome="rejected"` tool-call rate above 0.1/s for 5m. The single most interesting signal this server produces. |
| `PgllensReadOnlyGateRejection` | info | At least one rejected call in the last 15m; fires on the first event. |
| `PgllensHighToolErrorRate` | warning | More than 10% of tool calls `db_error`/`unknown_schema` for 10m (`rejected` excluded). |
| `PgllensConnectionErrorsFiring` | critical | Any `pgllens_connection_errors_total` increase over 10m, held 1m, so one burst pages within a minute. |
| `PgllensDown` | critical | `up{job="pgllens"} == 0` for 2m. Inhibits `PgllensHighToolErrorRate` and `AuditShippingStalled`. |
| `Watchdog` | none | `vector(1)`, always firing; the dead-man's switch. |
| `MonitoringTargetDown` | warning | An `observe` job (`prometheus|alertmanager|grafana|loki|tempo|alloy`) unscraped for 3m. When the job is `loki`, inhibits `AuditShippingDropping`. |
| `InfraTargetDown` | info | An `infra` exporter (`node|cadvisor|postgres`) unscraped for 10m. Expected when the profile is not running. |
| `AuditShippingStalled` | warning | Tool calls are happening but Alloy has read no new audit bytes in 10m. |
| `AuditShippingDropping` | critical | `loki_write_dropped_entries_total` increased: Alloy gave up on a batch and advanced past it. |
| `DiskPressure` | warning | Less than 15% free on a non-tmpfs/overlay/squashfs mount for 30m. |
| `PostgresDown` | critical | `pg_up == 0` for 2m (postgres-exporter). |
| `PostgresTooManyConnections` | warning | Connections above 80% of `max_connections` for 10m. |

### Self-monitoring

`Watchdog` fires continuously and is routed to Slack on its own 4h-repeat route so grouping never
merges it into another alert. If it stops arriving, the pipeline (Prometheus, Alertmanager, or the
Slack route) is broken, not the application.

`AuditShippingStalled` and `AuditShippingDropping` answer "would I know if the audit trail quietly
stopped reaching Loki?" separately from "is pgllens healthy": the first is a stuck tailer or bad
volume mount, the second is data loss in transit (the JSONL file itself is unaffected either way).
`PgllensDown` inhibits the first, since an app that is not running produces no audit lines, but
not the second: Alloy discarding a batch is real loss whatever the app is doing.

### Container logs in Loki

Grafana Alloy (`ops/alloy/config.alloy`) ships every stack container's stdout as a second Loki
stream labeled `job="pgllens-docker"`, `container=<name>`, and `level` (parsed from JSON or
logfmt; unmatched lines carry no `level`). Retention for this stream is 14 days (`336h`,
`retention_stream` override in `ops/loki/loki-config.yaml`), shorter than the 90-day default
(`LOKI_RETENTION`, `2160h`) the `job="pgllens-audit"` stream gets: container logs are operational
noise, the audit trail is the durable record.

```logql
{job="pgllens-docker", container="pgllens-tempo", level="error"}
```

## Proof of readiness

Four layers, each a superset of the one before:

```bash
uv run pytest -q tests/test_ops_alerts.py tests/test_ops_compose.py tests/test_ops_dashboards.py tests/test_ops_verify_script.py tests/test_ops_ci.py   # 1. unit: config-as-code
uv run pytest -q tests/test_ops_rules.py tests/test_ops_alertmanager.py tests/test_ops_alloy.py tests/test_ops_retention.py   # 2. rules and config verification (needs Docker)
GRAFANA_ADMIN_PASSWORD=x scripts/verify-stack.sh           # 3. stack
GRAFANA_ADMIN_PASSWORD=x scripts/verify-stack.sh --chaos   # 4. chaos
```

A green run of all four with `INFRA=1` (as CI runs it on Linux) is the definition of
production-ready for this repository. Without `INFRA=1` the run proves the Observed tier and the
infra exporters are covered by config tests only; that is the normal outcome on Docker Desktop for
Windows and macOS, where containers run in a VM the exporters cannot see the host through.

Layer 1 checks every YAML/JSON config in `ops/` without Docker. Layer 2 runs `promtool` against
the rules and validates the Alertmanager, Alloy, and retention configs (Docker for the `promtool`
container). Layers 3 and 4 drive `scripts/verify-stack.sh` against a live stack; it needs
`MCP_AUTH_MODE=none` on the pgllens instance, as CI runs it, or `MCP_BEARER` set to a valid token.
On Windows also set `PYTHON=python` (the script defaults to `python3`).

A default run is not read-only against your stack: it recreates the compose `pgllens` service
against the demo database and stops the whole project when done. `--keep` leaves the stack
running; `--no-up --only <check>` skips `compose up` and teardown and runs only the named checks.

Layer 3 checks:

- `targets_up`, `datasources_healthy`, `dashboards_provisioned`: Prometheus scrapes every target;
  Grafana has its datasources and dashboards.
- `first_event_preregistered`, `first_event_visible_to_increase`: every (tool, outcome) series is
  registered at zero so `increase()` sees the first real event. The second is provable only on a
  cold stack and reports `SKIP` once prior rejections exist.
- `rejection_in_prometheus`, `rejection_in_loki`, `rejection_in_grafana_panel`: a synthetic
  read-only-gate rejection shows up in metrics, logs, and the Grafana panel.
- `tempo_write_path`, `trace_searchable`, `exemplar_linked`: Tempo ingest and query, the app's
  export path, and the exemplar link back from Prometheus.
- `synthetic_alert_routed`, `watchdog_firing`: a synthetic alert reaches Alertmanager and the right
  receiver; `Watchdog` is firing and routed.
- `audit_file_matches_loki`: every audit line reached Loki at least once (distinct count, since
  re-shipping can duplicate but never drop).
- `audit_loki_no_duplicates` (WARN only): duplicates already in Loki cost storage and skew
  `count_over_time` but lose nothing.
- `audit_no_new_duplicates`: fails if the duplicate count grows above the startup baseline. A
  re-ship happening now is a defect.

`--chaos` (layer 4) adds `chaos_loki_outage_zero_loss` (stop and restart Loki, no audit lines
lost), `chaos_tempo_down_calls_ok` (tool calls succeed with Tempo stopped), and
`chaos_alloy_down_alert` (stopping Alloy triggers `AuditShippingStalled` within its window).
