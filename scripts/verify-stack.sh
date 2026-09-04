#!/usr/bin/env bash
# scripts/verify-stack.sh -- prove the observability stack end to end.
#
#   GRAFANA_ADMIN_PASSWORD=x scripts/verify-stack.sh            # stack layer
#   GRAFANA_ADMIN_PASSWORD=x scripts/verify-stack.sh --chaos    # + chaos layer
#   --no-up   assume the stack is already running
#   --keep    leave the stack up afterwards (default when --no-up)
#   --only <check_name>   run just that check (repeatable; implies --no-up)
#
# Env: PGLLENS_URL GRAFANA_URL PROM_URL AM_URL MCP_BEARER SLACK_WEBHOOK_URL
#      COMPOSE_PROJECT_NAME (picks the docker network / volume prefix)
#      INFRA=1 (also require the infra-tier exporters: node, cadvisor, postgres).
#
# Every check_* prints its PASS/WARN/FAIL line as it finishes (so a killed or
# timed-out run still shows progress) and repeats them as a table at the end; the
# script exits 1 if any failed. Each check polls with a deadline; there is no bare
# sleep-and-hope.
#
# GRAFANA_ADMIN_PASSWORD reaches Grafana via `curl -u`, so it is visible in `ps`
# for the duration. Deliberate: this is a stack-owner-run script on the box that
# already holds the compose secrets, not something a lesser-privileged user runs.
#
# Teardown is `compose stop`, never `down`: `down` is project-wide and would
# remove containers this script never started (e.g. the tunnel profile).
set -uo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

cd "$(dirname "$0")/.." || exit 1
PY=${PYTHON:-python3}
PGLLENS_URL=${PGLLENS_URL:-http://localhost:3000}
GRAFANA_URL=${GRAFANA_URL:-http://localhost:3001}
PROM_URL=${PROM_URL:-http://localhost:9090}
AM_URL=${AM_URL:-http://localhost:9093}
# Explicit -f flags override COMPOSE_FILE in .env, so the dev overlay is never merged
# here: the image ghcr.io/dmdufresne/pgllens:${PGLLENS_VERSION} must already exist
# (CI builds :ci first; locally run `docker compose build` before this script).
COMPOSE="docker compose --profile observe --profile infra -f docker-compose.yml -f ops/demo/docker-compose.yml"
PROJECT=${COMPOSE_PROJECT_NAME:-pgllens}
NET=${PROJECT}_default
AUDIT_VOL=${PROJECT}_pgllens-audit
# 31 tools x 6 outcomes: every (tool, outcome) series is registered at 0 at
# startup so the first real event is visible to increase()/rate(). Bump both
# this and tests/test_ops_verify_script.py when the tool count changes.
EXPECT_SERIES=186
# Infra tier (node-exporter, cAdvisor, postgres-exporter) is Linux-only in practice;
# CI sets INFRA=1, a Docker Desktop host leaves it 0 and proves the Observed tier.
INFRA=${INFRA:-0}
CHAOS=0; UP=1; KEEP=0; ONLY=""
while [ $# -gt 0 ]; do
  case $1 in
    --chaos) CHAOS=1;;
    --no-up) UP=0; KEEP=1;;
    --keep)  KEEP=1;;
    # --only implies --no-up/--keep: re-running one check is a debugging move on
    # a stack that is already up. Repeatable.
    --only)  ONLY="$ONLY ${2:?--only needs a check name}"; UP=0; KEEP=1; shift;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
  shift
done

FAILED=0; RESULTS=()
# Each result is printed the moment it is known and kept for the summary table:
# a CI job killed by its timeout still shows how far it got.
record() { RESULTS+=("$1"); printf '%s\n' "$1"; }
pass() { record "PASS  $1"; }
fail() { record "FAIL  $1: $2"; FAILED=1; }
# warn: worth surfacing, not worth failing the run on.
warn() { record "WARN  $1: $2"; }
# skip: the precondition for a meaningful assertion is absent, so the check made
# no claim either way. Never a failure.
skip() { record "SKIP  $1: $2"; }
# poll <seconds> <cmd...>: succeed when cmd exits 0 within the deadline. cmd may
# be a shell function, which is why no check here shells out through `bash -c`.
poll() { local deadline=$(( $(date +%s) + $1 )); shift; until "$@"; do [ "$(date +%s)" -ge "$deadline" ] && return 1; sleep 3; done; }
# Loki is not published to the host; query it from inside the compose network.
loki() { docker run --rm --network "$NET" curlimages/curl:8.11.1 -sG "http://loki:3100/loki/api/v1/$1" "${@:2}"; }
# ...and neither is pgllens' /metrics reachable without a proxy hop that adds
# X-Forwarded-For, which /metrics answers with 404 by design.
pgllens_metrics() { docker run --rm --network "$NET" curlimages/curl:8.11.1 -s http://pgllens:3000/metrics; }
json() { $PY -c "import sys,json; d=json.load(sys.stdin); $1"; }
promq() { curl -sG "$PROM_URL/api/v1/query" --data-urlencode "query=$1" | json "print(d['data']['result'][0]['value'][1] if d['data']['result'] else 0)"; }

# ---- stack layer ---------------------------------------------------------------

_targets_ok() {
  curl -s "$PROM_URL/api/v1/targets" | INFRA="$INFRA" $PY -c "
import os, sys, json
t = json.load(sys.stdin)['data']['activeTargets']
up = {x['labels']['job'] for x in t if x['health'] == 'up'}
want = {'pgllens','prometheus','alertmanager','grafana','loki','tempo','alloy'}
if os.environ['INFRA'] == '1':
    want |= {'node','cadvisor','postgres'}
sys.exit(0 if want <= up else 1)"
}
check_targets_up() {
  local tier; [ "$INFRA" = "1" ] && tier="(observe+infra)" || tier="(observe)"
  if poll 120 _targets_ok; then pass "targets_up $tier"
  else fail targets_up "$(curl -s "$PROM_URL/api/v1/targets" | json "print([(x['labels']['job'],x['health'],x['lastError']) for x in d['data']['activeTargets']])")"; fi
}

_ds_ok() { curl -s "${GF[@]}" "$GRAFANA_URL/api/datasources/uid/$1/health" | grep -q '"status":"OK"'; }
check_datasources_healthy() {
  local bad=""
  for u in prometheus loki tempo; do poll 60 _ds_ok "$u" || bad="$bad $u"; done
  [ -z "$bad" ] && pass datasources_healthy || fail datasources_healthy "unhealthy:$bad"
}

check_dashboards_provisioned() {
  local uids; uids=$(curl -s "${GF[@]}" "$GRAFANA_URL/api/search?type=dash-db" | json "print(' '.join(sorted(x['uid'] for x in d)))")
  local want="pgllens-access-audit pgllens-database pgllens-llm-activity pgllens-overview pgllens-platform pgllens-tool-calls pgllens-traces"
  [ "$uids" = "$want" ] && pass dashboards_provisioned || fail dashboards_provisioned "got: $uids"
}

# Phase 2's first-event guarantee, asserted at the source rather than inferred
# from a counter: every (tool, outcome) pair exists in the exposition at 0.
check_first_event_preregistered() {
  local n; n=$(pgllens_metrics | grep -c 'pgllens_tool_calls_total{')
  [ "$n" = "$EXPECT_SERIES" ] && pass first_event_preregistered \
    || fail first_event_preregistered "expected $EXPECT_SERIES pre-registered series, got $n"
}

# Fire one rejected write and one unknown-schema call; then look for them everywhere.
STAMP=$(date +%s)
FIRES_OK=0
FIRE_WHY=""
# A tool-level failure is exit 0 with an error envelope in the text -- only a
# transport or JSON-RPC failure is a nonzero exit. The envelope is asserted on
# EVERY call, not just the first: the chaos loops depend on these calls actually
# landing, and a silently-broken fire_signal would make them pass vacuously.
# Returns non-zero without recording a result; callers decide what a miss means.
fire_signal() {
  local out rc
  out=$($PY scripts/lib/mcp_call.py "$PGLLENS_URL" query "{\"sql\":\"DELETE FROM verify_$STAMP WHERE 1=0\"}" 2>&1); rc=$?
  [ $rc -eq 0 ] || { FIRE_WHY="transport error calling query: $out"; return 1; }
  case $out in *QUERY_REJECTED*) ;; *) FIRE_WHY="query envelope was not a rejection: $out"; return 1;; esac
  $PY scripts/lib/mcp_call.py "$PGLLENS_URL" list_tables "{\"schema\":\"no_such_schema_$STAMP\"}" >/dev/null 2>&1
  FIRES_OK=$((FIRES_OK + 1))
}

REJ_Q='pgllens_tool_calls_total{tool="query",outcome="rejected"}'
# Growth, not inequality: a counter reset or a scrape gap makes the reading go
# DOWN, and "!=" would have called that a pass. An empty/non-numeric reading
# (Prometheus down, query error) is a failure, not a silent zero.
_rejection_counter_moved() {
  local now; now=$(promq "$REJ_Q")
  case $now in ''|*[!0-9.]*) return 1;; esac
  awk 'BEGIN{exit !(ARGV[1]+0 > ARGV[2]+0)}' "$now" "$REJ_BEFORE"
}
check_rejection_in_prometheus() {
  REJ_BEFORE=$(promq "$REJ_Q")
  fire_signal || { fail rejection_in_prometheus "$FIRE_WHY"; return; }
  if poll 60 _rejection_counter_moved; then pass rejection_in_prometheus
  else fail rejection_in_prometheus "counter did not grow past $REJ_BEFORE (now $(promq "$REJ_Q"))"; fi
}

# First-event visibility (Phase 2): increase() sees the very first rejection
# because the series already existed at 0. Only provable on a cold stack -- once
# the counter is non-zero the series is no longer "first", so this SKIPs rather
# than pretending. Standalone so --only reaches it.
check_first_event_visible_to_increase() {
  local before=${REJ_BEFORE:-$(promq "$REJ_Q")}
  [ "$before" = "0" ] || { skip first_event_visible_to_increase "prior rejections exist; cold-stack only"; return; }
  local inc; inc=$(promq "increase(${REJ_Q}[10m])")
  [ "$inc" != "0" ] && pass first_event_visible_to_increase \
    || fail first_event_visible_to_increase "increase() saw 0 for the first rejection"
}

_loki_has_rejection() {
  loki query_range \
    --data-urlencode 'query={job="pgllens-audit"} | json | outcome="rejected" | tool="query"' \
    --data-urlencode 'limit=5' --data-urlencode "start=$((STAMP-60))000000000" | grep -q '"outcome": *"rejected"'
}
check_rejection_in_loki() {
  if poll 90 _loki_has_rejection; then pass rejection_in_loki
  else fail rejection_in_loki "no rejected audit line after $STAMP"; fi
}

_grafana_panel_has_rejection() {
  local body='{"from":"now-15m","to":"now","queries":[{"refId":"A","datasource":{"uid":"loki","type":"loki"},"expr":"sum by (outcome) (count_over_time({job=\"pgllens-audit\", event=\"tool_call\"} | json | __error__=\"\" [5m]))","queryType":"range","intervalMs":60000,"maxDataPoints":100}]}'
  curl -s "${GF[@]}" -X POST "$GRAFANA_URL/api/ds/query" -H 'Content-Type: application/json' -d "$body" | grep -q '"outcome":"rejected"'
}
check_rejection_in_grafana_panel() {
  if poll 60 _grafana_panel_has_rejection; then pass rejection_in_grafana_panel
  else fail rejection_in_grafana_panel "Access-audit query returned no rejected series"; fi
}

_tempo_has() { # <service.name>
  curl -sG "${GF[@]}" "$GRAFANA_URL/api/datasources/proxy/uid/tempo/api/search" \
    --data-urlencode "q={resource.service.name=\"$1\"}" \
    --data-urlencode "start=$((STAMP-900))" --data-urlencode "end=$(( $(date +%s) + 60 ))" | grep -q traceID
}
_tempo_has_pgllens() { _tempo_has pgllens; }
_tempo_has_verify_stack() { _tempo_has verify-stack; }
# Two rows, not one: a synthetic span proves Tempo's ingest+query path on its own,
# so an app that never exported is distinguishable from a Tempo that never stored.
check_tempo_write_path() {
  docker run --rm --network "$NET" ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.114.0 \
    traces --otlp-endpoint tempo:4317 --otlp-insecure --traces 1 --service verify-stack >/dev/null 2>&1
  if poll 60 _tempo_has_verify_stack; then pass tempo_write_path
  else fail tempo_write_path "synthetic verify-stack trace never became searchable in Tempo"; fi
}
check_trace_searchable() {
  # The app's own trace from fire_signal (OTEL on by default).
  if poll 60 _tempo_has_pgllens; then pass trace_searchable
  else fail trace_searchable "no pgllens traces in Tempo within 15m"; fi
}

# The metric->trace and log->trace joins the dashboards rely on. mcp_call.py sends
# no traceparent, so the server span is root and sampled (ParentBased(ALWAYS_ON)).
_exemplar_present() {
  curl -sG "$PROM_URL/api/v1/query_exemplars" \
    --data-urlencode 'query=pgllens_tool_call_duration_seconds_bucket{tool="query"}' \
    --data-urlencode "start=$((STAMP-60))" --data-urlencode "end=$(date +%s)" | $PY -c "
import sys, json
d = json.load(sys.stdin).get('data') or []
sys.exit(0 if any(e['labels'].get('trace_id') for s in d for e in s.get('exemplars', [])) else 1)"
}
_audit_line_has_trace_id() {
  loki query_range \
    --data-urlencode 'query={job="pgllens-audit"} | json | outcome="rejected" | trace_id != ""' \
    --data-urlencode 'limit=5' --data-urlencode "start=$((STAMP-60))000000000" | grep -q '"trace_id": *"'
}
check_exemplar_linked() {
  poll 90 _exemplar_present || { fail exemplar_linked "no trace_id exemplar on the query duration histogram"; return; }
  poll 90 _audit_line_has_trace_id || { fail exemplar_linked "rejected audit line in Loki carries no trace_id"; return; }
  pass exemplar_linked
}

_am_has() { curl -s "$AM_URL/api/v2/alerts" | grep -q "$1"; }
_am_routed_to() { # <alertname> <receiver name>
  curl -s "$AM_URL/api/v2/alerts/groups" | $PY -c "
import sys, json
d = json.load(sys.stdin)
sys.exit(0 if any(g['receiver']['name'] == '$2' and any(a['labels'].get('alertname') == '$1' for a in g['alerts']) for g in d) else 1)"
}
check_synthetic_alert_routed() {
  # date -d '+2 minutes' is GNU-only; this host is Git Bash.
  local ends; ends=$($PY -c "import datetime; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
  curl -s -o /dev/null -X POST "$AM_URL/api/v2/alerts" -H 'Content-Type: application/json' \
    -d "[{\"labels\":{\"alertname\":\"VerifyStackSynthetic\",\"severity\":\"warning\"},\"annotations\":{\"summary\":\"verify-stack\"},\"endsAt\":\"$ends\"}]"
  local want; [ -n "${SLACK_WEBHOOK_URL:-}" ] && want=slack || want=null
  if poll 30 _am_has VerifyStackSynthetic && poll 60 _am_routed_to VerifyStackSynthetic "$want"; then
    pass synthetic_alert_routed
  else fail synthetic_alert_routed "not routed to receiver '$want'"; fi
}

# Parsed, not grepped: a Watchdog sitting in `pending` matches the substring but
# is not yet a dead-man's switch, and the grep would have called that a pass.
_prom_watchdog() {
  curl -s "$PROM_URL/api/v1/alerts" | $PY -c "
import sys, json
d = json.load(sys.stdin)['data']['alerts']
sys.exit(0 if any(a['labels'].get('alertname') == 'Watchdog' and a.get('state') == 'firing' for a in d) else 1)"
}
check_watchdog_firing() {
  # Watchdog is the dead-man's switch: it must reach Alertmanager AND land in the
  # heartbeat receiver, not the pager one.
  local want; [ -n "${SLACK_WEBHOOK_URL:-}" ] && want=slack-heartbeat || want=null
  if poll 90 _prom_watchdog && poll 90 _am_has '"alertname":"Watchdog"' && poll 90 _am_routed_to Watchdog "$want"; then
    pass watchdog_firing
  else fail watchdog_firing "Watchdog not in Prometheus + Alertmanager receiver '$want'"; fi
}

audit_file_lines() { docker run --rm -v "$AUDIT_VOL":/a:ro alpine:3.20 sh -c 'cat /a/audit.jsonl* 2>/dev/null | wc -l' | tr -d ' \r'; }
# "<total> <distinct>" over the retained audit stream. Zero loss means every file
# line reached Loki at least once, so the file is compared against the DISTINCT
# count: a re-shipping Alloy can hold a line twice, never zero times. The surplus
# is reported separately by check_audit_loki_no_duplicates.
# Deliberately simple: a single query_range page, bounded by two Loki server limits --
#   * max_query_length (30d1h)            -> LOKI_WINDOW is 29d, so only the most
#                                            recent 29d of a 90d retention is compared;
#   * max_entries_limit_per_query (5000)  -> LOKI_LIMIT, detected below rather than
#                                            silently truncating the comparison.
# Paginate in 29d windows (and on the returned end timestamp) if the audit trail
# ever outgrows either.
# Deliberately simple: the file side is NOT windowed, so on a long-lived host every audit
#   line older than LOKI_WINDOW counts on the file side and not the Loki side,
#   and check_audit_file_matches_loki goes permanently red through no fault of
#   the shipping path. Paginate the Loki side, or trim the file comparison to the
#   same 29d window, before this stack outlives its first month.
LOKI_WINDOW=2505600
LOKI_LIMIT=5000
loki_audit_counts() {
  loki query_range --data-urlencode 'query={job="pgllens-audit"}' --data-urlencode "limit=$LOKI_LIMIT" \
    --data-urlencode "start=$(( $(date +%s) - LOKI_WINDOW ))000000000" | $PY -c "
import sys, json
v = [x[1] for s in (json.load(sys.stdin)['data']['result'] or []) for x in s['values']]
print(len(v), len(set(v)))"
}
loki_audit_distinct() { loki_audit_counts | cut -d' ' -f2; }
check_audit_file_matches_loki() {
  local counts file_n loki_total loki_n
  counts=$(loki_audit_counts); loki_total=${counts%% *}; loki_n=${counts##* }
  # A truncated page looks exactly like loss. Say which one it is.
  [ "$loki_total" -lt "$LOKI_LIMIT" ] || {
    fail audit_file_matches_loki "hit Loki's ${LOKI_LIMIT}-entry query cap; paginate or raise max_entries_limit_per_query"
    return
  }
  # Alloy ships asynchronously, so an equality taken the instant after a tool call
  # is a race against the shipping lag, not a loss check. Give it a deadline first.
  if poll 60 _audit_caught_up; then pass audit_file_matches_loki
  else
    file_n=$(audit_file_lines); loki_n=$(loki_audit_distinct)
    fail audit_file_matches_loki "file=$file_n loki_distinct=$loki_n"
  fi
}

loki_audit_duplicates() { local c; c=$(loki_audit_counts); echo $(( ${c%% *} - ${c##* } )); }

# Non-fatal: pre-existing duplicates cost storage and skew count_over_time, but
# lose nothing. New ones during this run are check_audit_no_new_duplicates' job.
check_audit_loki_no_duplicates() {
  local n; n=$(loki_audit_duplicates)
  [ "$n" -eq 0 ] && pass audit_loki_no_duplicates \
    || warn audit_loki_no_duplicates "$n duplicate lines (historical re-ship), baseline=$DUP_BASE"
}

# A run that ADDS duplicates is a re-ship happening now -- that is a real defect
# in the shipping path, not inherited history, so it fails.
check_audit_no_new_duplicates() {
  local n; n=$(loki_audit_duplicates)
  [ "$n" -le "$DUP_BASE" ] && pass audit_no_new_duplicates \
    || fail audit_no_new_duplicates "duplicates grew during this run: baseline=$DUP_BASE now=$n"
}

# ---- chaos layer ---------------------------------------------------------------

# -qx, not -q: the NOT-ready body is "Ingester not ready: ..." and contains the
# word "ready", so a substring match reports a starting Loki as up.
_loki_ready() { docker run --rm --network "$NET" curlimages/curl:8.11.1 -s http://loki:3100/ready | grep -qx ready; }
_audit_caught_up() { [ "$(audit_file_lines)" = "$(loki_audit_distinct)" ]; }
check_chaos_loki_outage_zero_loss() {
  # Baseline the file first: "Loki caught up" proves nothing if nothing was
  # written during the outage, and this check would then pass vacuously.
  local before after
  before=$(audit_file_lines)
  FIRES_OK=0
  $COMPOSE stop loki >/dev/null
  for _ in 1 2 3 4 5; do fire_signal; sleep 20; done      # ~2 minutes of calls with Loki down
  $COMPOSE start loki >/dev/null
  [ "$FIRES_OK" -gt 0 ] || { fail chaos_loki_outage_zero_loss "no tool call landed during the outage: $FIRE_WHY"; return; }
  after=$(audit_file_lines)
  [ "$after" -gt "$before" ] || { fail chaos_loki_outage_zero_loss "audit file did not grow during the outage (before=$before after=$after)"; return; }
  poll 120 _loki_ready || { fail chaos_loki_outage_zero_loss "loki did not come back"; return; }
  if poll 180 _audit_caught_up; then pass chaos_loki_outage_zero_loss
  else fail chaos_loki_outage_zero_loss "Loki never caught up with the file (before=$before after=$after)"; fi
}

check_chaos_tempo_down_calls_ok() {
  $COMPOSE stop tempo >/dev/null
  local out rc
  out=$($PY scripts/lib/mcp_call.py "$PGLLENS_URL" server_info '{}' 2>&1); rc=$?
  $COMPOSE start tempo >/dev/null
  # server_info succeeds outright, so here (unlike fire_signal) exit 0 plus a
  # non-error envelope is the real assertion.
  if [ $rc -eq 0 ] && [ "${out#*error}" = "$out" ]; then pass chaos_tempo_down_calls_ok
  else fail chaos_tempo_down_calls_ok "tool call failed with Tempo down: $out"; fi
}

ALLOY_READ_Q='sum(increase(loki_source_file_read_bytes_total{path="/audit/audit.jsonl"}[10m])) or on() vector(0)'
check_chaos_alloy_down_alert() {
  $COMPOSE stop alloy >/dev/null
  # 20m, not the 10m rule window + scrape slack. increase()[10m] extrapolates
  # across the whole window while any pre-stop sample is still inside it, so the
  # read-bytes term decays to zero only once the LAST scrape has aged out --
  # last_scrape + 10m, and `docker stop` plus the 30s poll granularity push that
  # past 13m. A live 13m run missed it by one evaluation. The loop breaks as soon
  # as the alert fires, so the larger deadline costs nothing on a pass.
  local deadline=$(( $(date +%s) + 1200 ))
  local ok=1
  FIRES_OK=0
  while [ "$(date +%s)" -lt $deadline ]; do
    fire_signal; sleep 30
    curl -s "$PROM_URL/api/v1/alerts" | grep -q '"alertname":"AuditShippingStalled"' && { ok=0; break; }
  done
  # Capture both rule terms before restarting Alloy, or the diagnosis reads the
  # recovered stack instead of the broken one.
  local why=""
  [ $ok -eq 0 ] || why="alloy_read_bytes_10m=$(promq "$ALLOY_READ_Q") tool_calls_10m=$(promq 'sum(increase(pgllens_tool_calls_total[10m]))')"
  $COMPOSE start alloy >/dev/null
  # The rule needs BOTH terms; with no tool calls the alert can never fire and a
  # pass would be luck rather than proof.
  [ "$FIRES_OK" -gt 0 ] || { fail chaos_alloy_down_alert "no tool call landed during the window: $FIRE_WHY"; return; }
  [ $ok -eq 0 ] && pass chaos_alloy_down_alert \
    || fail chaos_alloy_down_alert "AuditShippingStalled did not fire within 20m ($why)"
}

# ---- main ----------------------------------------------------------------------

_pgllens_healthy() { curl -sf "$PGLLENS_URL/health" >/dev/null; }

# Validate --only names before anything slow, so a typo costs a second not 13m.
for c in $ONLY; do
  declare -F "check_$c" >/dev/null || { echo "no such check: $c" >&2; exit 2; }
done

# After argument validation, deliberately: a bad flag should not also demand a
# credential. Every use of $GF is inside a check_*, none of which has run yet.
# An array, not a string: a password containing a space must not word-split.
: "${GRAFANA_ADMIN_PASSWORD:?set GRAFANA_ADMIN_PASSWORD}"
GF=(-u "admin:${GRAFANA_ADMIN_PASSWORD}")

if [ $UP -eq 1 ]; then
  # --wait with no --wait-timeout blocks forever on a container that never turns
  # healthy; 300s is well past a cold image pull and short of the CI step budget.
  $COMPOSE up -d --wait --wait-timeout 300 || { echo "compose up failed"; exit 1; }
fi
poll 120 _pgllens_healthy || { echo "pgllens never became healthy"; exit 1; }

# Duplicate lines already in Loki before this run started. Inherited history is a
# WARN; anything above this baseline afterwards is a failure.
DUP_BASE=$(loki_audit_duplicates)

# An interrupted chaos run must never leave a service stopped. Armed whenever a
# chaos check is going to run -- including via --only, which is how the alloy
# check gets re-run on its own.
CHAOS_TRAP=$CHAOS
case $ONLY in *chaos_*) CHAOS_TRAP=1;; esac
restore_chaos_services() { $COMPOSE start loki tempo alloy >/dev/null 2>&1; }
# Separate INT/TERM handler: with only an EXIT trap, bash's default SIGINT
# disposition still reports the script as killed-by-signal. Restore, then exit
# 130 explicitly so the EXIT trap's second restore is a harmless no-op.
if [ $CHAOS_TRAP -eq 1 ]; then
  trap 'restore_chaos_services; exit 130' INT TERM
  trap restore_chaos_services EXIT
fi

if [ -n "$ONLY" ]; then
  for c in $ONLY; do "check_$c"; done
  trap - EXIT INT TERM
  printf '\n%s\n' "---- verify-stack results ----"
  printf '%s\n' "${RESULTS[@]}"
  exit $FAILED
fi

check_targets_up
check_datasources_healthy
check_dashboards_provisioned
check_first_event_preregistered
check_rejection_in_prometheus
check_first_event_visible_to_increase
check_rejection_in_loki
check_rejection_in_grafana_panel
check_tempo_write_path
check_trace_searchable
check_exemplar_linked
check_synthetic_alert_routed
check_watchdog_firing
check_audit_file_matches_loki
check_audit_loki_no_duplicates
check_audit_no_new_duplicates

if [ $CHAOS -eq 1 ]; then
  check_chaos_loki_outage_zero_loss
  check_chaos_tempo_down_calls_ok
  check_chaos_alloy_down_alert
  # Re-run after the outages: a re-shipping Alloy shows up here, not before.
  check_audit_no_new_duplicates
fi
# Disarm before teardown, or the trap would restart what `compose stop` just stopped.
trap - EXIT INT TERM

printf '\n%s\n' "---- verify-stack results ----"
printf '%s\n' "${RESULTS[@]}"
[ $KEEP -eq 0 ] && $COMPOSE stop >/dev/null
exit $FAILED
