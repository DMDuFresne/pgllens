"""The alert rules are the operator-facing half of this phase's evidence story,
so they get the same treatment as any other config-as-code: parsed, and asserted
against the metric names the code actually emits.

This is not a substitute for `promtool check rules` (see the verification
checklist) -- it is the check that runs with no extra tooling installed.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
RULE_FILES = sorted((ROOT / "ops" / "prometheus" / "rules").glob("*.rules.yml"))
ALERTS = {
    r["alert"]: r
    for path in RULE_FILES
    for g in yaml.safe_load(path.read_text(encoding="utf-8"))["groups"]
    for r in g["rules"]
}


def test_the_four_phase_two_alerts_exist():
    for name in (
        "PgllensAuthFailureSpike",
        "PgllensRateLimitRejectionSpike",
        "PgllensHighToolErrorRate",
        "PgllensReadOnlyGateRejections",
        "PgllensReadOnlyGateRejection",
        "PgllensConnectionErrorsFiring",
    ):
        assert name in ALERTS, name


def test_gate_rejections_alert_on_the_rejected_outcome():
    # "a spike there is someone probing the SQL gate, which is the single most
    # interesting signal the server produces" -- the spec.
    expr = ALERTS["PgllensReadOnlyGateRejections"]["expr"]
    assert 'outcome="rejected"' in expr
    assert ALERTS["PgllensReadOnlyGateRejections"]["labels"]["severity"] == "critical"


def test_alerts_reference_metric_names_the_code_actually_emits():
    emitted = {
        "pgllens_tool_calls_total",
        "pgllens_tool_call_duration_seconds",
        "pgllens_query_duration_seconds",
        "pgllens_connection_errors_total",
        "pgllens_schema_cache_hits_total",
        "pgllens_schema_cache_misses_total",
        "pgllens_auth_failures_total",
        "pgllens_limit_rejections_total",
        "up",
        "loki_source_file_read_bytes_total",
        "loki_write_dropped_entries_total",
        "node_filesystem_avail_bytes",
        "node_filesystem_size_bytes",
        "pg_up",
        "pg_stat_activity_count",
        "pg_settings_max_connections",
    }
    for name, rule in ALERTS.items():
        referenced = {
            # Metrics without a label selector (e.g. `foo_total[5m]`) still carry
            # the range-vector bracket after a plain split on "{" -- strip both.
            token.split("{")[0].split("[")[0]
            for token in rule["expr"].replace("(", " ").replace(")", " ").split()
            if token.startswith(("pgllens_", "up{", "loki_", "node_", "pg_"))
        }
        assert referenced <= emitted, f"{name} references an unemitted metric: {referenced - emitted}"


def test_the_tool_error_rule_uses_a_real_outcome_value():
    # Phase 1 regression: the rule matched `unknown_database`, which pgllens
    # never emits (the outcome enum in tools/_util.py is
    # ok|rejected|unknown_schema|not_found|db_error), so it could never fire.
    expr = ALERTS["PgllensHighToolErrorRate"]["expr"]
    assert "unknown_database" not in expr
    assert "db_error" in expr


def test_single_rejection_alert_is_info_and_does_not_hold():
    rule = ALERTS["PgllensReadOnlyGateRejection"]
    assert rule["labels"]["severity"] == "info"
    assert rule["for"] == "0m"
    assert 'outcome="rejected"' in rule["expr"]


def test_connection_error_alert_window_exceeds_its_hold():
    # A single burst must stay inside the window for the whole `for` period.
    rule = ALERTS["PgllensConnectionErrorsFiring"]
    assert "[10m]" in rule["expr"]
    assert rule["for"] == "1m"


def test_no_annotation_still_claims_this_is_sql_server():
    for name, rule in ALERTS.items():
        text = yaml.safe_dump(rule.get("annotations", {}))
        assert "SQL Server" not in text, name


def test_no_dashboard_still_claims_this_is_sql_server():
    for path in (ROOT / "ops" / "grafana" / "dashboards").glob("*.json"):
        assert "SQL Server" not in path.read_text(encoding="utf-8"), path.name


def test_every_alert_has_a_promtool_case():
    # A rule with no promtool case is a rule nobody has proved can fire. Both
    # directions matter: a case naming an alert that no longer exists is dead too.
    tested = {
        case["alertname"]
        for path in (ROOT / "ops" / "prometheus" / "rules" / "tests").glob("*.test.yml")
        for t in yaml.safe_load(path.read_text(encoding="utf-8"))["tests"]
        for case in t.get("alert_rule_test", [])
    }
    assert tested == set(ALERTS)


def test_every_alert_has_a_runbook_entry():
    runbook = (ROOT / "docs" / "runbook.md").read_text(encoding="utf-8")
    for name in ALERTS:
        assert f"| `{name}` |" in runbook, name  # one table row per alert under "An alert fired"
