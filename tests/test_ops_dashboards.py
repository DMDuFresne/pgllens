"""Every panel is backed by data the code emits, or it does not ship.

Metric names come from obs/metrics.py (parsed, not hand-listed) plus the exporters'
well-known prefixes. Audit JSON fields come from tools/_util.py's audit() call.
"""
import json
import re
from pathlib import Path

import pytest

from tests.conftest_docker import OPS, ROOT

DASHBOARDS = sorted((OPS / "grafana" / "dashboards").glob("*.json"))
METRICS_SRC = (ROOT / "src" / "pgllens" / "obs" / "metrics.py").read_text(encoding="utf-8")
UTIL_SRC = (ROOT / "src" / "pgllens" / "tools" / "_util.py").read_text(encoding="utf-8")

EMITTED_METRICS = set(re.findall(r'"(pgllens_[a-z_]+)"', METRICS_SRC))
EXTERNAL_PREFIXES = ("up", "scrape_", "node_", "container_", "pg_", "loki_", "tempo_",
                     "prometheus_", "alertmanager_", "grafana_", "alloy_", "go_", "process_",
                     "ALERTS", "machine_")
# Suffixes prometheus_client adds to histograms/counters.
SUFFIXES = ("_bucket", "_count", "_sum", "_total", "_created")

# Slice the audit() call by its closing paren on its own line -- splitting on the first
# ")" would stop inside round(duration_s * 1000) and lose every field after it.
_call_start = UTIL_SRC.index("audit_mod.audit(")
_call_end = UTIL_SRC.index("\n            )", _call_start)
audit_call = UTIL_SRC[_call_start:_call_end]
# trace_id is pre-declared: Phase 2 adds it via **extra.
AUDIT_FIELDS = {"timestamp", "event", "trace_id"} | set(re.findall(r"\b(\w+)=", audit_call))


def panels(d: dict):
    for p in d.get("panels", []):
        yield p
        yield from p.get("panels", [])


def base_metric(name: str) -> str:
    for s in SUFFIXES:
        if name.endswith(s) and name[: -len(s)] in EMITTED_METRICS:
            return name[: -len(s)]
    return name


def test_audit_fields_parsed():
    assert {"tool", "outcome", "duration_ms", "sub", "client_id", "ip", "args_hash",
            "rows"} <= AUDIT_FIELDS


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_prometheus_queries_reference_emitted_metrics(path: Path):
    d = json.loads(path.read_text(encoding="utf-8"))
    for p in panels(d):
        for t in p.get("targets", []):
            ds = (t.get("datasource") or p.get("datasource") or {}).get("uid")
            if ds != "prometheus":
                continue
            for name in re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\{|\[)", t.get("expr", "")):
                if name.startswith(EXTERNAL_PREFIXES) or name in ("sum", "rate", "increase",
                        "histogram_quantile", "max_over_time", "count", "avg", "min", "max", "by", "le"):
                    continue
                assert base_metric(name) in EMITTED_METRICS, f"{path.name} / {p.get('title')}: {name}"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_loki_queries_reference_real_audit_fields(path: Path):
    d = json.loads(path.read_text(encoding="utf-8"))
    for p in panels(d):
        for t in p.get("targets", []):
            ds = (t.get("datasource") or p.get("datasource") or {}).get("uid")
            expr = t.get("expr", "")
            if ds != "loki" or 'job="pgllens-audit"' not in expr:
                continue
            fields = set(re.findall(r"by \(([a-z_, ]+)\)", expr))
            fields = {f.strip() for group in fields for f in group.split(",")}
            fields |= set(re.findall(r"\|\s*([a-z_]+)\s*(?:!=|=|=~|!~)", expr)) - {"__error__"}
            assert fields <= AUDIT_FIELDS, f"{path.name} / {p.get('title')}: {fields - AUDIT_FIELDS}"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_every_panel_has_a_description_and_units(path: Path):
    d = json.loads(path.read_text(encoding="utf-8"))
    for p in panels(d):
        if p["type"] in ("text", "row"):
            continue
        assert p.get("description"), f"{path.name} / {p.get('title')}: no description"
        if p["type"] in ("timeseries", "stat", "bargauge", "gauge"):
            assert p["fieldConfig"]["defaults"].get("unit"), f"{path.name} / {p.get('title')}: no unit"


def test_dashboards_are_code():
    prov = (OPS / "grafana" / "provisioning" / "dashboards" / "dashboards.yml").read_text(encoding="utf-8")
    assert "allowUiUpdates: false" in prov


def test_outcome_colours_are_consistent():
    # ok green, rejected red, error amber, everywhere.
    expected = {"ok": "green", "rejected": "red", "db_error": "orange", "unknown_schema": "orange",
                "not_found": "yellow"}
    for path in DASHBOARDS:
        text = path.read_text(encoding="utf-8")
        for outcome, colour in expected.items():
            pattern = (r'"matcher":\s*\{\s*"id":\s*"byName",\s*"options":\s*"'
                       + outcome + r'"\s*\}.*?"fixedColor":\s*"(\w+)"')
            for m in re.finditer(pattern, text, re.DOTALL):
                assert m.group(1) == colour, f"{path.name}: {outcome} is {m.group(1)}"
