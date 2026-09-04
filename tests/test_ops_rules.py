"""promtool unit tests for every alert rule, run through the pinned Prometheus image.

`ops/prometheus/rules/tests/*.test.yml` holds synthetic series and the alerts they
must (or must not) produce. This is the check that catches "the rule can never fire".
"""
from pathlib import Path

import pytest

from tests.conftest_docker import OPS, PROM_IMAGE, docker, run_in_image, run_promtool

TESTS = sorted((OPS / "prometheus" / "rules" / "tests").glob("*.test.yml"))
RULES = sorted((OPS / "prometheus" / "rules").glob("*.rules.yml"))


@docker
@pytest.mark.parametrize("rules_file", RULES, ids=lambda p: p.name)
def test_rule_files_are_valid(rules_file: Path):
    result = run_promtool(["check", "rules", f"/etc/prometheus/rules/{rules_file.name}"])
    assert result.returncode == 0, result.stdout + result.stderr


@docker
@pytest.mark.parametrize("test_file", TESTS, ids=lambda p: p.name)
def test_rules_behave_as_specified(test_file: Path):
    result = run_promtool(["test", "rules", f"/etc/prometheus/rules/tests/{test_file.name}"])
    assert result.returncode == 0, result.stdout + result.stderr


@docker
def test_prometheus_config_is_valid():
    result = run_promtool(["check", "config", "/etc/prometheus/prometheus.yml"])
    assert result.returncode == 0, result.stdout + result.stderr


@docker
def test_entrypoint_output_is_a_valid_config_with_remote_write():
    # The container never runs the committed prometheus.yml -- entrypoint.sh copies
    # it to /tmp and appends remote_write. Run the real script with its final `exec
    # prometheus` swapped for promtool, so what is checked is what would be served.
    script = (
        "sed 's|^exec .*|promtool check config /tmp/prometheus.yml|'"
        " /etc/prometheus/entrypoint.sh > /tmp/e.sh && sh /tmp/e.sh"
    )
    result = run_in_image(
        PROM_IMAGE, "/bin/sh", ["-c", script],
        {OPS / "prometheus": "/etc/prometheus"},
        env={"PROM_REMOTE_WRITE_URL": "https://example.invalid/api/v1/write"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SUCCESS" in result.stdout, result.stdout + result.stderr


def test_every_scrape_job_named_in_rules_exists():
    import re

    import yaml
    prom = yaml.safe_load((OPS / "prometheus" / "prometheus.yml").read_text(encoding="utf-8"))
    jobs = {s["job_name"] for s in prom["scrape_configs"]}
    for rules in (OPS / "prometheus" / "rules").glob("*.rules.yml"):
        for m in re.finditer(r'job=~?"([^"]+)"', rules.read_text(encoding="utf-8")):
            for job in m.group(1).split("|"):
                assert job in jobs, f"{rules.name} references unknown job {job}"


def test_rule_files_are_absolute_because_entrypoint_relocates_the_config():
    import yaml
    prom = yaml.safe_load((OPS / "prometheus" / "prometheus.yml").read_text(encoding="utf-8"))
    for entry in prom["rule_files"]:
        assert entry.startswith("/etc/prometheus/"), entry
