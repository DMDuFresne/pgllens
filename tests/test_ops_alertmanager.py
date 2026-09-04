"""Both Alertmanager renderings must parse, and the Slack one must route the way
the spec says: critical fast, warning slow, info once, Watchdog on its own route."""
import re

import yaml

from tests.conftest_docker import AM_IMAGE, OPS, docker, run_in_image

AM = OPS / "alertmanager"


def load(name: str) -> dict:
    return yaml.safe_load((AM / name).read_text(encoding="utf-8"))


def test_null_config_routes_everything_to_null():
    cfg = load("alertmanager.null.yml")
    assert cfg["route"]["receiver"] == "null"
    assert [r["name"] for r in cfg["receivers"]] == ["null"]


def test_slack_config_routes_by_severity():
    cfg = load("alertmanager.slack.yml")
    by_match = {r["matchers"][0]: r for r in cfg["route"]["routes"]}
    assert by_match['alertname="Watchdog"']["receiver"] == "slack-heartbeat"
    assert by_match['alertname="Watchdog"']["repeat_interval"] == "4h"
    assert by_match['alertname="Watchdog"']["group_wait"] == "0s"
    assert by_match['severity="critical"']["group_wait"] == "30s"
    assert by_match['severity="critical"']["repeat_interval"] == "4h"
    assert by_match['severity="warning"']["group_wait"] == "5m"
    assert by_match['severity="warning"']["repeat_interval"] == "12h"
    assert by_match['severity="info"']["repeat_interval"] == "8760h"
    assert by_match['severity="info"']["group_wait"] == "1m"


def test_slack_config_reads_webhook_from_file_not_inline():
    text = (AM / "alertmanager.slack.yml").read_text(encoding="utf-8")
    assert "api_url_file" in text and "hooks.slack.com" not in text


def test_slack_webhook_path_matches_what_the_entrypoint_writes():
    # Two files, one path. If either side moves, Alertmanager reads a file that
    # does not exist and every Slack notification fails silently.
    written = re.search(r"^\s*printf .* > (\S+)$",
                        (AM / "entrypoint.sh").read_text(encoding="utf-8"), re.MULTILINE)
    assert written, "entrypoint.sh no longer writes the webhook to a file"
    configured = {sc["api_url_file"]
                  for r in load("alertmanager.slack.yml")["receivers"]
                  for sc in r["slack_configs"]}
    assert configured == {written.group(1)}


def test_inhibitions_collapse_root_causes():
    cfg = load("alertmanager.slack.yml")
    sources = {tuple(i["source_matchers"]) for i in cfg["inhibit_rules"]}
    assert ('alertname="PgllensDown"',) in sources
    assert ('alertname="MonitoringTargetDown"', 'job="loki"') in sources


@docker
def test_both_configs_pass_amtool():
    for name in ("alertmanager.null.yml", "alertmanager.slack.yml"):
        result = run_in_image(AM_IMAGE, "amtool", ["check-config", f"/etc/alertmanager/{name}"],
                              {AM: "/etc/alertmanager"})
        assert result.returncode == 0, name + "\n" + result.stdout + result.stderr
