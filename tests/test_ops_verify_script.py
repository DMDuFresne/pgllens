"""Shape tests for scripts/verify-stack.sh.

The script itself can only be proven against a live stack; what a unit test can
pin is that every check the spec names is actually declared, that the file
parses under bash, and that git carries the executable bit (CI runs it directly).
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from pgllens.config import Settings
from pgllens.database.pool import Db
from pgllens.obs import metrics
from pgllens.server import create_mcp

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verify-stack.sh"
REL = SCRIPT.relative_to(ROOT).as_posix()
TEXT = SCRIPT.read_text(encoding="utf-8")

REQUIRED_CHECKS = [
    "targets_up",
    "datasources_healthy",
    "dashboards_provisioned",
    "first_event_preregistered",
    "rejection_in_prometheus",
    "first_event_visible_to_increase",
    "rejection_in_loki",
    "rejection_in_grafana_panel",
    "tempo_write_path",
    "trace_searchable",
    "exemplar_linked",
    "synthetic_alert_routed",
    "watchdog_firing",
    "audit_file_matches_loki",
    "audit_loki_no_duplicates",
    "audit_no_new_duplicates",
]
CHAOS_CHECKS = [
    "chaos_loki_outage_zero_loss",
    "chaos_tempo_down_calls_ok",
    "chaos_alloy_down_alert",
]


def test_script_declares_every_spec_check() -> None:
    for name in REQUIRED_CHECKS + CHAOS_CHECKS:
        assert f"check_{name}()" in TEXT, name


def test_every_declared_check_is_called() -> None:
    for name in REQUIRED_CHECKS + CHAOS_CHECKS:
        # once as the definition, at least once as a call
        assert TEXT.count(f"check_{name}") >= 2, name


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
def test_script_parses() -> None:
    # relative + cwd: Git Bash mangles a Windows absolute path passed as argv.
    proc = subprocess.run(
        ["bash", "-n", REL],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr


def test_script_is_executable_in_git() -> None:
    mode = subprocess.run(
        ["git", "ls-files", "-s", REL],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    ).stdout
    assert mode.startswith("100755"), mode


async def test_metrics_preregistration_count_matches_tool_matrix() -> None:
    """The script's EXPECT_SERIES must equal tools x outcomes, computed from the
    server rather than grepped: a substring check would still pass if the tool
    count changed and 186 happened to appear anywhere else in the file."""
    settings = Settings(
        _env_file=None,
        database_url="postgresql://u:p@localhost:5432/flux",
        exposed_schemas="public",
    )
    server = create_mcp(settings, Db(settings), intro=None)
    tools = await server.list_tools()
    expected = len(tools) * len(metrics.TOOL_OUTCOMES)

    match = re.search(r"^EXPECT_SERIES=(\d+)$", TEXT, re.MULTILINE)
    assert match, "verify-stack.sh must define EXPECT_SERIES=<n> on its own line"
    assert int(match.group(1)) == expected, f"tools={len(tools)} outcomes={len(metrics.TOOL_OUTCOMES)}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
def test_only_flag_rejects_an_unknown_check() -> None:
    """--only must validate before the slow path -- a typo should cost a second,
    not thirteen minutes of chaos wait. Exits before any docker/curl call."""
    # No GRAFANA_ADMIN_PASSWORD needed: the script validates arguments before it
    # demands the credential (and Git Bash on Windows ignores an env block passed
    # in by a Python parent anyway, so setting one here would not work).
    proc = subprocess.run(
        ["bash", REL, "--only", "nope"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=30,
        check=False,
    )
    assert proc.returncode == 2, proc.stdout + proc.stderr
    assert "no such check: nope" in proc.stderr
