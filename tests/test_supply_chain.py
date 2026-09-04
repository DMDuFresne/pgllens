"""The build's security properties, asserted as text.

A container that quietly regains a shell, a root user, or an unpinned install is
exactly the kind of regression nobody notices in review. These are cheap
assertions on the Dockerfile and compose file; the real proof is the deferred
`docker run` checks in the verification checklist.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "Dockerfile").read_text(encoding="utf-8")
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
WORKFLOW = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))


def test_dependencies_are_installed_with_hashes():
    # uv export emits hashes for every pinned dependency; --require-hashes makes
    # pip refuse anything the lock did not pin. Without it, a compromised index
    # can serve a different artifact for the same version.
    assert "--require-hashes" in DOCKERFILE
    assert "uv export" in DOCKERFILE


def test_the_final_stage_has_no_shell():
    # distroless: no /bin/sh at all, so an RCE has no interpreter to reach for.
    assert "distroless" in DOCKERFILE


def test_the_container_runs_as_a_non_root_user():
    assert "USER 1001" in DOCKERFILE or "USER lens" in DOCKERFILE


def test_the_compose_service_has_a_read_only_root_filesystem():
    service = COMPOSE["services"]["pgllens"]
    assert service["read_only"] is True
    # The app writes nowhere except the audit volume; /tmp is a tmpfs so a
    # read-only root does not break Python's own temp usage.
    assert any("/tmp" in str(t) for t in service["tmpfs"])


def test_the_compose_service_drops_capabilities_and_privilege_escalation():
    service = COMPOSE["services"]["pgllens"]
    assert service["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in service["security_opt"]


def test_the_sbom_script_exists_and_is_executable_text():
    script = (ROOT / "scripts" / "sbom.sh").read_text(encoding="utf-8")
    assert script.startswith("#!/usr/bin/env bash")
    assert "set -euo pipefail" in script
    assert "syft" in script


def test_ci_fails_the_build_on_a_fixable_critical():
    jobs = WORKFLOW["jobs"]
    scan = yaml.safe_dump(jobs["image"])
    assert "CRITICAL" in scan
    assert "ignore-unfixed" in scan
    assert "exit-code" in scan


def test_ci_runs_the_same_gates_this_plan_runs_locally():
    lint = yaml.safe_dump(WORKFLOW["jobs"]["test"])
    for command in ("ruff check", "mypy src", "pytest"):
        assert command in lint, command


def test_ci_publishes_an_sbom():
    assert "sbom" in yaml.safe_dump(WORKFLOW).lower()
