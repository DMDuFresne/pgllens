"""The compose file is the deployment contract. Every profile must render from a
minimal environment, no image may float, and every variable compose reads must be
documented in .env.example."""
import os
import re
import shutil
import subprocess

import pytest
import yaml

from tests.conftest_docker import ROOT

COMPOSE = ROOT / "docker-compose.yml"
TEXT = COMPOSE.read_text(encoding="utf-8")
MINIMAL_ENV = {
    # PROGRAMFILES is how the Windows docker CLI locates its compose plugin; without
    # it `docker compose` is an unknown command. The rest is the minimum to exec.
    **{k: v for k, v in os.environ.items()
       if k in ("PATH", "SYSTEMROOT", "HOME", "USERPROFILE", "PROGRAMFILES")},
    "DATABASE_URL": "postgresql://u:p@db:5432/d",
}
compose_cli = pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not available")


def render(*profiles: str, env_extra: dict[str, str] | None = None) -> dict:
    cmd = ["docker", "compose", "-f", str(COMPOSE), "--env-file", os.devnull]
    for p in profiles:
        cmd += ["--profile", p]
    cmd += ["config"]
    env = {**MINIMAL_ENV, **(env_extra or {})}
    out = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    assert out.returncode == 0, out.stderr
    return yaml.safe_load(out.stdout)


@compose_cli
def test_solo_tier_is_pgllens_alone():
    # DATABASE_URL alone must render. Compose interpolates the whole file before it
    # selects profiles, so any `:?` on an observe-only variable would break Solo.
    assert set(render()["services"]) == {"pgllens"}


@compose_cli
def test_observe_tier_adds_the_monitoring_plane():
    assert set(render("observe", env_extra={"GRAFANA_ADMIN_PASSWORD": "x"})["services"]) == {
        "pgllens", "prometheus", "alertmanager", "grafana", "loki", "alloy", "tempo"}


@compose_cli
def test_observe_tier_renders_without_grafana_password():
    # Enforcement lives in grafana's entrypoint (at start), not in interpolation.
    assert "grafana" in render("observe")["services"]


@compose_cli
def test_infra_tier_adds_exporters():
    services = set(render("observe", "infra",
                          env_extra={"GRAFANA_ADMIN_PASSWORD": "x"})["services"])
    assert {"node-exporter", "cadvisor", "postgres-exporter"} <= services


@compose_cli
def test_pgllens_never_depends_on_monitoring():
    cfg = render("observe", "infra", env_extra={"GRAFANA_ADMIN_PASSWORD": "x"})
    assert "depends_on" not in cfg["services"]["pgllens"]


def test_images_are_digest_pinned_and_never_latest():
    for m in re.finditer(r"^\s*image:\s*(\S+)", TEXT, re.MULTILINE):
        image = m.group(1)
        if image.startswith("ghcr.io/dmdufresne/pgllens"):
            continue  # our own image is version-tagged, not digest-pinned (see Task 8 notes)
        assert "@sha256:" in image, image
        assert ":latest" not in image, image


def test_app_image_is_ghcr():
    assert re.search(r"image:\s*ghcr\.io/dmdufresne/pgllens:\$\{PGLLENS_VERSION:-", TEXT)


def test_only_admin_uis_publish_ports():
    cfg = yaml.safe_load(TEXT)
    published = {name for name, svc in cfg["services"].items() if svc.get("ports")}
    assert published == {"pgllens", "prometheus", "alertmanager", "grafana"}


def test_every_stack_container_is_hardened():
    cfg = yaml.safe_load(TEXT)
    for name, svc in cfg["services"].items():
        assert svc.get("security_opt") == ["no-new-privileges:true"], name
        if name != "cadvisor":
            # cAdvisor needs caps to read cgroups on some kernels; documented
            # exception (docs/DEPLOY.md#security-posture). It is exempt from cap_drop only.
            assert svc.get("cap_drop") == ["ALL"], name
        assert "mem_limit" in svc, name


def test_every_variable_compose_reads_is_documented():
    documented = set(re.findall(r"^#?\s*([A-Z][A-Z0-9_]+)=", (ROOT / ".env.example").read_text(encoding="utf-8"), re.MULTILINE))
    used = set(re.findall(r"\$\{([A-Z][A-Z0-9_]+)", TEXT))
    assert used <= documented, used - documented


def test_tracing_is_on_by_default_and_exemplars_enabled():
    assert "OTEL_ENABLED: ${OTEL_ENABLED:-true}" in TEXT
    assert '"--enable-feature=exemplar-storage"' in TEXT


def test_base_compose_is_pull_only():
    # Prod runs the base file alone and must never need a source tree; the build
    # context lives in docker-compose.dev.yml.
    cfg = yaml.safe_load(TEXT)
    assert not any("build" in svc for svc in cfg["services"].values())


def test_dev_overlay_adds_build_and_nothing_else():
    cfg = yaml.safe_load((ROOT / "docker-compose.dev.yml").read_text(encoding="utf-8"))
    assert cfg == {"services": {"pgllens": {"build": "."}}}
