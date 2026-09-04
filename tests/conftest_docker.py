"""Run pinned upstream CLIs (promtool, amtool, alloy, loki, tempo) through Docker.

Tests that need these skip cleanly when Docker is absent, so `uv run pytest` on a
laptop without Docker stays green and CI (which has Docker) runs them.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "ops"

PROM_IMAGE = "prom/prometheus:v3.12.0"
AM_IMAGE = "prom/alertmanager:v0.33.0"
ALLOY_IMAGE = "grafana/alloy:v1.17.0"
LOKI_IMAGE = "grafana/loki:3.7.3"
TEMPO_IMAGE = "grafana/tempo:3.0.2"

docker = pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI not available")


def _host(path: Path) -> str:
    # Git Bash on Windows rewrites /c/... mount sources; Docker wants C:/...
    return str(path).replace("\\", "/")


def run_in_image(image: str, entrypoint: str | None, args: list[str],
                 mounts: dict[Path, str],
                 env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "run", "--rm"]
    if entrypoint:
        cmd += ["--entrypoint", entrypoint]
    for src, dst in mounts.items():
        cmd += ["-v", f"{_host(src)}:{dst}:ro"]
    for key, value in (env or {}).items():
        cmd += ["-e", f"{key}={value}"]
    cmd += [image, *args]
    run_env = {**os.environ, "MSYS_NO_PATHCONV": "1", "MSYS2_ARG_CONV_EXCL": "*"}
    return subprocess.run(cmd, capture_output=True, text=True, env=run_env,
                          check=False, timeout=180)


def run_promtool(args: list[str]) -> subprocess.CompletedProcess[str]:
    return run_in_image(PROM_IMAGE, "promtool", args,
                        {OPS / "prometheus": "/etc/prometheus"})
