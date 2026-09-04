"""Shape tests for .github/workflows/stack.yml.

Pins the pieces the task brief requires: the job exists, the path filter that
replaces the invalid `changed_files` expression is present, the chaos step is
gated, and the auth mode stays disabled for the demo stack.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "stack.yml"


def _load():
    # YAML 1.1 treats bare `on:` as boolean True; load the raw text as the
    # workflow's own key so we don't have to special-case that here.
    data = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return data


def test_workflow_parses():
    data = _load()
    assert isinstance(data, dict)


def test_stack_job_exists():
    data = _load()
    assert "stack" in data["jobs"]


def test_pull_request_paths_filter():
    data = _load()
    paths = data[True]["pull_request"]["paths"]
    for want in ("ops/**", "scripts/**", "Dockerfile", "pyproject.toml", "uv.lock"):
        assert want in paths, want


def test_chaos_step_conditional():
    """Substring, not the exact expression: the point is that the ~40-minute
    chaos layer is schedule-gated, not the punctuation of the condition."""
    data = _load()
    steps = data["jobs"]["stack"]["steps"]
    chaos = next(s for s in steps if s.get("name") == "Chaos layer")
    assert "schedule" in chaos["if"]
    assert "github.event_name" in chaos["if"]


def test_build_tag_matches_version_env():
    """The image tag compose then runs is a literal, so it silently drifts from
    PGLLENS_VERSION. Pin them together."""
    data = _load()
    version = data["jobs"]["stack"]["env"]["PGLLENS_VERSION"]
    build = next(s for s in data["jobs"]["stack"]["steps"] if s.get("name") == "Build the image compose will run")
    tags = build["with"]["tags"].split("#")[0].strip()
    assert tags.endswith(f":{version}"), f"{tags!r} does not end with :{version}"


def test_push_trigger_shares_the_pull_request_paths():
    data = _load()
    assert data[True]["push"]["paths"] == data[True]["pull_request"]["paths"]


def test_step_timeouts_fit_the_job_budget():
    data = _load()
    job = data["jobs"]["stack"]
    steps = {s["name"]: s for s in job["steps"] if "name" in s}
    assert steps["Stack layer"]["timeout-minutes"] == 15
    assert steps["Chaos layer"]["timeout-minutes"] == 40
    assert job["timeout-minutes"] == 60


def test_auth_mode_none():
    data = _load()
    assert data["jobs"]["stack"]["env"]["MCP_AUTH_MODE"] == "none"


def test_infra_tier_enabled():
    """CI is the run that proves the infra exporters; a Docker Desktop host cannot."""
    data = _load()
    assert data["jobs"]["stack"]["env"]["INFRA"] == "1"


RELEASE = ROOT / ".github" / "workflows" / "release.yml"


def test_release_publishes_tagged_image_to_ghcr():
    """A `vX.Y.Z` tag publishes ghcr.io/dmdufresne/pgllens:<X.Y.Z> and :latest,
    and refuses to when the tag disagrees with pyproject's version."""
    text = RELEASE.read_text(encoding="utf-8")
    data = yaml.safe_load(text)
    assert data[True]["push"]["tags"] == ["v*"]
    steps = data["jobs"]["release"]["steps"]
    assert any("pyproject.toml" in s.get("run", "") for s in steps)
    assert "ghcr.io/dmdufresne/pgllens" in text
    assert "type=semver,pattern={{version}}" in text
    # metadata-action's latest=auto stamps :latest itself; a raw line would also
    # stamp it on an rc tag.
    assert "type=raw,value=latest" not in text
