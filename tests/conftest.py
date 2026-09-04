from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from pgllens.config import Settings
from pgllens.database.format import QueryResult

FROZEN_NOW = datetime(2026, 9, 3, 15, 49, 3, tzinfo=UTC)
FROZEN_RID = "01TESTREQUESTID"


def pytest_addoption(parser):
    parser.addoption("--update-golden", action="store_true", default=False,
                      help="rewrite tests/golden/*.md from current output")


@pytest.fixture(autouse=True)
def frozen_render(monkeypatch):
    """Every rendered response in the unit suite carries the same timestamp and
    request id, so goldens and substring assertions are deterministic."""
    from pgllens.tools import _util
    monkeypatch.setattr(_util, "_now", lambda: FROZEN_NOW)
    monkeypatch.setattr(_util, "_request_id", lambda: FROZEN_RID)


class _FakeClock:
    """Simple clock for testing time-based logic. Monkeypatches time.monotonic."""

    def __init__(self):
        self._now = 0.0

    def advance(self, delta: float) -> None:
        """Advance the fake clock by delta seconds."""
        self._now += delta

    def monotonic(self) -> float:
        return self._now


@pytest.fixture
def fake_clock(monkeypatch):
    """Fixture providing a fake clock that advances in tests.

    Patches pgllens.oauth.ratelimit.time.monotonic to use the fake clock.
    Usage:
        fake_clock.advance(1.5)  # Advance 1.5 seconds
    """
    clock = _FakeClock()
    monkeypatch.setattr("pgllens.oauth.ratelimit.time.monotonic", clock.monotonic)
    return clock


@pytest.fixture(autouse=True)
def _no_env_file():
    """Unit tests must never read the developer's real .env."""
    prior = Settings.model_config.get("env_file")
    Settings.model_config["env_file"] = None
    yield
    Settings.model_config["env_file"] = prior


@pytest.fixture(autouse=True)
def _reset_limits_module_state():
    """`pgllens.limits.configure_limits` sets process-global module state, so
    without this a test that leaves the cost budget on (test_limits.py) would
    leak into any later test that happens to exercise query.py's cost gate --
    order-dependent flakiness, and a leftover store pointed at another test's
    (possibly already-closed) fake Redis client. Reset before, not after, so
    every test starts from "budget off" regardless of what ran before it."""
    import pgllens.limits as limits_mod

    limits_mod._budget_per_window = None
    limits_mod._budget_store = limits_mod.InMemoryLimitStore()
    yield


class FakeMCP:
    def __init__(self):
        self.tools = {}

    def tool(self, **kw):
        def deco(fn):
            self.tools[fn.__name__] = fn
            return fn

        return deco


def make_registered(mod, intro=None, caps=None):
    """Register a tool module against fakes; returns (mcp, db, intro)."""
    mcp, db, settings = FakeMCP(), MagicMock(), MagicMock()
    db.run_readonly = AsyncMock(return_value=QueryResult(["a"], [(1,)], False))
    db.run_system = AsyncMock(return_value=QueryResult(["a"], [(1,)], False))
    settings.exposed_schemas = ["public", "wms"]
    settings.default_schema = "public"
    settings.redact_columns = []
    settings.max_estimated_cost = None
    settings.domain_context_text = None
    settings.max_rows = 1000
    mod.register(mcp, db, settings, intro, caps)
    return mcp, db, intro
