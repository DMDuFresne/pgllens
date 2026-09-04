"""Prometheus metric instruments + the stable ``record_*`` helper API.

What is actually worth measuring for a read-only PostgreSQL lens:
tool-call outcomes (count + duration), database query duration, connection
errors, and schema-introspection cache hits/misses.

Uses ``prometheus_client`` directly rather than routing through an OTel
``MeterProvider`` -- there is nothing here that needs OTLP metric push, only a
``/metrics`` scrape endpoint, so the extra indirection buys nothing.

OPTIONAL DEPENDENCY DISCIPLINE: importing this module must NEVER fail even with
zero packages from the ``observability`` extra installed. Every public function
is a safe no-op when metrics are disabled or the dependency is absent -- a
missing optional dependency must never be a startup failure.
"""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from collections.abc import Iterable

    from pgllens.config import Settings

logger = logging.getLogger("pgllens")

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Histogram,
        generate_latest,
    )
    from prometheus_client.openmetrics.exposition import (
        CONTENT_TYPE_LATEST as OM_CONTENT_TYPE,
    )
    from prometheus_client.openmetrics.exposition import (
        generate_latest as om_generate_latest,
    )

    _DEPS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the extra installed
    _DEPS_AVAILABLE = False

_DEFAULT_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# The complete label universes. Pre-registered at startup so every series exists
# at 0 before its first event; a series that first appears at 1 is invisible to
# rate()/increase(). Adding an outcome here is a reviewed change: tools/_util.py's
# OUTCOME map and tests/test_tool_registry.py both pin the set.
TOOL_OUTCOMES: tuple[str, ...] = (
    "ok", "rejected", "unknown_schema", "not_found", "unavailable", "db_error",
)
# Deliberately simple: only the two outcomes database/pool.py actually emits.
# Connection failures have their own counter (pgllens_connection_errors_total),
# so a "connection_error" child here would be a series that can never leave 0.
QUERY_OUTCOMES: tuple[str, ...] = ("ok", "error")
LIMIT_KINDS: tuple[str, ...] = ("calls", "cost", "concurrency", "scope")

# --- Module state --------------------------------------------------------------
# Instruments are (re)created in configure_metrics() against a fresh registry each
# time, so repeated calls (e.g. across tests, or a re-built app) never hit
# prometheus_client's "Duplicated timeseries" error from re-registering onto the
# shared default REGISTRY.
_enabled: bool = False
_registry: Any | None = None
_tool_calls_total: Any | None = None
_tool_call_duration: Any | None = None
_query_duration: Any | None = None
_connection_errors_total: Any | None = None
_schema_cache_hits: Any | None = None
_schema_cache_misses: Any | None = None
_auth_failures_total: Any | None = None
_limit_rejections_total: Any | None = None


def deps_available() -> bool:
    """True when the ``observability`` extra (prometheus-client) is installed."""
    return _DEPS_AVAILABLE


def enabled() -> bool:
    """True only when ``configure_metrics`` was called with metrics enabled AND
    the optional dependency imported successfully."""
    return _enabled


def configure_metrics(settings: Settings) -> None:
    """Idempotently (re)configure the metric instruments from ``settings``.

    No-ops (leaves ``enabled()`` False) when ``settings.metrics_enabled`` is
    false or ``prometheus_client`` is unavailable. Never raises.
    """
    global _enabled, _registry
    global _tool_calls_total, _tool_call_duration
    global _query_duration, _connection_errors_total
    global _schema_cache_hits, _schema_cache_misses
    global _auth_failures_total, _limit_rejections_total

    if not settings.metrics_enabled or not _DEPS_AVAILABLE:
        if settings.metrics_enabled and not _DEPS_AVAILABLE:
            # A monitoring endpoint that is enabled-but-inert must announce itself:
            # otherwise /metrics 200s with an empty body and Prometheus reports the
            # target as "up" while scraping nothing, forever. But metrics_enabled
            # now defaults to true, so only an operator who *asked* for metrics gets
            # a WARNING; a plain base install gets one INFO line and no noise.
            if "metrics_enabled" in settings.model_fields_set:
                logger.warning(
                    "METRICS_ENABLED is true but prometheus-client is not installed; "
                    "/metrics will not be served. Install with: "
                    "pip install 'pgllens[observability]'"
                )
            else:
                logger.info("metrics unavailable (observability extra not installed)")
        _enabled = False
        return

    try:
        _registry = CollectorRegistry()
        _tool_calls_total = Counter(
            "pgllens_tool_calls_total",
            "MCP tool invocations by tool and outcome.",
            # LABEL CARDINALITY: `tool` and `outcome` are the ONLY labels on every
            # instrument below, both drawn from small fixed enums (the 31 tool
            # names; outcome kinds like ok/rejected/db_error) -- never a database
            # name, SQL text, or other user input. An unbounded label value is a
            # Prometheus/metrics-backend outage waiting to happen.
            ["tool", "outcome"],
            registry=_registry,
        )
        _tool_call_duration = Histogram(
            "pgllens_tool_call_duration_seconds",
            "Duration of an MCP tool call.",
            ["tool", "outcome"],
            registry=_registry,
        )
        _query_duration = Histogram(
            "pgllens_query_duration_seconds",
            "Duration of a single database round-trip, by outcome.",
            ["outcome"],
            registry=_registry,
        )
        _connection_errors_total = Counter(
            "pgllens_connection_errors_total",
            "Database connection failures.",
            registry=_registry,
        )
        _schema_cache_hits = Counter(
            "pgllens_schema_cache_hits_total",
            "Schema-introspection cache hits.",
            registry=_registry,
        )
        _schema_cache_misses = Counter(
            "pgllens_schema_cache_misses_total",
            "Schema-introspection cache misses.",
            registry=_registry,
        )
        _auth_failures_total = Counter(
            "pgllens_auth_failures_total",
            "Rejected authentication attempts (bad, expired or wrong-audience tokens).",
            registry=_registry,
        )
        _limit_rejections_total = Counter(
            "pgllens_limit_rejections_total",
            "Requests rejected by a limit, by kind.",
            # LABEL CARDINALITY: kind is a fixed four-value enum
            # (calls|cost|concurrency|scope) -- never a client id or an IP.
            ["kind"],
            registry=_registry,
        )
        _enabled = True
    except Exception:
        logger.warning("metrics setup failed; metrics disabled", exc_info=True)
        _enabled = False


def preregister_tools(tool_names: Iterable[str]) -> None:
    """Create every tool x outcome child at 0 so first events are visible to rate().

    Cardinality is fixed and small (31 tools x 6 outcomes). No-op when disabled.
    """
    if not _enabled:
        return
    with contextlib.suppress(Exception):
        for tool in tool_names:
            for outcome in TOOL_OUTCOMES:
                if _tool_calls_total is not None:
                    _tool_calls_total.labels(tool=tool, outcome=outcome)
                if _tool_call_duration is not None:
                    _tool_call_duration.labels(tool=tool, outcome=outcome)
        for outcome in QUERY_OUTCOMES:
            if _query_duration is not None:
                _query_duration.labels(outcome=outcome)
        for kind in LIMIT_KINDS:
            if _limit_rejections_total is not None:
                _limit_rejections_total.labels(kind=kind)


# --- Helper API (stable contract; all no-op-safe, none may raise) --------------


def record_tool_call(
    tool: str, outcome: str, duration_s: float, trace_id: str | None = None
) -> None:
    """Record one MCP tool invocation: outcome counter + duration histogram.

    ``trace_id`` becomes an OpenMetrics exemplar on the duration histogram --
    never a label (a trace id is unbounded cardinality; an exemplar is not
    indexed and is dropped entirely by plain-text exposition).
    """
    if not _enabled or _tool_calls_total is None or _tool_call_duration is None:
        return
    with contextlib.suppress(Exception):
        _tool_calls_total.labels(tool=tool, outcome=outcome).inc()
        hist = _tool_call_duration.labels(tool=tool, outcome=outcome)
        if trace_id:
            hist.observe(duration_s, exemplar={"trace_id": trace_id})
        else:
            hist.observe(duration_s)


def record_query_duration(outcome: str, duration_s: float) -> None:
    """Record one database round-trip's duration, labeled by outcome only."""
    if not _enabled or _query_duration is None:
        return
    with contextlib.suppress(Exception):
        _query_duration.labels(outcome=outcome).observe(duration_s)


def record_connection_error() -> None:
    """Count one database connection failure."""
    if not _enabled or _connection_errors_total is None:
        return
    with contextlib.suppress(Exception):
        _connection_errors_total.inc()


def record_schema_cache_access(*, hit: bool) -> None:
    """Count one schema-introspection cache access (hit or miss)."""
    if not _enabled:
        return
    with contextlib.suppress(Exception):
        counter = _schema_cache_hits if hit else _schema_cache_misses
        if counter is not None:
            counter.inc()


def record_auth_failure() -> None:
    """Count one rejected authentication attempt."""
    if not _enabled or _auth_failures_total is None:
        return
    with contextlib.suppress(Exception):
        _auth_failures_total.inc()


def record_limit_rejection(kind: str) -> None:
    """Count one rejection by a limit. `kind` is one of the fixed set
    calls|cost|concurrency|scope -- never an unbounded value."""
    if not _enabled or _limit_rejections_total is None:
        return
    with contextlib.suppress(Exception):
        _limit_rejections_total.labels(kind=kind).inc()


def render(accept: str = "") -> tuple[bytes, str]:
    """Return ``(exposition bytes, content-type)`` for ``/metrics``.

    OpenMetrics exposition (which is what carries exemplars) when the scraper's
    Accept header asks for it -- Prometheus 3 does by default; plain text
    otherwise. No-op safe: returns empty bytes + ``text/plain`` when metrics are
    disabled/absent.
    """
    if not _enabled or _registry is None:
        return b"", _DEFAULT_CONTENT_TYPE
    try:
        if "application/openmetrics-text" in accept:
            # prometheus_client ships py.typed but leaves the openmetrics
            # exposition module unannotated.
            return om_generate_latest(_registry), OM_CONTENT_TYPE  # type: ignore[no-untyped-call]
        return generate_latest(_registry), CONTENT_TYPE_LATEST
    except Exception:
        logger.warning("metrics render failed", exc_info=True)
        # An empty 200 body is the exact failure mode server.py works to avoid
        # (Prometheus scrapes "up" and sees nothing). If only the OpenMetrics
        # exposition blew up, plain text still carries every sample.
        try:
            return generate_latest(_registry), CONTENT_TYPE_LATEST
        except Exception:
            logger.warning("metrics plain-text render failed too", exc_info=True)
        return b"", _DEFAULT_CONTENT_TYPE
