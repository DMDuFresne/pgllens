"""OpenTelemetry tracing bootstrap with graceful degradation.

Metrics are handled entirely by ``obs.metrics`` (via ``prometheus_client`` directly, no OTel meter), so this
module owns only distributed tracing -- HTTP request spans (via the ASGI
middleware) and ad-hoc spans a caller wraps work in.

OPTIONAL DEPENDENCY DISCIPLINE: ALL OpenTelemetry imports are guarded behind
``_OTEL_AVAILABLE`` at module load -- importing this module must NEVER fail
even with zero OTel libraries installed. When tracing is disabled or the libs
are absent, every entry point here is a safe no-op.
"""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pgllens.config import Settings

logger = logging.getLogger("pgllens")

try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider

    _OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the extra installed
    _OTEL_AVAILABLE = False

_enabled: bool = False
_tracer: Any | None = None


class OnceEveryFilter(logging.Filter):
    """Let one record through, then suppress for `interval_s`. Installed on the OTLP
    exporter's logger so an absent Tempo (Solo tier) is reported once every ten
    minutes instead of on every batch."""

    def __init__(self, interval_s: float = 600.0) -> None:
        super().__init__()
        self.interval_s = interval_s
        self._last = float("-inf")

    def filter(self, record: logging.LogRecord) -> bool:
        # Deliberately simple: `_last` is a non-atomic read/modify/write across
        # exporter threads. Worst case two threads race and one duplicate line
        # escapes; cheaper than a lock on a log filter.
        now = time.monotonic()
        if now - self._last >= self.interval_s:
            self._last = now
            # The filter sits on the whole exporter logger, not just failures.
            record.msg = (f"{record.msg} (further exporter messages suppressed "
                          f"for {int(self.interval_s // 60)}m)")
            return True
        return False


_EXPORTER_LOGGERS = (
    "opentelemetry.exporter.otlp.proto.grpc.exporter",
    "opentelemetry.sdk.trace.export",
)


def deps_available() -> bool:
    """True when the ``observability`` extra (opentelemetry-*) is installed."""
    return _OTEL_AVAILABLE


def tracing_enabled() -> bool:
    """True only when ``configure_tracing`` was called with tracing enabled AND
    the optional dependency imported successfully."""
    return _enabled


def configure_tracing(settings: Settings) -> None:
    """Idempotently bootstrap a ``TracerProvider`` from ``settings``.

    No-ops (leaves ``tracing_enabled()`` False) when ``settings.otel_enabled`` is
    false or the OTel libraries are unavailable. When ``otel_exporter_otlp_endpoint``
    is set, spans are batch-exported there over OTLP/gRPC; otherwise a provider is
    still installed (so ``span()`` works) but nothing leaves the process. Never raises.
    """
    global _enabled, _tracer

    if not settings.otel_enabled or not _OTEL_AVAILABLE:
        if settings.otel_enabled and not _OTEL_AVAILABLE:
            # Same failure shape as metrics: tracing enabled but the extra absent
            # must not silently no-op -- announce the gap and the fix at startup.
            logger.warning(
                "OTEL_ENABLED is true but the opentelemetry packages are not installed; "
                "tracing will not be enabled. Install with: "
                "pip install 'pgllens[observability]'"
            )
        _enabled = False
        return

    try:
        resource = Resource.create({SERVICE_NAME: "pgllens"})
        provider = TracerProvider(resource=resource)

        endpoint = settings.otel_exporter_otlp_endpoint
        if endpoint:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))

            for name in _EXPORTER_LOGGERS:
                lg = logging.getLogger(name)
                if not any(isinstance(f, OnceEveryFilter) for f in lg.filters):
                    lg.addFilter(OnceEveryFilter())

        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer("pgllens")
        _enabled = True
    except Exception:
        logger.warning("tracing setup failed; tracing disabled", exc_info=True)
        _enabled = False


def instrument_asgi(app: Any) -> Any:
    """Wrap ``app`` in the OTel ASGI middleware (HTTP request spans) when tracing
    is enabled; otherwise return it unchanged."""
    if not _enabled or not _OTEL_AVAILABLE:
        return app
    try:
        from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware

        return OpenTelemetryMiddleware(app)
    except Exception:
        logger.warning("ASGI instrumentation failed; serving app un-instrumented", exc_info=True)
        return app


@contextlib.contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    """Context manager yielding the started span, or ``None`` when tracing is
    disabled/absent. An exception raised in the ``with`` body propagates unchanged --
    this manager never swallows a caller's exception."""
    if not _enabled or _tracer is None:
        yield None
        return
    try:
        cm = _tracer.start_as_current_span(name)
        span_obj = cm.__enter__()
    except Exception:
        logger.warning("span start failed; serving call un-traced", exc_info=True)
        yield None
        return
    try:
        try:
            for key, value in attributes.items():
                if value is not None:
                    span_obj.set_attribute(key, value)
        except Exception:  # noqa: BLE001, S110 -- an attribute-setting failure must
            # never break the traced call; a missing attribute on the span is harmless.
            pass
        yield span_obj
    except BaseException as exc:
        if not cm.__exit__(type(exc), exc, exc.__traceback__):
            raise
    else:
        try:
            cm.__exit__(None, None, None)
        except Exception:
            logger.warning("span cleanup failed on clean exit; ignoring", exc_info=True)


def current_trace_id() -> str | None:
    """32-hex trace id of the current sampled span, or None. Never raises."""
    if not _enabled:
        return None
    try:
        ctx = trace.get_current_span().get_span_context()
        if not ctx.is_valid or not ctx.trace_flags.sampled:
            return None
        return format(ctx.trace_id, "032x")
    except Exception:  # noqa: BLE001 -- a correlation id is never worth failing a
        # tool call or an audit line over; absent is the correct degraded answer.
        return None
