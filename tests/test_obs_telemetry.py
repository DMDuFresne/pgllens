from __future__ import annotations

import pytest

from pgllens.config import Settings
from pgllens.obs import telemetry

DSN = "postgresql://u:p@localhost:5432/flux"


def settings(**kw):
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


def test_tracing_is_a_noop_when_disabled():
    telemetry.configure_tracing(settings(otel_enabled=False))
    assert telemetry.tracing_enabled() is False


def test_warns_when_enabled_but_deps_missing(monkeypatch, caplog):
    # Same failure shape as metrics: OTEL_ENABLED=true with the opentelemetry
    # packages absent must not silently no-op -- announce the gap and the fix.
    monkeypatch.setattr(telemetry, "_OTEL_AVAILABLE", False)
    with caplog.at_level("WARNING", logger="pgllens"):
        telemetry.configure_tracing(settings(otel_enabled=True))
    assert telemetry.tracing_enabled() is False
    assert any(
        "observability" in r.message and "OTEL_ENABLED" in r.message for r in caplog.records
    )


def test_current_trace_id_is_none_outside_a_span():
    telemetry.configure_tracing(settings(otel_enabled=False))
    assert telemetry.current_trace_id() is None


@pytest.mark.skipif(not telemetry.deps_available(), reason="observability extra not installed")
def test_current_trace_id_inside_a_span_is_32_hex():
    telemetry.configure_tracing(settings(otel_enabled=True))
    with telemetry.span("t"):
        tid = telemetry.current_trace_id()
    assert tid is not None
    assert len(tid) == 32
    assert int(tid, 16) >= 0


def test_once_every_filter_lets_one_record_through_per_interval():
    import logging
    import time

    from pgllens.obs.telemetry import OnceEveryFilter

    f = OnceEveryFilter(interval_s=60)
    rec = logging.LogRecord("x", logging.ERROR, "", 0, "Failed to export", None, None)
    assert f.filter(rec) is True
    assert f.filter(rec) is False
    f._last = time.monotonic() - 61
    assert f.filter(rec) is True


@pytest.mark.skipif(not telemetry.deps_available(), reason="observability extra not installed")
def test_configure_tracing_installs_the_filter():
    import logging

    try:
        telemetry.configure_tracing(
            settings(otel_enabled=True, otel_exporter_otlp_endpoint="http://127.0.0.1:1")
        )
        names = [
            type(f).__name__
            for f in logging.getLogger(
                "opentelemetry.exporter.otlp.proto.grpc.exporter"
            ).filters
        ]
        assert "OnceEveryFilter" in names
    finally:
        telemetry.configure_tracing(settings(otel_enabled=False))
