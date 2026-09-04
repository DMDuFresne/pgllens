import logging

import httpx
import pytest

from pgllens.config import Settings
from pgllens.obs import metrics
from pgllens.server import build_app

DSN = "postgresql://u:p@localhost:5432/flux"


def settings(**kw):
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


def test_metrics_are_a_noop_when_disabled():
    metrics.configure_metrics(settings(metrics_enabled=False))
    assert metrics.enabled() is False
    metrics.record_tool_call("query", "ok", 0.1)   # must not raise


def test_metrics_endpoint_absent_when_disabled():
    async def go():
        app = build_app(settings(metrics_enabled=False))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics")
    import asyncio
    assert asyncio.run(go()).status_code == 404


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_recorded_tool_calls_appear_in_exposition():
    metrics.configure_metrics(settings(metrics_enabled=True))
    metrics.record_tool_call("query", "ok", 0.25)
    body, content_type = metrics.render()
    text = body.decode()
    assert "pgllens_tool_calls_total" in text
    assert 'tool="query"' in text and 'outcome="ok"' in text
    assert "text/plain" in content_type


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_metric_labels_never_include_database_or_sql():
    metrics.configure_metrics(settings(metrics_enabled=True))
    metrics.record_tool_call("query", "ok", 0.1)
    text = metrics.render()[0].decode()
    assert "flux" not in text and "SELECT" not in text


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_metrics_endpoint_serves_exposition_when_enabled():
    async def go():
        app = build_app(settings(metrics_enabled=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics")
    import asyncio
    resp = asyncio.run(go())
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
@pytest.mark.parametrize("trust_proxy_headers", [True, False])
def test_metrics_endpoint_404_for_any_proxied_request(trust_proxy_headers):
    async def go():
        app = build_app(settings(metrics_enabled=True, trust_proxy_headers=trust_proxy_headers))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics", headers={"X-Forwarded-For": "1.2.3.4"})
    import asyncio
    assert asyncio.run(go()).status_code == 404


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_metrics_endpoint_200_for_docker_network_scrape_without_xff():
    async def go():
        app = build_app(settings(metrics_enabled=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics")
    import asyncio
    resp = asyncio.run(go())
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]


def test_metrics_endpoint_404_when_disabled_regardless_of_xff():
    async def go():
        app = build_app(settings(metrics_enabled=False))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics", headers={"X-Forwarded-For": "1.2.3.4"})
    import asyncio
    assert asyncio.run(go()).status_code == 404


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_gated_metrics_404_is_byte_identical_to_disabled_metrics_404():
    # Pin: a scanner must not be able to tell "metrics disabled" apart from
    # "metrics enabled but this request looks proxied" by response shape.
    async def get(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics", headers={"X-Forwarded-For": "1.2.3.4"})
    import asyncio
    disabled_resp = asyncio.run(get(build_app(settings(metrics_enabled=False))))
    gated_resp = asyncio.run(get(build_app(settings(metrics_enabled=True))))
    assert disabled_resp.status_code == gated_resp.status_code == 404
    assert disabled_resp.content == gated_resp.content
    assert disabled_resp.headers["content-type"] == gated_resp.headers["content-type"]
    assert disabled_resp.headers["content-length"] == gated_resp.headers["content-length"]


def test_record_tool_call_never_raises_with_bad_inputs():
    metrics.configure_metrics(settings(metrics_enabled=False))
    metrics.record_query_duration("ok", 0.1)
    metrics.record_connection_error()
    metrics.record_schema_cache_access(hit=True)
    metrics.record_schema_cache_access(hit=False)


def test_warns_when_enabled_but_deps_missing(monkeypatch, caplog):
    # Simulate the image built without the `observability` extra: METRICS_ENABLED=true
    # but prometheus-client absent must not silently no-op -- it must announce the
    # gap and the fix, so an operator following docs doesn't get a monitoring stack
    # that measures nothing with no explanation.
    monkeypatch.setattr(metrics, "_DEPS_AVAILABLE", False)
    with caplog.at_level("WARNING", logger="pgllens"):
        metrics.configure_metrics(settings(metrics_enabled=True))
    assert metrics.enabled() is False
    assert any(
        "observability" in r.message and "METRICS_ENABLED" in r.message for r in caplog.records
    )


def test_metrics_endpoint_not_mounted_when_enabled_but_deps_missing(monkeypatch):
    # The route must not be mounted at all when deps are missing (matching the
    # metrics-disabled 404), rather than serving a 200 with an empty body that
    # Prometheus would read as a healthy scrape of nothing.
    monkeypatch.setattr(metrics, "_DEPS_AVAILABLE", False)

    async def go():
        app = build_app(settings(metrics_enabled=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return await c.get("/metrics")
    import asyncio
    assert asyncio.run(go()).status_code == 404


def test_auth_failures_are_counted():
    metrics.configure_metrics(settings(metrics_enabled=True))
    if not metrics.enabled():
        pytest.skip("observability extra not installed")
    metrics.record_auth_failure()
    metrics.record_auth_failure()
    body, _ct = metrics.render()
    assert b"pgllens_auth_failures_total 2.0" in body


def test_limit_rejections_are_counted_by_kind():
    metrics.configure_metrics(settings(metrics_enabled=True))
    if not metrics.enabled():
        pytest.skip("observability extra not installed")
    metrics.record_limit_rejection("calls")
    metrics.record_limit_rejection("scope")
    body, _ct = metrics.render()
    assert b'pgllens_limit_rejections_total{kind="calls"} 1.0' in body
    assert b'pgllens_limit_rejections_total{kind="scope"} 1.0' in body


def test_the_counters_are_safe_no_ops_when_metrics_are_disabled():
    metrics.configure_metrics(settings(metrics_enabled=False))
    metrics.record_auth_failure()
    metrics.record_limit_rejection("cost")  # must not raise


def test_metrics_default_on():
    # Play tier: `docker run ghcr.io/dmdufresne/pgllens` must serve /metrics with no
    # extra configuration. Same exposure class as /health.
    assert settings().metrics_enabled is True


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_preregistered_children_appear_at_zero_before_any_call():
    metrics.configure_metrics(settings(metrics_enabled=True))
    metrics.preregister_tools(["query", "list_tables"])
    text = metrics.render()[0].decode()
    for outcome in metrics.TOOL_OUTCOMES:
        assert f'pgllens_tool_calls_total{{outcome="{outcome}",tool="query"}} 0.0' in text
        assert f'pgllens_tool_call_duration_seconds_count{{outcome="{outcome}",tool="query"}} 0.0' \
            in text
    for outcome in metrics.QUERY_OUTCOMES:
        assert f'pgllens_query_duration_seconds_count{{outcome="{outcome}"}} 0.0' in text
    for kind in metrics.LIMIT_KINDS:
        assert f'pgllens_limit_rejections_total{{kind="{kind}"}} 0.0' in text


def test_preregister_is_a_noop_when_disabled():
    metrics.configure_metrics(settings(metrics_enabled=False))
    metrics.preregister_tools(["query"])  # must not raise


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_server_preregisters_every_registered_tool():
    import asyncio

    from pgllens.tools._util import registered_tool_names

    async def go():
        app = build_app(settings(metrics_enabled=True))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            return (await c.get("/metrics")).text

    text = asyncio.run(go())
    names = registered_tool_names()
    assert len(names) == 31
    for name in names:
        assert f'tool="{name}"' in text


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_openmetrics_exposition_carries_exemplars():
    metrics.configure_metrics(settings(metrics_enabled=True))
    metrics.record_tool_call("query", "ok", 0.2, trace_id="0af7651916cd43dd8448eb211c80319c")
    body, ctype = metrics.render(accept="application/openmetrics-text; version=1.0.0")
    assert "openmetrics" in ctype
    assert 'trace_id="0af7651916cd43dd8448eb211c80319c"' in body.decode()


@pytest.mark.skipif(not metrics.deps_available(), reason="observability extra not installed")
def test_plain_exposition_still_served_without_openmetrics_accept():
    metrics.configure_metrics(settings(metrics_enabled=True))
    metrics.record_tool_call("query", "ok", 0.2, trace_id="0af7651916cd43dd8448eb211c80319c")
    body, ctype = metrics.render(accept="text/plain")
    assert "text/plain" in ctype
    assert "trace_id" not in body.decode()


@pytest.mark.skipif(metrics.deps_available(), reason="needs the extra absent")
def test_missing_extra_warns_only_when_metrics_enabled_was_explicit(caplog):
    with caplog.at_level(logging.INFO, logger="pgllens"):
        metrics.configure_metrics(settings())          # default-on, not asked for
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="pgllens"):
        metrics.configure_metrics(settings(metrics_enabled=True))   # explicit opt-in
    assert len([r for r in caplog.records if r.levelno >= logging.WARNING]) == 1
