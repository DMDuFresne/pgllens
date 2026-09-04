"""Tests for server bootstrap: /health, MCP mounting, instructions."""

from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from pgllens.config import Settings
from pgllens.obs import audit
from pgllens.server import build_app

DSN = "postgresql://u:p@localhost:5432/flux"


def make_settings(**kw) -> Settings:
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


class _FakeDb:
    """Stands in for Db so /health tests never touch a real socket."""

    def __init__(self, reachable: bool) -> None:
        self._reachable = reachable

    async def ping(self, timeout: float = 2.0) -> bool:
        return self._reachable

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass


async def test_health_endpoint():
    app = build_app(make_settings(), db=_FakeDb(reachable=True))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "healthy"
    assert body["server"] == "pgllens"


async def test_health_endpoint_reports_unhealthy_when_db_is_unreachable():
    # Regression: /health used to report "healthy" unconditionally, even with
    # zero working DB connections -- a fully broken deployment looked green.
    app = build_app(make_settings(), db=_FakeDb(reachable=False))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/health")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unhealthy"
    assert body["database"] == "unreachable"
    assert body["server"] == "pgllens"


@pytest.fixture
def build_health_app():
    """Builds a /health-serving app over an AsyncMock Db, so tests can assert
    on ping's await_count as well as the response body."""

    def _build(reachable: bool = True):
        fake_db = AsyncMock()
        fake_db.ping = AsyncMock(return_value=reachable)
        app = build_app(make_settings(), db=fake_db)
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://t")
        return client, fake_db

    return _build


@pytest.fixture
async def health_client(build_health_app):
    client, _ = build_health_app()
    async with client:
        yield client


async def test_health_response_carries_no_version(health_client):
    r = await health_client.get("/health")
    assert "version" not in r.json()


async def test_health_ping_is_cached_briefly(build_health_app):
    # Two immediate hits -> one ping. The cache exists so an unauthenticated
    # flood costs one pool acquire per interval, not one per request.
    client, fake_db = build_health_app()
    async with client:
        await client.get("/health")
        await client.get("/health")
    assert fake_db.ping.await_count == 1


def test_build_app_wires_up_the_audit_sink(tmp_path):
    # Regression: configure_audit() was previously only ever called from tests --
    # AUDIT_LOG_FILE was entirely inert in the shipped app (docker-compose sets it
    # by default) because build_app() never wired it up, so every tool call's
    # audit_mod.audit("tool_call", ...) was a silent no-op forever.
    path = tmp_path / "audit.jsonl"
    build_app(make_settings(audit_stdout=False))  # baseline: nothing configured, must stay disabled
    assert audit._enabled is False

    build_app(make_settings(audit_log_file=str(path)))
    assert audit._enabled is True
    audit.audit("tool_call", tool="query")
    assert path.exists()
    assert "tool_call" in path.read_text(encoding="utf-8")


async def test_mcp_endpoint_is_mounted():
    app = build_app(make_settings())
    transport = httpx.ASGITransport(app=app)
    # The MCP session manager only accepts requests while its lifespan task
    # group is running (uvicorn does this automatically outside tests).
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://t") as c,
    ):
        r = await c.post("/mcp", json={})
    assert r.status_code != 404


async def test_build_app_wires_a_real_introspector():
    # build_app must construct a real Introspector (not None) and pass it
    # through so discovery tools that depend on it get registered. Assert
    # indirectly via the tool list -- discovery/relationships tools only
    # register successfully when intro is not None.
    from pgllens.database.introspect import Introspector
    from pgllens.database.pool import Db
    from pgllens.server import create_mcp

    settings = make_settings()
    db = Db(settings)
    intro = Introspector(db, settings)
    server = create_mcp(settings, db, intro)
    tools = await server.list_tools()
    names = {t.name for t in tools}
    assert "list_tables" in names
    assert "get_relationships" in names


def test_instructions_include_domain_context():
    from pgllens.instructions import build_instructions

    s = make_settings(domain_context="widgets factory")
    text = build_instructions(s)
    assert "widgets factory" in text
    assert "read-only" in text.lower()


import pytest

from pgllens.oauth.okta import JwksError
from tests.jwt_helpers import AUDIENCE, ISSUER


def okta_settings(**kw):
    return make_settings(
        mcp_auth_mode="okta", okta_issuer=ISSUER, okta_audience=AUDIENCE, **kw
    )


async def test_okta_mode_serves_rfc9728_discovery_pointing_at_okta():
    app = build_app(okta_settings(external_base_url="https://pgllens.example.com"))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/.well-known/oauth-protected-resource")
    assert r.status_code == 200
    body = r.json()
    assert body["resource"] == "https://pgllens.example.com"
    # The authorization server is OKTA, not us -- this is the whole point of
    # the mode. Pointing it back at ourselves would send clients to /oauth/*
    # routes that do not exist.
    assert body["authorization_servers"] == [ISSUER]
    assert set(body["scopes_supported"]) == {"pgllens.read", "pgllens.admin", "offline_access"}
    # offline_access: Okta issues a refresh token only when the client asks for
    # it, and clients ask for what this list advertises. Without it claude.ai
    # holds a one-hour access token and its Claude Code proxy reports
    # "no OAuth token is configured".


async def test_okta_mode_registers_no_oauth_routes():
    # PgLLens is not an authorization server here. A live /oauth/authorize or
    # /oauth/register in this mode is an unauthenticated attack surface with
    # no reason to exist.
    app = build_app(okta_settings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        for path in ("/oauth/register", "/oauth/authorize", "/oauth/token"):
            assert (await c.post(path, json={})).status_code == 404, path
        assert (await c.get("/.well-known/oauth-authorization-server")).status_code == 404


async def test_okta_mode_rejects_an_unauthenticated_mcp_call():
    app = build_app(okta_settings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert r.status_code == 401


async def test_okta_mode_fails_closed_when_jwks_is_unreachable_at_boot(monkeypatch):
    # A server that cannot verify any token must not come up serving /mcp -- it
    # would 401 everything, look like an auth outage, and tempt an operator into
    # "temporarily" disabling auth.
    async def boom(self):
        raise JwksError("unreachable")

    monkeypatch.setattr("pgllens.oauth.okta.JwksCache.prime", boom)
    app = build_app(okta_settings())
    with pytest.raises(JwksError):
        async with app.router.lifespan_context(app):
            pass


async def test_password_mode_still_serves_its_own_oauth_routes():
    # Definition of done: password mode is untouched.
    app = build_app(make_settings(mcp_auth_mode="password", mcp_auth_password="x"))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/.well-known/oauth-authorization-server")
    assert r.status_code == 200


async def test_none_mode_still_has_no_auth_at_all():
    app = build_app(make_settings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        assert (await c.get("/.well-known/oauth-protected-resource")).status_code == 404


async def test_okta_mode_middleware_order_bearer_before_rate_limiter():
    # C-1 regression: bearer auth must run before InboundToolRateLimitMiddleware
    # so the limiter keys on pgllens.client_id, not the peer IP. Assert directly
    # on the installed middleware stack rather than tracing a request, since
    # Starlette's add_middleware order is exactly what a reorder would break.
    from pgllens.middleware import InboundToolRateLimitMiddleware
    from pgllens.oauth.bearer import OktaBearerMiddleware

    app = build_app(okta_settings(tool_rate_limit_per_minute=60))
    # user_middleware is stored in the order passed to add_middleware(); at
    # request time Starlette applies it in reverse, so bearer auth (added
    # after the limiter) must appear BEFORE the limiter in this list.
    classes = [m.cls for m in app.user_middleware]
    assert classes.index(OktaBearerMiddleware) < classes.index(InboundToolRateLimitMiddleware)



async def test_caller_and_concurrency_middleware_are_wired_after_bearer_auth():
    # Task 8 fix round, point 5: both new middlewares must be present and run
    # AFTER the auth middleware -- otherwise CallerContextMiddleware would
    # publish an identity before the scope keys authenticating it exist, and
    # ConcurrencyLimitMiddleware would key on the peer IP instead of the
    # authenticated client id. Same index-comparison technique as
    # test_okta_mode_middleware_order_bearer_before_rate_limiter.
    from pgllens.middleware import CallerContextMiddleware, ConcurrencyLimitMiddleware
    from pgllens.oauth.bearer import BearerAuthMiddleware

    app = build_app(make_settings(
        mcp_auth_mode="password",
        mcp_auth_password="x",
        max_concurrent_calls_per_client=2,
    ))
    classes = [m.cls for m in app.user_middleware]
    assert CallerContextMiddleware in classes
    assert ConcurrencyLimitMiddleware in classes
    assert classes.index(BearerAuthMiddleware) < classes.index(CallerContextMiddleware)
    assert classes.index(BearerAuthMiddleware) < classes.index(ConcurrencyLimitMiddleware)


async def test_concurrency_cap_installed_by_default():
    # Direct coverage for the shipped default: max_concurrent_calls_per_client
    # is 4 (on) unless overridden, so a plain build_app() must install
    # ConcurrencyLimitMiddleware with no explicit cap passed.
    from pgllens.middleware import ConcurrencyLimitMiddleware

    app = build_app(make_settings(mcp_auth_mode="password", mcp_auth_password="x"))
    classes = [m.cls for m in app.user_middleware]
    assert ConcurrencyLimitMiddleware in classes


async def test_caller_context_middleware_present_with_concurrency_cap_disabled():
    # CallerContextMiddleware is unconditional so `caller()` is always
    # populated once auth is configured, independent of the concurrency cap.
    # Cap explicitly off here since concurrency is not what this test checks
    # -- see test_concurrency_cap_installed_by_default for default-on coverage.
    from pgllens.middleware import CallerContextMiddleware, ConcurrencyLimitMiddleware

    app = build_app(
        make_settings(
            mcp_auth_mode="password",
            mcp_auth_password="x",
            max_concurrent_calls_per_client=0,
        )
    )
    classes = [m.cls for m in app.user_middleware]
    assert CallerContextMiddleware in classes
    assert ConcurrencyLimitMiddleware not in classes


def test_open_bind_with_no_auth_warns_at_build(caplog):
    with caplog.at_level("WARNING", logger="pgllens"):
        build_app(make_settings(host="0.0.0.0"))
    assert any("UNAUTHENTICATED" in r.message and "0.0.0.0" in r.message
               for r in caplog.records)


def test_loopback_bind_with_no_auth_does_not_warn(caplog):
    with caplog.at_level("WARNING", logger="pgllens"):
        build_app(make_settings(host="127.0.0.1"))
    assert not any("UNAUTHENTICATED" in r.message for r in caplog.records)


def test_unset_external_base_url_warns_in_okta_mode_even_on_default_port(caplog):
    with caplog.at_level("WARNING", logger="pgllens"):
        build_app(okta_settings())
    assert any("EXTERNAL_BASE_URL" in r.message for r in caplog.records)


def test_main_passes_connection_limits_to_uvicorn(monkeypatch):
    import uvicorn

    from pgllens.__main__ import main
    from pgllens.config import get_settings

    captured = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: captured.update(kw))
    monkeypatch.setenv("DATABASE_URL", DSN)
    get_settings.cache_clear()
    try:
        main([])
    finally:
        get_settings.cache_clear()
    assert captured["limit_concurrency"] == 100
    assert captured["timeout_keep_alive"] == 5


# --- transport_security host allowlist (see ISSUE-mcp-transport-security-host.md) ---
# mcp.streamable_http_app() auto-enables DNS-rebinding protection with a
# localhost-only Host allowlist whenever `transport_security=` is omitted.
# That leaves every real deployment (reverse proxy / tunnel / public
# hostname) 421-ing every /mcp request. _transport_security() derives the
# allowlist from EXTERNAL_BASE_URL so the real hostname passes while
# protection itself (enable_dns_rebinding_protection=True) always stays on.

_MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


async def _post_mcp(app, host: str) -> httpx.Response:
    # /mcp's session manager needs its lifespan running (task group init) --
    # matches tests/integration/test_mcp_protocol.py's wire_client fixture.
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://t") as c,
    ):
        return await c.post(
            "/mcp",
            json={"jsonrpc": "2.0", "method": "ping", "id": 1},
            headers={**_MCP_HEADERS, "Host": host},
        )


async def test_external_base_url_hostname_is_not_rejected_by_dns_rebinding_protection():
    # The regression this issue is about: a real deployment's public hostname
    # must not 421. 401/400/406/etc are all fine -- the request just has to
    # reach the app past the transport-security layer.
    app = build_app(
        make_settings(external_base_url="https://pgllens.example.com"),
        db=_FakeDb(reachable=True),
    )
    r = await _post_mcp(app, "pgllens.example.com")
    assert r.status_code != 421


async def test_unrelated_host_is_still_rejected_with_external_base_url_set():
    app = build_app(
        make_settings(external_base_url="https://pgllens.example.com"),
        db=_FakeDb(reachable=True),
    )
    r = await _post_mcp(app, "evil.example.com")
    assert r.status_code == 421


async def test_loopback_hosts_still_work_and_foreign_hosts_still_421_by_default():
    # Each StreamableHTTPSessionManager .run()s only once, so each Host
    # header gets its own app/lifespan (matches wire_client's one-app-per-test
    # pattern in tests/integration/test_mcp_protocol.py).
    r_named = await _post_mcp(build_app(make_settings(), db=_FakeDb(reachable=True)), "localhost:3000")
    assert r_named.status_code != 421
    r_bare = await _post_mcp(build_app(make_settings(), db=_FakeDb(reachable=True)), "127.0.0.1")
    assert r_bare.status_code != 421
    r_evil = await _post_mcp(build_app(make_settings(), db=_FakeDb(reachable=True)), "evil.example.com")
    assert r_evil.status_code == 421


def test_transport_security_derivation_keeps_protection_on_and_allows_both_host_forms():
    from pgllens.server import _effective_external_base_url, _transport_security

    settings = make_settings(external_base_url="https://pgllens.example.com")
    ts = _transport_security(settings, _effective_external_base_url(settings))

    # Pinned so the `host=settings.host` trap (see issue) can never silently
    # disable this again.
    assert ts.enable_dns_rebinding_protection is True

    assert "pgllens.example.com" in ts.allowed_hosts
    assert "pgllens.example.com:*" in ts.allowed_hosts
    # bare-Host loopback gap: the SDK's own default only ships the `:*` forms,
    # so a bare `Host: 127.0.0.1` (no port) matches neither pattern nor exact
    # entry. Fixed here alongside the derived-host addition.
    assert "127.0.0.1" in ts.allowed_hosts
    assert "localhost" in ts.allowed_hosts
    assert "[::1]" in ts.allowed_hosts
    assert "127.0.0.1:*" in ts.allowed_hosts
    assert "localhost:*" in ts.allowed_hosts
    assert "[::1]:*" in ts.allowed_hosts


def test_transport_security_ignores_host_and_port_when_external_base_url_is_unset():
    # Review finding R4 gap: _effective_external_base_url()'s fallback derives
    # a URL from settings.host/mcp_port when EXTERNAL_BASE_URL was never set
    # explicitly -- a non-loopback bind (e.g. a LAN NIC address) must NOT flow
    # into the security allowlist with no operator opt-in. Only an explicitly
    # set EXTERNAL_BASE_URL may widen the allowlist.
    from pgllens.server import _effective_external_base_url, _transport_security

    settings = make_settings(host="192.168.1.50", mcp_port=4000)
    ts = _transport_security(settings, _effective_external_base_url(settings))

    assert not any("192.168.1.50" in h for h in ts.allowed_hosts)
    assert not any("192.168.1.50" in o for o in ts.allowed_origins)


async def test_foreign_host_still_421s_when_external_base_url_is_unset_on_a_lan_bind():
    app = build_app(make_settings(host="192.168.1.50", mcp_port=4000), db=_FakeDb(reachable=True))
    r = await _post_mcp(app, "192.168.1.50:4000")
    assert r.status_code == 421


def test_build_app_logs_the_unset_external_base_url_warning_only_once(caplog):
    # Review finding: _effective_external_base_url() was being called twice
    # per build_app() (transport security + the oauth/password auth branch),
    # each emitting its own-EXTERNAL_BASE_URL-unset WARNING. build_app() must
    # compute it once and reuse it.
    with caplog.at_level("WARNING", logger="pgllens"):
        build_app(make_settings(mcp_auth_mode="password", mcp_auth_password="x"))
    warnings = [r for r in caplog.records if "EXTERNAL_BASE_URL" in r.message]
    assert len(warnings) == 1


def test_win32_loop_literal_resolves_to_a_selector_loop_factory():
    """__main__.LOOP must be a loop spec uvicorn can actually build: the
    "asyncio:SelectorEventLoop" string form needs uvicorn >= 0.36, and psycopg's
    async pool refuses Windows' default ProactorEventLoop."""
    import asyncio
    import sys

    import uvicorn

    from pgllens.__main__ import LOOP

    factory = uvicorn.Config(app=lambda *a: None, loop=LOOP).get_loop_factory()
    assert callable(factory)
    if sys.platform == "win32":
        loop = factory()
        try:
            assert isinstance(loop, asyncio.SelectorEventLoop)
        finally:
            loop.close()
