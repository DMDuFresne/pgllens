import base64
import hashlib
import re
from contextlib import asynccontextmanager

import httpx
import pytest
from fakeredis.aioredis import FakeRedis

from pgllens.config import Settings
from pgllens.limits import RedisLimitStore
from pgllens.obs import metrics
from pgllens.server import build_app

DSN = "postgresql://u:p@localhost:5432/flux"


def settings(**kw):
    base = {"database_url": DSN, "exposed_schemas": "public",
            "mcp_auth_password": "letmein", "mcp_auth_mode": "password"}
    base.update(kw)
    return Settings(_env_file=None, **base)


def pkce():
    verifier = "v" * 64
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")
    return verifier, challenge


@asynccontextmanager
async def client(app):
    # The MCP session manager only accepts requests while its lifespan task
    # group is running (uvicorn does this automatically outside tests) --
    # see tests/test_server.py::test_mcp_endpoint_is_mounted for precedent.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://testserver") as c:
            yield c


class _FakeDb:
    """/health now checks DB connectivity (see server.py) -- this test is
    about the *auth* gate on /health, not the DB, so stub the pool out."""

    async def ping(self, timeout: float = 2.0) -> bool:
        return True

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass


async def test_health_is_reachable_without_a_token():
    async with client(build_app(settings(), db=_FakeDb())) as c:
        assert (await c.get("/health")).status_code == 200


async def test_mcp_requires_bearer_token():
    async with client(build_app(settings())) as c:
        r = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1})
    assert r.status_code == 401
    assert "Bearer" in r.headers.get("www-authenticate", "")


async def test_discovery_documents_are_served():
    async with client(build_app(settings())) as c:
        pr = await c.get("/.well-known/oauth-protected-resource")
        as_ = await c.get("/.well-known/oauth-authorization-server")
    assert pr.status_code == 200
    body = as_.json()
    assert body["authorization_endpoint"].endswith("/oauth/authorize")
    assert body["token_endpoint"].endswith("/oauth/token")
    assert "S256" in body["code_challenge_methods_supported"]


async def test_full_authorization_code_pkce_flow_yields_a_usable_token():
    verifier, challenge = pkce()
    app = build_app(settings())
    async with client(app) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "Claude", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz"})
        assert form.status_code == 200 and "password" in form.text
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz", "csrf_token": csrf},
            follow_redirects=False)
        assert auth.status_code in (302, 303)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        tok = (await c.post("/oauth/token", data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": "http://localhost/cb", "client_id": reg["client_id"],
            "code_verifier": verifier})).json()
        assert tok["token_type"].lower() == "bearer" and tok["access_token"]
        ok = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1},
                          headers={"Authorization": f"Bearer {tok['access_token']}"})
    assert ok.status_code != 401


async def test_wrong_password_does_not_issue_a_code():
    _, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code",
            "client_id": reg["client_id"], "redirect_uri": "http://localhost/cb",
            "code_challenge": challenge, "code_challenge_method": "S256", "state": "s"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        bad = await c.post("/oauth/authorize", data={
            "password": "wrong", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s", "csrf_token": csrf},
            follow_redirects=False)
    assert bad.status_code not in (302, 303) or "code=" not in bad.headers.get("location", "")


async def test_token_exchange_rejects_wrong_pkce_verifier():
    _, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code",
            "client_id": reg["client_id"], "redirect_uri": "http://localhost/cb",
            "code_challenge": challenge, "code_challenge_method": "S256", "state": "s"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s", "csrf_token": csrf},
            follow_redirects=False)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        bad = await c.post("/oauth/token", data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": "http://localhost/cb", "client_id": reg["client_id"],
            "code_verifier": "not-the-verifier"})
    assert bad.status_code == 400


async def test_code_cannot_be_replayed():
    verifier, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code",
            "client_id": reg["client_id"], "redirect_uri": "http://localhost/cb",
            "code_challenge": challenge, "code_challenge_method": "S256", "state": "s"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s", "csrf_token": csrf},
            follow_redirects=False)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        data = {"grant_type": "authorization_code", "code": code,
                "redirect_uri": "http://localhost/cb",
                "client_id": reg["client_id"], "code_verifier": verifier}
        assert (await c.post("/oauth/token", data=data)).status_code == 200
        assert (await c.post("/oauth/token", data=data)).status_code == 400


async def test_redirect_uri_must_match_registration():
    _verifier, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        evil = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://evil.example/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s"}, follow_redirects=False)
    assert evil.status_code not in (302, 303) or "evil.example" not in evil.headers.get("location", "")


async def test_oauth_disabled_leaves_mcp_open_and_serves_no_oauth_routes():
    app = build_app(Settings(_env_file=None, database_url=DSN, exposed_schemas="public"))
    async with client(app) as c:
        assert (await c.get("/oauth/authorize")).status_code == 404
        r = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1})
    assert r.status_code != 401


async def test_token_endpoint_locks_out_after_repeated_garbage():
    # 5 attempts (mcp_rate_limit_attempts default) with a bad grant_type,
    # then the per-IP throttle answers 429 before any grant logic runs.
    async with client(build_app(settings())) as c:
        for _ in range(5):
            r = await c.post("/oauth/token", data={"grant_type": "nope"})
            assert r.status_code == 400
        r = await c.post("/oauth/token", data={"grant_type": "nope"})
    assert r.status_code == 429


async def test_token_endpoint_oversize_body_413s_without_an_unhandled_exception(caplog):
    # Regression for the body-cap middleware's mid-stream trip path: /oauth/token
    # calls request.form(), which raises ClientDisconnect on the http.disconnect
    # BodySizeLimitMiddleware feeds it after a trip. That must unwind quietly --
    # a security rejection (413) must never surface as a server-error traceback.
    # No Content-Length header (httpx streams a generator body as chunked), so
    # this exercises the running-total trip mid-stream, not the cheap
    # Content-Length pre-check short-circuit.
    async def oversize_chunks():
        for _ in range(9):
            yield b"a=" + b"x" * (128 * 1024) + b"&"  # 9 * ~128 KiB > 1 MiB default cap

    async with client(build_app(settings())) as c:
        r = await c.post("/oauth/token", content=oversize_chunks(),
                         headers={"content-type": "application/x-www-form-urlencoded"})
    assert r.status_code == 413
    assert "ClientDisconnect" not in caplog.text
    assert "Traceback" not in caplog.text


async def test_login_page_escapes_interpolated_values():
    from pgllens.oauth.pages import login_page
    html = login_page(client_id='"><script>alert(1)</script>', redirect_uri="http://x/cb",
                      code_challenge="c", code_challenge_method="S256", state="s",
                      error=None)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html or "&lt;" in html


# --- gate 1: register endpoint is capped and rate-limited ---


def test_client_store_refuses_registration_beyond_cap():
    from pgllens.oauth.store import ClientStore
    store = ClientStore(max_clients=2)
    assert store.register({"client_name": "a", "redirect_uris": ["http://x/cb"]}) is not None
    assert store.register({"client_name": "b", "redirect_uris": ["http://x/cb"]}) is not None
    assert store.register({"client_name": "c", "redirect_uris": ["http://x/cb"]}) is None


async def test_register_endpoint_rate_limits_repeated_rejected_registrations_from_one_ip():
    # Only rejected/cap-hit attempts count toward the register throttle, so
    # drive it with malformed (rejected) bodies rather than successful ones.
    app = build_app(settings(mcp_rate_limit_attempts=3))
    async with client(app) as c:
        for _ in range(3):
            r = await c.post("/oauth/register", json={"client_name": "x"})  # no redirect_uris
            assert r.status_code == 400
        locked = await c.post("/oauth/register", json={"client_name": "x"})
    assert locked.status_code == 429


async def test_successful_registrations_under_the_cap_do_not_lock_out_a_legitimate_registration():
    # Successes must not count toward the register throttle -- otherwise a
    # handful of legitimate DCR registrations (or a couple of restarts, since
    # ClientStore is in-memory) would lock out real users.
    app = build_app(settings(mcp_rate_limit_attempts=3))
    async with client(app) as c:
        for _ in range(5):
            r = await c.post("/oauth/register", json={
                "client_name": "x", "redirect_uris": ["http://localhost/cb"]})
            assert r.status_code == 201
        one_more = await c.post("/oauth/register", json={
            "client_name": "x", "redirect_uris": ["http://localhost/cb"]})
    assert one_more.status_code == 201


# --- gate 2: CSRF token required on POST /oauth/authorize ---


async def test_authorize_post_rejects_missing_or_foreign_csrf_token_even_with_correct_password():
    _, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        # missing csrf_token entirely
        missing = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s"}, follow_redirects=False)
        assert missing.status_code not in (302, 303)

        # foreign csrf_token (never issued by GET /oauth/authorize for this flow)
        foreign = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s",
            "csrf_token": "totally-made-up"}, follow_redirects=False)
        assert foreign.status_code not in (302, 303)


# --- other adversarial cases ---


def test_csrf_store_stops_growing_past_its_cap():
    from pgllens.oauth.routes import _CsrfStore
    store = _CsrfStore(max_tokens=3)
    for i in range(10):
        store.issue(f"client-{i}", "http://x/cb")
    assert len(store._tokens) <= 3


async def test_authorize_get_rate_limits_repeated_requests_from_one_ip():
    _, challenge = pkce()
    app = build_app(settings(mcp_rate_limit_attempts=3))
    async with client(app) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        params = {
            "response_type": "code", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s"}
        for _ in range(3):
            r = await c.get("/oauth/authorize", params=params)
            assert r.status_code == 200
        locked = await c.get("/oauth/authorize", params=params)
    assert locked.status_code == 429


async def test_authorize_get_rejects_wrong_or_missing_response_type():
    _, challenge = pkce()
    async with client(build_app(settings())) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        missing = await c.get("/oauth/authorize", params={
            "client_id": reg["client_id"], "redirect_uri": "http://localhost/cb",
            "code_challenge": challenge, "code_challenge_method": "S256"})
        wrong = await c.get("/oauth/authorize", params={
            "response_type": "token", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256"})
    assert missing.status_code == 400
    assert wrong.status_code == 400


async def test_bearer_scheme_is_case_insensitive():
    app = build_app(settings())
    async with client(app) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "C", "redirect_uris": ["http://localhost/cb"]})).json()
        verifier, challenge = pkce()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "s", "csrf_token": csrf},
            follow_redirects=False)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        tok = (await c.post("/oauth/token", data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": "http://localhost/cb", "client_id": reg["client_id"],
            "code_verifier": verifier})).json()
        ok = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1},
                          headers={"Authorization": f"bearer {tok['access_token']}"})
    assert ok.status_code != 401


async def test_discovery_documents_reflect_a_non_default_port():
    app = build_app(settings(mcp_port=8080))
    async with client(app) as c:
        pr = await c.get("/.well-known/oauth-protected-resource")
        as_ = await c.get("/.well-known/oauth-authorization-server")
    assert ":8080" in pr.json()["resource"]
    body = as_.json()
    assert ":8080" in body["authorization_endpoint"]
    assert ":8080" in body["token_endpoint"]


async def test_password_mode_401_feeds_the_auth_failure_counter_a_good_token_does_not():
    # Regression: BearerAuthMiddleware's invalid-token 401 path never called
    # metrics.record_auth_failure() -- only OktaBearerMiddleware did -- so
    # PgllensAuthFailureSpike could never fire under MCP_AUTH_MODE=password.
    metrics.configure_metrics(settings(metrics_enabled=True))
    if not metrics.enabled():
        pytest.skip("observability extra not installed")

    verifier, challenge = pkce()
    app = build_app(settings())
    async with client(app) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "Claude", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz", "csrf_token": csrf},
            follow_redirects=False)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        tok = (await c.post("/oauth/token", data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": "http://localhost/cb", "client_id": reg["client_id"],
            "code_verifier": verifier})).json()

        bad = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1},
                            headers={"Authorization": "Bearer not-a-real-token"})
        assert bad.status_code == 401
        body, _ct = metrics.render()
        assert b"pgllens_auth_failures_total 1.0" in body

        good = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1},
                             headers={"Authorization": f"Bearer {tok['access_token']}"})
        assert good.status_code != 401
        body, _ct = metrics.render()
        assert b"pgllens_auth_failures_total 1.0" in body  # unchanged


async def test_full_flow_works_when_a_redis_backed_limit_store_is_selected(monkeypatch):
    """Password mode + REDIS_URL (multi-replica config): the token minted by
    /oauth/token must validate against the SAME store BearerAuthMiddleware
    checks. Before the fix, build_app swapped in a RedisTokenStore for the
    validating side but /oauth/token kept issuing into an in-memory
    OAuthState.tokens no one read from again -- every token 401'd. This test
    fails on that wiring and passes once build_oauth's token_store is
    injected from the same RedisLimitStore.client build_app already built.
    """
    fake_client = FakeRedis()
    monkeypatch.setattr(
        "pgllens.server.build_limit_store", lambda settings: RedisLimitStore(fake_client)
    )
    verifier, challenge = pkce()
    app = build_app(settings())
    async with client(app) as c:
        reg = (await c.post("/oauth/register", json={
            "client_name": "Claude", "redirect_uris": ["http://localhost/cb"]})).json()
        form = await c.get("/oauth/authorize", params={
            "response_type": "code", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz"})
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', form.text).group(1)
        auth = await c.post("/oauth/authorize", data={
            "password": "letmein", "client_id": reg["client_id"],
            "redirect_uri": "http://localhost/cb", "code_challenge": challenge,
            "code_challenge_method": "S256", "state": "xyz", "csrf_token": csrf},
            follow_redirects=False)
        code = re.search(r"code=([^&]+)", auth.headers["location"]).group(1)
        tok = (await c.post("/oauth/token", data={
            "grant_type": "authorization_code", "code": code,
            "redirect_uri": "http://localhost/cb", "client_id": reg["client_id"],
            "code_verifier": verifier})).json()
        assert tok["token_type"].lower() == "bearer" and tok["access_token"]
        ok = await c.post("/mcp", json={"jsonrpc": "2.0", "method": "ping", "id": 1},
                          headers={"Authorization": f"Bearer {tok['access_token']}"})
    # Matches test_full_authorization_code_pkce_flow_yields_a_usable_token's
    # convention: this harness's ASGI transport sends Host: testserver, which
    # mcp's transport_security middleware (unrelated to auth) turns into 421
    # further down the stack -- a pre-existing, unrelated quirk. What this
    # assertion proves is that BearerAuthMiddleware did NOT 401 the token,
    # i.e. it found the token /oauth/token issued. Before the fix (two
    # independent token stores) this was 401 every time.
    assert ok.status_code != 401
