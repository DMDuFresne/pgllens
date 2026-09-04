import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from pgllens.middleware import ADMIN_TOOLS, ScopeEnforcementMiddleware
from pgllens.oauth.bearer import OktaBearerMiddleware
from pgllens.oauth.okta import JwksCache, JwtVerifier
from tests.jwt_helpers import AUDIENCE, ISSUER, mint, rsa_keypair

SEEN = {}


def build(private_key_holder, *, scopes_middleware=True):
    private_key, doc = rsa_keypair()
    private_key_holder.append(private_key)

    def handler(request):
        return httpx.Response(200, json=doc, headers={"cache-control": "max-age=300"})

    verifier = JwtVerifier(
        JwksCache("https://test.okta.com/oauth2/aus1/v1/keys",
                  client=httpx.AsyncClient(transport=httpx.MockTransport(handler))),
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    async def mcp(request):
        SEEN["sub"] = request.scope.get("pgllens.sub")
        SEEN["client_id"] = request.scope.get("pgllens.client_id")
        SEEN["scopes"] = request.scope.get("pgllens.scopes")
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    # add_middleware applies last-added FIRST, so bearer auth must be added
    # after the scope gate for it to run before it.
    if scopes_middleware:
        app.add_middleware(ScopeEnforcementMiddleware, protected_path="/mcp")
    app.add_middleware(
        OktaBearerMiddleware,
        verifier=verifier,
        protected_path="/mcp",
        resource_metadata_url="https://pgllens.example.com/.well-known/oauth-protected-resource",
    )
    return app


def call(tool="query"):
    return {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": {}}}


async def post(app, body, token=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post("/mcp", json=body, headers=headers)


@pytest.fixture
def app_and_key():
    holder = []
    app = build(holder)
    return app, holder[0]


async def test_a_valid_token_reaches_the_app_and_stamps_identity(app_and_key):
    app, key = app_and_key
    r = await post(app, call(), mint(key, scp=["pgllens.read"]))
    assert r.status_code == 200
    assert SEEN["sub"] == "00u1testuser"
    assert SEEN["client_id"] == "0oa1testclient"
    assert SEEN["scopes"] == frozenset({"pgllens.read"})


async def test_no_token_is_401_with_rfc9728_discovery(app_and_key):
    app, _key = app_and_key
    r = await post(app, call())
    assert r.status_code == 401
    assert "resource_metadata=" in r.headers["www-authenticate"]


async def test_a_bad_token_is_401_and_never_echoed_back(app_and_key):
    app, _key = app_and_key
    r = await post(app, call(), "totally.bogus.token")
    assert r.status_code == 401
    assert "totally.bogus.token" not in r.text
    assert "totally.bogus.token" not in r.headers["www-authenticate"]


async def test_read_scope_cannot_call_an_admin_tool(app_and_key):
    # get_active_sessions/get_blocking/get_query_store return SQL text written
    # by other users -- pgllens.read must not reach them.
    app, key = app_and_key
    token = mint(key, scp=["pgllens.read"])
    for tool in sorted(ADMIN_TOOLS):
        r = await post(app, call(tool), token)
        assert r.status_code == 403, tool


async def test_admin_scope_can_call_an_admin_tool(app_and_key):
    app, key = app_and_key
    token = mint(key, scp=["pgllens.read", "pgllens.admin"])
    r = await post(app, call("get_active_sessions"), token)
    assert r.status_code == 200


async def test_admin_scope_alone_still_grants_the_read_tools(app_and_key):
    # Admin is a superset in practice; a token with only pgllens.admin calling
    # `query` must not be a confusing 403.
    app, key = app_and_key
    r = await post(app, call("query"), mint(key, scp=["pgllens.admin"]))
    assert r.status_code == 200


async def test_a_token_with_no_pgllens_scope_cannot_call_anything(app_and_key):
    app, key = app_and_key
    r = await post(app, call("query"), mint(key, scp=["openid", "profile"]))
    assert r.status_code == 403


async def test_a_batched_call_is_rejected_if_any_member_needs_admin(app_and_key):
    # Batching must not be a scope-escalation path.
    app, key = app_and_key
    token = mint(key, scp=["pgllens.read"])
    r = await post(app, [call("query"), call("get_blocking")], token)
    assert r.status_code == 403


async def test_an_oversize_body_is_rejected_413_not_silently_unenforced(app_and_key):
    # Before the fix, ScopeEnforcementMiddleware `break`s out of the read loop
    # at 1 MiB and parses the truncated buffer -- unparsable JSON makes
    # _tool_names() return [], which skips the scope check entirely and lets an
    # oversize request through unenforced (final-review.md Minor 1). It must
    # reject outright instead, mirroring the rate limiter's own 413 guard.
    app, key = app_and_key
    token = mint(key, scp=["pgllens.read"])
    huge = call("get_active_sessions")  # an admin tool a read-only token can't call
    huge["params"]["arguments"] = {"padding": "x" * (2 * 1024 * 1024)}
    r = await post(app, huge, token)
    assert r.status_code == 413


async def test_tools_list_and_initialize_are_not_scope_gated(app_and_key):
    # A client that cannot list tools looks like a broken server, and the list
    # itself leaks nothing beyond tool names the docs already publish.
    app, key = app_and_key
    token = mint(key, scp=["pgllens.read"])
    for method in ("tools/list", "initialize", "ping"):
        r = await post(app, {"jsonrpc": "2.0", "id": 1, "method": method}, token)
        assert r.status_code == 200, method


async def test_forged_kid_401_never_leaks_jwks_url_or_kid(app_and_key):
    # Task 3's TokenError can wrap a JwksError whose text contains the JWKS URL
    # and the attacker-supplied kid -- neither may ever reach the client.
    app, key = app_and_key
    forged_kid = "attacker-controlled-kid-value"
    token = mint(key, kid=forged_kid, scp=["pgllens.read"])
    r = await post(app, call(), token)
    assert r.status_code == 401
    assert forged_kid not in r.text
    assert forged_kid not in r.headers["www-authenticate"]
    assert "test.okta.com" not in r.text
    assert "test.okta.com" not in r.headers["www-authenticate"]


async def test_health_is_not_behind_the_bearer_gate(app_and_key):
    # protected_path scoping: only /mcp is gated.
    app, _key = app_and_key
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/nope", json={})
    assert r.status_code != 401
