import asyncio
import json

import httpx
import pytest
from fakeredis.aioredis import FakeRedis
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from pgllens.caller import Caller, caller, set_caller
from pgllens.limits import InMemoryLimitStore, RedisLimitStore, charge_cost, configure_limits
from pgllens.middleware import (
    CallerContextMiddleware,
    ConcurrencyLimitMiddleware,
    InboundToolRateLimitMiddleware,
)
from pgllens.oauth.store import RedisTokenStore, TokenStore


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


def _call():
    return {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "query", "arguments": {}}}


def _app(per_minute, store):
    async def mcp(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    app.add_middleware(
        InboundToolRateLimitMiddleware,
        protected_path="/mcp",
        per_minute=per_minute,
        trust_proxy_headers=False,
        store=store,
    )
    return app


async def _post(app, body):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post("/mcp", json=body)


# --- the store itself ---------------------------------------------------------

async def test_in_memory_store_counts_within_a_window():
    store = InMemoryLimitStore()
    assert await store.incr("k", 1, 60) == 1
    assert await store.incr("k", 1, 60) == 2


async def test_in_memory_store_rolls_over_when_the_window_elapses():
    clock = [1000.0]
    store = InMemoryLimitStore(now=lambda: clock[0])
    await store.incr("k", 1, 60)
    clock[0] += 61
    assert await store.incr("k", 1, 60) == 1


async def test_redis_store_counts_and_expires(redis_client):
    store = RedisLimitStore(redis_client)
    assert await store.incr("k", 1, 60) == 1
    assert await store.incr("k", 1, 60) == 2
    # An abandoned key must not live forever: every counter carries a TTL.
    keys = await redis_client.keys("pgllens:limit:*")
    assert keys and await redis_client.ttl(keys[0]) > 0


async def test_redis_store_accepts_fractional_amounts(redis_client):
    # The cost budget (Task 8) charges planner cost units, which are floats.
    store = RedisLimitStore(redis_client)
    assert await store.incr("cost", 12.5, 60) == pytest.approx(12.5)
    assert await store.incr("cost", 2.5, 60) == pytest.approx(15.0)


# --- the spec's two-replica requirement ---------------------------------------


# --- the gauge (acquire/release) -----------------------------------------------
#
# Fix round 1, Critical: the concurrency cap used to be `incr(key, 1, 0)` /
# `release`, a windowed counter pressed into gauge duty. window_s=0 gave the
# Redis key a 1s TTL (see the old `_key`), so a query running longer than 1s
# saw its own gauge key expire mid-flight; the eventual `release()` then
# DECRemented a key that had reset to 0, driving it to -1 -- permanently
# under the cap, defeating the limit for exactly the deployment (Redis,
# multi-replica) this was built for. `acquire`/`release` replace that with a
# real gauge: an atomic INCR against a long, refreshed safety TTL, and a
# release that's clamped so it can never go negative.

async def test_in_memory_gauge_enforces_the_limit_and_release_frees_a_slot():
    store = InMemoryLimitStore()
    assert await store.acquire("g", 2) is True
    assert await store.acquire("g", 2) is True
    assert await store.acquire("g", 2) is False  # 3 > 2
    await store.release("g")  # the rejected caller undoes its own increment
    await store.release("g")  # one of the two granted holders finishes
    assert await store.acquire("g", 2) is True  # the freed slot is usable again


async def test_in_memory_gauge_release_never_goes_negative():
    store = InMemoryLimitStore()
    await store.release("g")  # released before anything ever acquired it
    await store.release("g")
    assert "g" not in store._gauges  # clamped to 0, then popped -- never left dangling
    assert await store.acquire("g", 1) is True  # starts clean at 1, not -1


async def test_gauge_key_is_deleted_when_released_to_zero():
    store = InMemoryLimitStore()
    await store.acquire("k", 5)
    await store.release("k")
    assert "k" not in store._gauges


async def test_redis_gauge_enforces_the_limit(redis_client):
    store = RedisLimitStore(redis_client)
    assert await store.acquire("g", 1) is True
    assert await store.acquire("g", 1) is False  # already at the cap
    await store.release("g")  # the rejected caller undoes its own increment
    await store.release("g")  # the one granted holder finishes
    assert await store.acquire("g", 1) is True


async def test_redis_gauge_survives_past_the_old_broken_1s_ttl(redis_client):
    # The regression itself: hold one permit for longer than the old
    # window_s=0 gauge's 1s TTL. With the bug, the key would have expired and
    # a second acquire would wrongly see an empty key (count 1, "granted").
    # With the fix, the safety TTL is long (3600s default), so the key is
    # still alive and the second acquire correctly sees the cap is full.
    store = RedisLimitStore(redis_client)
    assert await store.acquire("g", 1) is True
    await asyncio.sleep(1.2)  # > the old 1s TTL that used to cause the bug
    assert await store.acquire("g", 1) is False
    keys = await redis_client.keys("pgllens:gauge:*")
    assert keys and await redis_client.ttl(keys[0]) > 60  # nowhere near 1s


async def test_redis_gauge_release_never_goes_negative(redis_client):
    store = RedisLimitStore(redis_client)
    await store.release("g")  # released before anything ever acquired it
    await store.release("g")
    assert await redis_client.get("pgllens:gauge:g") == "0"
    assert await store.acquire("g", 1) is True  # starts clean at 1, not -1


async def test_redis_gauge_acquire_fails_open_on_backend_error(caplog):
    class _RaisingGaugeClient:
        def pipeline(self):
            raise ConnectionError("redis unavailable")

    store = RedisLimitStore(_RaisingGaugeClient())
    with caplog.at_level("WARNING", logger="pgllens"):
        granted = await store.acquire("g", 1)
    assert granted is True  # fail open: a backend error must not block a call
    assert any("failing open" in r.message for r in caplog.records)


# --- the spec's two-replica requirement ---------------------------------------

async def test_two_replicas_share_one_rate_limit_budget(redis_client):
    # Spec, definition of done: "Two replicas share one rate-limit budget,
    # proven by test." Two independently constructed apps, one Redis.
    replica_a = _app(4, RedisLimitStore(redis_client))
    replica_b = _app(4, RedisLimitStore(redis_client))
    assert (await _post(replica_a, _call())).status_code == 200
    assert (await _post(replica_a, _call())).status_code == 200
    assert (await _post(replica_b, _call())).status_code == 200
    assert (await _post(replica_b, _call())).status_code == 200
    # Five calls against a budget of four -- whichever replica gets it, 429.
    assert (await _post(replica_b, _call())).status_code == 429
    assert (await _post(replica_a, _call())).status_code == 429


async def test_in_memory_replicas_do_not_share_a_budget():
    # The bug the Redis store fixes, pinned so nobody "simplifies" the store
    # away later: two in-memory replicas each grant the full limit.
    replica_a = _app(1, InMemoryLimitStore())
    replica_b = _app(1, InMemoryLimitStore())
    assert (await _post(replica_a, _call())).status_code == 200
    assert (await _post(replica_b, _call())).status_code == 200


# --- the token store ----------------------------------------------------------

async def test_in_memory_token_store_round_trips():
    store = TokenStore(ttl_seconds=60)
    token, expires_in = await store.issue("client-1", "mcp")
    assert expires_in == 60
    assert (await store.validate(token)) == {"client_id": "client-1", "scope": "mcp"}
    await store.revoke(token)
    assert await store.validate(token) is None


async def test_redis_token_store_round_trips(redis_client):
    store = RedisTokenStore(redis_client, ttl_seconds=60)
    token, expires_in = await store.issue("client-1", "mcp")
    assert expires_in == 60
    assert (await store.validate(token)) == {"client_id": "client-1", "scope": "mcp"}
    await store.revoke(token)
    assert await store.validate(token) is None


async def test_redis_token_store_never_stores_the_raw_token(redis_client):
    # Same at-rest property the in-memory store already has: a dump of the
    # store must yield no usable token.
    store = RedisTokenStore(redis_client, ttl_seconds=60)
    token, _ = await store.issue("client-1", "mcp")
    keys = await redis_client.keys("*")
    assert keys
    assert all(token not in key for key in keys)


async def test_redis_token_store_survives_a_new_instance(redis_client):
    # The point of the exercise: a token issued by one replica validates on
    # another, and across a restart.
    issuer = RedisTokenStore(redis_client, ttl_seconds=60)
    token, _ = await issuer.issue("client-1", "mcp")
    other_replica = RedisTokenStore(redis_client, ttl_seconds=60)
    assert (await other_replica.validate(token)) is not None


async def test_redis_token_store_expires_tokens(redis_client):
    store = RedisTokenStore(redis_client, ttl_seconds=1)
    await store.issue("client-1", "mcp")
    keys = await redis_client.keys("pgllens:token:*")
    assert await redis_client.ttl(keys[0]) <= 1


# --- fix round 1: Redis-outage failure semantics -------------------------------
#
# Controller ruling: the rate limiter and the token store deliberately fail in
# opposite directions when Redis is unreachable. The limiter fails OPEN (an
# unthrottled window beats a total outage of every /mcp call); the token store
# fails CLOSED (a Redis error must never be treated as "this token is valid").
# Both are handled at the store boundary (RedisLimitStore/RedisTokenStore), not
# at every call site in middleware.py/bearer.py.


class _RaisingClient:
    """A client whose Redis calls always raise -- stands in for a dead/
    unreachable Redis without needing a real connection-refused error."""

    def pipeline(self):
        raise ConnectionError("redis unavailable")

    async def get(self, _key):
        raise ConnectionError("redis unavailable")


async def test_redis_limit_store_incr_fails_open_on_backend_error(caplog):
    store = RedisLimitStore(_RaisingClient())
    with caplog.at_level("WARNING", logger="pgllens"):
        total = await store.incr("k", 1, 60)
    assert total == 0.0  # "0 consumed so far" always compares as under budget
    assert any("failing open" in r.message for r in caplog.records)


async def test_redis_limit_store_incr_failure_does_not_spam_logs(caplog):
    # One WARNING per outage, not one per request.
    store = RedisLimitStore(_RaisingClient())
    with caplog.at_level("WARNING", logger="pgllens"):
        for _ in range(5):
            await store.incr("k", 1, 60)
    assert sum("failing open" in r.message for r in caplog.records) == 1


async def test_tool_call_passes_when_the_rate_limit_backend_is_down(caplog):
    # End-to-end: a request through the real middleware must get 200, not a
    # 500 or a hang, when its LimitStore's incr() raises.
    app = _app(1, RedisLimitStore(_RaisingClient()))
    with caplog.at_level("WARNING", logger="pgllens"):
        r = await _post(app, _call())
    assert r.status_code == 200
    assert any("failing open" in rec.message for rec in caplog.records)


async def test_redis_token_store_validate_fails_closed_on_backend_error(caplog):
    store = RedisTokenStore(_RaisingClient(), ttl_seconds=60)
    with caplog.at_level("WARNING", logger="pgllens"):
        info = await store.validate("some-token")
    assert info is None
    assert any("denying" in r.message for r in caplog.records)


async def test_bearer_middleware_rejects_not_500s_when_token_store_backend_is_down():
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    from pgllens.oauth.bearer import BearerAuthMiddleware

    async def mcp(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    app.add_middleware(
        BearerAuthMiddleware,
        token_store=RedisTokenStore(_RaisingClient(), ttl_seconds=60),
        protected_path="/mcp",
        resource_metadata_url="http://t/.well-known/oauth-protected-resource",
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/mcp", json=_call(), headers={"Authorization": "Bearer whatever"})
    assert r.status_code == 401  # rejected, not 500 and not authenticated


async def test_redis_token_store_malformed_record_is_invalid_not_a_crash(redis_client):
    # Minor fix: a record that's valid JSON but not the expected shape (or not
    # even a dict) must be treated as an invalid token, not raise KeyError/TypeError.
    store = RedisTokenStore(redis_client, ttl_seconds=60)
    await redis_client.set(store._key("bad-token"), json.dumps({"unexpected": "shape"}))
    assert await store.validate("bad-token") is None
    await redis_client.set(store._key("bad-token-2"), json.dumps(["not", "a", "dict"]))
    assert await store.validate("bad-token-2") is None


async def test_build_limit_store_falls_back_when_redis_is_unreachable_at_boot(caplog):
    # Boot-time probe: REDIS_URL is set but nothing answers -- must fall back
    # to in-memory with a WARNING, not hand back a store whose first real use
    # discovers the outage under load. Port 1 is a privileged, essentially
    # always-closed port on loopback -- refused immediately, no need to wait
    # out a timeout.
    from pgllens.config import Settings
    from pgllens.limits import build_limit_store

    settings = Settings(
        _env_file=None,
        database_url="postgresql://x/y",
        redis_url="redis://127.0.0.1:1/0",
    )
    with caplog.at_level("WARNING", logger="pgllens"):
        store = build_limit_store(settings)
    assert isinstance(store, InMemoryLimitStore)
    assert any("unreachable at boot" in r.message for r in caplog.records)


# --- Task 8: cost budget, caller identity, concurrency cap ---------------------


def budget_settings(**kw):
    from pgllens.config import Settings

    base = {"database_url": "postgresql://u:p@localhost:5432/f", "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


async def test_cost_budget_allows_calls_until_the_window_budget_is_spent():
    store = InMemoryLimitStore()
    configure_limits(budget_settings(tool_cost_budget_per_minute=100), store)
    assert await charge_cost("c1", 40) is True
    assert await charge_cost("c1", 40) is True
    assert await charge_cost("c1", 40) is False  # 120 > 100


async def test_cost_budget_is_per_client():
    store = InMemoryLimitStore()
    configure_limits(budget_settings(tool_cost_budget_per_minute=100), store)
    assert await charge_cost("c1", 90) is True
    assert await charge_cost("c2", 90) is True


async def test_cost_budget_is_off_by_default():
    configure_limits(budget_settings(), InMemoryLimitStore())
    for _ in range(100):
        assert await charge_cost("c1", 1e9) is True


async def test_two_replicas_share_one_cost_budget(redis_client):
    # Same shared-budget property as the call limiter, for cost.
    configure_limits(budget_settings(tool_cost_budget_per_minute=100),
                     RedisLimitStore(redis_client))
    assert await charge_cost("c1", 60) is True
    configure_limits(budget_settings(tool_cost_budget_per_minute=100),
                     RedisLimitStore(redis_client))
    assert await charge_cost("c1", 60) is False


async def test_caller_context_middleware_publishes_the_identity():
    seen = {}

    async def endpoint(request):
        seen["caller"] = caller()
        return JSONResponse({"ok": True})

    async def stamp(scope, receive, send):
        scope["pgllens.client_id"] = "0oa1client"
        scope["pgllens.sub"] = "00u1user"
        await app_inner(scope, receive, send)

    app_inner = Starlette(routes=[Route("/mcp", endpoint, methods=["POST"])])
    app_inner.add_middleware(CallerContextMiddleware, trust_proxy_headers=False)
    transport = httpx.ASGITransport(app=stamp)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        await c.post("/mcp", json={})
    assert seen["caller"].client_id == "0oa1client"
    assert seen["caller"].sub == "00u1user"


async def test_caller_defaults_are_anonymous_outside_a_request():
    set_caller(Caller())
    assert caller().client_id == "anonymous"
    assert caller().sub is None


async def test_concurrency_cap_rejects_the_over_the_cap_request():
    # httpx's ASGITransport presents a client address of 127.0.0.1, and these
    # requests carry no authenticated client id, so the middleware's key is
    # "conc:127.0.0.1". Seed it to simulate two calls already in flight.
    store = InMemoryLimitStore()
    await store.acquire("conc:127.0.0.1", 2)
    await store.acquire("conc:127.0.0.1", 2)

    async def mcp(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    app.add_middleware(
        ConcurrencyLimitMiddleware, protected_path="/mcp", max_concurrent=2,
        store=store, trust_proxy_headers=False,
    )
    r = await _post(app, _call())
    assert r.status_code == 429


async def test_concurrency_slot_is_released_after_the_request():
    store = InMemoryLimitStore()

    async def mcp(request):
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/mcp", mcp, methods=["POST"])])
    app.add_middleware(
        ConcurrencyLimitMiddleware, protected_path="/mcp", max_concurrent=1,
        store=store, trust_proxy_headers=False,
    )
    for _ in range(5):
        # Sequential requests never overlap, so a cap of 1 must never reject
        # them -- a leaked slot would make the second call 429.
        assert (await _post(app, _call())).status_code == 200


async def test_concurrency_slot_is_released_even_when_the_app_raises():
    store = InMemoryLimitStore()

    async def boom(request):
        raise RuntimeError("tool blew up")

    app = Starlette(routes=[Route("/mcp", boom, methods=["POST"])])
    app.add_middleware(
        ConcurrencyLimitMiddleware, protected_path="/mcp", max_concurrent=1,
        store=store, trust_proxy_headers=False,
    )
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await _post(app, _call())
    # The slot is released in a `finally`, so three failed calls leave the gauge
    # at zero rather than permanently wedging this client at the cap.
    assert store._gauges.get("conc:127.0.0.1", 0) == 0
