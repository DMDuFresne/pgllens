"""Same contract as tests/test_limits.py, against a REAL Redis.

Skips cleanly when PGLLENS_TEST_REDIS_URL is unset, exactly like the
PGLLENS_TEST_DSN pattern in tests/integration/conftest.py -- fakeredis is a good
double but it is not redis-server, and the TTL/INCRBYFLOAT semantics this relies
on are worth confirming once against the real thing.
"""

import os

import pytest

from pgllens.limits import RedisLimitStore

REDIS_ENV = "PGLLENS_TEST_REDIS_URL"

pytestmark = pytest.mark.integration


@pytest.fixture
async def live_redis():
    url = os.environ.get(REDIS_ENV)
    if not url:
        pytest.skip(f"{REDIS_ENV} is not set; skipping live Redis tests")
    try:
        import redis.asyncio as aioredis

        client = aioredis.from_url(url, decode_responses=True)
        await client.ping()
    except Exception as e:  # noqa: BLE001 -- any failure to reach Redis is a skip
        pytest.skip(f"{REDIS_ENV} is set but unreachable: {e}")
    yield client
    await client.flushdb()
    await client.aclose()


async def test_counts_and_expires_against_a_real_redis(live_redis):
    store = RedisLimitStore(live_redis, prefix="pgllens-test")
    assert await store.incr("k", 1, 60) == 1
    assert await store.incr("k", 2.5, 60) == pytest.approx(3.5)
    keys = await live_redis.keys("pgllens-test:limit:*")
    assert keys and await live_redis.ttl(keys[0]) > 0
