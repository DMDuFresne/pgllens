"""Counters that survive more than one process.

Phase 1's rate limiter and token store were in-memory dicts with a comment
naming Redis as the upgrade path (`middleware.py`, `oauth/store.py`). On the
internet the single-process ceiling is two bugs: limits reset on every restart,
and two replicas behind a load balancer each grant the full limit.

This is the seam. `InMemoryLimitStore` keeps the local default exactly as it
behaves today; `RedisLimitStore` shares one budget across replicas. Both are
fixed-window counters, matching the semantics the Phase 1 middleware already
had; a sliding window would be a behaviour change, not a hardening.

OPTIONAL DEPENDENCY DISCIPLINE: importing this module must never fail without
the `redis` extra installed, the same rule obs/metrics.py holds itself to.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pgllens.config import Settings

logger = logging.getLogger("pgllens")


class LimitStore(Protocol):
    """A shared, expiring counter, plus a gauge for in-flight concurrency.

    `incr` is a fixed-window counter (cost budget, call-rate limiting):
    windows roll over and a stale key is simply gone. A gauge is a different
    shape -- it tracks "how many are in flight right now", with no window to
    roll over, so it gets its own `acquire`/`release` pair instead of being
    shoehorned into `incr` with `window_s=0` (that shape bit us once already:
    Redis's per-key TTL from `window_s` expired mid-request and `release`
    decremented a reset-to-zero key negative, silently disabling the cap --
    see git history on this file for the incident).
    """

    async def incr(self, key: str, amount: float, window_s: float) -> float:
        """Add `amount` to `key`'s current window and return the new total."""
        ...

    async def acquire(self, key: str, limit: int) -> bool:
        """Atomically take one gauge slot for `key` and report whether the
        result is within `limit`. Always increments -- a caller that gets
        `False` back must call `release` to give the slot back."""
        ...

    async def release(self, key: str) -> None:
        """Give back one gauge slot for `key`. Clamped at zero: releasing more
        than was acquired must never leave the gauge negative (a negative
        gauge would raise the effective cap for every future acquire)."""
        ...


class InMemoryLimitStore:
    """Single-process fixed-window counters, the local default.

    Deliberately simple: a plain dict. Bounded by eviction on window rollover, so
    a flood of attacker-chosen keys cannot retain state past one window.
    """

    def __init__(self, now: Callable[[], float] = time.monotonic) -> None:
        self._now = now
        self._buckets: dict[str, tuple[float, float]] = {}
        # Gauges (acquire/release) are a separate dict from windowed counters:
        # a live process holds this in memory for exactly as long as the
        # request is in flight, so there is no window to roll over and no TTL
        # to leak past -- a crash just drops the whole dict with the process.
        self._gauges: dict[str, int] = {}

    async def incr(self, key: str, amount: float, window_s: float) -> float:
        now = self._now()
        self._sweep(now, window_s)
        start, total = self._buckets.get(key, (now, 0.0))
        if window_s > 0 and now - start >= window_s:
            start, total = now, 0.0
        total += amount
        self._buckets[key] = (start, total)
        return total

    async def acquire(self, key: str, limit: int) -> bool:
        current = self._gauges.get(key, 0) + 1
        self._gauges[key] = current
        return current <= limit

    async def release(self, key: str) -> None:
        new = self._gauges.get(key, 0) - 1
        if new <= 0:
            self._gauges.pop(key, None)
        else:
            self._gauges[key] = new

    def _sweep(self, now: float, window_s: float) -> None:
        if window_s <= 0:
            return
        stale = [k for k, (start, _t) in self._buckets.items() if now - start >= window_s]
        for k in stale:
            del self._buckets[k]


class RedisLimitStore:
    """Fixed-window counters in Redis, shared by every replica.

    One key per (name, window index): INCRBYFLOAT plus an EXPIRE set on first
    write. INCRBYFLOAT (not INCR) because the cost budget charges planner cost
    units, which are floats. The TTL is what bounds memory -- nothing is ever
    swept explicitly.
    """

    def __init__(
        self, client: Any, *, prefix: str = "pgllens", gauge_ttl_s: float = 3600.0
    ) -> None:
        self.client = client
        self._client = client
        self._prefix = prefix
        # Safety-net TTL for gauge keys (acquire/release), refreshed on every
        # acquire -- NOT the request's actual duration. A long default means a
        # normal, even slow, request never sees its slot expire mid-flight (the
        # bug this replaces: window_s=0 gave the old incr()-based gauge a 1s
        # TTL). It exists purely so a replica that crashes mid-request doesn't
        # leak the slot forever; configurable for tests that want to observe
        # the cleanup without waiting an hour.
        self._gauge_ttl_s = gauge_ttl_s
        # Deliberately simple: a bool, not a timer. Good enough to stop one outage
        # from spamming a WARNING per request; cleared the moment a call succeeds
        # again. Add a time-based "once per window" if this ever proves noisy.
        self._backend_down = False

    def _key(self, key: str, window_s: float) -> str:
        window_index = int(time.time() // window_s) if window_s > 0 else 0
        return f"{self._prefix}:limit:{key}:{window_index}"

    def _gauge_key(self, key: str) -> str:
        return f"{self._prefix}:gauge:{key}"

    async def incr(self, key: str, amount: float, window_s: float) -> float:
        # Fail OPEN: unlike RedisTokenStore.validate (fail closed by design --
        # see oauth/store.py), a Redis error here must not block traffic. An
        # unthrottled window beats a total outage of every /mcp call. Any
        # backend error (connection refused, timeout, eviction) is caught at
        # this boundary -- not at every _allow() call site -- and reported as
        # "0 consumed so far", which always compares as under budget.
        redis_key = self._key(key, window_s)
        try:
            pipe = self._client.pipeline()
            pipe.incrbyfloat(redis_key, amount)
            # Re-set on every write rather than only on creation: one extra
            # command in the same round-trip, and it removes the "created
            # without a TTL" failure mode entirely (a key that outlived its
            # EXPIRE is a permanent lockout).
            pipe.expire(redis_key, int(window_s) + 1)
            total, _ = await pipe.execute()
        except Exception as exc:  # noqa: BLE001 -- any backend error fails open
            if not self._backend_down:
                logger.warning(
                    "RedisLimitStore.incr backend error, failing open (allowing "
                    "the call): %s", exc,
                )
                self._backend_down = True
            return 0.0
        self._backend_down = False
        return float(total)

    async def acquire(self, key: str, limit: int) -> bool:
        # Fail OPEN, same rule as incr(): a backend error must not block
        # traffic, so an unreachable Redis reports "slot granted" rather than
        # rejecting every call. INCR (atomic) + EXPIRE refreshed on every
        # acquire in one pipeline -- see __init__ for why the TTL is long.
        redis_key = self._gauge_key(key)
        try:
            pipe = self._client.pipeline()
            pipe.incr(redis_key)
            pipe.expire(redis_key, int(self._gauge_ttl_s) + 1)
            count, _ = await pipe.execute()
        except Exception as exc:  # noqa: BLE001 -- any backend error fails open
            if not self._backend_down:
                logger.warning(
                    "RedisLimitStore.acquire backend error, failing open "
                    "(allowing the call): %s", exc,
                )
                self._backend_down = True
            return True
        self._backend_down = False
        return int(count) <= limit

    async def release(self, key: str) -> None:
        # Same fail-open rule as acquire(): a missed release just leaves a
        # gauge one unit higher than it should be, not a blocked request.
        #
        # Clamped at zero via GET-then-conditional-SET rather than a bare
        # DECR: a double release (a bug, or a race between the reject path
        # and the `finally` in ConcurrencyLimitMiddleware) must not leave the
        # gauge negative, since a negative gauge silently raises the
        # effective cap for every future acquire on this key. This is not a
        # single atomic op (fakeredis has no Lua/EVAL support to lean on, and
        # the store already documents itself as fail-open/best-effort under
        # backend errors) -- a release racing another release at the zero
        # boundary can transiently under-clamp by one, which self-heals on
        # the next release. A real concurrency cap violation (two acquires
        # both granted) is what `acquire`'s atomic INCR prevents; this only
        # guards the decrement side.
        redis_key = self._gauge_key(key)
        try:
            new_value = await self._client.decr(redis_key)
            if new_value < 0:
                pipe = self._client.pipeline()
                pipe.set(redis_key, 0)
                pipe.expire(redis_key, int(self._gauge_ttl_s) + 1)
                await pipe.execute()
        except Exception as exc:  # noqa: BLE001 -- fail open, see acquire()
            if not self._backend_down:
                logger.warning("RedisLimitStore.release backend error, ignoring: %s", exc)
                self._backend_down = True


def build_limit_store(settings: Settings) -> LimitStore:
    """Redis when REDIS_URL is set, the extra is installed, AND Redis answers
    a boot-time ping, else in-memory.

    A REDIS_URL that cannot be honoured -- extra missing, or Redis simply
    unreachable -- is a WARNING and a fall back to in-memory, not a boot
    failure: losing shared limits degrades a control, while refusing to start
    takes the whole lens down. The auth path has the opposite trade-off and
    fails closed (see server.py's JWKS prime).

    The ping is a plain blocking `redis.Redis.ping()` (the sync client, not
    `redis.asyncio`), not an `await`: this function is called from
    `build_app()`, which is not itself async, and a one-shot boot-time probe
    doesn't need the async client's connection pool. It exists so a dead
    Redis at boot fails the same way a dead Redis mid-flight does (WARNING +
    fallback), instead of silently handing back a `RedisLimitStore` whose
    first real use is the one that discovers the outage.
    """
    if not settings.redis_url:
        return InMemoryLimitStore()
    try:
        import redis
        import redis.asyncio as aioredis
    except ImportError:
        logger.warning(
            "REDIS_URL is set but the redis extra is not installed; falling back "
            "to in-memory limits. Install with: pip install 'pgllens[redis]'"
        )
        return InMemoryLimitStore()

    try:
        redis.Redis.from_url(
            settings.redis_url, socket_connect_timeout=1.0, socket_timeout=1.0
        ).ping()
    except Exception as exc:  # noqa: BLE001 -- any probe failure is a fallback
        logger.warning(
            "REDIS_URL is set but Redis is unreachable at boot (%s); falling "
            "back to in-memory limits.", exc,
        )
        return InMemoryLimitStore()

    return RedisLimitStore(aioredis.from_url(settings.redis_url, decode_responses=True))


# --- Cost budget (module state, matching obs/metrics.py's configure_* pattern) ---
_budget_per_window: float | None = None
_budget_window_s: float = 60.0
_budget_store: LimitStore = InMemoryLimitStore()


def configure_limits(settings: Settings, store: LimitStore) -> None:
    """Point the cost budget at `store`. Idempotent; never raises."""
    global _budget_per_window, _budget_store
    _budget_per_window = settings.tool_cost_budget_per_minute
    _budget_store = store


async def charge_cost(client_id: str, cost: float) -> bool:
    """Charge `cost` planner units to `client_id`'s window.

    Returns False once the window's budget is exceeded. Off (always True) when
    TOOL_COST_BUDGET_PER_MINUTE is unset. The charge happens before the decision
    so an over-budget query still counts against the window -- otherwise a
    client could retry an expensive query forever at zero cost.
    """
    if not _budget_per_window:
        return True
    total = await _budget_store.incr(f"cost:{client_id}", cost, _budget_window_s)
    return total <= _budget_per_window
