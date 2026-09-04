"""Per-key login/attempt rate limiting with a fixed lockout window.

A plain attempts/window limiter for e.g. OAuth token attempts. Tool-call
throttling is a transport-layer concern and lives in pgllens/middleware.py.

`time.monotonic` is imported via the module (not `from time import monotonic`)
so tests can monkeypatch `pgllens.oauth.ratelimit.time.monotonic`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class _State:
    attempts: int = 0
    window_start: float = 0.0
    lockout_until: float = 0.0


class RateLimiter:
    # Deliberately simple: in-memory dict, single process; counters die on restart
    # and across workers. Fine for a single-instance MCP server; move to redis or a
    # shared store if this ever runs behind multiple processes.
    def __init__(self, attempts: int, window_ms: int) -> None:
        self._max_attempts = attempts
        self._window_s = window_ms / 1000.0
        self._state: dict[str, _State] = {}

    def _expired(self, state: _State, now: float) -> bool:
        if state.lockout_until:
            return state.lockout_until <= now
        return now - state.window_start >= self._window_s

    def check(self, key: str) -> bool:
        """True if `key` is currently allowed. Never counts as an attempt itself.

        Reclaims the entry when its window (or lockout) has fully elapsed, so
        neither a flood of distinct keys nor a slow trickle of sub-threshold
        failures retains state forever -- memory is bounded by keys active in
        the current window, not by every key ever seen.
        """
        state = self._state.get(key)
        if state is None:
            return True
        now = time.monotonic()
        if self._expired(state, now):
            del self._state[key]
            return True
        return not state.lockout_until > now

    def record_failure(self, key: str) -> None:
        now = time.monotonic()
        state = self._state.get(key)
        if state is None or self._expired(state, now):
            state = _State(window_start=now)
            self._state[key] = state
        state.attempts += 1
        if state.attempts >= self._max_attempts:
            state.lockout_until = now + self._window_s

    def reset(self, key: str) -> None:
        self._state.pop(key, None)
