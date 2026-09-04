import time

from pgllens.oauth.ratelimit import RateLimiter


def test_allows_until_attempts_exhausted():
    rl = RateLimiter(attempts=3, window_ms=900000)
    for _ in range(3):
        assert rl.check("1.2.3.4") is True
        rl.record_failure("1.2.3.4")
    assert rl.check("1.2.3.4") is False  # locked out


def test_lockout_is_per_key():
    rl = RateLimiter(attempts=1, window_ms=900000)
    rl.record_failure("1.2.3.4")
    assert rl.check("1.2.3.4") is False
    assert rl.check("5.6.7.8") is True


def test_window_expiry_clears_lockout(monkeypatch):
    rl = RateLimiter(attempts=1, window_ms=1000)
    rl.record_failure("1.2.3.4")
    assert rl.check("1.2.3.4") is False
    # Offset relative to the real clock, not an absolute constant — see the
    # matching comment in test_oauth_store.py::test_expired_code_is_not_consumable.
    later = time.monotonic() + 10_000.0
    monkeypatch.setattr("pgllens.oauth.ratelimit.time.monotonic", lambda: later)
    assert rl.check("1.2.3.4") is True


def test_success_resets_counter():
    rl = RateLimiter(attempts=2, window_ms=900000)
    rl.record_failure("k")
    rl.reset("k")
    rl.record_failure("k")
    assert rl.check("k") is True


# --- adversarial cases ---


def test_check_does_not_itself_count_as_an_attempt():
    rl = RateLimiter(attempts=1, window_ms=900000)
    for _ in range(50):
        assert rl.check("k") is True
    rl.record_failure("k")
    assert rl.check("k") is False


def test_elapsed_lockout_entries_are_evicted_not_just_ignored(monkeypatch):
    # A flood of distinct keys (e.g. one per spoofed X-Forwarded-For value)
    # must not retain state forever once their lockout window elapses.
    rl = RateLimiter(attempts=1, window_ms=1000)
    rl.record_failure("1.2.3.4")
    assert "1.2.3.4" in rl._state
    later = time.monotonic() + 10_000.0
    monkeypatch.setattr("pgllens.oauth.ratelimit.time.monotonic", lambda: later)
    assert rl.check("1.2.3.4") is True
    assert "1.2.3.4" not in rl._state


def test_attempts_age_out_after_the_window(fake_clock):
    rl = RateLimiter(attempts=5, window_ms=1000)
    for _ in range(4):
        rl.record_failure("ip")  # below threshold
    fake_clock.advance(1.1)  # window elapses
    rl.record_failure("ip")  # would be the 5th -- but it's a new window
    assert rl.check("ip") is True


def test_sub_lockout_entries_are_reclaimed_by_check(fake_clock):
    rl = RateLimiter(attempts=5, window_ms=1000)
    rl.record_failure("ip")  # 1 attempt, never locked
    fake_clock.advance(1.1)
    assert rl.check("ip") is True
    assert "ip" not in rl._state  # entry reclaimed, not retained forever
