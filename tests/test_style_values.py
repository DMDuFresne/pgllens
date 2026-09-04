from datetime import UTC, datetime, timedelta, timezone

import pytest

from pgllens.llens_style.values import count, duration, estimate, ident, iso, nof, size


@pytest.mark.parametrize(("n", "out"), [
    (0, "0"), (82, "82"), (999, "999"), (1000, "~1.0K"), (9187, "~9.2K"),
    (1_250_000, "~1.3M"), (2_000_000_000, "~2.0B"), (9187.0, "~9.2K"),
])
def test_estimate(n, out):
    assert estimate(n) == out


@pytest.mark.parametrize(("n", "out"), [(0, "0"), (999, "999"), (1000, "1,000"), (128000, "128,000")])
def test_count(n, out):
    assert count(n) == out


@pytest.mark.parametrize(("n", "out"), [
    (0, "0 B"), (512, "512 B"), (1024, "1.0 kB"), (1_258_291, "1.2 MB"),
    (4 * 1024**3, "4.0 GB"), (3 * 1024**4, "3.0 TB"),
])
def test_size(n, out):
    assert size(n) == out


@pytest.mark.parametrize(("s", "out"), [
    (0, "0s"), (42, "42s"), (90, "1m 30s"), (1682.3, "28m 2s"), (3600, "1h"),
    (3661, "1h 1m"), (273600, "3d 4h"), (86400 * 10, "10d"),
])
def test_duration(s, out):
    assert duration(s) == out


def test_iso_converts_to_utc_seconds_z():
    dt = datetime(2026, 9, 3, 11, 49, 3, 123456, tzinfo=timezone(timedelta(hours=-4)))
    assert iso(dt) == "2026-09-03T15:49:03Z"


def test_iso_rejects_naive():
    with pytest.raises(ValueError, match="aware"):
        iso(datetime(2026, 9, 3))  # noqa: DTZ001 -- naive datetime is the point of this test


def test_iso_utc_passthrough():
    assert iso(datetime(2026, 9, 3, 15, 49, 3, tzinfo=UTC)) == "2026-09-03T15:49:03Z"


def test_ident_wraps_in_backticks():
    assert ident("app_core.assets") == "`app_core.assets`"


@pytest.mark.parametrize(("n", "singular", "plural", "out"), [
    (1, "row", None, "1 row"),
    (2, "row", None, "2 rows"),
    (0, "row", None, "0 rows"),
    (1, "foreign key", None, "1 foreign key"),
    (2, "foreign key", None, "2 foreign keys"),
    (1000, "row", None, "1,000 rows"),
    (1, "index", "indexes", "1 index"),
    (2, "index", "indexes", "2 indexes"),
])
def test_nof(n, singular, plural, out):
    assert nof(n, singular, plural) == out
