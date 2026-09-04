"""Human-readable value formatting. The only place these rules live."""

from __future__ import annotations

import math
from datetime import UTC, datetime

_ESTIMATE_UNITS = (("B", 1_000_000_000), ("M", 1_000_000), ("K", 1_000))
_SIZE_UNITS = ("B", "kB", "MB", "GB", "TB", "PB")


def estimate(n: int | float) -> str:  # noqa: PYI041 -- int|float is the public interface
    """`~9.2K` above 1,000; exact-looking below. Callers tag `(estimate)`."""
    n = float(n)
    for suffix, unit in _ESTIMATE_UNITS:
        if n >= unit:
            # round-half-up so 1,250,000 reads ~1.3M, not banker's-rounded ~1.2M
            scaled = math.floor((n / unit) * 10 + 0.5) / 10
            return f"~{scaled:.1f}{suffix}"
    return f"{int(n)}"


def count(n: int) -> str:
    """Exact count with thousands separators."""
    return f"{n:,}"


def nof(n: int, singular: str, plural: str | None = None) -> str:
    """`n` plus the plural-aware noun: `nof(1, "row") == "1 row"`,
    `nof(2, "row") == "2 rows"`. `plural` overrides the default `singular + "s"`
    for an irregular noun (`nof(1, "index", "indexes")`)."""
    return f"{count(n)} {singular if n == 1 else (plural or singular + 's')}"


def size(n_bytes: int) -> str:
    """Base-1024 with one decimal: `1.2 MB`. Bytes stay integral."""
    value = float(n_bytes)
    for unit in _SIZE_UNITS:
        if value < 1024 or unit == _SIZE_UNITS[-1]:
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def duration(seconds: float) -> str:
    """At most two components: `3d 4h`, `1h 1m`, `28m 2s`, `42s`."""
    total = int(seconds)
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = [(days, "d"), (hours, "h"), (minutes, "m"), (secs, "s")]
    shown = [f"{v}{u}" for v, u in parts if v]
    if not shown:
        return "0s"
    return " ".join(shown[:2])


def iso(dt: datetime) -> str:
    """ISO-8601 UTC to the second with a Z suffix. Naive datetimes are rejected."""
    if dt.tzinfo is None:
        raise ValueError("iso() needs an aware datetime")
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def ident(s: str) -> str:
    """Identifier in inline code, exactly as the API accepts it."""
    return f"`{s}`"
