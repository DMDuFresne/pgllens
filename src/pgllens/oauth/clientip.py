"""Best-effort client IP extraction, for rate-limiting keys and audit trails.

A single `client_ip(request)` taking a Starlette-style request object
(`.headers.get(name)` / `.client.host`) and reading the trust decision from
`Settings.trust_proxy_headers`. There is no CF-Connecting-IP precedence step —
PgLLens assumes no particular tunnel/CDN. The core security property: X-Forwarded-For (spoofable by any client unless
a trusted proxy overwrites it) is honoured ONLY when the operator has opted
in via `trust_proxy_headers`; otherwise the TCP peer address is authoritative.

SECURITY: this reads the RIGHTMOST hop of X-Forwarded-For, not the leftmost.
The common reverse-proxy idiom (nginx's `$proxy_add_x_forwarded_for`, and
equivalents elsewhere) APPENDS the address it saw to any existing header
value rather than replacing it — so the rightmost entry is the one added by
the proxy sitting directly in front of this server, which a client cannot
forge. The leftmost entry is whatever the client itself claimed and is fully
attacker-controlled: trusting it would let a client mint a fresh rate-limit
identity on every request by sending an arbitrary `X-Forwarded-For` value.
This assumes exactly one trusted reverse proxy is directly adjacent to this
server appending its own hop last; only enable `trust_proxy_headers` behind
such a proxy. A chain of multiple untrusted hops in front of that proxy is
out of scope here (would need a configurable trusted-hop count).
"""

from __future__ import annotations

from starlette.requests import Request

from pgllens.config import get_settings


def client_ip(request: Request, trust_proxy_headers: bool | None = None) -> str:
    """Return the best client IP for `request`.

    When `trust_proxy_headers` is True, the RIGHTMOST hop of `X-Forwarded-For`
    is used when present — see the module docstring for why rightmost, not
    leftmost, is the only safe choice behind an append-style proxy.
    Otherwise, and always as a fallback, the TCP peer address
    (`request.client.host`) is used. Never raises; returns "unknown" if
    nothing usable is found.

    `trust_proxy_headers` defaults to `None`, which reads
    `Settings.trust_proxy_headers` via `get_settings()` -- the original
    behaviour, for callers with a configured global Settings. A caller that
    already has the decision in hand (e.g. middleware.py, built directly from
    `settings.trust_proxy_headers` in server.py) should pass it explicitly
    rather than round-tripping through `get_settings()`, so this is also
    usable with no configured Settings object at all (as in tests).
    """
    if trust_proxy_headers is None:
        try:
            trust_proxy_headers = get_settings().trust_proxy_headers
        except Exception:  # noqa: BLE001 -- settings load failure must not break IP extraction
            trust_proxy_headers = False

    try:
        if trust_proxy_headers:
            xff = request.headers.get("x-forwarded-for")
            if xff:
                last = xff.split(",")[-1].strip()
                if last:
                    return last

        client = request.client
        if client is not None and client.host:
            return client.host

        return "unknown"
    except Exception:  # noqa: BLE001 -- this helper must never raise into the request path
        return "unknown"
