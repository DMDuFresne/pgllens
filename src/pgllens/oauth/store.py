"""In-memory OAuth state: registered clients, single-use authorization codes,
and bearer tokens.

Three small classes (`ClientStore`, `CodeStore`, `TokenStore`) instead of
one god-object. Provides: single-use codes, expiring
codes/tokens, constant-time-safe token issuance via `secrets.token_urlsafe`,
and at-rest token hashing (bearer tokens are keyed by SHA-256 in `TokenStore`,
never stored raw — a leaked state dump yields no usable tokens).

KNOWN OPEN ITEM (explicitly deferred to Task 3, not fixed here): `ClientStore`
has no cap and no pruning. Once `/oauth/register` is wired up unauthenticated,
this is an OOM vector — Task 3 must add a registration cap and a
`RateLimiter`-backed rate limit on that endpoint before it is reachable.

Deliberately not implemented:
- Disk persistence (atomic file write, 0o600 permissions, load-on-init) —
  PgLLens has no requirement yet for tokens/clients to survive a restart;
  Task 3's brief treats this as pure library code with no wiring. Revisit if
  a real deployment needs restart-durable DCR clients.
- Consent CSRF tokens for a browser-based consent-screen flow — PgLLens's
  OAuth interface (this brief) has no consent-screen step.
- Client idle-pruning / activity tracking / max_clients cap — operational
  concerns (bounding DCR churn) not asked for here.
- Audience binding on tokens (`validate_token(token, expected_audience)`) —
  the brief's `TokenStore.validate(token)` interface takes no audience
  argument, so there is nothing to bind against yet. Add if/when Task 3
  needs anti-passthrough enforcement.
- Namespaced per-purpose rate limiting (register/token prefixes) — that's
  `RateLimiter`'s job now (oauth/ratelimit.py), used per-purpose by the
  caller via distinct keys, not baked into the store.

`time.monotonic` is imported via the module (not `from time import
monotonic`) so tests can monkeypatch `pgllens.oauth.store.time.monotonic`,
and TTLs are constructor args (`ttl_seconds`) rather than hardcoded.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from pgllens.oauth.crypto import new_token

logger = logging.getLogger("pgllens")


def _token_key(token: str) -> str:
    """At-rest key for a bearer token: SHA-256 hex. The raw token is never
    stored, so a leaked state dump or heap/traceback yields no usable tokens."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

# Deliberately simple: ClientStore/CodeStore stay in-process on purpose. A DCR
# client and a 60-second authorization code are both password-mode-only and
# short-lived, so neither needs to survive a restart or coordinate across
# replicas. TokenStore, which does need that, has an async interface and a
# Redis-backed sibling (RedisTokenStore, below) via limits.py's seam.

_DEFAULT_CODE_TTL_S = 60.0
_DEFAULT_TOKEN_TTL_S = 604800.0  # 7 days, matches Settings.mcp_oauth_token_expires_in


@dataclass
class _Client:
    client_id: str
    client_secret: str
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass
class _Code:
    client_id: str
    redirect_uri: str
    code_challenge: str
    code_challenge_method: str
    scope: str
    expires_at: float


@dataclass
class _Token:
    client_id: str
    scope: str
    expires_at: float


_DEFAULT_MAX_CLIENTS = 100


class ClientStore:
    """Registered OAuth clients (dynamic client registration).

    RFC 7591 registration is unauthenticated by design, so without a cap this
    is a plain OOM vector (an attacker loops POST /oauth/register until the
    process dies). `max_clients` bounds that: once full, `register` refuses
    new clients (returns None) rather than evicting a client that might still
    be in active use. Callers (routes.py) should also rate-limit the register
    endpoint per-IP so a single attacker can't even reach the cap quickly.
    """

    # Allowlist, not a denylist of reserved keys: caller-supplied metadata can
    # ONLY ever populate these fields. This is the version that stays safe if
    # RFC 7591 grows more registration fields later — a denylist has to be
    # remembered and updated every time a new server-generated field appears;
    # an allowlist fails closed by default. In particular client_id and
    # client_secret can never be set by the caller — only server-generated
    # values are ever stored/returned for those.
    _ALLOWED_METADATA_FIELDS = frozenset({"client_name", "redirect_uris"})

    def __init__(self, max_clients: int = _DEFAULT_MAX_CLIENTS) -> None:
        self._clients: dict[str, _Client] = {}
        self._max_clients = max_clients

    def register(self, metadata: dict[str, object]) -> dict[str, object] | None:
        """Register a client, or return None if the cap is reached."""
        if len(self._clients) >= self._max_clients:
            return None
        client_id = new_token(16)
        client_secret = new_token(32)
        safe_metadata = {
            k: v for k, v in metadata.items() if k in self._ALLOWED_METADATA_FIELDS
        }
        self._clients[client_id] = _Client(
            client_id=client_id, client_secret=client_secret, metadata=safe_metadata
        )
        return {"client_id": client_id, "client_secret": client_secret, **safe_metadata}

    def get(self, client_id: str) -> dict[str, object] | None:
        client = self._clients.get(client_id)
        if client is None:
            return None
        return {
            "client_id": client.client_id,
            "client_secret": client.client_secret,
            **client.metadata,
        }


class CodeStore:
    """Single-use, short-lived authorization codes."""

    def __init__(self, ttl_seconds: float = _DEFAULT_CODE_TTL_S) -> None:
        self._ttl = ttl_seconds
        self._codes: dict[str, _Code] = {}

    def issue(
        self,
        client_id: str,
        redirect_uri: str,
        code_challenge: str,
        code_challenge_method: str,
        scope: str,
    ) -> str:
        self._sweep()
        code = new_token()
        self._codes[code] = _Code(
            client_id=client_id,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            scope=scope,
            expires_at=time.monotonic() + self._ttl,
        )
        return code

    def consume(self, code: str) -> dict[str, str] | None:
        """Pop and return the code's data, or None if unknown/expired.

        Single-use: popped regardless of outcome, so a replay (even from a
        different client than originally issued to) can never succeed twice.
        """
        entry = self._codes.pop(code, None)
        if entry is None or entry.expires_at <= time.monotonic():
            return None
        return {
            "client_id": entry.client_id,
            "redirect_uri": entry.redirect_uri,
            "code_challenge": entry.code_challenge,
            "code_challenge_method": entry.code_challenge_method,
            "scope": entry.scope,
        }

    def _sweep(self) -> None:
        """Reclaim codes past their TTL that were never consumed.

        Read-time expiry (in `consume`) already prevents an expired code from
        being usable; this only bounds memory for abandoned codes nobody ever
        comes back for. O(n) over live codes, called opportunistically on issue.
        """
        now = time.monotonic()
        expired = [k for k, v in self._codes.items() if v.expires_at <= now]
        for k in expired:
            del self._codes[k]


class TokenStore:
    """Bearer tokens, expiring, revocable."""

    def __init__(self, ttl_seconds: float = _DEFAULT_TOKEN_TTL_S) -> None:
        self._ttl = ttl_seconds
        self._tokens: dict[str, _Token] = {}

    async def issue(self, client_id: str, scope: str) -> tuple[str, int]:
        self._sweep()
        token = new_token()
        self._tokens[_token_key(token)] = _Token(
            client_id=client_id,
            scope=scope,
            expires_at=time.monotonic() + self._ttl,
        )
        return token, int(self._ttl)  # raw token to the caller; only its hash is retained

    async def validate(self, token: str) -> dict[str, str] | None:
        key = _token_key(token)
        entry = self._tokens.get(key)
        if entry is None:
            return None
        if entry.expires_at <= time.monotonic():
            self._tokens.pop(key, None)
            return None
        return {"client_id": entry.client_id, "scope": entry.scope}

    async def revoke(self, token: str) -> None:
        self._tokens.pop(_token_key(token), None)

    def _sweep(self) -> None:
        """Reclaim tokens past their TTL that were never validated/revoked."""
        now = time.monotonic()
        expired = [k for k, v in self._tokens.items() if v.expires_at <= now]
        for k in expired:
            del self._tokens[k]


class RedisTokenStore:
    """Bearer tokens in Redis: shared across replicas, durable across restarts.

    Same at-rest property as TokenStore: the key is the SHA-256 of the token, so
    a dump of the keyspace yields nothing usable. Expiry is Redis's own TTL, so
    there is no sweep and no way to leak a token past its lifetime.
    """

    def __init__(self, client: Any, *, ttl_seconds: float, prefix: str = "pgllens") -> None:
        self._client = client
        self._ttl = ttl_seconds
        self._prefix = prefix

    def _key(self, token: str) -> str:
        return f"{self._prefix}:token:{_token_key(token)}"

    async def issue(self, client_id: str, scope: str) -> tuple[str, int]:
        token = new_token()
        await self._client.set(
            self._key(token),
            json.dumps({"client_id": client_id, "scope": scope}),
            ex=int(self._ttl),
        )
        return token, int(self._ttl)

    async def validate(self, token: str) -> dict[str, str] | None:
        # Fail CLOSED: unlike the rate limiter (see limits.py's RedisLimitStore),
        # a Redis error here must never be treated as "valid" -- the controller
        # ruling draws the line here on purpose (JWKS-style fail-closed, not the
        # rate limiter's fail-open). Any backend error, or a stored record that
        # doesn't parse into the expected shape, is simply an invalid token.
        try:
            raw = await self._client.get(self._key(token))
        except Exception as exc:  # noqa: BLE001 -- any backend error fails closed
            logger.warning("RedisTokenStore.validate backend error, denying: %s", exc)
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            return {"client_id": str(data["client_id"]), "scope": str(data["scope"])}
        except (ValueError, KeyError, TypeError):
            logger.warning("RedisTokenStore.validate: malformed record, denying")
            return None

    async def revoke(self, token: str) -> None:
        await self._client.delete(self._key(token))
