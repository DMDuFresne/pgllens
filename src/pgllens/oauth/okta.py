"""Okta resource-server mode: JWKS caching and JWT verification.

In this mode PgLLens is NOT an authorization server. It fetches Okta's JWKS,
verifies the signature and the `iss`/`aud`/`exp`/`nbf` claims of the token it
was handed, and maps the token's scopes onto tool groups. This module is the
audience binding that `oauth/bearer.py`'s Phase 1 comment said had to exist
before any third-party issuer was accepted.

PyJWT is used rather than its own PyJWKClient: PyJWKClient ignores the
Cache-Control header Okta returns and has no rate limit on refresh-by-unknown
-kid, so a flood of forged kid values would turn this server into a DoS
amplifier aimed at the client's Okta tenant.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx
import jwt
from jwt.algorithms import RSAAlgorithm

SCOPE_READ = "pgllens.read"
SCOPE_ADMIN = "pgllens.admin"

# Clock skew allowance on exp/nbf. 60s is the usual IdP recommendation; it is a
# constant rather than a setting because an operator raising it is weakening a
# security control by config, which is exactly what this phase is removing.
LEEWAY_S = 60

_DEFAULT_MAX_AGE_S = 300.0
_DEFAULT_MIN_REFRESH_INTERVAL_S = 60.0
_MAX_AGE_RE = re.compile(r"max-age\s*=\s*(\d+)", re.IGNORECASE)


class JwksError(RuntimeError):
    """The signing key for a token could not be obtained."""


def _max_age(cache_control: str | None) -> float:
    if not cache_control:
        return _DEFAULT_MAX_AGE_S
    match = _MAX_AGE_RE.search(cache_control)
    if not match:
        return _DEFAULT_MAX_AGE_S
    return float(match.group(1))


class JwksCache:
    """Okta's signing keys, cached for the `Cache-Control` max-age it returns.

    An unknown `kid` triggers at most one refetch per `min_refresh_interval_s`
    -- see the module docstring. A known key whose max-age has elapsed while
    refresh is rate-limited is still served: the signature it verifies is no
    less valid, and Okta overlaps keys across a rotation.
    """

    def __init__(
        self,
        url: str,
        *,
        client: httpx.AsyncClient | None = None,
        min_refresh_interval_s: float = _DEFAULT_MIN_REFRESH_INTERVAL_S,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._url = url
        self._client = client or httpx.AsyncClient(timeout=5.0)
        self._min_refresh = min_refresh_interval_s
        self._now = now
        self._keys: dict[str, Any] = {}
        self._expires_at = 0.0
        self._last_fetch = float("-inf")

    async def prime(self) -> None:
        """Fetch once at boot. Raises JwksError -- the caller must fail closed."""
        await self._fetch()

    async def key_for(self, kid: str) -> Any:
        now = self._now()
        if kid in self._keys and now < self._expires_at:
            return self._keys[kid]
        if now - self._last_fetch < self._min_refresh:
            key = self._keys.get(kid)
            if key is None:
                raise JwksError(f"unknown key id {kid!r}; JWKS refresh is rate-limited")
            return key
        # Stamped BEFORE the request so a hanging/failing endpoint still
        # consumes the refresh window -- otherwise a JWKS outage plus forged
        # kids is an unbounded request loop. Not stamped for prime()'s own
        # fetch: prime() runs once at boot and must not eat into the budget
        # for the very first post-boot refresh.
        self._last_fetch = now
        await self._fetch()
        key = self._keys.get(kid)
        if key is None:
            raise JwksError(f"unknown key id {kid!r}")
        return key

    async def _fetch(self) -> None:
        try:
            response = await self._client.get(self._url)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:  # any transport/parse failure is one error here
            raise JwksError(f"JWKS fetch from {self._url} failed: {e}") from e

        keys: dict[str, Any] = {}
        raw_keys = payload.get("keys") if isinstance(payload, dict) else None
        try:
            for jwk in raw_keys or []:
                kid = jwk.get("kid") if isinstance(jwk, dict) else None
                if not kid or jwk.get("kty") != "RSA":
                    continue
                # from_jwk takes a JSON string on every PyJWT 2.x; dict support is
                # newer. Dumping is the version-proof call.
                keys[str(kid)] = RSAAlgorithm.from_jwk(json.dumps(jwk))
        except Exception as e:
            raise JwksError(f"JWKS document at {self._url} contained an unparseable key: {e}") from e
        if not keys:
            raise JwksError(f"JWKS document at {self._url} contained no usable RSA keys")

        self._keys = keys
        self._expires_at = self._now() + _max_age(response.headers.get("cache-control"))


class TokenError(ValueError):
    """The presented token is not a valid PgLLens access token."""


@dataclass(frozen=True)
class Claims:
    """The only three things PgLLens takes from a verified token."""

    sub: str
    client_id: str
    scopes: frozenset[str]


class JwtVerifier:
    """Verify an Okta-minted access token for THIS resource.

    Every check is a keyword argument to a single jwt.decode call rather than a
    hand-rolled comparison: audience, issuer, expiry, not-before and signature
    are enforced together by the library, and `require` makes a token that
    simply omits a claim fail exactly like one that gets it wrong.
    """

    def __init__(
        self,
        jwks: JwksCache,
        *,
        issuer: str,
        audience: str,
        leeway_s: int = LEEWAY_S,
    ) -> None:
        self._jwks = jwks
        self._issuer = issuer
        self._audience = audience
        self._leeway = leeway_s

    async def verify(self, token: str) -> Claims:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as e:
            raise TokenError("malformed token header") from e

        # Pin the algorithm from the header BEFORE fetching a key: alg=none and
        # HS256-signed-with-the-public-key are both refused here, and
        # algorithms=["RS256"] below refuses them again at decode time.
        if header.get("alg") != "RS256":
            raise TokenError("unsupported token algorithm")
        kid = header.get("kid")
        if not kid:
            raise TokenError("token has no key id")

        try:
            key = await self._jwks.key_for(str(kid))
        except JwksError as e:
            raise TokenError(str(e)) from e

        try:
            payload: dict[str, Any] = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._leeway,
                options={"require": ["exp", "iss", "aud", "sub"]},
            )
        except jwt.PyJWTError as e:
            raise TokenError(f"token rejected: {e}") from e

        sub = str(payload["sub"])
        raw_scopes = payload.get("scp") or payload.get("scope") or []
        scopes = (
            frozenset(raw_scopes.split())
            if isinstance(raw_scopes, str)
            else frozenset(str(s) for s in raw_scopes)
        )
        client_id = str(payload.get("cid") or payload.get("client_id") or sub)
        return Claims(sub=sub, client_id=client_id, scopes=scopes)
