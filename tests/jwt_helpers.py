"""Locally generated RSA key material for Okta-mode tests.

There is no Okta tenant in this project's test environment (and there must
never be a network call from the unit suite), so every Okta test mints its own
tokens with the same library that verifies them, against a JWKS document this
module builds by hand in Okta's exact shape.
"""

from __future__ import annotations

import base64
import time
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

KID = "test-key-1"
ISSUER = "https://test.okta.com/oauth2/aus1"
AUDIENCE = "api://pgllens"


def _b64u(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def rsa_keypair(kid: str = KID) -> tuple[Any, dict[str, object]]:
    """Return (private_key, jwks_document) -- the document in Okta's shape."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private_key.public_key().public_numbers()
    jwk = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
        "n": _b64u(numbers.n),
        "e": _b64u(numbers.e),
    }
    return private_key, {"keys": [jwk]}


def mint(private_key: Any, kid: str = KID, **claims: Any) -> str:
    """Mint an RS256 JWT. Defaults are a valid pgllens.read token; pass a claim
    explicitly to override it, or pass None to omit it entirely."""
    now = int(time.time())
    payload: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "00u1testuser",
        "cid": "0oa1testclient",
        "scp": ["pgllens.read"],
        "iat": now,
        "nbf": now - 5,
        "exp": now + 300,
    }
    payload.update(claims)
    payload = {k: v for k, v in payload.items() if v is not None}
    return jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": kid})
