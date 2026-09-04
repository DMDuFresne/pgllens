"""Pure crypto helpers for the OAuth flow — no I/O, no global state.

PKCE supports S256 (RFC 7636) and `plain`; anything else is rejected.
Comparisons are constant-time (`hmac.compare_digest`).

There is no redirect-URI allowlist helper here and no strict "challenge is
mandatory" variant — PKCE mandatoriness is decided where this is wired up.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets


def new_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_pkce(verifier: str, challenge: str, method: str) -> bool:
    """True iff `verifier` matches `challenge` under `method` ("S256" or "plain").

    Empty verifier or challenge never verifies, even under `plain`. A verifier
    with non-ASCII characters is invalid per RFC 7636 (unreserved chars only)
    and returns False rather than raising.
    """
    if not verifier or not challenge:
        return False
    if method == "S256":
        try:
            digest = hashlib.sha256(verifier.encode("ascii")).digest()
        except UnicodeEncodeError:
            return False
        computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return constant_time_equals(computed, challenge)
    if method == "plain":
        return constant_time_equals(verifier, challenge)
    return False
