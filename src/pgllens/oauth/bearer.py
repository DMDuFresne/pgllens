"""Bearer-token gate on the MCP endpoint (pure ASGI).

Uses PgLLens's TokenStore interface (no audience argument — see the note
below). A 401 carries a
WWW-Authenticate header with resource_metadata so RFC 9728 clients (Claude
Code, etc.) can discover the authorization server.
"""

from __future__ import annotations

import json
import logging

from starlette.types import ASGIApp, Receive, Scope, Send

from pgllens.oauth.okta import JwtVerifier, TokenError
from pgllens.oauth.store import RedisTokenStore, TokenStore
from pgllens.obs import metrics

logger = logging.getLogger("pgllens")


async def reject_401(
    send: Send,
    error: str,
    description: str,
    resource_metadata_url: str,
    *,
    with_error: bool = True,
) -> None:
    """RFC 6750 / RFC 9728 401 challenge. SECRETS NEVER LOGGED / RETURNED:
    never echo the submitted token in the body or the header."""
    challenge = f'Bearer resource_metadata="{resource_metadata_url}"'
    if with_error:
        challenge = (
            f'Bearer error="{error}", error_description="{description}", '
            f'resource_metadata="{resource_metadata_url}"'
        )
    body = json.dumps({"error": error, "error_description": description}).encode()
    await send({
        "type": "http.response.start",
        "status": 401,
        "headers": [
            (b"content-type", b"application/json"),
            (b"www-authenticate", challenge.encode("latin-1")),
        ],
    })
    await send({"type": "http.response.body", "body": body})


# SECURITY (audience binding): TokenStore.validate() does not check an
# `aud`/audience claim -- tokens issued by this server are accepted for any
# request to `protected_path` without verifying they were minted *for* this
# resource. That is safe ONLY as long as PgLLens is both the sole issuer
# (its own /oauth/token) and the sole resource server (its own /mcp) for
# every token it accepts here. If this middleware, or the token validation
# it calls, is ever changed to accept tokens issued by a third-party
# authorization server, audience binding MUST be added first -- otherwise a
# token minted for a different resource could be replayed against this one
# (a "confused deputy").
#
# Phase 2 note: third-party (Okta) tokens are NOT handled here. They go through
# OktaBearerMiddleware below, which binds `aud` via oauth/okta.py's JwtVerifier
# before any token from an issuer other than this server is accepted. The
# precondition above therefore still holds for this class.
class BearerAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        token_store: TokenStore | RedisTokenStore,
        protected_path: str,
        resource_metadata_url: str,
    ) -> None:
        self.app = app
        self.token_store = token_store
        self.protected_path = protected_path
        self.resource_metadata_url = resource_metadata_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") != self.protected_path:
            await self.app(scope, receive, send)
            return

        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        auth = headers.get(b"authorization", b"").decode("latin-1")

        # RFC 6750 §2.1: the "Bearer" auth-scheme is case-insensitive. Only the
        # scheme keyword is case-folded here -- the token itself is never
        # case-normalized, and an empty token after the scheme still fails.
        scheme, _, rest = auth.partition(" ")
        if scheme.lower() != "bearer" or not rest.strip():
            await self._reject(send, "invalid_request", "Bearer token required", with_error=False)
            return
        token = rest.strip()
        info = await self.token_store.validate(token)
        if info is None:
            metrics.record_auth_failure()
            await self._reject(send, "invalid_token", "Invalid or expired token")
            return

        scope["pgllens.client_id"] = info["client_id"]
        await self.app(scope, receive, send)

    async def _reject(
        self, send: Send, error: str, description: str, *, with_error: bool = True
    ) -> None:
        await reject_401(
            send, error, description, self.resource_metadata_url, with_error=with_error
        )


class OktaBearerMiddleware:
    """Bearer gate for `MCP_AUTH_MODE=okta`: PgLLens as a pure resource server.

    Stamps the verified identity on the ASGI scope for the scope gate, the rate
    limiter, and the audit trail: `pgllens.sub` (the human/service principal),
    `pgllens.client_id` (the OAuth client), `pgllens.scopes` (frozenset).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        verifier: JwtVerifier,
        protected_path: str,
        resource_metadata_url: str,
    ) -> None:
        self.app = app
        self.verifier = verifier
        self.protected_path = protected_path
        self.resource_metadata_url = resource_metadata_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") != self.protected_path:
            await self.app(scope, receive, send)
            return

        headers = {k.lower(): v for k, v in scope.get("headers", [])}
        auth = headers.get(b"authorization", b"").decode("latin-1")
        auth_scheme, _, rest = auth.partition(" ")
        if auth_scheme.lower() != "bearer" or not rest.strip():
            await reject_401(
                send, "invalid_request", "Bearer token required",
                self.resource_metadata_url, with_error=False,
            )
            return

        try:
            claims = await self.verifier.verify(rest.strip())
        except TokenError as e:
            logger.warning(
                "okta token rejected", extra={"event": "auth.rejected", "reason": str(e)}
            )
            metrics.record_auth_failure()
            await reject_401(
                send, "invalid_token", "Invalid or expired token",
                self.resource_metadata_url,
            )
            return

        scope["pgllens.sub"] = claims.sub
        scope["pgllens.client_id"] = claims.client_id
        scope["pgllens.scopes"] = claims.scopes
        await self.app(scope, receive, send)
