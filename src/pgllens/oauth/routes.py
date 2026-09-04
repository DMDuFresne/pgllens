"""Starlette routes for the OAuth flow: discovery, DCR, authorize, token.

Mounted by server.build_app only when OAuth is enabled. Streamable-HTTP /mcp
is unchanged; these are sibling routes on the same Starlette app.

Built on the store interfaces (ClientStore/CodeStore/TokenStore/RateLimiter/
client_ip) with the three gates flagged in Task 2's adversarial review:

1. ClientStore is capped (see store.py) and /oauth/register is additionally
   rate-limited per client IP here, so unauthenticated DCR can't OOM the
   process or be hammered from one source.
2. GET /oauth/authorize embeds a single-use, expiring CSRF token; POST
   verifies it before checking the password, so a forged POST (no token, or
   a token this flow never issued) is refused even with the right password.
3. Audience binding is intentionally NOT implemented -- see the comment in
   bearer.py for why that's safe today and what would break it.
"""

from __future__ import annotations

import time
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.routing import Route

from pgllens.config import Settings
from pgllens.oauth.clientip import client_ip
from pgllens.oauth.crypto import constant_time_equals, new_token, verify_pkce
from pgllens.oauth.pages import login_page
from pgllens.oauth.ratelimit import RateLimiter
from pgllens.oauth.store import ClientStore, CodeStore, RedisTokenStore, TokenStore

_SCOPE = "mcp"
_MAX_CLIENTS = 100
_CSRF_TTL_S = 600.0
_MAX_CSRF_TOKENS = 1000


class _CsrfStore:
    """Single-use, expiring CSRF tokens for the authorize form, bound to the
    (client_id, redirect_uri) they were issued for.

    Plain in-memory dict, single process, matching every other store in this
    package (see the note above ClientStore in store.py). Swept opportunistically
    on issue so abandoned tokens don't accumulate forever, AND capped at
    `max_tokens` (Fix round 1, gate 1 regression): unlike ClientStore, a
    dropped CSRF token only forces one visitor to reload GET /oauth/authorize
    (nobody's registered client is destroyed), so drop-oldest is the right
    call here (vs. ClientStore's refuse-when-full, where eviction would kick
    a real client out from under an in-flight user).
    """

    def __init__(self, ttl_seconds: float = _CSRF_TTL_S, max_tokens: int = _MAX_CSRF_TOKENS) -> None:
        self._ttl = ttl_seconds
        self._max_tokens = max_tokens
        self._tokens: dict[str, tuple[float, str, str]] = {}

    def issue(self, client_id: str, redirect_uri: str) -> str:
        self._sweep()
        while len(self._tokens) >= self._max_tokens:
            # dict preserves insertion order -- drop the oldest live entry.
            self._tokens.pop(next(iter(self._tokens)))
        token = new_token()
        self._tokens[token] = (time.monotonic() + self._ttl, client_id, redirect_uri)
        return token

    def consume(self, token: str, client_id: str, redirect_uri: str) -> bool:
        """True iff `token` was live, unused, and issued for this exact
        (client_id, redirect_uri) pair. Single-use: popped either way."""
        entry = self._tokens.pop(token, None)
        if entry is None:
            return False
        expires_at, bound_client_id, bound_redirect_uri = entry
        if expires_at <= time.monotonic():
            return False
        return bound_client_id == client_id and bound_redirect_uri == redirect_uri

    def _sweep(self) -> None:
        now = time.monotonic()
        for key in [k for k, v in self._tokens.items() if v[0] <= now]:
            del self._tokens[key]


class OAuthState:
    """Owns every piece of in-memory OAuth state for one running server."""

    def __init__(
        self, settings: Settings, token_store: TokenStore | RedisTokenStore | None = None
    ) -> None:
        self.clients = ClientStore(max_clients=_MAX_CLIENTS)
        self.codes = CodeStore()
        # Injectable so server.build_app can hand in the SAME store instance
        # BearerAuthMiddleware validates against (a RedisTokenStore when
        # REDIS_URL is configured). Defaulting to a fresh in-memory TokenStore
        # keeps every other caller (tests, oauth_routes()) unchanged.
        self.tokens = token_store if token_store is not None else TokenStore(
            ttl_seconds=settings.mcp_oauth_token_expires_in
        )
        self.csrf = _CsrfStore()
        # Separate limiters: a burst of register attempts must not lock out
        # someone entering their password, and vice versa. `authorize_limiter`
        # bounds GET /oauth/authorize (Fix round 1) so that endpoint can't be
        # hammered to grow the CSRF store even with the cap above in place.
        self.register_limiter = RateLimiter(
            settings.mcp_rate_limit_attempts, settings.mcp_rate_limit_window_ms
        )
        self.login_limiter = RateLimiter(
            settings.mcp_rate_limit_attempts, settings.mcp_rate_limit_window_ms
        )
        self.authorize_limiter = RateLimiter(
            settings.mcp_rate_limit_attempts, settings.mcp_rate_limit_window_ms
        )
        # Audit M2 (merged low): /oauth/token is unauthenticated and does
        # real work (form parse, code lookup, PKCE) -- same per-IP throttle
        # as register/authorize, separate limiter for the same reason.
        self.token_limiter = RateLimiter(
            settings.mcp_rate_limit_attempts, settings.mcp_rate_limit_window_ms
        )


def _err(error: str, description: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"error": error, "error_description": description}, status_code=status)


def _validate_client(
    state: OAuthState, client_id: str | None, redirect_uri: str | None
) -> tuple[dict[str, object], None] | tuple[None, JSONResponse]:
    if not client_id:
        return None, _err("invalid_request", "client_id is required.")
    client = state.clients.get(client_id)
    if client is None:
        return None, _err("invalid_client", "Unknown client_id. Register first.")
    if not redirect_uri:
        return None, _err("invalid_request", "redirect_uri is required.")
    registered_uris = client.get("redirect_uris", [])
    if not isinstance(registered_uris, list) or redirect_uri not in registered_uris:
        return None, _err("invalid_request", "redirect_uri does not match a registered URI.")
    return client, None


def build_oauth(
    settings: Settings,
    base_url: str | None = None,
    token_store: TokenStore | RedisTokenStore | None = None,
) -> tuple[list[Route], OAuthState]:
    state = OAuthState(settings, token_store=token_store)
    base = (base_url or settings.external_base_url).rstrip("/")

    async def protected_resource_metadata(_request: Request) -> JSONResponse:
        return JSONResponse({
            "resource": base,
            "authorization_servers": [base],
            "bearer_methods_supported": ["header"],
            "scopes_supported": [_SCOPE],
        })

    async def authorization_server_metadata(_request: Request) -> JSONResponse:
        return JSONResponse({
            "issuer": base,
            "authorization_endpoint": f"{base}/oauth/authorize",
            "token_endpoint": f"{base}/oauth/token",
            "registration_endpoint": f"{base}/oauth/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": [_SCOPE],
        })

    async def register(request: Request) -> JSONResponse:
        ip = client_ip(request)
        # [Gate 1] per-IP throttle on the unauthenticated register endpoint.
        if not state.register_limiter.check(ip):
            return _err(
                "too_many_requests", "Too many registration attempts. Try again later.", 429
            )
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001 -- malformed body must 400, never raise
            body = {}
        redirect_uris = body.get("redirect_uris") if isinstance(body, dict) else None
        if not isinstance(redirect_uris, list) or not redirect_uris:
            state.register_limiter.record_failure(ip)
            return _err("invalid_client_metadata", "redirect_uris is required.")

        client = state.clients.register({
            "client_name": body.get("client_name"),
            "redirect_uris": [str(u) for u in redirect_uris],
        })
        if client is None:
            # [Gate 1] cap reached -- refuse rather than evict a client that may
            # still be in active use. Count this against the per-IP throttle so
            # an attacker spinning /register against a full cap still trips it --
            # but a *successful* registration does NOT count (Fix round 1):
            # ClientStore is in-memory, so a couple of restarts inside the
            # window would otherwise lock out real, legitimate re-registrations.
            state.register_limiter.record_failure(ip)
            return _err(
                "temporarily_unavailable", "Registration capacity reached; retry later.", 503
            )
        return JSONResponse({
            "client_id": client["client_id"],
            "client_id_issued_at": int(time.time()),
            "redirect_uris": client["redirect_uris"],
            "client_name": client.get("client_name"),
            "token_endpoint_auth_method": "none",
        }, status_code=201)

    async def authorize_get(request: Request) -> HTMLResponse | JSONResponse:
        # [Fix round 1] per-IP throttle: GET /oauth/authorize is what mints
        # CSRF tokens, so without this an attacker could grind the cap in
        # _CsrfStore regardless of its size.
        ip = client_ip(request)
        if not state.authorize_limiter.check(ip):
            return _err("too_many_requests", "Too many requests. Try again later.", 429)
        state.authorize_limiter.record_failure(ip)

        q = request.query_params
        client, err = _validate_client(state, q.get("client_id"), q.get("redirect_uri"))
        if err is not None:
            return err
        assert client is not None  # guaranteed by _validate_client's (dict, None) | (None, err) shape
        response_type = q.get("response_type")
        if response_type != "code":
            return _err(
                "unsupported_response_type", "Only response_type=code is supported."
            )
        code_challenge = q.get("code_challenge", "")
        code_challenge_method = (q.get("code_challenge_method") or "S256").upper()
        if not code_challenge:
            return _err("invalid_request", "code_challenge is required (S256).")
        if code_challenge_method != "S256":
            return _err("invalid_request", "code_challenge_method must be S256.")
        redirect_uri = q.get("redirect_uri", "")
        client_id = str(client["client_id"])
        csrf_token = state.csrf.issue(client_id, redirect_uri)
        page = login_page(
            client_id=client_id,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            state=q.get("state", ""),
            csrf_token=csrf_token,
        )
        return HTMLResponse(page)

    async def authorize_post(request: Request) -> HTMLResponse | JSONResponse | RedirectResponse:
        form = await request.form()
        client_id = str(form.get("client_id") or "")
        redirect_uri = str(form.get("redirect_uri") or "")
        req_state = str(form.get("state") or "")
        code_challenge = str(form.get("code_challenge") or "")
        code_challenge_method = (str(form.get("code_challenge_method") or "") or "S256").upper()
        csrf_token = str(form.get("csrf_token") or "")
        provided_password = str(form.get("password") or "")

        _client, err = _validate_client(state, client_id, redirect_uri)
        if err is not None:
            return err
        if not code_challenge:
            return _err("invalid_request", "code_challenge is required (S256).")
        if code_challenge_method != "S256":
            return _err("invalid_request", "code_challenge_method must be S256.")

        # [Gate 2] CSRF: a POST without a valid, single-use token issued by
        # GET /oauth/authorize for this exact (client_id, redirect_uri) is
        # refused before the password is even checked.
        if not csrf_token or not state.csrf.consume(csrf_token, client_id, redirect_uri):
            return _err("invalid_request", "Missing or invalid CSRF token.")

        ip = client_ip(request)
        if not state.login_limiter.check(ip):
            return HTMLResponse(
                login_page(
                    client_id=client_id, redirect_uri=redirect_uri, state=req_state,
                    code_challenge=code_challenge, code_challenge_method=code_challenge_method,
                    csrf_token=state.csrf.issue(client_id, redirect_uri),
                    error="Too many attempts. Try again later.",
                ),
                status_code=429,
            )

        configured_password = settings.mcp_auth_password
        password_value = configured_password.get_secret_value() if configured_password else None
        # Timing-safe comparison; a missing configured password never matches.
        if not password_value or not constant_time_equals(provided_password, password_value):
            state.login_limiter.record_failure(ip)
            return HTMLResponse(
                login_page(
                    client_id=client_id, redirect_uri=redirect_uri, state=req_state,
                    code_challenge=code_challenge, code_challenge_method=code_challenge_method,
                    csrf_token=state.csrf.issue(client_id, redirect_uri),
                    error="Invalid password",
                ),
                status_code=401,
            )

        state.login_limiter.reset(ip)
        code = state.codes.issue(
            client_id, redirect_uri, code_challenge, code_challenge_method, _SCOPE
        )
        query = {"code": code}
        if req_state:
            query["state"] = req_state
        sep = "&" if "?" in redirect_uri else "?"
        return RedirectResponse(f"{redirect_uri}{sep}{urlencode(query)}", status_code=302)

    async def token(request: Request) -> JSONResponse:
        no_store = {"Cache-Control": "no-store", "Pragma": "no-cache"}

        def token_err(error: str, description: str, status: int = 400) -> JSONResponse:
            return JSONResponse(
                {"error": error, "error_description": description},
                status_code=status,
                headers=no_store,
            )

        ip = client_ip(request)
        if not state.token_limiter.check(ip):
            return token_err("too_many_requests",
                             "Too many token requests. Try again later.", 429)

        form = await request.form()
        grant_type = str(form.get("grant_type") or "")
        if grant_type != "authorization_code":
            state.token_limiter.record_failure(ip)
            return token_err("unsupported_grant_type", "Only authorization_code is supported.")

        code = str(form.get("code") or "")
        client_id = str(form.get("client_id") or "")
        redirect_uri = str(form.get("redirect_uri") or "")
        code_verifier = str(form.get("code_verifier") or "")

        auth_code = state.codes.consume(code)  # single-use pop + TTL enforced on read
        if auth_code is None:
            state.token_limiter.record_failure(ip)
            return token_err("invalid_grant", "Invalid or expired authorization code.")
        if client_id != auth_code["client_id"]:
            state.token_limiter.record_failure(ip)
            return token_err("invalid_grant", "client_id does not match the code.")
        if redirect_uri != auth_code["redirect_uri"]:
            state.token_limiter.record_failure(ip)
            return token_err("invalid_grant", "redirect_uri does not match /authorize.")
        if not verify_pkce(
            code_verifier, auth_code["code_challenge"], auth_code["code_challenge_method"]
        ):
            state.token_limiter.record_failure(ip)
            return token_err("invalid_grant", "PKCE verification failed.")

        access_token, expires_in = await state.tokens.issue(
            auth_code["client_id"], auth_code["scope"]
        )
        state.token_limiter.reset(ip)
        return JSONResponse({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": expires_in,
            "scope": auth_code["scope"],
        }, headers=no_store)

    routes = [
        Route(
            "/.well-known/oauth-protected-resource",
            protected_resource_metadata,
            methods=["GET"],
        ),
        Route(
            "/.well-known/oauth-authorization-server",
            authorization_server_metadata,
            methods=["GET"],
        ),
        Route("/oauth/register", register, methods=["POST"]),
        Route("/oauth/authorize", authorize_get, methods=["GET"]),
        Route("/oauth/authorize", authorize_post, methods=["POST"]),
        Route("/oauth/token", token, methods=["POST"]),
    ]
    return routes, state


def oauth_routes(settings: Settings) -> list[Route]:
    """Public interface: routes only, for callers that don't need the state
    object. server.build_app uses build_oauth directly so it can also share
    the TokenStore with BearerAuthMiddleware."""
    routes, _state = build_oauth(settings)
    return routes
