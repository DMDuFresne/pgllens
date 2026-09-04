"""MCP server assembly: tools, /health, instructions."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from urllib.parse import urlparse

from mcp.server.apps import Apps
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from pgllens import __version__
from pgllens.config import Settings, get_settings
from pgllens.database.capability import Capabilities
from pgllens.database.introspect import Introspector
from pgllens.database.pool import Db
from pgllens.instructions import build_instructions
from pgllens.limits import RedisLimitStore, build_limit_store, configure_limits
from pgllens.middleware import (
    BodySizeLimitMiddleware,
    CallerContextMiddleware,
    ConcurrencyLimitMiddleware,
    InboundToolRateLimitMiddleware,
    ScopeEnforcementMiddleware,
)
from pgllens.oauth.bearer import BearerAuthMiddleware, OktaBearerMiddleware
from pgllens.oauth.okta import SCOPE_ADMIN, SCOPE_READ, JwksCache, JwtVerifier
from pgllens.oauth.routes import build_oauth
from pgllens.oauth.store import RedisTokenStore, TokenStore
from pgllens.obs import audit, metrics, telemetry
from pgllens.obs.correlation import CorrelationMiddleware
from pgllens.tools import erd, register_all
from pgllens.tools._util import registered_tool_names

SERVER_NAME = "pgllens"
logger = logging.getLogger("pgllens")

_DEFAULT_EXTERNAL_BASE_URL = Settings.model_fields["external_base_url"].default


def _effective_external_base_url(settings: Settings) -> str:
    """external_base_url, corrected for the actual port when the operator
    never set it explicitly: the field's baked-in default names port 3000
    regardless of what the server actually binds to."""
    if settings.external_base_url != _DEFAULT_EXTERNAL_BASE_URL:
        return settings.external_base_url
    if settings.mcp_port == 3000:
        logger.warning(
            "EXTERNAL_BASE_URL not set; OAuth/RFC 9728 discovery will advertise "
            "%s. Set EXTERNAL_BASE_URL explicitly for a public/HTTPS deployment.",
            settings.external_base_url,
        )
        return settings.external_base_url
    host = "localhost" if settings.host in ("0.0.0.0", "::") else settings.host
    derived = f"http://{host}:{settings.mcp_port}"
    logger.warning(
        "EXTERNAL_BASE_URL not set; deriving %s from host/mcp_port for OAuth discovery. "
        "Set EXTERNAL_BASE_URL explicitly for a public/HTTPS deployment.",
        derived,
    )
    return derived


def _transport_security(settings: Settings, base: str) -> TransportSecuritySettings:
    """Host/Origin allowlist for mcp.streamable_http_app()'s DNS-rebinding
    guard. Omitting `transport_security=` entirely makes the SDK auto-enable
    it with a localhost-only allowlist (see ISSUE-mcp-transport-security-host.md)
    -- every real deployment's Host header then 421s before reaching any tool.
    Protection itself must always stay on; only the allowlist is widened, by
    deriving one extra host (EXTERNAL_BASE_URL's) beyond loopback. No new
    setting: EXTERNAL_BASE_URL is already required in okta mode and is by
    definition the public hostname clients use.

    `base` is the caller's already-computed `_effective_external_base_url(settings)`
    (build_app needs that value too, for OAuth discovery -- computed once there
    so its warning-on-unset log line doesn't fire twice per build_app call).

    The extra-host branch below only fires when the operator explicitly set
    EXTERNAL_BASE_URL. `_effective_external_base_url`'s fallback path derives
    a URL from `settings.host`/`mcp_port` when it's unset -- on a non-loopback
    bind (e.g. a LAN NIC address) that would silently widen the allowlist to
    whatever HOST happens to be, with no operator opt-in. Gating on the
    explicit-set check keeps that fallback's derived value OAuth-discovery-only,
    never security-allowlist input.
    """
    # Both bare (`Host: example.com`, what a :443 proxy sends) and `:*` forms
    # are needed -- `_validate_host` does exact match or `prefix:` for `:*`
    # patterns, so neither form alone covers both shapes. This also fixes the
    # SDK's own default, which only ships the `:*` forms and so 421s a bare
    # `Host: 127.0.0.1`/`Host: localhost`/`Host: [::1]` too.
    hosts = [
        "127.0.0.1", "127.0.0.1:*",
        "localhost", "localhost:*",
        "[::1]", "[::1]:*",
    ]
    origins = ["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"]
    if settings.external_base_url != _DEFAULT_EXTERNAL_BASE_URL:
        parsed = urlparse(base)
        if parsed.hostname:
            hosts += [parsed.netloc, f"{parsed.hostname}:*"]
            origins.append(f"{parsed.scheme}://{parsed.netloc}")
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=origins,
    )


def create_mcp(
    settings: Settings, db: Db | None, intro: Introspector | None = None,
    caps: Capabilities | None = None,
) -> MCPServer:
    # db=None is a parity-test-only affordance (create_mcp(settings, db=None) in
    # tests/test_tool_annotations.py etc., where nothing queries at registration
    # time) -- Db is cast back to its real type below for register_all/register_apps,
    # which are typed for the always-real production caller (build_app).
    real_db = cast(Db, db)
    # intro=None/caps=None (tests, or a caller with no schema-cache/capability
    # needs of its own) construct fresh instances here, matching the original
    # design intent -- register_all/register_apps always get real objects,
    # never None.
    intro = intro or Introspector(real_db, settings)
    caps = caps or Capabilities(real_db)
    # The Apps extension (io.modelcontextprotocol/ui, SEP-1865 -- see
    # https://modelcontextprotocol.io/specification/draft/extensions/apps) is what
    # actually ADVERTISES ui support to the client (ServerCapabilities.extensions);
    # a hand-stamped _meta.ui.resourceUri without this never gets negotiated, so a
    # capable host has no signal to ever render the widget at all. Its tool/resource
    # bindings are consumed once, synchronously, inside MCPServer.__init__ below --
    # they MUST be registered on `apps` before that call, which is why
    # get_erd_widget is registered here rather than through the uniform
    # register_all() pass (which only runs after mcp exists, and which does
    # register its plain-text sibling get_erd).
    apps = Apps()
    erd.register_apps(apps, real_db, settings, intro)
    mcp = MCPServer(
        SERVER_NAME,
        instructions=build_instructions(settings),
        version=__version__,
        extensions=[apps],
    )
    register_all(mcp, real_db, settings, intro, caps)
    # Plain resource template (see erd.py's module docstring): must come after MCPServer exists, unlike register_apps above.
    erd.register_erd_resource_template(mcp)
    return mcp


def build_app(settings: Settings | None = None, oauth: bool = False,
              db: Db | None = None) -> Starlette:
    settings = settings or get_settings()
    # Audit M1 (owner-modified): serving /mcp with no auth on a non-loopback
    # bind is a legitimate LAN deployment mode, but never a silent one.
    if settings.mcp_auth_mode == "none" and settings.host not in ("127.0.0.1", "::1", "localhost"):
        logger.warning(
            "PgLLens is serving /mcp UNAUTHENTICATED on %s:%s -- anyone who can "
            "reach this port can query the exposed schemas. Set MCP_AUTH_MODE "
            "(password/okta) or bind HOST=127.0.0.1 behind a proxy.",
            settings.host, settings.mcp_port,
        )
    # configure_audit() was previously only ever called from tests -- AUDIT_LOG_FILE
    # is otherwise entirely inert in the shipped app. Wired up here, not in
    # __main__, so it's covered by build_app() in tests too.
    audit.configure_audit(settings)
    # Computed once and reused below (transport security + both auth
    # branches) -- _effective_external_base_url() logs a WARNING when
    # EXTERNAL_BASE_URL is unset, and that log line must fire once per
    # build_app() call, not once per caller.
    base = _effective_external_base_url(settings).rstrip("/")
    # db= exists for tests: a fake Db with a stubbed ping() lets the /health
    # unit tests cover both branches without a live database.
    db = db or Db(settings)
    intro = Introspector(db, settings)
    caps = Capabilities(db)
    mcp = create_mcp(settings, db, intro, caps)
    # Hoisted above the auth branch below (which is the only place that ever
    # assigns it) so the `lifespan` closure defined next can close over the
    # name -- Python resolves closure names at call time, not definition
    # time, so this only needs to exist before `lifespan` RUNS, not before it
    # is defined. Per controller ruling C-1: the auth branch itself and every
    # add_middleware call stay exactly where Phase 1 left them.
    _jwks_to_prime: JwksCache | None = None

    # Audit L4: cache the ping so an unauthenticated /health flood costs one
    # pool acquire per interval, and drop the version (fingerprinting fodder;
    # operators get it from `docker image ls` / the MCP initialize handshake).
    _health_cache: dict[str, object] = {"at": float("-inf"), "ok": False}
    _HEALTH_TTL_S = 2.0

    async def health(_: Request) -> JSONResponse:
        # A DB-less "healthy" is worse than no health check at all -- every
        # deploy script trusts this endpoint, and a broken pool (bad DSN,
        # network partition, ...) leaves every MCP tool call failing while
        # /health still says everything is fine. ping() is a cheap, short-
        # timeout SELECT 1 so this stays a fast check, not a full query.
        now = time.monotonic()
        if now - cast(float, _health_cache["at"]) >= _HEALTH_TTL_S:
            _health_cache["ok"] = await db.ping()
            _health_cache["at"] = now
        if _health_cache["ok"]:
            return JSONResponse({"status": "healthy", "server": SERVER_NAME})
        return JSONResponse({"status": "unhealthy", "database": "unreachable",
                             "server": SERVER_NAME}, status_code=503)

    # streamable_http_app() already serves the MCP endpoint at the given path
    # AND wires its lifespan to run the session manager, so we just add
    # /health onto it rather than Mount()-ing it (which would nest the path
    # to /mcp/mcp) or re-running the session manager ourselves.
    app = mcp.streamable_http_app(
        streamable_http_path="/mcp", stateless_http=True,
        transport_security=_transport_security(settings, base),
    )
    app.routes.insert(0, Route("/health", health))

    # psycopg's AsyncConnectionPool needs a running event loop to open (Db.open()
    # lazily opens it on first query otherwise, which works but means the first
    # request pays connection-setup latency and any construction-time errors
    # surface mid-request instead of at boot). This Starlette version dropped
    # add_event_handler()/on_startup=/on_shutdown= entirely -- the only hook left
    # is `Router.lifespan_context`, which mcp.streamable_http_app() has already
    # set to run the MCP session manager. Wrap it (rather than replace it) so
    # both the pool AND the session manager get opened/closed, in that order.
    mcp_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(started_app: Starlette) -> AsyncIterator[None]:
        # Fail closed: a JWKS that cannot be fetched at boot means no token can
        # ever be verified, so refuse to start rather than 401 everything.
        # INVARIANT (carried forward from Task 2's review): JwksCache.prime()
        # has no self rate-limit of its own -- it must only ever be called
        # here, at boot, and never from a request path.
        if _jwks_to_prime is not None:
            await _jwks_to_prime.prime()
        await db.open()
        try:
            async with mcp_lifespan(started_app):
                yield
        finally:
            await db.close()

    app.router.lifespan_context = lifespan

    # add_middleware mutates the existing Starlette app in place (no re-wrapping, no
    # double-mounting), so it keeps owning the session-manager lifespan set up above.
    app.add_middleware(CorrelationMiddleware)

    # Audit M2: the body ceiling must not depend on the rate limiter being on,
    # and must cover /oauth/* too. Added early (Starlette applies add_middleware
    # in reverse) so at request time it runs LAST of our middlewares -- inside
    # the buffering /mcp guards, whose own 1 MiB caps stay the outer wall.
    app.add_middleware(BodySizeLimitMiddleware)

    # Metrics are on by default; METRICS_ENABLED=false removes the route
    # entirely (404) -- there is nothing to bearer-gate either way, since
    # it's an ops endpoint like /health, not an MCP client-facing one. configure_metrics
    # is idempotent and re-scoped to this settings object each call, so re-building
    # the app (as tests do, repeatedly, with different settings) never trips
    # prometheus_client's "duplicate timeseries" error from a stale registry.
    #
    # metrics.enabled() (checked AFTER configure_metrics, not settings.metrics_enabled)
    # is the gate on mounting the route: METRICS_ENABLED=true with the observability
    # extra missing must not serve a 200-with-empty-body -- Prometheus would read
    # that as a healthy scrape of nothing, forever. Not mounting the route makes a
    # deps-missing misconfiguration 404 identically to metrics being off outright,
    # which is a real error signal (and configure_metrics already logged a WARNING
    # above naming the fix) rather than a silently-succeeding empty scrape.
    if settings.metrics_enabled:
        metrics.configure_metrics(settings)
        # Every tool x outcome child at 0 before the first call: a Prometheus
        # series that first appears at 1 is invisible to rate()/increase().
        # ORDER DEPENDENCY: tool_errors decorations run inside create_mcp()'s
        # register()/register_apps(); this call must stay after create_mcp or the
        # registry is empty.
        metrics.preregister_tools(registered_tool_names())

        if metrics.enabled():

            async def metrics_endpoint(request: Request) -> Response:
                # Local-only, unconditionally: any request carrying X-Forwarded-For
                # arrived through a proxy hop (cloudflared or any sane reverse proxy
                # stamps it, and an external client cannot strip a header its own
                # proxy adds) -- the docker-network scrape (pgllens:3000/metrics)
                # never carries it. 404, not 403: indistinguishable from
                # METRICS_ENABLED=false, so a scanner learns nothing. This restores,
                # from inside the app, the proxy-level block the decommissioned
                # Caddy profile used to provide.
                #
                # Deliberately simple, known ceiling: a LAN/dev client hitting the
                # bind directly (no proxy in front) sends no XFF either and still
                # gets metrics. That's bind exposure, not path exposure, and is the
                # accepted posture for an unproxied deployment.
                #
                # raise HTTPException rather than hand-build a Response: Starlette's
                # own exception handler renders the identical body/content-type/
                # content-length as an unmounted route's 404, so there is one source
                # of truth for "not found" and a scanner can't tell metrics-gated
                # apart from metrics-disabled by content-length.
                if "x-forwarded-for" in request.headers:
                    raise HTTPException(status_code=404)
                body, content_type = metrics.render(request.headers.get("accept", ""))
                return Response(body, media_type=content_type)

            app.routes.append(Route("/metrics", metrics_endpoint))

    # The inbound tool-call rate limiter is added AFTER CorrelationMiddleware and
    # BEFORE BearerAuthMiddleware is added below. Starlette applies add_middleware
    # in reverse (last-added runs first), so bearer auth runs first at request
    # time and this limiter can key on the authenticated client id it stamps on
    # the scope. 0 (the ge=0 field's floor) disables it entirely.
    limit_store = build_limit_store(settings)
    configure_limits(settings, limit_store)

    if settings.tool_rate_limit_per_minute:
        app.add_middleware(
            InboundToolRateLimitMiddleware,
            protected_path="/mcp",
            per_minute=settings.tool_rate_limit_per_minute,
            trust_proxy_headers=settings.trust_proxy_headers,
            store=limit_store,
        )

    # OAuth is opt-in and off by default: with no config, /mcp stays open and
    # no /oauth/* routes are registered at all (404s), matching today's
    # behaviour exactly. mcp_auth_mode is the source of truth (a mode, not a
    # boolean -- see config.py); mcp_oauth_enabled is a deprecated alias that
    # config.py's validator already folds into mcp_auth_mode == "password".
    # ORDERING: add_middleware applies in reverse. These are added BEFORE the
    # auth branch below so that at request time they run AFTER it, keying on
    # the authenticated client id rather than on the peer address.
    app.add_middleware(CallerContextMiddleware,
                       trust_proxy_headers=settings.trust_proxy_headers)
    if settings.max_concurrent_calls_per_client:
        app.add_middleware(
            ConcurrencyLimitMiddleware,
            protected_path="/mcp",
            max_concurrent=settings.max_concurrent_calls_per_client,
            store=limit_store,
            trust_proxy_headers=settings.trust_proxy_headers,
        )

    if oauth or settings.mcp_auth_mode == "password":
        # Built BEFORE build_oauth and injected into it, so /oauth/token
        # (which issues into oauth_state.tokens) and BearerAuthMiddleware
        # (which validates against `token_store` below) hit the SAME backend.
        # Previously these were two independent stores -- a RedisLimitStore
        # swapped the validating side to Redis but /oauth/token kept issuing
        # into an in-memory OAuthState.tokens no one ever read from again,
        # so every issued token 401'd. See final-review.md Critical 1.
        token_store: TokenStore | RedisTokenStore
        if isinstance(limit_store, RedisLimitStore):
            token_store = RedisTokenStore(
                limit_store.client, ttl_seconds=settings.mcp_oauth_token_expires_in
            )
        else:
            token_store = TokenStore(ttl_seconds=settings.mcp_oauth_token_expires_in)
        oauth_routes, _oauth_state = build_oauth(settings, base_url=base, token_store=token_store)
        for route in oauth_routes:
            app.routes.append(route)
        app.add_middleware(
            BearerAuthMiddleware,
            token_store=token_store,
            protected_path="/mcp",
            resource_metadata_url=f"{base}/.well-known/oauth-protected-resource",
        )
    elif settings.mcp_auth_mode == "okta":
        # Resource-server mode: PgLLens issues nothing. build_oauth() is NOT
        # called, so /oauth/register, /oauth/authorize, /oauth/token and the
        # authorization-server metadata document do not exist here at all.
        metadata_url = f"{base}/.well-known/oauth-protected-resource"
        jwks = JwksCache(settings.okta_jwks_url)
        verifier = JwtVerifier(
            jwks,
            issuer=settings.okta_issuer or "",
            audience=settings.okta_audience or "",
        )

        async def protected_resource_metadata(_: Request) -> JSONResponse:
            # RFC 9728: point MCP clients at OKTA as the authorization server.
            return JSONResponse({
                "resource": base,
                "authorization_servers": [settings.okta_issuer],
                "bearer_methods_supported": ["header"],
                # offline_access: Okta only mints a refresh token when asked;
                # without one claude.ai's Claude Code proxy sees "no token".
                "scopes_supported": [SCOPE_READ, SCOPE_ADMIN, "offline_access"],
            })

        app.routes.append(
            Route("/.well-known/oauth-protected-resource", protected_resource_metadata)
        )
        # ScopeEnforcementMiddleware is added BEFORE OktaBearerMiddleware so
        # that, since Starlette applies add_middleware in reverse, bearer
        # auth runs first at request time and the scope gate sees the
        # scopes it stamps. The tool-call rate limiter was already added
        # above both (Phase 1, unmoved), so it runs last of the three and
        # keys on the authenticated client id -- see
        # test_okta_mode_middleware_order_bearer_before_rate_limiter.
        app.add_middleware(ScopeEnforcementMiddleware, protected_path="/mcp")
        app.add_middleware(
            OktaBearerMiddleware,
            verifier=verifier,
            protected_path="/mcp",
            resource_metadata_url=metadata_url,
        )
        _jwks_to_prime = jwks

    # Tracing is optional and off by default; instrument_asgi is a no-op wrapper
    # returning app unchanged unless settings.otel_enabled AND the OTel libs are
    # installed, so this line is always safe to run.
    telemetry.configure_tracing(settings)
    # instrument_asgi is typed Any -> Any (it may wrap `app` in OpenTelemetry's
    # generic ASGI middleware, which implements the ASGI protocol but isn't a
    # Starlette subclass); build_app's callers only need an ASGI-callable, which
    # both the wrapped and unwrapped forms are.
    app = cast(Starlette, telemetry.instrument_asgi(app))
    return app
