"""The reverse-proxy config carries two security controls, so it gets tests.

Not a lint of the whole Caddyfile -- just the properties that, if silently
dropped in an edit, would quietly break per-IP rate limiting and the audit trail.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CADDYFILE = (ROOT / "ops" / "caddy" / "Caddyfile").read_text(encoding="utf-8")
COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def test_caddy_overwrites_x_forwarded_for_rather_than_appending():
    # Caddy APPENDS to X-Forwarded-For by default. clientip.py reads the
    # RIGHTMOST hop, so an appended header is safe -- but an overwrite makes it
    # unforgeable regardless of how many hops a client claims. This is the
    # spec's explicit requirement.
    assert "header_up X-Forwarded-For {remote_host}" in CADDYFILE


def test_caddy_sets_hsts():
    assert "Strict-Transport-Security" in CADDYFILE
    assert "max-age=31536000" in CADDYFILE


def test_caddy_service_is_decommissioned_not_shipped_untested():
    # 2026-09-01: this deployment fronts pgllens with a Cloudflare Tunnel; the
    # caddy service was never verified against a live hostname, so it is
    # commented out in docker-compose.yml rather than shipped as untested code.
    # The Caddyfile itself stays (content pinned by the tests above) for a
    # future revival, which must re-add the service AND complete a live
    # verification pass before this test is inverted back.
    assert "caddy" not in COMPOSE["services"]


def test_cloudflared_tunnel_is_behind_a_profile_so_local_use_is_unchanged():
    assert COMPOSE["services"]["cloudflared"]["profiles"] == ["tunnel"]


def test_the_app_port_bind_address_is_configurable():
    # Behind the proxy the plaintext port must not be independently reachable:
    # APP_BIND=127.0.0.1 makes :3000 loopback-only.
    ports = COMPOSE["services"]["pgllens"]["ports"]
    assert any("${APP_BIND" in str(p) for p in ports)


def test_the_app_port_bind_default_is_loopback_not_all_interfaces():
    # The spec requires the plaintext port to NOT be independently reachable
    # behind a proxy -- that has to be the DEFAULT, not something an operator
    # must remember to opt into. Pins against someone flipping the fallback
    # back to 0.0.0.0.
    ports = COMPOSE["services"]["pgllens"]["ports"]
    assert any("${APP_BIND:-127.0.0.1}" in str(p) for p in ports)


def test_caddy_pins_x_forwarded_proto_and_host_rather_than_relying_on_defaults():
    # Discovery-URL (EXTERNAL_BASE_URL) correctness depends on the app seeing
    # the real scheme/host, not Caddy's default reverse_proxy behavior.
    assert "header_up X-Forwarded-Proto {scheme}" in CADDYFILE
    assert "header_up Host {host}" in CADDYFILE


def test_trust_proxy_headers_is_documented_with_the_overwrite_requirement():
    env = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "TRUST_PROXY_HEADERS" in env
    assert "overwrite" in env.lower()


def test_metrics_is_blocked_at_the_public_proxy():
    # docs/OBSERVABILITY.md "The /metrics exposure decision": /metrics is never
    # bearer-gated, so the tls profile must not proxy it to the public
    # hostname even though it's proxied for everything else.
    assert "respond @metrics 404" in CADDYFILE
    assert "path /metrics" in CADDYFILE


def test_compose_delivers_the_phase_2_settings_into_the_app_container():
    # docker-compose reads .env for variable SUBSTITUTION only -- an env var
    # missing from the service's own `environment:` block never reaches the
    # container no matter what's in .env. These are the settings DEPLOY.md's
    # tls quickstart and a live verification pass depend on actually arriving.
    env = COMPOSE["services"]["pgllens"]["environment"]
    for key in (
        "TRUST_PROXY_HEADERS",
        "MCP_AUTH_MODE",
        "OKTA_ISSUER",
        "OKTA_AUDIENCE",
        "OKTA_JWKS_URL_OVERRIDE",
        "REDIS_URL",
        "DB_REQUIRE_VERIFY_FULL",
        "EXTERNAL_BASE_URL",
    ):
        assert key in env, f"{key} is not passed into the pgllens service"


def test_trust_proxy_headers_defaults_to_false_in_compose():
    # The default must stay the safe one: enabling it with no proxy in front
    # lets any client spoof its own IP (see .env.example's warning).
    assert COMPOSE["services"]["pgllens"]["environment"]["TRUST_PROXY_HEADERS"] == (
        "${TRUST_PROXY_HEADERS:-false}"
    )
