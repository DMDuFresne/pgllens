"""Environment-validated configuration (pydantic-settings)."""

from __future__ import annotations

from functools import cached_property, lru_cache
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

_TEMPLATE_SENTINEL = "pgllens:template"


# `%` = any run, `_` literal, case-insensitive (format.py::matches_redacted).
# `ssn`/`token` are `_`-word-bounded so classname/businessname/token_count stay visible.
DEFAULT_REDACT_COLUMNS: tuple[str, ...] = (
    "%password%", "%passwd%", "%secret%", "%api_key%",
    "ssn", "%_ssn", "ssn_%", "%_ssn_%",
    "token", "%_token", "%_token_%",
)


class Settings(BaseSettings):
    # env_ignore_empty: `AUDIT_STDOUT=` in a .env/compose file means "unset", not
    # "the empty string" -- without it an empty value hits the bool|None parser
    # and kills boot with a ValidationError.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False,
                                      env_ignore_empty=True)

    # --- Connection ---
    database_url: str
    # Refuse to start unless DATABASE_URL pins TLS to the database. Off by
    # default so local/dev DSNs keep working; on for any internet deployment.
    db_require_verify_full: bool = False

    # --- Scope ---
    exposed_schemas: Annotated[list[str], NoDecode] = Field(default_factory=lambda: ["public"])
    default_schema_: str | None = Field(default=None, alias="default_schema")

    # Default-on display masking for the obviously sensitive column names. Empty/
    # unset keeps this default; REDACT_COLUMNS=off disables masking entirely.
    redact_columns: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: list(DEFAULT_REDACT_COLUMNS))

    # --- Server ---
    host: str = "0.0.0.0"
    mcp_port: int = 3000

    # --- Limits ---
    query_timeout_ms: int = Field(default=30000, gt=0)
    max_rows: int = Field(default=200, gt=0)
    schema_refresh_interval_ms: int = Field(default=300000, gt=0)
    # Planner cost units (EXPLAIN total_cost of the top node). Unset = gate off.
    max_estimated_cost: float | None = Field(default=None, gt=0)
    # Inbound MCP tool calls allowed per client per minute. 0 = off.
    tool_rate_limit_per_minute: int = Field(default=120, ge=0)
    # Planner cost units a single client may spend per minute. 120 SELECT 1s and
    # 120 seq scans of a big table are not the same load; the call-count limit
    # alone cannot tell them apart. Unset = off.
    tool_cost_budget_per_minute: float | None = Field(default=None, gt=0)
    # In-flight tool calls per client. Stops one client occupying the whole
    # psycopg pool. 0 = off. Default ON (audit M3): the pool is small and
    # /health shares it, so an uncapped client starves the whole server.
    max_concurrent_calls_per_client: int = Field(default=4, ge=0)
    # psycopg pool ceiling. /health shares this pool, so the
    # `_concurrency_cap_below_pool_size` validator requires it to exceed
    # max_concurrent_calls_per_client.
    db_pool_max_size: int = Field(default=5, gt=0)

    # --- Domain context ---
    domain_context: str | None = None
    domain_context_file: str | None = None

    # --- Observability ---
    log_level: str = "INFO"
    log_format: str = "json"
    audit_log_file: str | None = None
    # Ship the audit trail off the container: "host:port" of a UDP syslog
    # collector. The compose stack already tails the JSONL with Alloy into Loki;
    # this is the option for deployments that are not that stack.
    audit_syslog: str | None = None
    # Where the audit trail goes when no file/syslog is configured (Play tier):
    # stdout, so `docker logs` is the audit trail. None = only as that fallback;
    # True = always, alongside the file; False = never.
    audit_stdout: bool | None = None
    # Size-based rotation of the audit file. The container root is read-only and
    # the volume is finite; 100 MB x 10 covers millions of calls. 0 = never rotate
    # (for deployments that ship and rotate the file themselves).
    audit_log_max_bytes: int = Field(default=100_000_000, ge=0)
    audit_log_backups: int = Field(default=10, ge=0)
    # On by default (Play tier): /metrics is the same exposure class as /health.
    # Bind exposure, not path exposure, is the control -- see docs/OBSERVABILITY.md.
    metrics_enabled: bool = True
    otel_enabled: bool = False
    otel_exporter_otlp_endpoint: str | None = None

    # --- Auth (opt-in; off by default) ---
    # A mode, not a boolean: Phase 2 adds "okta" here and one branch in
    # server.py. mcp_oauth_enabled is kept as a deprecated alias so existing
    # .env files and the docker-compose defaults keep working unchanged.
    mcp_auth_mode: Literal["none", "password", "okta"] = "none"
    mcp_oauth_enabled: bool = False
    mcp_auth_password: SecretStr | None = None

    # --- Okta resource-server mode (MCP_AUTH_MODE=okta) ---
    # PgLLens is NOT an authorization server in this mode: it validates a token
    # Okta minted and serves RFC 9728 discovery, nothing else. Requires an Okta
    # *custom* authorization server (API Access Management) -- the Org server
    # mints opaque tokens with no custom audience. See docs/OKTA.md.
    okta_issuer: str | None = None
    okta_audience: str | None = None
    okta_jwks_url_override: str | None = None
    external_base_url: str = "http://localhost:3000"
    mcp_oauth_token_expires_in: int = 604800
    mcp_rate_limit_attempts: int = 5
    mcp_rate_limit_window_ms: int = 900000
    trust_proxy_headers: bool = False

    # Shared state for multi-replica deployments. Unset = in-memory (local).
    redis_url: str | None = None

    @field_validator("database_url")
    @classmethod
    def _must_be_postgres(cls, v: str) -> str:
        if not v.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must be a postgresql:// URL")
        return v

    @field_validator("exposed_schemas", mode="before")
    @classmethod
    def _split_csv(cls, v: object) -> list[str]:
        if isinstance(v, str):
            names = [x.strip() for x in v.split(",") if x.strip()]
            if not names:
                raise ValueError("EXPOSED_SCHEMAS must list at least one schema")
            return names
        return v  # type: ignore[return-value]

    @field_validator("redact_columns", mode="before")
    @classmethod
    def _split_redact_csv(cls, v: object) -> list[str]:
        if isinstance(v, str):
            if v.strip().lower() == "off":
                return []
            parts = [x.strip() for x in v.split(",") if x.strip()]
            return parts or list(DEFAULT_REDACT_COLUMNS)
        return v  # type: ignore[return-value]

    @field_validator("max_estimated_cost", "tool_cost_budget_per_minute", mode="before")
    @classmethod
    def _empty_cost_is_off(cls, v: object) -> object:
        return None if v == "" else v

    @field_validator("external_base_url", mode="before")
    @classmethod
    def _empty_external_base_url_is_default(cls, v: object) -> object:
        # docker-compose passes EXTERNAL_BASE_URL: ${EXTERNAL_BASE_URL:-} so the
        # var can be listed with everything else; an unset host var must fall
        # back to the field's own default, not to "" (which would blank out
        # every OAuth discovery URL -- see server._effective_external_base_url).
        return "http://localhost:3000" if v == "" else v

    @model_validator(mode="after")
    def _auth_mode_from_legacy_flag(self) -> Settings:
        # MCP_OAUTH_ENABLED=true with no explicit mode means password mode.
        if self.mcp_oauth_enabled and self.mcp_auth_mode == "none":
            object.__setattr__(self, "mcp_auth_mode", "password")
        if self.mcp_auth_mode == "password" and self.mcp_auth_password is None:
            raise ValueError("MCP_AUTH_MODE=password requires MCP_AUTH_PASSWORD")
        return self

    @model_validator(mode="after")
    def _okta_mode_is_fully_configured(self) -> Settings:
        # Fail closed at boot. A missing audience is the confused-deputy hole
        # the whole mode exists to close, so a partial config must not start.
        if self.mcp_auth_mode == "okta":
            if not self.okta_issuer:
                raise ValueError("MCP_AUTH_MODE=okta requires OKTA_ISSUER")
            if not self.okta_audience:
                raise ValueError("MCP_AUTH_MODE=okta requires OKTA_AUDIENCE")
        return self

    @model_validator(mode="after")
    def _default_schema_is_exposed(self) -> Settings:
        # An unexposed DEFAULT_SCHEMA is a silent hole: every tool that defaults
        # its `schema` argument would reach outside the allowlist. Fail at boot.
        if self.default_schema_ and self.default_schema_ not in self.exposed_schemas:
            raise ValueError(
                f"DEFAULT_SCHEMA {self.default_schema_!r} is not in EXPOSED_SCHEMAS "
                f"({', '.join(self.exposed_schemas)})"
            )
        return self

    @model_validator(mode="after")
    def _database_tls_is_pinned(self) -> Settings:
        # sslmode=require encrypts but authenticates nothing (any TLS endpoint
        # answering the port is accepted); verify-ca accepts any host under the
        # CA. Only verify-full binds the certificate to the hostname, and only a
        # pinned sslrootcert stops any CA in the system store impersonating it.
        if not self.db_require_verify_full:
            return self
        if "sslmode=verify-full" not in self.database_url:
            raise ValueError(
                "DB_REQUIRE_VERIFY_FULL=true requires sslmode=verify-full in DATABASE_URL"
            )
        if "sslrootcert=" not in self.database_url:
            raise ValueError(
                "DB_REQUIRE_VERIFY_FULL=true requires a pinned sslrootcert= in DATABASE_URL"
            )
        return self

    @model_validator(mode="after")
    def _concurrency_cap_below_pool_size(self) -> Settings:
        # A cap >= the pool size silently fails to protect: one client at the
        # cap holds every connection and /health starves. Fail at boot.
        cap = self.max_concurrent_calls_per_client
        if cap and cap >= self.db_pool_max_size:
            raise ValueError(
                f"MAX_CONCURRENT_CALLS_PER_CLIENT ({cap}) must be below "
                f"DB_POOL_MAX_SIZE ({self.db_pool_max_size}) so /health and "
                f"other clients always have a free connection"
            )
        return self

    @property
    def default_schema(self) -> str:
        return self.default_schema_ or self.exposed_schemas[0]

    @property
    def okta_jwks_url(self) -> str:
        """Okta's JWKS endpoint for the configured authorization server.

        Okta serves it at `{issuer}/v1/keys` for a custom authorization server;
        the override exists for an internal mirror, not for pointing at a
        different tenant.
        """
        if self.okta_jwks_url_override:
            return self.okta_jwks_url_override
        return f"{(self.okta_issuer or '').rstrip('/')}/v1/keys"

    @cached_property
    def domain_context_text(self) -> str | None:
        """The effective domain context, or None when none is really configured.
        A missing file, an empty file, and the shipped ops/context.md template (which
        carries the `pgllens:template` marker until an operator fills it in) are
        all "not configured", so boilerplate never leaks into get_ontology."""
        text = self.domain_context
        if not text and self.domain_context_file:
            path = Path(self.domain_context_file)
            if not path.is_file():
                return None
            text = path.read_text(encoding="utf-8")
        if not text or not text.strip() or _TEMPLATE_SENTINEL in text:
            return None
        return text

    def conninfo(self) -> str:
        """DSN with the read-only and timeout guarantees baked in as server-side
        session options. These are the hard guarantee; safety.py is the polite
        first wall in front of them."""
        opts = (
            f"-c default_transaction_read_only=on "
            f"-c statement_timeout={self.query_timeout_ms} "
            f"-c idle_in_transaction_session_timeout={self.query_timeout_ms} "
            f"-c application_name=pgllens"
        )
        sep = "&" if "?" in self.database_url else "?"
        # quote(..., safe="") escapes every reserved char, including the `=`
        # inside each `-c key=value` pair -- an unescaped `=` here reads to
        # libpq's URI parser as a second key/value separator inside the
        # "options" query param and it rejects the whole DSN outright.
        return f"{self.database_url}{sep}options={quote(opts, safe='')}"


@lru_cache
def get_settings() -> Settings:
    return Settings()
