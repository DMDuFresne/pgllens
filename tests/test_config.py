import pytest
from pydantic import ValidationError

from pgllens.config import Settings

DSN = "postgresql://u:p@localhost:5432/flux"


def test_max_rows_defaults_to_200():
    s = Settings(database_url=DSN, exposed_schemas="public")
    assert s.max_rows == 200


def test_exposed_schemas_parses_csv():
    s = Settings(database_url=DSN, exposed_schemas="public, wms , task")
    assert s.exposed_schemas == ["public", "wms", "task"]


def test_default_schema_falls_back_to_first_exposed():
    s = Settings(database_url=DSN, exposed_schemas="wms,public")
    assert s.default_schema == "wms"


def test_explicit_default_schema_must_be_exposed():
    with pytest.raises(ValidationError, match="not in EXPOSED_SCHEMAS"):
        Settings(database_url=DSN, exposed_schemas="public", default_schema="secret")


def test_empty_exposed_schemas_rejected():
    with pytest.raises(ValidationError):
        Settings(database_url=DSN, exposed_schemas="  ,  ")


def test_database_url_must_be_postgres():
    with pytest.raises(ValidationError, match="postgresql"):
        Settings(database_url="mysql://u:p@h/db", exposed_schemas="public")


def test_empty_max_estimated_cost_is_off():
    # docker-compose passes "" for an unset ${MAX_ESTIMATED_COST:-}
    assert Settings(database_url=DSN, exposed_schemas="public",
                    max_estimated_cost="").max_estimated_cost is None


def test_legacy_oauth_flag_selects_password_mode():
    s = Settings(database_url=DSN, exposed_schemas="public",
                 mcp_oauth_enabled=True, mcp_auth_password="hunter2")
    assert s.mcp_auth_mode == "password"


def test_password_mode_without_a_password_is_rejected():
    # Failing at boot beats serving an auth mode that can never succeed.
    with pytest.raises(ValidationError, match="MCP_AUTH_PASSWORD"):
        Settings(database_url=DSN, exposed_schemas="public", mcp_auth_mode="password")


def test_template_context_file_is_inert(tmp_path):
    f = tmp_path / "context.md"
    f.write_text("<!-- pgllens:template -->\nReplace me.\n", encoding="utf-8")
    s = Settings(database_url=DSN, exposed_schemas="public",
                 domain_context_file=str(f))
    assert s.domain_context_text is None


def _s(**kw):
    base = {"database_url": DSN, "exposed_schemas": "public"}
    base.update(kw)
    return Settings(_env_file=None, **base)


def test_okta_mode_requires_issuer_and_audience():
    # Fail closed at boot: a half-configured okta mode must never start, because
    # an empty audience would disable the one check that stops a cross-resource
    # token replay.
    with pytest.raises(ValidationError, match="OKTA_ISSUER"):
        _s(mcp_auth_mode="okta", okta_audience="api://pgllens")
    with pytest.raises(ValidationError, match="OKTA_AUDIENCE"):
        _s(mcp_auth_mode="okta", okta_issuer="https://t.okta.com/oauth2/aus1")


def test_okta_mode_accepts_a_full_config():
    s = _s(
        mcp_auth_mode="okta",
        okta_issuer="https://t.okta.com/oauth2/aus1",
        okta_audience="api://pgllens",
    )
    assert s.mcp_auth_mode == "okta"
    assert s.okta_jwks_url == "https://t.okta.com/oauth2/aus1/v1/keys"


def test_okta_jwks_url_can_be_overridden():
    s = _s(
        mcp_auth_mode="okta",
        okta_issuer="https://t.okta.com/oauth2/aus1",
        okta_audience="api://pgllens",
        okta_jwks_url_override="https://mirror.internal/keys",
    )
    assert s.okta_jwks_url == "https://mirror.internal/keys"


def test_okta_mode_does_not_require_a_password():
    # Regression guard: the password validator must not fire in okta mode.
    s = _s(
        mcp_auth_mode="okta",
        okta_issuer="https://t.okta.com/oauth2/aus1",
        okta_audience="api://pgllens",
    )
    assert s.mcp_auth_password is None


def test_password_mode_is_unchanged():
    # Definition of done: local password use keeps working with no okta config.
    s = _s(mcp_auth_mode="password", mcp_auth_password="hunter2")
    assert s.mcp_auth_mode == "password"
    with pytest.raises(ValidationError, match="MCP_AUTH_PASSWORD"):
        _s(mcp_auth_mode="password")


def test_none_mode_is_still_the_default():
    assert _s().mcp_auth_mode == "none"


def test_require_verify_full_rejects_a_plaintext_dsn():
    # A read-only lens still ships the client's inventory data over the wire.
    with pytest.raises(ValidationError, match="sslmode=verify-full"):
        _s(db_require_verify_full=True)


def test_require_verify_full_rejects_verify_ca_and_require():
    for mode in ("require", "verify-ca", "prefer"):
        with pytest.raises(ValidationError, match="sslmode=verify-full"):
            _s(database_url=f"{DSN}?sslmode={mode}", db_require_verify_full=True)


def test_require_verify_full_demands_a_pinned_root_cert():
    # verify-full without a pinned root falls back to the system trust store:
    # any CA in it can then impersonate the database.
    with pytest.raises(ValidationError, match="sslrootcert"):
        _s(database_url=f"{DSN}?sslmode=verify-full", db_require_verify_full=True)


def test_require_verify_full_accepts_a_fully_specified_dsn():
    s = _s(
        database_url=f"{DSN}?sslmode=verify-full&sslrootcert=/certs/root.crt",
        db_require_verify_full=True,
    )
    assert "sslmode=verify-full" in s.conninfo()


def test_require_verify_full_is_off_by_default():
    assert _s().db_require_verify_full is False


def test_db_pool_max_size_defaults_to_five():
    assert Settings(database_url=DSN).db_pool_max_size == 5


def test_db_pool_max_size_rejects_zero():
    with pytest.raises(ValidationError):
        Settings(database_url=DSN, db_pool_max_size=0)


def test_concurrency_cap_defaults_on_below_pool_size():
    s = Settings(database_url=DSN)
    assert s.max_concurrent_calls_per_client == 4
    assert s.max_concurrent_calls_per_client < s.db_pool_max_size


def test_concurrency_cap_at_or_above_pool_size_refuses_to_boot():
    # A cap >= the pool size is worse than no cap: the operator believes
    # they are protected while one client can still drain the whole pool.
    with pytest.raises(ValidationError):
        Settings(database_url=DSN, db_pool_max_size=5,
                 max_concurrent_calls_per_client=5)


def test_concurrency_cap_zero_still_means_off():
    assert Settings(
        database_url=DSN, max_concurrent_calls_per_client=0
    ).max_concurrent_calls_per_client == 0


def test_empty_env_values_are_treated_as_unset(monkeypatch):
    # `AUDIT_STDOUT=` in a compose file / .env must mean "unset", not a boolean
    # parse error that kills boot (env_ignore_empty in config.py).
    monkeypatch.setenv("AUDIT_STDOUT", "")
    monkeypatch.setenv("METRICS_ENABLED", "")
    s = Settings(_env_file=None, database_url=DSN, exposed_schemas="public")
    assert s.audit_stdout is None
    assert s.metrics_enabled is True
