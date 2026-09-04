"""REDACT_COLUMNS is on by default; `off` disables it; a value replaces it."""

import pytest

from pgllens.config import DEFAULT_REDACT_COLUMNS, Settings
from pgllens.database.format import matches_redacted

DSN = "postgresql://u:p@localhost:5432/db"


def test_redaction_defaults_on_when_unset_or_empty():
    assert Settings(_env_file=None, database_url=DSN).redact_columns == list(DEFAULT_REDACT_COLUMNS)
    assert Settings(_env_file=None, database_url=DSN, redact_columns="").redact_columns == list(DEFAULT_REDACT_COLUMNS)


def test_redaction_off_sentinel_and_explicit_patterns():
    assert Settings(_env_file=None, database_url=DSN, redact_columns="off").redact_columns == []
    assert Settings(_env_file=None, database_url=DSN, redact_columns="%pin%, %card%").redact_columns == ["%pin%", "%card%"]


@pytest.mark.parametrize("name", ["classname", "businessname", "addressnumber", "token_count", "tokens_used"])
def test_default_patterns_skip_substring_false_positives(name):
    assert not matches_redacted(name, list(DEFAULT_REDACT_COLUMNS))


@pytest.mark.parametrize("name", ["ssn", "SSN_last4", "user_ssn", "api_token", "access_token", "UserPassword", "api_key"])
def test_default_patterns_match_sensitive_names(name):
    assert matches_redacted(name, list(DEFAULT_REDACT_COLUMNS))
