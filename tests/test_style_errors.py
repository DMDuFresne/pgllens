import pytest

from pgllens.llens_style.errors import HINTS, ErrorCode, hint_for


def test_every_code_has_a_hint_template():
    assert set(HINTS) == set(ErrorCode)


def test_codes_are_upper_snake():
    for c in ErrorCode:
        assert c.value == c.value.upper() and " " not in c.value


def test_hint_for_formats_template():
    assert hint_for(ErrorCode.EXTENSION_MISSING, extension="timescaledb") == (
        "Run `CREATE EXTENSION timescaledb;` as a superuser, then retry.")


def test_hint_for_arg_out_of_range():
    assert hint_for(ErrorCode.ARG_OUT_OF_RANGE, arg="limit", lo=1, hi=1000) == (
        "Pass `limit` between 1 and 1000.")


def test_hint_missing_kwarg_is_a_programming_error():
    with pytest.raises(KeyError):
        hint_for(ErrorCode.EXTENSION_MISSING)
