import pytest

from pgllens.llens_style.errors import ErrorCode
from pgllens.llens_style.model import (
    Bullet,
    Bullets,
    Call,
    Caveat,
    Code,
    Error,
    Response,
    Section,
    Table,
)


def _resp(**kw):
    base = {
        "server": "pgllens", "tool": "list_roles", "scope": None, "plane": "catalog",
        "sections": (Section(None, (Bullets((Bullet("a", "1"),)),)),),
    }
    base.update(kw)
    return Response(**base)


def test_minimal_response_constructs():
    r = _resp()
    assert r.tally == () and r.next == () and r.status is None


def test_single_section_may_not_have_heading():
    with pytest.raises(ValueError, match="heading"):
        _resp(sections=(Section("only", (Bullets((Bullet("a", "1"),)),)),))


def test_multiple_sections_need_headings():
    with pytest.raises(ValueError, match="heading"):
        _resp(sections=(Section(None, (Bullets((Bullet("a", "1"),)),)),
                        Section(None, (Bullets((Bullet("b", "2"),)),))))


def test_heading_must_be_lowercase_short():
    with pytest.raises(ValueError, match="lowercase"):
        Section("Columns", (Bullets((Bullet("a", "1"),)),))


def test_table_requires_tally():
    with pytest.raises(ValueError, match="tally"):
        _resp(sections=(Section(None, (Table(("a",), (("1",),)),)),))


def test_table_with_tally_ok():
    _resp(sections=(Section(None, (Table(("a",), (("1",),)),)),), tally=("1 row",))


def test_table_row_width_must_match_columns():
    with pytest.raises(ValueError, match="columns"):
        Table(("a", "b"), (("1",),))


def test_next_max_three():
    with pytest.raises(ValueError, match="next"):
        _resp(next=tuple(Call("x") for _ in range(4)))


def test_code_lang_required():
    with pytest.raises(ValueError, match="lang"):
        Code("", "select 1")


def test_plane_allowlist():
    with pytest.raises(ValueError, match="plane"):
        _resp(plane="live")


def test_bullet_key_lowercase():
    with pytest.raises(ValueError, match="lowercase"):
        Bullet("Product", "x")


def test_caveat_one_sentence():
    with pytest.raises(ValueError, match="sentence"):
        Caveat("First. Second.")


def test_error_requires_hint():
    with pytest.raises(ValueError, match="hint"):
        Error("pgllens", "query", ErrorCode.DB_ERROR, "boom", "")


def test_error_ok():
    e = Error("pgllens", "query", ErrorCode.TIMEOUT, "m", "h", retry_after="22s")
    assert e.retry_after == "22s"


def test_scope_rejects_error():
    with pytest.raises(ValueError, match="scope"):
        _resp(scope="error")
    with pytest.raises(ValueError, match="scope"):
        _resp(scope="Error")


def test_scope_rejects_newline():
    with pytest.raises(ValueError, match="scope"):
        _resp(scope="app_core\nDROP TABLE x")


def test_scope_rejects_separator():
    with pytest.raises(ValueError, match="scope"):
        _resp(scope="a · b")


def test_next_requires_tally():
    with pytest.raises(ValueError, match="tally"):
        _resp(next=(Call("x"),))
