from pgllens.database.format import (
    QueryResult,
    cell,
    matches_redacted,
    redact,
    table_from,
)


def test_table_from_formats_cells():
    r = QueryResult(columns=["Id", "Name"], rows=[(1, "a"), (2, None)], truncated=False)
    t = table_from(r)
    assert t.columns == ("Id", "Name")
    assert t.rows == (("1", "a"), ("2", "NULL"))


def test_table_from_leaves_pipes_and_newlines_raw():
    # Pipe/newline escaping is llens_style.render's job now, not table_from's.
    r = QueryResult(columns=["v"], rows=[("a|b\nc",)], truncated=False)
    t = table_from(r)
    assert t.rows == (("a|b\nc",),)


def test_table_from_column_override_and_truncated_untouched():
    r = QueryResult(columns=["x"], rows=[(1,)], truncated=True)
    t = table_from(r, columns=["y"])
    assert t.columns == ("y",)
    assert r.truncated is True  # table_from is pure and never touches the QueryResult


def test_cell_renders_booleans_lowercase():
    assert cell(True) == "true"
    assert cell(False) == "false"


def test_cell_renders_null():
    assert cell(None) == "NULL"


def test_table_from_empty_rows():
    r = QueryResult(columns=["x"], rows=[], truncated=False)
    assert table_from(r).rows == ()


def test_redact_masks_matching_columns_case_insensitively():
    r = QueryResult(["UserID", "Password", "user_password"],
                    [(1, "hunter2", "s3cret"), (2, None, "x")], False)
    out = redact(r, ["%password%"])
    assert out.rows == [(1, "[masked]", "[masked]"), (2, "[masked]", "[masked]")]
    assert out.columns == r.columns and out.truncated is False


def test_redact_noop_when_no_patterns_or_no_match():
    r = QueryResult(["a"], [(1,)], False)
    assert redact(r, []) is r
    assert redact(r, ["%password%"]) is r


def test_redact_underscore_is_literal_not_one_char_wildcard():
    # `_` is literal so the default `%_ssn` means "ends in _ssn" (user_ssn),
    # not "any char then ssn" (classname). `%` is still the run wildcard.
    r = QueryResult(["pin_", "pin1", "user_ssn", "classname"], [("1", "2", "3", "4")], False)
    out = redact(r, ["pin_", "%_ssn"])
    assert out.rows == [("[masked]", "2", "[masked]", "4")]


def test_redact_matches_source_column_when_alias_hides_it():
    r = QueryResult(["p", "UserID"], [("hunter2", 1)], False)
    out = redact(r, ["%password%"], source_columns=["Password", "UserID"])
    assert out.rows == [("[masked]", 1)]


def test_redact_source_none_entries_fall_back_to_output_name():
    r = QueryResult(["total"], [(42,)], False)
    assert redact(r, ["%password%"], source_columns=[None]) is r


# matches_redacted is the name/glob half of redact(), lifted out so tools that
# print stored values without building a QueryResult (describe_table's sampled
# values) can honour REDACT_COLUMNS through the same matcher.
def test_matches_redacted_uses_percent_wildcard_case_insensitively():
    assert matches_redacted("UserPassword", ["%password%"])
    assert not matches_redacted("pin", ["pi_"])
    assert matches_redacted("pi_", ["pi_"])
    assert not matches_redacted("total", ["%password%"])


def test_matches_redacted_is_false_without_patterns():
    assert not matches_redacted("password", [])


from pgllens.database.format import MAX_CELL_CHARS


def test_cell_passes_a_value_at_the_cap_untouched():
    v = "x" * MAX_CELL_CHARS
    assert cell(v) == v


def test_cell_truncates_over_the_cap_and_always_states_the_total_length():
    v = "y" * 20481
    out = cell(v)
    assert out.startswith("y" * MAX_CELL_CHARS)
    assert "[truncated: 20,481 chars total" in out
    assert "substr(" in out, "the marker must tell the caller how to read the rest"
    assert len(out) < MAX_CELL_CHARS + 200


def test_cell_cap_applies_to_non_string_values_too():
    out = cell(list(range(5000)))  # a long repr, e.g. a vector column's list
    assert "[truncated:" in out


def test_table_from_applies_the_cell_cap():
    r = QueryResult(columns=["v"], rows=[("z" * 3000,)], truncated=False)
    t = table_from(r)
    assert "[truncated: 3,000 chars total" in t.rows[0][0]
