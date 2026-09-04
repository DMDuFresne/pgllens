"""Read-only SQL gate for PostgreSQL.

Neutralize literals, dollar-quoted bodies and comments, then require a single
statement that opens with SELECT/WITH/TABLE/VALUES and contains no write,
transaction-control, or filesystem/session-manipulating keyword.
"""

from __future__ import annotations

import re


class UnsafeQueryError(ValueError):
    """Raised when a query is not provably read-only."""


# Deliberately simple: regex + hand-rolled scanners over a real parser. The hard
# guarantee is default_transaction_read_only=on (config.conninfo) plus a
# least-privilege role; this is the polite first wall that returns a readable
# error instead of a driver exception. Upgrade path: pglast (libpg_query
# bindings) if this ever needs to reason about statement structure rather than
# tokens.

_DOLLAR_TAG = re.compile(r"\$([A-Za-z_]\w*)?\$")


def _strip_dollar_quoted(sql: str) -> str:
    """Blank out $$...$$ / $tag$...$tag$ bodies.

    Not expressible as one regex alternative alongside the others: the closing
    tag must BACKREFERENCE the opening one, and a backreference's group number
    shifts as soon as the pattern is combined with other alternatives. Scanned
    explicitly instead.
    """
    out: list[str] = []
    i = 0
    while True:
        m = _DOLLAR_TAG.search(sql, i)
        if not m:
            out.append(sql[i:])
            return "".join(out)
        out.append(sql[i:m.start()])
        close = sql.find(m.group(0), m.end())
        if close == -1:
            # Unterminated -- emit the marker so the stray check below rejects it.
            out.append("$")
            return "".join(out)
        out.append("''")
        i = close + len(m.group(0))


def _strip_block_comments(sql: str) -> str:
    """Blank out /* ... */, honouring PostgreSQL's NESTED block comments.

    Postgres nests these; SQL Server does not. A non-nesting stripper ends the
    comment at the first */ and then sees the remainder of the outer comment as
    live SQL -- which both false-rejects valid queries and gives an attacker a
    way to shape what the scanner sees.
    """
    out: list[str] = []
    depth = 0
    i = 0
    while i < len(sql):
        if sql.startswith("/*", i):
            depth += 1
            i += 2
        elif sql.startswith("*/", i):
            if depth == 0:
                out.append("*/")  # stray closer -> rejected below
                i += 2
                continue
            depth -= 1
            i += 2
        else:
            if depth == 0:
                out.append(sql[i])
            i += 1
    if depth:
        out.append("/*")  # unterminated -> rejected below
    return "".join(out)


_NEUTRALIZE = re.compile(
    r"""
      E'(?:[^'\\]|\\.|'')*'   # escape strings: backslash escapes are live here
    | '(?:[^']|'')*'          # standard string literals ('' escapes)
    | "(?:[^"]|"")*"          # quoted identifiers
    | --[^\n\r]*              # line comments
    """,
    re.VERBOSE | re.DOTALL | re.IGNORECASE,
)


def _neutralize_sub(m: re.Match[str]) -> str:
    # A line comment must not collapse to '' -- "-- note\nSELECT 1" would
    # glue the comment straight onto SELECT with no boundary, and the
    # _ALLOWED_START check would fail on otherwise-valid SQL. Strings and
    # quoted identifiers collapse to an empty literal as before.
    return " " if m.group(0).startswith("--") else "''"


# Anything left after neutralization means an unbalanced string, dollar quote,
# or block comment -- ambiguous input, reject rather than guess.
_STRAY_MARKER = re.compile(r"['\"$]|/\*|\*/")

_ALLOWED_START = re.compile(r"^\s*\(*\s*(?:SELECT|WITH|TABLE|VALUES)\b", re.IGNORECASE)

# Every alternative is a fixed word, never an open-ended \w*: a widened
# \bUPDATE\w*\b happily eats ordinary identifiers like updated_at. Word
# boundaries already stop these matching inside longer identifiers, so
# created_at / deleted_at / updated_at are unaffected.
_BLOCKED = re.compile(
    r"\b(?:"
    # DML / DDL
    r"INSERT|UPDATE|DELETE|MERGE|UPSERT|DROP|CREATE|ALTER|TRUNCATE|COMMENT"
    r"|GRANT|REVOKE|REINDEX|CLUSTER|VACUUM|REFRESH|IMPORT|SECURITY"
    # transaction / session control
    # END is deliberately excluded: as a standalone statement it's transaction
    # control, but _ALLOWED_START already rejects anything not opening with
    # SELECT/WITH/TABLE/VALUES, and CASE...END is ordinary read-only SQL.
    r"|BEGIN|START|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|ABORT"
    r"|SET|RESET|DISCARD|LOCK|PREPARE|EXECUTE|DEALLOCATE"
    # server-side code execution and out-of-band channels
    r"|COPY|DO|CALL|LISTEN|UNLISTEN|NOTIFY|CHECKPOINT|LOAD"
    r")\b",
    re.IGNORECASE,
)

# Read-shaped calls that write, read the filesystem, mutate session GUCs, or burn the server.
_BLOCKED_FUNCS = re.compile(
    r"\b(?:"
    r"pg_read_file|pg_read_binary_file|pg_ls_dir|pg_stat_file"
    r"|lo_import|lo_export|lo_unlink"
    r"|dblink\w*|pg_terminate_backend|pg_cancel_backend|pg_reload_conf"
    r"|pg_rotate_logfile|pg_promote|pg_create_restore_point"
    r"|setval|nextval|pg_sleep\w*|pg_(?:try_)?advisory_(?:xact_)?(?:lock|unlock)\w*"
    r"|query_to_xml|pg_logical_emit_message"
    r"|set_config"
    r")\s*\(",
    re.IGNORECASE,
)


def scrub(sql: str) -> str:
    """Blank out dollar-quoted bodies, block comments, string literals, quoted
    identifiers and line comments, keeping offsets so keyword matching only ever
    sees real SQL. Shared with the tools' own keyword checks (query.py's
    LIMIT guard) so they agree with this module on what is code."""
    return _NEUTRALIZE.sub(_neutralize_sub, _strip_block_comments(_strip_dollar_quoted(sql)))


def assert_read_only(sql: str) -> str:
    """Return `sql` unchanged if it is provably read-only, else raise."""
    if not sql or not sql.strip():
        raise UnsafeQueryError("empty query")

    scrubbed = scrub(sql)
    # Bare parameter placeholders ($1, $2, ...) aren't dollar-quote tags -- blank
    # them out before the stray-marker check so a parameterized query doesn't
    # get a misleading "unbalanced dollar-quote" error; an unbound placeholder
    # still fails at the server with its own real error.
    scrubbed = re.sub(r"\$\d+\b", "", scrubbed)

    stray = _STRAY_MARKER.search(scrubbed.replace("''", ""))
    if stray:
        raise UnsafeQueryError(
            f"unbalanced quote, dollar-quote or block comment near {stray.group(0)!r}"
        )

    if not _ALLOWED_START.match(scrubbed):
        raise UnsafeQueryError("query must start with SELECT, WITH, TABLE or VALUES")

    # Statement counting is unsound on its own -- Postgres accepts a second
    # statement with no separator in some shapes, and a trailing ; is harmless --
    # so the keyword blocklist below is the real backstop, not this.
    single_stmt = re.sub(r";\s*$", "", scrubbed.rstrip(), count=1)
    if ";" in single_stmt:
        raise UnsafeQueryError("only a single statement is allowed")

    blocked = _BLOCKED.search(scrubbed)
    if blocked:
        raise UnsafeQueryError(f"{blocked.group(0).upper()} is not permitted (read-only lens)")

    fn = _BLOCKED_FUNCS.search(scrubbed)
    if fn:
        raise UnsafeQueryError(
            f"{fn.group(0).rstrip('( ')} is not permitted (read-only lens)"
        )

    return sql
