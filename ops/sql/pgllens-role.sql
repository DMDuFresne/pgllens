-- ops/sql/pgllens-role.sql
--
-- The least-privilege PostgreSQL role PgLLens connects as. This is the guarantee
-- that src/pgllens/database/safety.py's regex gate sits in FRONT of -- on the
-- internet it has to actually exist rather than being assumed.
--
-- Run as a superuser, connected to the database PgLLens will serve:
--
--   psql -v pgllens_password="$(openssl rand -base64 32)" \
--        -v pgllens_db=mydb -v pgllens_schema=public \
--        -f ops/sql/pgllens-role.sql "$SUPERUSER_DSN"
--
-- Re-runnable: every statement is guarded or idempotent except the password,
-- which is always reset to the value you pass.
--
-- Verify afterwards with:  PGLLENS_TEST_DSN=... uv run pytest tests/integration/test_db_posture.py

\set ON_ERROR_STOP on

-- 1. The role. NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOINHERIT are spelled out
--    rather than left to defaults, so a future default change cannot widen this.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgllens') THEN
    CREATE ROLE pgllens LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
      NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

ALTER ROLE pgllens WITH PASSWORD :'pgllens_password';
ALTER ROLE pgllens WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- 2. Server-side session guarantees, attached to the ROLE, not the DSN. If the
--    conninfo is ever misconfigured (options dropped by a pooler, a hand-edited
--    URL), these still hold.
ALTER ROLE pgllens SET default_transaction_read_only = on;
ALTER ROLE pgllens SET statement_timeout = '30s';
ALTER ROLE pgllens SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE pgllens SET lock_timeout = '5s';
-- Never let a search_path trick resolve an unqualified name into a schema the
-- operator did not expose. `public` is deliberately NOT on the path: pgllens
-- schema-qualifies every catalog and extension object it reads (it looks up
-- pg_stat_statements' schema from pg_extension), so nothing here needs it.
--
-- NOTE: running this script once per schema (see section 7) overwrites
-- search_path each time -- it ends up as whichever schema you passed last. Set
-- it deliberately afterwards for a multi-schema EXPOSED_SCHEMAS.
ALTER ROLE pgllens SET search_path = :'pgllens_schema', pg_catalog;

-- 3. Connect to exactly one database.
--
--    This REVOKE stops every OTHER role reaching THIS database. It does not, as
--    an earlier version of this comment claimed, stop pgllens reaching the rest
--    of the cluster: PUBLIC holds CONNECT on every database by default, and
--    revoking it here has no bearing on `postgres`, `template1`, or any other
--    database. Verified -- the role could still open `postgres` with only this
--    statement applied.
REVOKE ALL ON DATABASE :"pgllens_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"pgllens_db" TO pgllens;

--    To actually confine the ROLE to one database you must revoke CONNECT on
--    the others, which is a CLUSTER-WIDE change affecting every non-superuser
--    on that server -- not something this script will do to a shared cluster on
--    your behalf. Run it by hand, per database, once you know what else lives
--    there:
--
--      REVOKE CONNECT ON DATABASE postgres  FROM PUBLIC;
--      REVOKE CONNECT ON DATABASE template1 FROM PUBLIC;
--
--    Superusers bypass this, so administrative access is unaffected. Skipping
--    it is defensible: pgllens is configured with a single DSN and cannot
--    switch databases at runtime, so this is defence in depth rather than a
--    live hole. ops/demo/03-role.sql does apply it, because that cluster
--    contains nothing else.

-- 4. Read-only on the exposed schema. USAGE lets it resolve names; SELECT lets
--    it read. Nothing else is granted -- no INSERT/UPDATE/DELETE/TRUNCATE, no
--    CREATE, no EXECUTE-by-default.
GRANT USAGE ON SCHEMA :"pgllens_schema" TO pgllens;
GRANT SELECT ON ALL TABLES IN SCHEMA :"pgllens_schema" TO pgllens;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA :"pgllens_schema" TO pgllens;
-- Tables created later must inherit the same grant, or discovery silently goes
-- blind on new tables. Run this as the role that OWNS the schema's objects.
ALTER DEFAULT PRIVILEGES IN SCHEMA :"pgllens_schema" GRANT SELECT ON TABLES TO pgllens;
ALTER DEFAULT PRIVILEGES IN SCHEMA :"pgllens_schema" GRANT SELECT ON SEQUENCES TO pgllens;

-- 5. Statistics for the diagnostic tools (get_active_sessions, get_blocking,
--    get_query_store). pg_monitor is the read-only bundle for exactly this and
--    grants no write capability. Drop this grant if the operator would rather
--    those tools return "insufficient privilege" than expose other users' SQL.
--
--    WITH INHERIT TRUE is required: the role itself is NOINHERIT (section 1),
--    so by default this membership only activates via SET ROLE pg_monitor --
--    which pgllens can never issue, because database/safety.py's SQL-text gate
--    blocks SET. Per-membership INHERIT (PostgreSQL 16+) turns this one grant
--    on without loosening the role's NOINHERIT default for anything else. On
--    PostgreSQL <16, WITH INHERIT TRUE is not available; fall back to
--    `ALTER ROLE pgllens INHERIT` instead, which makes ALL of pgllens' role
--    memberships inherit (there are none besides pg_monitor today, so this is
--    equivalent in practice, just less future-proof).
GRANT pg_monitor TO pgllens WITH INHERIT TRUE;

-- pg_monitor alone is not enough for get_query_store. That tool reads the
-- pg_stat_statements VIEW (and pg_stat_statements_info, extension 1.9+), which
-- needs USAGE on whichever schema CREATE EXTENSION put them in (public by
-- default) plus SELECT on the views themselves. USAGE on a schema grants
-- access to no relation on its own. Guarded, so this script still runs on a
-- cluster without the extension (get_query_store then reports it as missing).
DO $$
DECLARE
  ext_schema text;
BEGIN
  SELECT n.nspname INTO ext_schema
  FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_stat_statements';
  IF ext_schema IS NULL THEN
    RAISE NOTICE 'pg_stat_statements not installed; get_query_store will report it missing';
  ELSE
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO pgllens', ext_schema);
    EXECUTE format('GRANT SELECT ON %I.pg_stat_statements TO pgllens', ext_schema);
    IF to_regclass(format('%I.pg_stat_statements_info', ext_schema)) IS NOT NULL THEN
      EXECUTE format('GRANT SELECT ON %I.pg_stat_statements_info TO pgllens', ext_schema);
    END IF;
  END IF;
END
$$;

-- 6. Explicitly NOT granted, listed so a reviewer can see the decision:
--      pg_read_server_files      -- SQL -> host filesystem
--      pg_write_server_files     -- SQL -> host filesystem
--      pg_execute_server_program -- SQL -> command execution
--      pg_signal_backend         -- cancel/terminate other sessions
--    Assert they were never inherited from somewhere else:
DO $$
DECLARE
  bad text;
BEGIN
  SELECT string_agg(r.rolname, ', ') INTO bad
  FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles g ON g.oid = m.member
  WHERE g.rolname = 'pgllens'
    AND r.rolname IN ('pg_read_server_files', 'pg_write_server_files',
                      'pg_execute_server_program', 'pg_signal_backend');
  IF bad IS NOT NULL THEN
    RAISE EXCEPTION 'pgllens is a member of forbidden role(s): %', bad;
  END IF;
END
$$;

-- 7. Repeat sections 4 for every additional schema in EXPOSED_SCHEMAS.

-- Optional: lets postgres_exporter (the `infra` compose profile) read
-- pg_stat_* views under the same read-only role. Harmless if unused.
-- GRANT pg_monitor TO pgllens;
