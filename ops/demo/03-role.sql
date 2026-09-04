-- ops/demo/03-role.sql
--
-- The read-only `pgllens` role for the DEMO database only.
--
-- ops/sql/pgllens-role.sql is the authoritative version and stays that way --
-- it takes a generated password and a schema via psql -v. This file exists
-- because /docker-entrypoint-initdb.d passes no psql variables, so the demo
-- needs a self-contained variant: a fixed password, and all three schemas of
-- EXPOSED_SCHEMAS covered instead of one.
--
-- DO NOT copy this file to production. The password is in git.

\set ON_ERROR_STOP on

-- 1. The role. Every attribute spelled out rather than left to defaults, so a
--    future default change cannot widen it.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pgllens') THEN
    CREATE ROLE pgllens LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
      NOREPLICATION NOBYPASSRLS;
  END IF;
END
$$;

ALTER ROLE pgllens WITH PASSWORD 'pgllens-demo';

-- 2. Session guarantees attached to the ROLE, not the DSN, so they survive a
--    hand-edited connection string or a pooler that drops options.
ALTER ROLE pgllens SET default_transaction_read_only = on;
ALTER ROLE pgllens SET statement_timeout = '30s';
ALTER ROLE pgllens SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE pgllens SET lock_timeout = '5s';
-- app_core first: it is DEFAULT_SCHEMA. `public` is on the path only because
-- CREATE EXTENSION put pg_stat_statements there and tools/statements.py queries
-- it UNQUALIFIED -- without this, get_query_store fails with "relation
-- pg_stat_statements does not exist" even though the extension is installed.
-- public holds no demo tables (they are all in app_*), and EXPOSED_SCHEMAS
-- does not list it, so nothing there is browsable either way. pg_catalog last
-- and explicit, so no search_path trick resolves an unqualified name into an
-- unexposed schema.
ALTER ROLE pgllens SET search_path = app_core, app_audit, app_custom, public, pg_catalog;

-- 3. Exactly one database.
--
--    Two different REVOKEs, doing two different jobs -- worth being precise,
--    because ops/sql/pgllens-role.sql only does the first and its comment
--    claims that confines pgllens to one database. It does not.
--
--    (a) On `demo`: stops every OTHER role reaching this database.
REVOKE ALL ON DATABASE demo FROM PUBLIC;
GRANT CONNECT ON DATABASE demo TO pgllens;

--    (b) On the other databases: this is what actually stops `pgllens` from
--        connecting elsewhere in the cluster, since PUBLIC holds CONNECT on
--        every database by default. Superusers bypass this, so postgres keeps
--        working. Belt-and-braces -- the app is configured with a single DSN
--        and cannot switch databases at runtime -- but the role should not be
--        able to do what the app is not allowed to do.
REVOKE CONNECT ON DATABASE postgres  FROM PUBLIC;
REVOKE CONNECT ON DATABASE template1 FROM PUBLIC;

-- 4. Read-only on each exposed schema. USAGE to resolve names, SELECT to read;
--    nothing else. ALTER DEFAULT PRIVILEGES so tables added later stay visible
--    instead of silently going dark to discovery.
DO $$
DECLARE
  s text;
BEGIN
  FOREACH s IN ARRAY ARRAY['app_core', 'app_audit', 'app_custom'] LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO pgllens', s);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO pgllens', s);
    EXECUTE format('GRANT SELECT ON ALL SEQUENCES IN SCHEMA %I TO pgllens', s);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO pgllens', s);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON SEQUENCES TO pgllens', s);
  END LOOP;
END
$$;

-- USAGE (not SELECT) on public: enough to resolve pg_stat_statements per the
-- search_path note above, and it grants no access to any relation.
GRANT USAGE ON SCHEMA public TO pgllens;
GRANT SELECT ON pg_stat_statements TO pgllens;

-- The demo function is STABLE and reads only granted tables, but EXECUTE is not
-- granted by default to a non-owner in every configuration -- be explicit.
GRANT EXECUTE ON FUNCTION app_core.fn_asset_reading_stats(integer, timestamptz, timestamptz) TO pgllens;

-- 5. pg_monitor is the read-only statistics bundle that makes
--    get_active_sessions / get_blocking / get_query_store return other sessions'
--    activity. It grants no write capability. Revoke it if you would rather
--    those three tools report insufficient privilege.
--
--    WITH INHERIT TRUE is required: the role itself is NOINHERIT (section 1),
--    so by default this membership only activates via SET ROLE pg_monitor --
--    which pgllens can never issue, because database/safety.py's SQL-text gate
--    blocks SET. Per-membership INHERIT (PostgreSQL 16+) turns this one grant
--    on without loosening the role's NOINHERIT default for anything else. This
--    demo targets PostgreSQL 18, so no <16 fallback is needed here.
GRANT pg_monitor TO pgllens WITH INHERIT TRUE;

-- 6. Explicitly NOT granted, listed so the decision is visible:
--      pg_read_server_files      -- SQL -> host filesystem
--      pg_write_server_files     -- SQL -> host filesystem
--      pg_execute_server_program -- SQL -> command execution
--      pg_signal_backend         -- cancel/terminate other sessions
--    Fail the init if any of them was inherited from somewhere else.
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

-- 7. Prove the read-only guarantee holds at the SERVER, not just in the app's
--    SQL-text gate. If this block does not raise, the init fails loudly rather
--    than shipping a writable demo.
DO $$
DECLARE
  ok boolean := false;
BEGIN
  SET LOCAL ROLE pgllens;
  BEGIN
    EXECUTE 'CREATE TABLE app_core.should_not_exist (x int)';
  EXCEPTION WHEN insufficient_privilege OR read_only_sql_transaction THEN
    ok := true;
  END;
  RESET ROLE;
  IF NOT ok THEN
    RAISE EXCEPTION 'pgllens role was able to create a table -- read-only posture is NOT enforced';
  END IF;
  RAISE NOTICE 'pgllens role verified read-only';
END
$$;
