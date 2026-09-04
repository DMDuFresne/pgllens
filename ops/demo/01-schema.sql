-- ops/demo/01-schema.sql
--
-- Demo schema for the pgllens demo stack (ops/demo/docker-compose.yml). Runs once,
-- as the superuser, via the postgres image's /docker-entrypoint-initdb.d hook.
--
-- Shaped to exercise all 27 tools rather than to model anything real:
--   * three schemas, matching EXPOSED_SCHEMAS=app_core,app_audit,app_custom
--   * foreign keys in both directions           -> get_relationships, get_erd
--   * enum types and column comments            -> describe_table, get_ontology
--   * a view, and a view with duplicate column
--     names across two joined tables            -> get_view_definition, query
--   * a plpgsql function                        -> list_functions, get_function_source
--   * one deliberately unindexed foreign key    -> get_index_health
--   * one wide/high-row-count table             -> get_space_usage, get_table_stats
--   * pg_stat_statements                        -> get_query_store

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS app_core;
CREATE SCHEMA IF NOT EXISTS app_audit;
CREATE SCHEMA IF NOT EXISTS app_custom;

COMMENT ON SCHEMA app_core   IS 'Operational plant data: sites, assets, sensors, readings, work orders.';
COMMENT ON SCHEMA app_audit  IS 'Append-only change history. Never written to by the application path.';
COMMENT ON SCHEMA app_custom IS 'Per-deployment extensions: tag aliases and reporting views.';

-- Preloaded in the compose command; this makes the view available.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ---------------------------------------------------------------- enum types

CREATE TYPE app_core.asset_kind AS ENUM (
  'pump', 'compressor', 'chiller', 'inverter', 'meter', 'valve'
);
COMMENT ON TYPE app_core.asset_kind IS 'Equipment class. Drives which sensor tags are expected.';

CREATE TYPE app_core.work_order_status AS ENUM (
  'open', 'in_progress', 'blocked', 'closed', 'cancelled'
);

CREATE TYPE app_core.reading_quality AS ENUM ('good', 'suspect', 'stale', 'bad');

-- ---------------------------------------------------------------- app_core

CREATE TABLE app_core.site (
  site_id      integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code         text        NOT NULL UNIQUE,
  name         text        NOT NULL,
  timezone     text        NOT NULL DEFAULT 'UTC',
  commissioned date,
  created_at   timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE  app_core.site           IS 'Physical plants. Top of the site -> asset -> sensor -> reading chain.';
COMMENT ON COLUMN app_core.site.code      IS 'Short operator-facing identifier, e.g. BR01. Unique.';
COMMENT ON COLUMN app_core.site.timezone  IS 'IANA name; reading timestamps are stored UTC and rendered in this zone.';

CREATE TABLE app_core.technician (
  technician_id integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  full_name     text        NOT NULL,
  email         text        NOT NULL UNIQUE,
  phone         text,
  badge_pin     text,
  api_token     text,
  hired_on      date        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE  app_core.technician            IS 'Field staff who close work orders.';
COMMENT ON COLUMN app_core.technician.badge_pin IS 'Demo column for REDACT_COLUMNS: fake 4-digit PIN, matches %pin% and renders as [masked].';
COMMENT ON COLUMN app_core.technician.api_token IS 'Demo column for REDACT_COLUMNS: fake API token, matches %token% and renders as [masked].';

CREATE TABLE app_core.asset (
  asset_id      integer              GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  site_id       integer              NOT NULL REFERENCES app_core.site(site_id),
  name          text                 NOT NULL,
  kind          app_core.asset_kind  NOT NULL,
  serial_number text                 NOT NULL UNIQUE,
  installed_on  date                 NOT NULL,
  is_active     boolean              NOT NULL DEFAULT true,
  rated_kw      numeric(10,2),
  created_at    timestamptz          NOT NULL DEFAULT now(),
  CONSTRAINT asset_rated_kw_positive CHECK (rated_kw IS NULL OR rated_kw > 0)
);
COMMENT ON TABLE app_core.asset IS 'Individual pieces of equipment at a site.';
CREATE INDEX asset_site_id_idx ON app_core.asset (site_id);
CREATE INDEX asset_kind_idx    ON app_core.asset (kind) WHERE is_active;

CREATE TABLE app_core.sensor (
  sensor_id  integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id   integer     NOT NULL REFERENCES app_core.asset(asset_id) ON DELETE CASCADE,
  tag        text        NOT NULL UNIQUE,
  unit       text        NOT NULL,
  range_low  numeric(12,4),
  range_high numeric(12,4),
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sensor_range_ordered CHECK (
    range_low IS NULL OR range_high IS NULL OR range_low < range_high
  )
);
COMMENT ON TABLE  app_core.sensor     IS 'Measurement points on an asset.';
COMMENT ON COLUMN app_core.sensor.tag IS 'Canonical instrument tag, e.g. BR01-PMP-003-PRES.';
CREATE INDEX sensor_asset_id_idx ON app_core.sensor (asset_id);

CREATE TABLE app_core.reading (
  reading_id bigint                  GENERATED ALWAYS AS IDENTITY,
  sensor_id  integer                 NOT NULL REFERENCES app_core.sensor(sensor_id) ON DELETE CASCADE,
  recorded_at timestamptz            NOT NULL,
  value      numeric(14,4)           NOT NULL,
  quality    app_core.reading_quality NOT NULL DEFAULT 'good',
  -- TimescaleDB requires every unique index to include the partition column.
  PRIMARY KEY (reading_id, recorded_at)
);
COMMENT ON TABLE app_core.reading IS 'Time-series sensor samples. The bulk table -- one row per sensor per hour for 14 days (~9k rows after seeding). A TimescaleDB hypertable on recorded_at (1-day chunks, compression policy at 7 days) so list_hypertables has something real to report. get_table_health/get_table_stats query this parent relation directly, so they report ~0 rows/pages -- the actual ~9k rows live in the per-day child chunks under _timescaledb_internal, which pg_class/pg_stat_user_tables do not roll up onto the parent.';
CREATE INDEX reading_sensor_recorded_idx ON app_core.reading (sensor_id, recorded_at DESC);

CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('app_core.reading', 'recorded_at',
                         chunk_time_interval => interval '1 day');
ALTER TABLE app_core.reading SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id',
  timescaledb.compress_orderby   = 'recorded_at DESC'
);
SELECT add_compression_policy('app_core.reading', interval '7 days');

CREATE TABLE app_core.work_order (
  work_order_id integer                    GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- Deliberately simple: asset_id and technician_id are INTENTIONALLY unindexed. This is
  -- the finding get_index_health is supposed to surface (unindexed foreign key);
  -- adding the index here would make the demo show a clean bill of health.
  asset_id      integer                    NOT NULL REFERENCES app_core.asset(asset_id),
  technician_id integer                    REFERENCES app_core.technician(technician_id),
  status        app_core.work_order_status  NOT NULL DEFAULT 'open',
  summary       text                       NOT NULL,
  opened_at     timestamptz                NOT NULL DEFAULT now(),
  closed_at     timestamptz,
  labour_hours  numeric(6,2),
  CONSTRAINT work_order_closed_after_opened CHECK (closed_at IS NULL OR closed_at >= opened_at),
  CONSTRAINT work_order_closed_has_time     CHECK (status <> 'closed' OR closed_at IS NOT NULL)
);
COMMENT ON TABLE app_core.work_order IS 'Maintenance jobs raised against an asset.';

-- View: exercises get_view_definition and gives query something readable.
CREATE VIEW app_core.v_asset_latest_reading AS
SELECT a.asset_id,
       a.name        AS asset_name,
       a.kind,
       s.code        AS site_code,
       sn.tag        AS sensor_tag,
       sn.unit,
       r.value,
       r.quality,
       r.recorded_at
FROM app_core.asset a
JOIN app_core.site s   ON s.site_id  = a.site_id
JOIN app_core.sensor sn ON sn.asset_id = a.asset_id
JOIN LATERAL (
  SELECT value, quality, recorded_at
  FROM app_core.reading r2
  WHERE r2.sensor_id = sn.sensor_id
  ORDER BY r2.recorded_at DESC
  LIMIT 1
) r ON true;
COMMENT ON VIEW app_core.v_asset_latest_reading IS 'Most recent sample per sensor, with asset and site context.';

-- Function: exercises list_functions and get_function_source.
CREATE FUNCTION app_core.fn_asset_reading_stats(
  p_asset_id integer,
  p_from     timestamptz DEFAULT now() - interval '7 days',
  p_to       timestamptz DEFAULT now()
) RETURNS TABLE (sensor_tag text, samples bigint, avg_value numeric, max_value numeric)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT sn.tag,
         count(*),
         round(avg(r.value), 4),
         max(r.value)
  FROM app_core.sensor sn
  JOIN app_core.reading r ON r.sensor_id = sn.sensor_id
  WHERE sn.asset_id = p_asset_id
    AND r.recorded_at >= p_from
    AND r.recorded_at <  p_to
    AND r.quality = 'good'
  GROUP BY sn.tag
  ORDER BY sn.tag;
END;
$$;
COMMENT ON FUNCTION app_core.fn_asset_reading_stats IS 'Per-sensor summary for one asset over a window.';

-- ---------------------------------------------------------------- app_audit

CREATE TABLE app_audit.change_log (
  change_id   bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  table_name  text        NOT NULL,
  row_pk      text        NOT NULL,
  operation   text        NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
  changed_by  text        NOT NULL,
  changed_at  timestamptz NOT NULL DEFAULT now(),
  old_value   jsonb,
  new_value   jsonb
);
COMMENT ON TABLE app_audit.change_log IS 'Append-only row history. jsonb payloads exercise describe_table type rendering.';
CREATE INDEX change_log_table_changed_idx ON app_audit.change_log (table_name, changed_at DESC);
CREATE INDEX change_log_new_value_gin     ON app_audit.change_log USING gin (new_value);

CREATE TABLE app_audit.login_event (
  login_event_id bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  principal      text        NOT NULL,
  source_ip      inet,
  succeeded      boolean     NOT NULL,
  occurred_at    timestamptz NOT NULL DEFAULT now()
);
COMMENT ON COLUMN app_audit.login_event.source_ip IS 'inet column -- checks non-scalar type formatting.';

-- ---------------------------------------------------------------- app_custom

CREATE TABLE app_custom.tag_alias (
  tag_alias_id  integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sensor_id     integer     NOT NULL REFERENCES app_core.sensor(sensor_id) ON DELETE CASCADE,
  alias         text        NOT NULL,
  source_system text        NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_system, alias)
);
COMMENT ON TABLE app_custom.tag_alias IS 'Cross-schema foreign key into app_core.sensor -- shows up in get_erd.';
CREATE INDEX tag_alias_sensor_id_idx ON app_custom.tag_alias (sensor_id);

CREATE VIEW app_custom.v_alias_resolved AS
SELECT ta.alias,
       ta.source_system,
       sn.tag        AS canonical_tag,
       sn.unit,
       a.name        AS asset_name
FROM app_custom.tag_alias ta
JOIN app_core.sensor sn ON sn.sensor_id = ta.sensor_id
JOIN app_core.asset a   ON a.asset_id   = sn.asset_id;
COMMENT ON VIEW app_custom.v_alias_resolved IS 'Alias -> canonical tag -> asset. Spans app_custom and app_core.';

-- The duplicate-column-name regression (the bug the TypeScript server shipped;
-- see docs/runbook.md) can only be demonstrated with an ad-hoc SELECT, NOT a
-- view: CREATE VIEW rejects two output columns of the same name. Run this
-- through the `query` tool -- both created_at values come back, because
-- database/pool.py returns positional tuples rather than a dict-keyed row:
--
--   SELECT ta.created_at, sn.created_at
--   FROM app_custom.tag_alias ta
--   JOIN app_core.sensor sn ON sn.sensor_id = ta.sensor_id
--   LIMIT 5;
