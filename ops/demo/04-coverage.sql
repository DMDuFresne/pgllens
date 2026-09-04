-- ops/demo/04-coverage.sql
--
-- Objects that exercise tool branches the base demo (01-schema.sql /
-- 02-seed.sql) can't reach. Runs after 03-role.sql in postgres initdb
-- filename order, so the pgllens role already exists for the column REVOKE.

\set ON_ERROR_STOP on

-- get_triggers happy path + get_function_source on a trigger function.
CREATE FUNCTION app_core.fn_stamp_closed_at() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status = 'closed' AND NEW.closed_at IS NULL THEN
    NEW.closed_at := now();
  END IF;
  RETURN NEW;
END;
$$;
COMMENT ON FUNCTION app_core.fn_stamp_closed_at() IS 'Demo trigger fn: stamps closed_at when a work order closes.';

CREATE TRIGGER work_order_stamp_closed_at
  BEFORE UPDATE ON app_core.work_order
  FOR EACH ROW EXECUTE FUNCTION app_core.fn_stamp_closed_at();

-- get_constraints validated = NOT VALID.
ALTER TABLE app_core.work_order
  ADD CONSTRAINT work_order_labour_hours_sane CHECK (labour_hours <= 24) NOT VALID;

-- get_table_stats 0-row divide-by-zero guard; list_tables 0 estimate.
CREATE TABLE app_custom.import_scratch (
  import_scratch_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload text
);
COMMENT ON TABLE app_custom.import_scratch IS 'Intentionally empty: exercises the 0-row stats guard.';

-- get_view_definition materialized-view path.
CREATE MATERIALIZED VIEW app_core.mv_sensor_reading_counts AS
  SELECT s.sensor_id, s.tag, count(r.*) AS reading_count
  FROM app_core.sensor s LEFT JOIN app_core.reading r USING (sensor_id)
  GROUP BY s.sensor_id, s.tag;
COMMENT ON MATERIALIZED VIEW app_core.mv_sensor_reading_counts IS 'Per-sensor reading counts; exercises the materialized-view path.';

-- get_index_health duplicate-index call-out (mirrors sensor_asset_id_idx).
CREATE INDEX sensor_asset_id_dup_idx ON app_core.sensor (asset_id);

-- get_table_health "Needs attention": dead tuples with no vacuum.
UPDATE app_core.reading SET quality = quality
  WHERE recorded_at >= now() - interval '2 days';

-- list_roles column-level ACL / get_sample_data grant-failure path.
-- A column-level REVOKE alone is a no-op here: 03-role.sql already granted
-- table-wide SELECT on every app_core table, and Postgres falls back to that
-- blanket grant whenever no column-level ACL exists to override it. To
-- actually deny badge_pin (matching the get_sample_data docstring's claim
-- that a real column-level REVOKE makes SELECT *-shaped tools error), revoke
-- the table-wide grant and re-grant SELECT on every other column explicitly.
REVOKE SELECT ON app_core.technician FROM pgllens;
GRANT SELECT (technician_id, full_name, email, phone, api_token, hired_on, created_at)
  ON app_core.technician TO pgllens;

ANALYZE app_core.work_order;
ANALYZE app_custom.import_scratch;
ANALYZE app_core.mv_sensor_reading_counts;
-- No VACUUM on app_core.reading, by design (leave the dead tuples).
