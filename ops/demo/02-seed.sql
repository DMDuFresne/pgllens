-- ops/demo/02-seed.sql
--
-- Example data for the demo stack. Runs once, after 01-schema.sql.
--
-- Identity columns are GENERATED ALWAYS, so nothing here supplies a surrogate
-- key: rows are wired together by looking up the natural key (site.code,
-- asset.serial_number, sensor.tag). Slightly longer than hardcoding ids, and it
-- means re-ordering these statements cannot silently mis-link a foreign key.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------- sites

INSERT INTO app_core.site (code, name, timezone, commissioned) VALUES
  ('BR01', 'Brantford Compression',  'America/Toronto',  '2019-04-15'),
  ('BR02', 'Brantford North Solar',  'America/Toronto',  '2022-08-01'),
  ('HAM1', 'Hamilton Pump Station',  'America/Toronto',  '2016-11-20'),
  ('WIN1', 'Windsor Chiller Plant',  'America/Detroit',  '2021-02-08');

-- ---------------------------------------------------------------- technicians

INSERT INTO app_core.technician (full_name, email, phone, badge_pin, api_token, hired_on) VALUES
  ('Avery Nakamura', 'avery.nakamura@example.com', '+1-519-555-0111', '4821', 'demo-token-1', '2018-06-04'),
  ('Kai Oyelaran',   'kai.oyelaran@example.com',   '+1-519-555-0142', '9073', 'demo-token-2', '2020-01-13'),
  ('Rowan Petrov',   'rowan.petrov@example.com',   '+1-905-555-0188', '3312', 'demo-token-3', '2021-09-27'),
  ('Sasha Bergström','sasha.bergstrom@example.com', NULL,             '7756', 'demo-token-4', '2023-03-06'),
  ('Devin Achebe',   'devin.achebe@example.com',   '+1-226-555-0170', '8429', 'demo-token-5', '2024-07-22');

-- ---------------------------------------------------------------- assets

INSERT INTO app_core.asset (site_id, name, kind, serial_number, installed_on, is_active, rated_kw)
SELECT s.site_id, v.name, v.kind::app_core.asset_kind, v.serial, v.installed, v.active, v.kw
FROM (VALUES
  ('BR01', 'Compressor A',      'compressor', 'CMP-BR01-0001', DATE '2019-05-02', true,   450.00),
  ('BR01', 'Compressor B',      'compressor', 'CMP-BR01-0002', DATE '2019-05-02', true,   450.00),
  ('BR01', 'Inlet Valve 1',     'valve',      'VLV-BR01-0011', DATE '2019-05-10', true,   NULL),
  ('BR01', 'Gas Meter Primary', 'meter',      'MTR-BR01-0031', DATE '2019-06-01', true,   NULL),
  ('BR02', 'Inverter String 1', 'inverter',   'INV-BR02-0101', DATE '2022-08-14', true,  1250.00),
  ('BR02', 'Inverter String 2', 'inverter',   'INV-BR02-0102', DATE '2022-08-14', true,  1250.00),
  ('BR02', 'Inverter String 3', 'inverter',   'INV-BR02-0103', DATE '2022-08-14', false, 1250.00),
  ('HAM1', 'Transfer Pump 1',   'pump',       'PMP-HAM1-0201', DATE '2016-12-05', true,   185.50),
  ('HAM1', 'Transfer Pump 2',   'pump',       'PMP-HAM1-0202', DATE '2016-12-05', true,   185.50),
  ('HAM1', 'Booster Pump',      'pump',       'PMP-HAM1-0203', DATE '2020-03-18', true,    75.00),
  ('WIN1', 'Chiller Unit 1',    'chiller',    'CHL-WIN1-0301', DATE '2021-02-22', true,   900.00),
  ('WIN1', 'Chiller Unit 2',    'chiller',    'CHL-WIN1-0302', DATE '2021-02-22', true,   900.00)
) AS v(site_code, name, kind, serial, installed, active, kw)
JOIN app_core.site s ON s.code = v.site_code;

-- ---------------------------------------------------------------- sensors
-- Every asset gets the tag set appropriate to its kind, so tags stay plausible
-- and search_columns / schema_overview have something structured to find.

INSERT INTO app_core.sensor (asset_id, tag, unit, range_low, range_high)
SELECT a.asset_id,
       a.serial_number || '-' || t.suffix,
       t.unit,
       t.lo,
       t.hi
FROM app_core.asset a
JOIN (VALUES
  ('compressor', 'PRES', 'kPa',   0.0,  1200.0),
  ('compressor', 'TEMP', 'degC', -20.0,  140.0),
  ('compressor', 'VIBR', 'mm/s',   0.0,   25.0),
  ('pump',       'FLOW', 'm3/h',   0.0,  600.0),
  ('pump',       'PRES', 'kPa',    0.0,  900.0),
  ('pump',       'TEMP', 'degC', -20.0,  120.0),
  ('chiller',    'TEMP', 'degC', -40.0,   40.0),
  ('chiller',    'POWR', 'kW',     0.0, 1000.0),
  ('inverter',   'POWR', 'kW',     0.0, 1400.0),
  ('inverter',   'VOLT', 'V',      0.0, 1000.0),
  ('meter',      'FLOW', 'm3/h',   0.0, 2000.0),
  ('valve',      'POSN', 'pct',    0.0,  100.0)
) AS t(kind, suffix, unit, lo, hi)
  ON t.kind = a.kind::text;

-- ---------------------------------------------------------------- readings
-- 14 days of hourly samples per sensor. With 30 sensors that lands around
-- 10k rows; enough for get_table_stats / get_space_usage to be interesting and
-- for explain_query to pick the (sensor_id, recorded_at) index.

INSERT INTO app_core.reading (sensor_id, recorded_at, value, quality)
SELECT sn.sensor_id,
       gs.ts,
       -- Deterministic pseudo-signal inside each sensor's declared range: a
       -- daily sine plus a per-sensor offset. No random() -- a reproducible
       -- demo dataset is worth more than a realistic-looking one.
       -- The ::numeric cast is required, not cosmetic: sin() returns double
       -- precision, which makes the whole expression double precision, and
       -- there is no round(double precision, integer) in PostgreSQL.
       round(
         (coalesce(sn.range_low, 0)
          + (coalesce(sn.range_high, 100) - coalesce(sn.range_low, 0))
            * (0.5 + 0.35 * sin(extract(epoch FROM gs.ts) / 13750.0 + sn.sensor_id))
         )::numeric
       , 4),
       CASE
         WHEN extract(hour FROM gs.ts) = 3 AND sn.sensor_id % 7 = 0 THEN 'suspect'::app_core.reading_quality
         WHEN extract(day  FROM gs.ts) % 11 = 0 AND sn.sensor_id % 5 = 0 THEN 'stale'::app_core.reading_quality
         ELSE 'good'::app_core.reading_quality
       END
FROM app_core.sensor sn
CROSS JOIN generate_series(
  date_trunc('hour', now()) - interval '14 days',
  date_trunc('hour', now()),
  interval '1 hour'
) AS gs(ts);

-- ---------------------------------------------------------------- work orders

INSERT INTO app_core.work_order (asset_id, technician_id, status, summary, opened_at, closed_at, labour_hours)
SELECT a.asset_id,
       t.technician_id,
       v.status::app_core.work_order_status,
       v.summary,
       now() - v.opened_days_ago * interval '1 day',
       CASE WHEN v.closed_days_ago IS NULL THEN NULL
            ELSE now() - v.closed_days_ago * interval '1 day' END,
       v.hours
FROM (VALUES
  ('CMP-BR01-0001', 'avery.nakamura@example.com', 'closed',      'Replace inlet filter element',            42, 41,   3.50),
  ('CMP-BR01-0001', 'kai.oyelaran@example.com',   'closed',      'Vibration above alarm threshold',         28, 26,  11.25),
  ('CMP-BR01-0002', 'kai.oyelaran@example.com',   'in_progress', 'Oil analysis flagged particulates',        6, NULL,  2.00),
  ('VLV-BR01-0011', 'rowan.petrov@example.com',   'closed',      'Actuator recalibration',                  19, 19,   1.75),
  ('INV-BR02-0103', 'rowan.petrov@example.com',   'blocked',     'String offline, awaiting DC isolator',    31, NULL,  4.00),
  ('INV-BR02-0101', NULL,                          'open',        'Quarterly thermal scan',                   2, NULL, NULL),
  ('PMP-HAM1-0201', 'sasha.bergstrom@example.com','closed',      'Seal weep on discharge flange',           55, 53,   6.00),
  ('PMP-HAM1-0202', 'sasha.bergstrom@example.com','closed',      'Bearing replacement',                     37, 35,   9.50),
  ('PMP-HAM1-0203', 'devin.achebe@example.com',   'in_progress', 'Suction strainer differential rising',     4, NULL,  1.25),
  ('CHL-WIN1-0301', 'devin.achebe@example.com',   'closed',      'Condenser tube cleaning',                 66, 64,  14.00),
  ('CHL-WIN1-0302', 'avery.nakamura@example.com', 'cancelled',   'Duplicate of WO for unit 1',              64, NULL, NULL),
  ('CHL-WIN1-0302', 'avery.nakamura@example.com', 'open',        'Refrigerant charge low',                   1, NULL, NULL),
  ('MTR-BR01-0031', NULL,                          'open',        'Annual custody transfer verification',     9, NULL, NULL)
) AS v(serial, tech_email, status, summary, opened_days_ago, closed_days_ago, hours)
JOIN app_core.asset a       ON a.serial_number = v.serial
LEFT JOIN app_core.technician t ON t.email = v.tech_email;

-- ---------------------------------------------------------------- tag aliases

INSERT INTO app_custom.tag_alias (sensor_id, alias, source_system)
SELECT sn.sensor_id,
       replace(sn.tag, '-', '_'),
       'SCADA'
FROM app_core.sensor sn
WHERE sn.sensor_id % 2 = 0;

INSERT INTO app_custom.tag_alias (sensor_id, alias, source_system)
SELECT sn.sensor_id,
       'hist.' || lower(replace(sn.tag, '-', '.')),
       'HISTORIAN'
FROM app_core.sensor sn
WHERE sn.sensor_id % 3 = 0;

-- ---------------------------------------------------------------- audit rows

INSERT INTO app_audit.change_log (table_name, row_pk, operation, changed_by, changed_at, old_value, new_value)
SELECT 'app_core.work_order',
       wo.work_order_id::text,
       'UPDATE',
       'etl@example.com',
       wo.opened_at + interval '1 hour',
       jsonb_build_object('status', 'open'),
       jsonb_build_object('status', wo.status::text, 'labour_hours', wo.labour_hours)
FROM app_core.work_order wo
WHERE wo.status <> 'open';

INSERT INTO app_audit.change_log (table_name, row_pk, operation, changed_by, changed_at, old_value, new_value)
SELECT 'app_core.asset',
       a.asset_id::text,
       'INSERT',
       'commissioning@example.com',
       a.created_at,
       NULL,
       jsonb_build_object('name', a.name, 'kind', a.kind::text, 'rated_kw', a.rated_kw)
FROM app_core.asset a;

INSERT INTO app_audit.login_event (principal, source_ip, succeeded, occurred_at)
SELECT t.email,
       ('10.20.0.' || (t.technician_id * 7 % 250 + 1))::inet,
       (gs.n % 9) <> 0,
       now() - gs.n * interval '3 hours'
FROM app_core.technician t
CROSS JOIN generate_series(1, 12) AS gs(n);

-- ---------------------------------------------------------------- dead tuples
-- get_table_health and get_table_stats are only worth demoing against a table
-- that actually has dead rows. Insert then delete without vacuuming, so
-- n_dead_tup is non-zero and the vacuum recommendation has something to say.
-- Deliberately NOT followed by VACUUM.

INSERT INTO app_audit.login_event (principal, source_ip, succeeded, occurred_at)
SELECT 'churn-' || gs.n || '@example.com',
       '10.99.0.1'::inet,
       false,
       now() - gs.n * interval '1 minute'
FROM generate_series(1, 4000) AS gs(n);

DELETE FROM app_audit.login_event WHERE principal LIKE 'churn-%';

-- ---------------------------------------------------------------- statistics
-- Without this the planner has no stats until autovacuum's first pass, and
-- explain_query / MAX_ESTIMATED_COST would report nonsense row estimates on a
-- freshly seeded database. ANALYZE only -- no VACUUM, per the note above.

ANALYZE app_core.site;
ANALYZE app_core.technician;
ANALYZE app_core.asset;
ANALYZE app_core.sensor;
ANALYZE app_core.reading;
ANALYZE app_core.work_order;
ANALYZE app_audit.change_log;
ANALYZE app_custom.tag_alias;
