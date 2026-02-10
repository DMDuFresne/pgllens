# ProveIT MES Database Context

This document provides domain context for AI assistants querying the ProveIT Manufacturing Execution System (MES) database.

---

## MES (Manufacturing Execution System) Concepts

### Core Entities
- **Asset**: Physical or logical equipment (machines, lines, cells, areas) organized in a hierarchy
- **Product**: Items being manufactured, grouped into families for reporting
- **State**: Operational status of equipment (Running, Down, Idle, etc.)
- **Production Log**: Records of manufacturing runs with start/end times
- **Count Log**: Quantity tracking (infeed, outfeed, scrap) tied to production runs
- **Measurement Log**: Quality measurements with tolerance checking
- **KPI Log**: Calculated performance indicators over time windows

### Key Metrics
- **OEE (Overall Equipment Effectiveness)**: Availability × Performance × Quality
- **Availability**: (Run Time) / (Planned Production Time)
- **Performance**: (Actual Output) / (Theoretical Output at ideal rate)
- **Quality**: (Good Units) / (Total Units Produced)

### State Types
| State | Description | Counts as Downtime? |
|-------|-------------|---------------------|
| Running | Equipment is actively producing | No |
| Down | Equipment is stopped (unplanned) | Yes |
| Idle | Equipment is available but not producing | No |
| Changeover | Transitioning between products | Depends on config |
| Maintenance | Scheduled maintenance activity | Yes (planned) |

### Data Patterns
- All log tables use **soft-delete** (`removed=true`) instead of hard delete
- Timestamps use `TIMESTAMPTZ` for timezone awareness
- Descriptive fields are **snapshotted** at log time for historical accuracy
- `JSONB additional_info` columns store flexible metadata

---

## Database Schema Overview

### Schemas
| Schema | Purpose |
|--------|---------|
| `mes_core` | Primary MES tables, views, and functions |
| `mes_audit` | Change tracking and audit logs |
| `mes_custom` | Customer-specific extensions |

### Table Categories

#### Lookup Tables (Reference Data)
| Table | Description |
|-------|-------------|
| `asset_type` | Categories of assets (Machine, Line, Cell, Area, Enterprise, Site) |
| `state_type` | State categories with `is_downtime` flag |
| `state_definition` | Specific states mapped to state types |
| `downtime_reason` | Reasons for downtime with `is_planned` flag |
| `count_type` | Types of counts (Good, Scrap, Rework) |
| `measurement_type` | Types of measurements (Weight, Length, Temperature, etc.) |
| `kpi_definition` | KPI definitions with formulas |

#### Master Data Tables
| Table | Description |
|-------|-------------|
| `asset_definition` | Asset hierarchy with parent-child relationships via `parent_asset_id`. `tag_path` links to Ignition SCADA tags. |
| `product_family` | Groups related products for reporting |
| `product_definition` | Individual products/SKUs. `ideal_cycle_time` is target seconds per unit. |
| `performance_target` | Expected rates per asset+product combination |

#### Log Tables (Time-Series Data)
| Table | Description |
|-------|-------------|
| `state_log` | Immutable state transition log. Each row represents entering a state. |
| `production_log` | Production run records. `end_ts IS NULL` indicates active run. |
| `count_log` | Quantity events tied to production runs. `product_id=1` indicates missing product data. |
| `measurement_log` | Quality measurements with target/actual values and tolerance checking. |
| `kpi_log` | Pre-calculated KPI values over time windows. |

#### Note Tables
- `state_log_note`, `production_log_note`, `count_log_note`, `measurement_log_note`, `kpi_log_note`
- Allow operator annotations on log entries
- `general_note` for standalone notes not linked to specific events

---

## Key Views

### State Views
| View | Description | Performance Notes |
|------|-------------|-------------------|
| `vw_state_active` | Current (most recent) state per asset | Fast - use for dashboards |
| `vw_state_timeline` | State history with calculated durations | Filter by time range |
| `vw_state_downtime_events` | Filtered view of downtime events only | Includes `is_planned` |
| `vw_state_duration_hourly` | Hourly aggregation by state type per asset | |
| `vw_state_duration_daily` | Daily aggregation by state type per asset | |

### Production Views
| View | Description |
|------|-------------|
| `vw_production_log` | Production runs with aggregated count totals |
| `vw_production_current` | Active (open) production runs (`end_ts IS NULL`) |
| `vw_production_yield` | Yield percentage per run (good_quantity / total_quantity) |
| `vw_production_throughput_rate` | Actual vs ideal rate, performance percentage |
| `vw_production_state_summary` | Time spent in each state category during a run |
| `vw_production_count_summary` | Count totals by type during production runs |
| `vw_production_measurement_summary` | Measurement statistics (avg, min, max) per run |

### Quality & KPI Views
| View | Description |
|------|-------------|
| `vw_measurement_summary_by_product` | Measurement statistics aggregated by product |
| `vw_measurement_out_of_tolerance` | Measurements that failed tolerance checks |
| `vw_kpi_latest` | Most recent KPI value per asset and KPI definition |

### Data Quality Views
| View | Description |
|------|-------------|
| `vw_dq_unknown_product_counts` | Counts with `product_id=1` (Unknown product) |
| `vw_dq_unknown_product_summary_hourly` | Hourly summary of unknown product issues |
| `vw_dq_unknown_product_summary_daily` | Daily summary with percentage of issues |
| `vw_dq_assets_with_unknown_products` | Assets that have logged against Unknown product |

### ⚠️ Expensive View
| View | Warning |
|------|---------|
| `vw_unified_event_log` | Combined view of ALL event types. **ALWAYS filter by `logged_at` AND `asset_id`** |

---

## Key Functions

| Function | Description |
|----------|-------------|
| `fn_get_asset_tree(root_id, max_level)` | Returns asset hierarchy starting from a root asset |
| `fn_search_asset_ancestors(asset_id)` | Finds all parent assets up to enterprise level |
| `fn_search_asset_descendants(asset_id)` | Finds all child assets under a parent |
| `fn_assets_without_state()` | Returns assets that have never logged a state |
| `fn_insert_state_log(...)` | Wrapper for inserting state transitions |
| `fn_insert_production_log(...)` | Wrapper for inserting production runs |
| `fn_insert_count_log(...)` | Wrapper for inserting count events |
| `fn_insert_measurement_log(...)` | Wrapper for inserting measurements |
| `fn_insert_kpi_log(...)` | Wrapper for inserting KPI records |

---

## Query Examples

### Get Current State for All Assets
```sql
SELECT * FROM mes_core.vw_state_active ORDER BY asset_name;
```

### Get State History for an Asset (Last 24 Hours)
```sql
SELECT * FROM mes_core.vw_state_timeline
WHERE asset_id = 1
  AND start_time >= NOW() - INTERVAL '24 hours'
ORDER BY start_time DESC;
```

### Get Downtime Events with Reasons
```sql
SELECT
  asset_name,
  state_name,
  downtime_reason_name,
  is_planned,
  start_time,
  duration_seconds / 60.0 as duration_minutes
FROM mes_core.vw_state_downtime_events
WHERE start_time >= NOW() - INTERVAL '7 days'
ORDER BY start_time DESC;
```

### Get Active Production Runs
```sql
SELECT * FROM mes_core.vw_production_current;
```

### Calculate OEE Components for a Production Run
```sql
-- Performance from throughput view
SELECT
  production_log_id,
  asset_name,
  product_name,
  performance_percent
FROM mes_core.vw_production_throughput_rate
WHERE production_log_id = 123;

-- Quality from yield view
SELECT
  production_log_id,
  yield_percent as quality_percent
FROM mes_core.vw_production_yield
WHERE production_log_id = 123;
```

### Get Latest KPIs per Asset
```sql
SELECT
  asset_name,
  kpi_name,
  kpi_value,
  start_ts,
  end_ts
FROM mes_core.vw_kpi_latest
ORDER BY asset_name, kpi_name;
```

### Get Asset Hierarchy
```sql
SELECT * FROM mes_core.fn_get_asset_tree(1, 10);
```

### Find Assets with Data Quality Issues
```sql
SELECT * FROM mes_core.vw_dq_assets_with_unknown_products;
```

### Query Unified Event Log (ALWAYS filter!)
```sql
SELECT * FROM mes_core.vw_unified_event_log
WHERE logged_at >= NOW() - INTERVAL '1 hour'
  AND asset_id = 1
ORDER BY logged_at DESC
LIMIT 100;
```

---

## ⚠️ Important Warnings

### Performance
- **`vw_unified_event_log`**: ALWAYS filter by `logged_at` AND/OR `asset_id` - this view queries 5 tables
- Log tables can be very large - always use time-based filters
- Use `LIMIT` to prevent returning excessive rows
- For dashboard queries, prefer specific views over raw tables

### Data Integrity
- All tables use soft-delete: check `removed IS DISTINCT FROM TRUE` or `removed = false`
- Views already filter out removed records
- Production runs with `end_ts IS NULL` are still active (open)
- State transitions only record entry time; duration is calculated via views using `LEAD()`

### Reserved IDs
- `product_id = 1` is reserved for **"Unknown" product** (data quality indicator)
- Check `vw_dq_unknown_product_counts` to monitor data quality
- Assets logging against Unknown product may have misconfigured tag paths

### Timezone Handling
- All timestamps are stored as `TIMESTAMPTZ`
- When comparing times, use `AT TIME ZONE` or ensure consistent timezone handling
- `NOW()` returns server time with timezone

---

## Common Patterns

### Finding "Orphaned" Data
```sql
-- Assets without any state transitions
SELECT * FROM mes_core.fn_assets_without_state();

-- Production runs without any counts
SELECT pl.* FROM mes_core.production_log pl
LEFT JOIN mes_core.count_log cl ON pl.id = cl.production_log_id
WHERE cl.id IS NULL AND pl.end_ts IS NOT NULL;
```

### Aggregating by Time Window
```sql
-- Hourly production counts
SELECT
  date_trunc('hour', logged_at) as hour,
  asset_id,
  SUM(quantity) as total_quantity
FROM mes_core.count_log
WHERE logged_at >= NOW() - INTERVAL '24 hours'
GROUP BY date_trunc('hour', logged_at), asset_id
ORDER BY hour, asset_id;
```

### Joining with Asset Hierarchy
```sql
-- Get all counts for an area and its children
WITH asset_tree AS (
  SELECT * FROM mes_core.fn_get_asset_tree(5, 10)  -- 5 = area asset_id
)
SELECT cl.*
FROM mes_core.count_log cl
JOIN asset_tree at ON cl.asset_id = at.id
WHERE cl.logged_at >= NOW() - INTERVAL '1 day';
```
