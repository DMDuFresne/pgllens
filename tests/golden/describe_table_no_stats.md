## pgllens · describe_table · app_audit.events
*catalog+stats · 2026-09-03T15:49:03Z*

### identity
- kind: `table`
- primary key: `event_id`
- rows: `82` (estimate)

### columns
| column | type | null | default | pk | comment | values |
|---|---|---|---|---|---|---|
| `event_id` | `bigint` | no |  | ✓ |  |  |
| `created_at` | `timestamptz` | no | `now()` |  |  |  |

> No planner statistics yet for this table; run ANALYZE for distinct-value samples.

---
2 columns · 82 rows (estimate)
*Next: get_sample_data(table="app_audit.events", limit=5) · get_relationships(table="app_audit.events") · get_constraints(table="app_audit.events")*
