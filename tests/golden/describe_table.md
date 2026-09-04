## pgllens · describe_table · app_core.assets
*catalog+stats · 2026-09-03T15:49:03Z*

### identity
- kind: `table`
- primary key: `asset_id`
- rows: `~4.1K` (estimate)

### columns
| column | type | null | default | pk | comment | values |
|---|---|---|---|---|---|---|
| `asset_id` | `bigint` | no | `identity (always)` | ✓ |  |  |
| `site_id` | `bigint` | no |  |  | FK to sites | values: 1, 2, 3 |
| `tag_name` | `text` | no |  |  |  |  |
| `installed_at` | `timestamptz` | yes |  |  |  |  |
| `metadata` | `jsonb` | yes | `'{}'::jsonb` |  |  |  |

---
5 columns · ~4.1K rows (estimate)
*Next: get_sample_data(table="app_core.assets", limit=5) · get_relationships(table="app_core.assets") · get_constraints(table="app_core.assets")*
