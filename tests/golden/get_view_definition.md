## pgllens · get_view_definition · app_core.v_assets
*catalog · 2026-09-03T15:49:03Z*

### identity
- kind: `view`

### columns
| column | type | comment |
|---|---|---|
| `asset_id` | `bigint` |  |

### definition
```sql
SELECT asset_id FROM app_core.assets
```

---
1 column
*Next: get_sample_data(table="app_core.v_assets", limit=5) · explain_query(sql="SELECT * FROM app_core.v_assets LIMIT 100")*
