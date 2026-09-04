## pgllens · find_path · app_core.assets → app_core.sites
*catalog · 2026-09-03T15:49:03Z*

### hops
| hop | from | to | constraint |
|---|---|---|---|
| 1 | `app_core.assets.site_id` | `app_core.sites.site_id` | `assets_site_id_fkey` |

### join
```sql
FROM "app_core"."assets" "as2" JOIN "app_core"."sites" "si" ON "si"."site_id" = "as2"."site_id"
```

---
1 hop · 1 path
*Next: get_relationships(table="app_core.assets") · get_relationships(table="app_core.sites")*
