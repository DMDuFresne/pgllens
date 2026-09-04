## pgllens · get_constraints
*catalog · 2026-09-03T15:49:03Z*

| schema | table | constraint | type | definition | references | validated |
|---|---|---|---|---|---|---|
| `app_core` | `assets` | `assets_pkey` | primary key | `PRIMARY KEY (asset_id)` |  | yes |
| `app_core` | `assets` | `assets_site_id_fkey` | foreign key | `FOREIGN KEY (site_id) REFERENCES sites(site_id)` | `app_core.sites` | yes |

---
2 constraints
*Next: get_relationships(table="app_core.assets")*
