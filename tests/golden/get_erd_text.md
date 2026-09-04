## pgllens · get_erd · app_core
*catalog · 2026-09-03T15:49:03Z*

### tables
| table | kind | rows (estimate) | tag | columns |
|---|---|---|---|---|
| `app_core.assets` | table | ~4.1K |  | `asset_id` pk, `site_id` fk, `tag_name`, `installed_at`, `metadata` |
| `app_core.sites` | table | 12 |  | `site_id` pk, `name` |

### relationships
| from | to | constraint |
|---|---|---|
| `app_core.assets.site_id` | `app_core.sites.site_id` | `assets_site_id_fkey` |

---
2 tables · 1 relationship
*Next: describe_table(table="app_core.assets") · get_relationships(table="app_core.assets")*
