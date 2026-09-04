## pgllens · get_ontology
*catalog · 2026-09-03T15:49:03Z*

### hubs
| table | referenced by | comment |
|---|---|---|

> Ranked by distinct referencing tables; views never count.

### conventions
- lookup tables: app_core.sites (heuristic)

### relationships
| from | to | constraint |
|---|---|---|
| `app_core.assets.(site_id)` | `app_core.sites.(site_id)` | `assets_site_id_fkey` |

---
4 objects · 0 hubs · 1 foreign key
*Next: describe_table(table="app_core.sites") · get_erd()*
