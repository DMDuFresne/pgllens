## pgllens · get_index_health · app_core
*stats · 2026-09-03T15:49:03Z*

### indexes
| schema | table | index | scans | size |
|---|---|---|---|---|
| `app_core` | `assets` | `assets_pkey` | 1,204 | 16.0 kB |
| `app_core` | `assets` | `assets_site_idx` | 0 | 8.0 kB |
| `app_core` | `assets` | `assets_site_idx2` | 0 | 8.0 kB |

> Scan counts accumulated since 2026-08-01T00:00:00Z: 33 days.

> Scope is app_core; also exposed: app_audit.

### unused
| index |
|---|
| `app_core.assets.assets_site_idx` |
| `app_core.assets.assets_site_idx2` |

### duplicates
| indexes |
|---|
| `app_core.assets.assets_site_idx`, `app_core.assets.assets_site_idx2` |

---
3 indexes · 2 unused · 0 invalid · 1 duplicate set · 0 uncovered fks
*Next: get_table_health(schema="app_core") · get_space_usage(schema="app_core")*
