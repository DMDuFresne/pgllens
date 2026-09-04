## pgllens · get_table_health · app_core
*catalog+stats · 2026-09-03T15:49:03Z*

### tables
| table | rows (estimate) | dead | dead % | bloat est. | xid age | last autovacuum |
|---|---|---|---|---|---|---|
| `assets` | ~4.1K | 410 | 9.1% | 4.3% | 48,213 | 2026-09-01T02:00:00Z |
| `sites` | 12 | 0 | 0% | n/a | 48,213 | never |

> database xid age 61,902 (0% of the 2^31 wraparound ceiling).

> Scope is app_core; also exposed: app_audit.

### needs attention
| table | why |
|---|---|
| `assets` | 9.1% dead tuples |

---
2 tables · 1 need attention
*Next: get_table_stats(table="app_core.assets") · get_index_health(schema="app_core")*
