## pgllens · list_hypertables
*catalog · 2026-09-03T15:49:03Z*

### hypertables
| hypertable | time column | chunk interval | compression |
|---|---|---|---|
| `app_core.readings` | `ts` | 7 days | yes |

### jobs
| hypertable | job type | schedule | config | next run |
|---|---|---|---|---|
| `readings` | Compression Policy | 1 day | `{"compress_after": "30 days"}` | 2026-09-04 02:00:00 |

### chunks
| hypertable | chunks | range | total | compressed | ratio |
|---|---|---|---|---|---|
| `readings` | 12 | 2026-06-01 → 2026-09-01 | 5.0 MB | 1.0 MB | 5.0x |

---
1 hypertable · 1 job · 0 continuous aggregates
*Next: get_table_health(schema="app_core")*
