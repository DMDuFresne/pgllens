## pgllens · get_triggers
*catalog · 2026-09-03T15:49:03Z*

| schema | table | trigger | enabled | definition | function |
|---|---|---|---|---|---|
| `app_core` | `assets` | `trg_touch` | enabled | `CREATE TRIGGER trg_touch BEFORE UPDATE ON app_core.assets FOR EACH ROW EXECUTE FUNCTION app_core.touch()` | `app_core.touch` |

---
1 trigger
*Next: get_function_source(function="touch", schema="app_core")*
