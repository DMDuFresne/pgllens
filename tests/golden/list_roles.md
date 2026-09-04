## pgllens · list_roles
*catalog · 2026-09-03T15:49:03Z*

### roles
| role | login | super | createdb | createrole | conn limit | member of |
|---|---|---|---|---|---|---|
| `app_rw` | ✓ |  |  |  | unlimited | `app_ro` |
| `app_ro` |  |  |  |  | 5 |  |

> 12 built-in pg_* roles hidden; pass include_builtin=True to show them.

### grants
| role | on | privileges |
|---|---|---|
| `app_ro` | 2 relations in `app_core` | SELECT |
| `app_rw` | `app_core.assets` | INSERT, SELECT |

### visibility
> Only roles and privileges visible to the current database user are shown.

---
2 roles · 12 hidden · 2 grant rows
*Next: list_extensions()*
