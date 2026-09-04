---
name: pgllens-using
description: Use when working with a PostgreSQL database through the pgllens MCP server — at the start of any database task (writing SQL, exploring a schema, tuning a slow query, checking blocking, index health, vacuum/bloat, hypertables, or general DBA/diagnostic work), or when unsure which pgllens tool or companion skill fits. Triggers on PostgreSQL, Postgres, SQL, database, schema, table, query, blocking, index, vacuum, TimescaleDB, hypertable, DBA.
---

# pgllens-using

## Overview

pgllens is a **read-only** lens onto a PostgreSQL database: schema, business rules, sample
data, execution plans, and live DBA diagnostics — 31 tools, all read-only. It cannot create an
index, force a plan, kill a session, or write a single row: every query runs inside a session
opened with `default_transaction_read_only=on`, on top of an application-level SQL-text gate
(`database/safety.py`) that rejects anything but a single `SELECT`/`WITH` statement. This shapes
every answer: pgllens reports what is true of the database now; a human operator is the one who
acts on it. This skill orients you and routes you to the right companion skill.

## Discovery before guessing

Never write a query against a schema you haven't looked at. The workflow:

1. `list_tables` or `schema_overview` — what exists in the exposed schemas.
2. `get_ontology` — what the schema *means* (see below) before writing SQL against it.
3. Only then `query` (or `validate_query` / `explain_query` first, for anything nontrivial).

Every discovery and query tool takes an optional **`schema`** argument. Catalog and discovery
tools (`list_tables`, `describe_table`, `get_relationships`, `find_path`, `get_constraints`,
`get_triggers`, `get_erd`, …) default to **all exposed schemas**; the per-schema DBA reports
(`get_table_health`, `get_space_usage`, `get_index_health`, `get_function_source`) default to
the server's configured **default schema** (the first entry in `EXPOSED_SCHEMAS`) and append a
`Scope: ...` footer naming the schemas they didn't cover. Each tool's own description states its
default. Asking about a schema outside the allowlist doesn't error — it comes back as a clear
rejection. There is no way to see or query a schema the operator didn't expose.

## House rules

These apply to every query you write against a pgllens-exposed database, not just the obvious
cases:

- **Prefer `v_*` views over raw tables when one exists for what you need**, and read its
  definition with `get_view_definition` first. A view usually already encodes the join, the
  soft-delete filter, and the business rule you'd otherwise have to reconstruct by hand — read
  it before reinventing it.
- **Filter `removed_at IS NULL` on every table in a join chain, not just the leaf table.** A
  soft-deleted row upstream (e.g. a removed location) can still join through to a row that
  looks live at the bottom of the chain. Add the filter per-table, not once at the end.
- **Never sum quantities across unit-of-measure (UoM).** A quantity column without a `GROUP BY
  uom` silently adds eaches to pallets to cases. Always `GROUP BY uom` (or convert explicitly)
  before aggregating any quantity.
- **Alias every column in a join, as a readability habit.** This used to be load-bearing:
  earlier MCP servers in this lineage used a dict-based row factory that silently collapsed two
  same-named columns (`SELECT a.created_at, b.created_at`) into one, dropping a value with no
  error. pgllens 2.0.0 fixed that at the source — `database/pool.py` returns positional
  `(columns, rows)` pairs, never a dict, so a join selecting two columns of the same name now
  returns both values correctly even without aliasing. Keep aliasing anyway: it's what makes the
  output readable to a human or a model reading the result, not a workaround for a bug that no
  longer exists.

## Structure vs. meaning

Two tools look similar but answer different questions:

- **`describe_table`** carries **structure**: column types, nullability, defaults, primary key.
  What the table is shaped like.
- **`get_ontology`** carries **business meaning**: which tables are hubs (most referenced), how
  tables connect, and naming conventions in use (soft delete, audit columns, lookup tables,
  junction tables), plus any operator-supplied domain context. What the schema *means* — a
  `removed_at` column naming the soft-delete convention, a hub table telling you where most
  relationships converge.

Read `get_ontology` before inferring semantics from a column name alone — `flag`, `type`, and
`status` columns lie constantly; the ontology's naming-convention and constraint detail usually
doesn't.

## Permission and capability gaps are not "nothing wrong"

Some tools depend on an extension or a role grant that a least-privilege pgllens user may not
have:

- `get_query_store` needs the `pg_stat_statements` extension.
- `list_hypertables` needs the `timescaledb` extension.
- `get_active_sessions`, `get_blocking`, `get_wait_stats` read `pg_stat_activity`, which shows
  full query text and other sessions' details only to a superuser, a role with the `pg_monitor`
  built-in role, or the session's own user — a restricted role sees a narrower result, not an
  error.

When a required extension is absent, the tool stays **registered and visible** in the tool list
(a tool that disappears is indistinguishable from a broken server) and instead returns an
`EXTENSION_MISSING` error whose hint names the `CREATE EXTENSION` statement that would enable it.
**Read that hint and say so** — "no data" because of a missing extension or grant is not the same
claim as "no data because nothing is wrong." Report the gap, don't silently treat it as a clean
bill of health.

## Which skill / tool

| The user wants… | Reach for |
|---|---|
| To get oriented in an unfamiliar database, or understand what tables mean | **pgllens-explore-a-database** |
| A slow query fixed, or to understand why a query is slow | **pgllens-tune-a-query** |
| A general "is this database healthy" sweep — vacuum/bloat, index health, blocking, space | **pgllens-health-check** |
| One value or a quick lookup | `query` directly (`limit` for the page size, `explain_first=true` to see the plan's estimate first) |
| A schema diagram (interactive, drill-down) | `get_erd_widget` |
| A Mermaid diagram or plain-text listing any host can read | `get_erd` (`format="mermaid"` by default) |
| A plain relationship list | `get_relationships` |
| How to join two tables that aren't directly related | `find_path` |
| The `CHECK`/`UNIQUE`/`EXCLUDE` rules a table actually enforces | `get_constraints` |
| What fires on insert/update/delete | `get_triggers` |

When a task spans several of these, start with the most specific skill.
