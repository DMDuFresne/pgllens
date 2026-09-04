---
name: pgllens-health-check
description: Use for a general PostgreSQL health sweep — "is this database healthy", "check for problems", "DBA health check", "audit this instance", "any bloat or vacuum issues", "is anything blocked right now", "check space usage" — a triage-ordered pass across vacuum/bloat, index health, blocking, waits, and space usage. Triggers on health check, DBA audit, database status, vacuum, bloat, TimescaleDB, hypertable.
---

# pgllens-health-check

## Overview

A read-only sweep of a PostgreSQL database's health with pgllens, run in **triage order**:
highest-consequence findings first. pgllens cannot fix anything it finds — every finding is a
report for the operator to act on, not a change pgllens made.

**REQUIRED BACKGROUND:** read pgllens-using first.

## The sweep, in order

1. **`server_info`** — version, uptime, total connection count, and a curated slice of notable
   settings (memory, workers, planner cost, WAL level). Establishes what you're looking at
   before judging anything else.
2. **`get_table_health`** — vacuum/bloat health for every table in a schema: live/dead tuple
   counts, dead-tuple percentage, and when autovacuum last ran (NULL reads as "never"). Flags
   tables over 5% dead tuples or never vacuumed under "Needs attention". Lead with this: a table
   that's never been vacuumed or is heavily bloated degrades every query against it, and the fix
   (tuning `autovacuum` settings, or a manual `VACUUM`) is cheap relative to letting it compound.
3. **`get_blocking`** — current blocking chains as a query-pair list (blocked session and query,
   the blocker(s), and how long the blocked session has waited), built from
   `pg_blocking_pids()`. `"No blocking detected."` means nothing is blocked *right now* — this
   is a point-in-time read, not a history, so a clean result doesn't rule out intermittent
   blocking.
4. **`get_wait_stats`** — active sessions grouped by `wait_event_type`/`wait_event` with a
   count. Also a point-in-time sample (unlike SQL Server's cumulative wait-stats DMV, PostgreSQL
   has no built-in wait-history accumulator without the `pg_wait_sampling` extension). Heavy
   `Lock` waits corroborate a blocking problem found above; `IO`/`BufferPin` waits point at
   storage or buffer pressure instead.
5. **`get_index_health`** — every index with scan count and size, plus call-outs for unused
   indexes (`idx_scan = 0`), invalid indexes (a failed `CREATE INDEX CONCURRENTLY` left behind),
   and duplicate indexes (same table, identical column list). Comes after blocking/waits because
   it's the least urgent of the availability-affecting findings: an unused or duplicate index is
   a storage/write-overhead problem, not an active incident.
6. **`get_space_usage`** — total/table/index size per table, plus overall database size. Round
   this out last: useful context for everything above (a bloated table is also usually a large
   one) but not itself a triage-order finding.
7. **`list_hypertables`**, if TimescaleDB is in use — hypertable chunk intervals, compression
   and policy state, continuous aggregates, and chunk statistics. Needs the `timescaledb`
   extension; report its absence rather than treating a bare-Postgres database as unhealthy for
   not having it.

## The degradation contract

Some tools in this sweep can legitimately come back with less than the full picture, and the two
reasons are different — tell them apart and say which one applies:

- **A missing extension.** `list_hypertables` needs `timescaledb`; the query-tuning companion
  skill's `get_query_store` needs `pg_stat_statements`. Neither exists in a bare PostgreSQL
  install with no extensions loaded, and no amount of role privilege adds a feature the database
  doesn't have installed. Both tools stay visible in the tool list and return an
  `EXTENSION_MISSING` error whose hint names the `CREATE EXTENSION` statement, rather than
  silently returning nothing.
- **A missing grant.** `get_active_sessions`, `get_blocking`, and `get_wait_stats` read
  `pg_stat_activity`, which PostgreSQL itself restricts: a non-superuser role without
  `pg_monitor` sees other sessions' state and wait info but not their query text (it reads as
  `<insufficient privilege>`) unless it's `pg_monitor` or the query's own owner. This is a
  narrower result, not a tool error — say so rather than reporting "no blocking" when the real
  answer is "can't see the query text to judge."

**"No data" from a missing extension or grant is never the same claim as "nothing wrong."** A
health-check report that silently treats an absent `pg_stat_statements` extension as "nothing to
tune" is wrong — say instead that query-level stats couldn't be checked, and name what would
enable it. Every section of the final report should be one of three states: **found an issue**,
**checked, clean**, or **couldn't check — here's what would enable it**. Never collapse the third
into the second.
