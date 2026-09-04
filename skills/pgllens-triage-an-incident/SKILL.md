---
name: pgllens-triage-an-incident
description: Use when a PostgreSQL database is misbehaving right now and someone needs to know why -- "the database is slow right now", "the app is hanging", "queries are stuck", "is something blocked", "what is running right now", "who is holding the lock", "connections are piling up", "CPU is pegged on the DB" -- a fixed-order live pass across connections, blocking, sessions, waits, and query stats that ends in a named operator action. Triggers on incident, outage, hanging, stuck, blocked, lock, slow right now, connection storm, pg_stat_activity.
---

# pgllens-triage-an-incident

## Overview

Live triage of a PostgreSQL database that is on fire right now, with pgllens. This is the urgent
counterpart to **pgllens-health-check**: that skill is the calm, periodic sweep (vacuum, bloat,
index health, space); this one answers "what is hurting us at this minute, and who has to do
what". pgllens cannot terminate a backend, cancel a query, run `VACUUM`, create or drop an index,
or change a setting, so every finding here ends in an operator action, never in a fix pgllens made.

**REQUIRED BACKGROUND:** read pgllens-using first.

## The triage, in order

Run these in sequence and stop early only when a step names the cause outright.

1. **`server_info`** -- version, `started`, uptime, and `connections` alongside `max_connections`
   in the settings slice. Compare the two first: connections at or near the ceiling explains
   "the app is hanging" (new clients are refused) before any query is at fault. A `started`
   timestamp minutes old means the server restarted; say so.
2. **`get_blocking`** -- blocked pid and query, blocker pid and query, and how long the blocked
   session has waited, from `pg_blocking_pids()`. This is the "who is holding the lock" answer.
   A footer of `0 blocked sessions` means clean at this instant, not clean for the last hour.
3. **`get_active_sessions(include_idle=false, include_background=false)`** -- pid, user,
   application, client, state, backend, wait type, wait event, duration, query. Sort by duration
   in your head and look for two shapes: a long-running query nobody expected, and
   `idle in transaction`, which holds locks and snapshots while doing nothing. pgllens's own
   connection and its pooled connections are always excluded, so what you see is the workload.
   Add `include_idle=true` when you suspect a connection leak (many idle sessions from one
   application). Add `include_background=true` to see autovacuum launcher, checkpointer,
   walwriter, TimescaleDB background workers, and io workers, which matters when a hypertable
   is being compressed or a vacuum is grinding through a large `<table>`.
4. **`get_wait_stats(include_background=false)`** -- counts by wait type and event, a
   point-in-time sample of `pg_stat_activity`, not a cumulative counter. `Client / ClientRead`
   is sessions idle waiting on their client, which is normal. Many `Lock` waits corroborate step 2;
   `IO` waits point at storage, not at the queries.
5. **`get_query_store`** -- needs `pg_stat_statements`. Use `order_by="mean_time"` for "what is
   slow per call" and `order_by="calls"` for "what is hammering the server"; `limit` is 1..100
   and `since` (ISO timestamp, PG17+ / extension 1.11+) narrows to statements first seen at or
   after that point. Stats are cumulative since reset and a caveat line names the reset time: a
   one-off seed script that loaded `<table>` shows as very high `calls` with an old `stats_since`.
   That is history, not live load; do not report it as the incident.
6. **`get_table_health(schema)`** -- dead tuple %, last autovacuum (`never` together with dead
   tuples or heavy inserts is a finding; a small quiet table reading `never` is not), xid age
   against wraparound, bloat estimate, and a `needs attention` list. Reach for this when
   the symptom is "everything got slow" with no blocker and no hot query: autovacuum starvation
   on a churny `<schema>.<table>` degrades every plan against it.
7. **Hand off.** Anything not on fire (unused or duplicate indexes, space, TimescaleDB policy
   state) belongs to **pgllens-health-check**. `get_index_health(schema)` is the one exception
   worth pulling forward: an invalid index left by a failed `CREATE INDEX CONCURRENTLY` or a
   foreign key with no covering index can be the direct cause of a lock pile-up.

## Re-sample before you call it a pattern

`get_blocking`, `get_active_sessions`, and `get_wait_stats` are snapshots. Take two samples 30 to
60 seconds apart before naming a pattern. The same blocker pid in both samples with a growing
wait duration is a real blocking chain; a blocker that vanished between samples was a transaction
that finished on its own. Report which of the two you saw.

## Gaps are not "nothing running"

- **Missing extension.** `get_query_store` without `pg_stat_statements` returns an
  `EXTENSION_MISSING` error whose hint names the `CREATE EXTENSION` statement; the tool stays
  visible. Report "query stats unavailable, enable with ..." and continue with steps 1 to 4,
  which need no extension.
- **Missing grant.** A role outside `pg_monitor` sees other sessions' pid, state, and wait
  info but not their query text. Say "cannot see query text for pid N", never "nothing running".
  If the pgllens role is a member of `pg_monitor`, full text is expected; if it is missing anyway,
  check the role's memberships with `list_roles` before blaming the workload.

## The finding contract

Every finding is one line an operator can act on without re-deriving it, in this shape:

`pid N (<user>, <application_name>) has held <lock or state> on <schema>.<table> for T; blocking
M sessions; operator action: <cancel pid N | terminate pid N | investigate <application_name> |
raise max_connections | run VACUUM on <schema>.<table>>.`

Name the pid, the user, the application, the duration, and the action. pgllens cannot run
`pg_cancel_backend`, `pg_terminate_backend`, `VACUUM`, `CREATE INDEX`, or `ALTER SYSTEM`; the
operator does, and the report is only finished when they know exactly which one to run. If
nothing was found in steps 1 to 6, say "checked, clean at HH:MM:SS, two samples" and point the
non-urgent remainder at pgllens-health-check.
