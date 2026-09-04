---
name: pgllens-tune-a-query
description: Use when a PostgreSQL query is slow and needs tuning — "why is this query slow", "optimize this SQL", "check the execution plan", "is this index being used", "should I add an index for this query", "why did this query regress", "find a missing index" — the validate/explain/index/pg_stat_statements loop for query performance. Triggers on execution plan, index, query performance, slow query, pg_stat_statements, EXPLAIN ANALYZE.
---

# pgllens-tune-a-query

## Overview

Tuning a slow query with pgllens, a **read-only** lens. pgllens can diagnose exhaustively —
plans, index health, aggregate statement stats, wait stats — but it **cannot create an index,
run `ANALYZE`/`VACUUM`, force a plan, or change any server setting**. Every finding here ends in
a recommendation an operator has to act on; pgllens reports, it does not remediate.

**REQUIRED BACKGROUND:** read pgllens-using first.

## The loop

1. **Validate first.** `validate_query` checks the query is read-only and parses/plans without
   running it — free confidence that the query is well-formed before spending time reading its
   plan.
2. **Explain — estimated first.** `explain_query` with `analyze=false` (the default) runs
   `EXPLAIN (FORMAT JSON)` to get the *planned* plan built from statistics, without executing
   the query. This is always the first move: it costs nothing and is safe against anything,
   including a query you suspect is expensive against a huge table.
   - **`analyze=True` EXECUTES the query** via `EXPLAIN (ANALYZE)` to capture the *actual* plan
     with real row counts and timings. It's still safe in the sense that the query must first
     pass the same read-only gate as any other query and the session itself is
     `default_transaction_read_only=on` — but it does real work against the database. Only use
     it when the estimated plan alone doesn't explain the slowness (e.g. a cardinality-estimate
     mismatch you need actual row counts to confirm), and the query is cheap or safe enough to
     actually run. Never use `analyze=True` on an expensive query just to "be thorough" — that
     runs the exact slow query you're trying to avoid running twice.
3. **Index health.** `get_index_health` (scoped with `schema`) reports index scan counts and
   sizes, and calls out unused indexes (`idx_scan = 0`), invalid indexes (a failed
   `CREATE INDEX CONCURRENTLY` left behind), and duplicate indexes (same table, identical column
   list). Treat an unused index as a lead to verify against the actual query pattern, not a
   prescription to drop blindly — a low-traffic index can still be the one this exact query
   needs and simply hasn't been exercised yet in the sampled window.
4. **Aggregate statement stats.** `get_query_store` (named for parity with the tool this ported
   from; PostgreSQL's actual mechanism is the `pg_stat_statements` extension) reports the top
   tracked statements by `order_by` (`total_time`, `mean_time`, `calls`, or `rows`; default
   `total_time`), with call count, total/mean execution time, rows produced, and shared-buffer
   hit/read counts. Needs the `pg_stat_statements` extension — if it's not installed, the tool
   names the `CREATE EXTENSION pg_stat_statements` statement an operator would need to run;
   `pgllens` cannot run it. Use this to confirm a query you suspect is heavy is actually a top
   consumer in aggregate, not just slow the one time you happened to run it.
5. **Wait stats — when the plan isn't the story.** If the plan and index health look fine but
   the query is still slow, the bottleneck may not be the query at all. `get_wait_stats` reports
   what active sessions are waiting on right now (`wait_event_type`/`wait_event`, grouped with a
   count) — a point-in-time sample of `pg_stat_activity`, not a cumulative history. Heavy `IO`
   waits point at storage; heavy `Lock` waits mean contention (see `get_blocking`). This
   reframes "why is my query slow" from a single-query question into a database-wide contention
   or I/O question.

## What pgllens cannot do

It cannot create the missing index it found, force the good plan it identified, run `ANALYZE`
to refresh stale planner statistics, or install `pg_stat_statements`/`timescaledb`. Every tool
here ends in a report and a named next step — "create this index", "run this
`CREATE EXTENSION` statement", "investigate this blocker" — for a human with write access to
carry out.
