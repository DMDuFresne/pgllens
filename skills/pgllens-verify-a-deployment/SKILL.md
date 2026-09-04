---
name: pgllens-verify-a-deployment
description: Use when an operator has just deployed or upgraded pgllens, or changed EXPOSED_SCHEMAS, REDACT_COLUMNS, or the database role, and wants proof through the MCP client that it works -- "verify the deployment", "smoke test pgllens", "did the upgrade work", "test every tool", "is the connector wired up", "check the new server", "exercise the tools" -- a checklist that calls all 31 tools, fires every safety gate on purpose, and confirms each capability gap degrades to the right error envelope. Triggers on verify, smoke test, deployment, upgrade, connector, new server, exercise tools.
---

# pgllens-verify-a-deployment

## Overview

A smoke test of a pgllens deployment, run entirely through the MCP client. It proves three
things: every tool group answers, every safety gate rejects what it must, and every capability
gap comes back as a clean error envelope instead of a raw driver error. Nothing here changes the
database -- the gate checks in step 3 are *supposed* to be rejected, and the read-only DSN would
hold even if they weren't.

**REQUIRED BACKGROUND:** read pgllens-using first.

Know the two shapes you are checking against before you start. Every success starts
`## pgllens · <tool>`, optionally followed by ` · <scope>` (a schema or table name), then an
italic `*<source> · <timestamp>*` line (source is `catalog`, `stats`, `catalog+stats`, or
`query`), optionally followed by ` · <status>`, then tables or bullets, then a `---` footer
tally, then a `*Next: ...*` line. Every error is `## pgllens · <tool> · error` with four bullets
(`code`, `message`, `hint`, `request_id`) plus a `retry_after` bullet on a `TIMEOUT` error. The
`request_id` is what the operator greps in the server logs (the log line carries it as
`correlation_id`) and in the audit trail.

## The run, in order

1. **Identity.** `server_info` (PostgreSQL version, started, uptime, connections, curated
   settings). `list_extensions` (installed vs available, with an upgrade column). `list_roles`:
   the pgllens role should have login, no superuser, and membership of `pg_monitor`, with its
   grants collapsed to one row like "N relations in <schemas> SELECT"; `include_builtin=true`
   adds the `pg_*` roles. `schema_overview` must list exactly the schemas the operator set in
   `EXPOSED_SCHEMAS`, no more, no fewer. `refresh_schema` returns the cached object count.
2. **One call per tool group.** Pick any table from the `schema_overview` output and use it
   throughout; for `find_path`, pick a second table from the same output.
   - Discovery: `list_tables`, `describe_table`, `search_columns`, `get_sample_data` with
     `limit=3`, `get_table_stats`.
   - Structure: `get_ontology`, `get_relationships`, `find_path`, `get_constraints`, `get_erd`
     with `format="text"`, `get_erd_widget`.
   - Programmable objects: `list_functions`, `get_function_source`, `get_view_definition`,
     `get_triggers`.
   - Query: `validate_query`, `explain_query`, `query`.
   - DBA: `get_active_sessions`, `get_blocking`, `get_wait_stats`, `get_index_health`,
     `get_table_health`, `get_space_usage`, `get_query_store`, `list_hypertables`.
   With step 1 that is all 31. A tool you did not call is a tool you did not verify.
3. **The gates.** Each of these must come back as an error envelope with the exact code and
   message below. A success here is a deployment fault, stop and report it.
   | Call | Expected |
   |---|---|
   | `validate_query("DELETE FROM x")` | `QUERY_REJECTED`, "query must start with SELECT, WITH, TABLE or VALUES." |
   | `query("SELECT pg_sleep(1)")` | `QUERY_REJECTED`, "pg_sleep is not permitted (read-only lens)." |
   | `describe_table(table, schema="public")`, public not exposed | `SCHEMA_UNKNOWN`, "Schema 'public' is not exposed. Available: ..." |
   | `describe_table("nope_table")` | `TABLE_NOT_FOUND`, hint points at `search_columns` or `list_tables` |
   | `get_sample_data(table, limit=0)` | `ARG_OUT_OF_RANGE`, "`limit` must be between 1 and 1000 (got 0)." |
   | `query(sql, max_estimated_rows=100)` on the largest table `list_tables` shows | `QUERY_REJECTED`, "Planner estimates N rows, above `max_estimated_rows`=100." |
   | `query("SELECT current_setting('default_transaction_read_only')")` | a *success* returning `on` |
   The last row is the primary wall: PostgreSQL itself enforcing read-only, independent of the
   SQL-text gate the other rows exercise.
4. **Redaction.** Use `search_columns` to find a column whose name matches one of the operator's
   `REDACT_COLUMNS` patterns. `query` on it must render `[masked]`; `describe_table` on its table
   must show no sampled values for that column (a blank cell, not `[masked]`). If no column
   matches, record "no redactable column, masking not exercised". Do not attempt to bypass it;
   redaction is display masking by output-column name, and the smoke test only confirms it is on.
5. **Capability gaps.** `get_query_store` without `pg_stat_statements`, and `list_hypertables`
   without `timescaledb`, must return `EXTENSION_MISSING` with a hint naming the exact
   `CREATE EXTENSION` statement, and both tools must still appear in the tool list. If both
   extensions are installed, record "present" for each rather than skipping the check.
   Separately, a table granted `SELECT` per column with no table-level grant makes
   `get_sample_data` and `SELECT *` return `DB_ERROR` "permission denied for table <name>", while
   a `query` with an explicit column list works. That is least-privilege doing its job, not a
   fault; note it as expected. If the deployment has no such table, record "not applicable".
6. **Observability cross-check**, if the operator runs the ops stack. Each call from steps 1-5
   appears as a `tool` label in Prometheus (`pgllens_tool_calls_total` by `tool` and `outcome`:
   `ok`, `rejected`, `unknown_schema`, `not_found`, `unavailable`, `db_error`), as a JSON audit
   line in Loki (`job="pgllens-audit"`, fields `tool`, `outcome`, `duration_ms`, `sub`,
   `client_id`, `ip`, `args_hash`, `rows`, `trace_id`), and as a `tools/call <name>` span in
   Tempo. The deliberate rejections in step 3 fire the info-level alert
   `PgllensReadOnlyGateRejection` for 15 minutes. That is the alert working; say so in the report
   so nobody pages on it.

## Reporting contract

The report has four parts, in this order:

1. A table of 31 rows, `tool / outcome / note`, one row per tool, including the step 1 identity
   tools. `outcome` is the response shape observed (`ok`, or the error code), `note` is one line
   (row count, "present", "least-privilege, expected", the `request_id` of anything odd).
2. A gates table, expected vs observed, one row per line of the step 3 table plus the redaction
   check from step 4.
3. The gaps, each with the exact `hint` text pgllens returned, or "present" when the extension
   is installed.
4. One final line: either **"all 31 respond, all gates hold"** or the list of deviations.

Never call a deployment verified while any tool is untested or any gate result is missing. A
gap you could not check is reported as "couldn't check", never folded into a pass.
